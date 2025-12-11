#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2016
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA
#
import datetime
import os
import sys
import time
import pytest

from dlg import common
from dlg import constants
from dlg.utils import DlgFormatter

# Note this test will only run with a full installation of DALiuGE.
pexpect = pytest.importorskip("dlg.dropmake.web.translator_utils")

from dlg.dropmake.web.translator_utils import (unroll_and_partition_with_params,
                                               prepare_lgt)
from dlg.manager.composite_manager import DataIslandManager
from dlg.ddap_protocol import DROPStates
from dlg.testutils import ManagerStarter
from test.dlg_engine_testutils import NMTestsMixIn, DROPManagerUtils

import logging


LOG_FMT =  ("%(asctime)-15s [%(levelname)5.5s] [%(threadName)15.15s] "  
            "%(name)s#%(funcName)s:%(lineno)s %(message)s")


def create_full_hostname(server_info, event_port, rpc_port):

    return (f"{server_info.server.listen}:"
            f"{server_info.server.port}:{event_port}:{rpc_port}")


class Runner(ManagerStarter):
    """Runner class for executing DALiuGE logical graphs with multiple managers.

    This class handles the setup and execution of DALiuGE graphs using multiple node managers.
    It manages the lifecycle of managers and monitors graph execution.
    """

    BASE_DIR = "/tmp/compatibility/"
    LOGS_DIR = "logs"
    DEFAULT_LOG_LEVEL = 'DEBUG'
    MAX_WAIT_TIME = 30  # seconds
    POLL_INTERVAL = 1  # second

    def __init__(self, lg_path: str):
        """
        Initialize the runner with a logical graph path.

        :param lg_path: Path to the logical graph file to execute
        """
        # Setup base directory
        self.lg_path = lg_path
        lgname = os.path.basename(lg_path)
        try:
            os.makedirs(self.BASE_DIR, exist_ok=True)
            os.environ["DLG_ROOT"] = self.BASE_DIR

            # Setup log management 
            os.makedirs(self.LOGS_DIR, exist_ok=True)
            logfile = f"{self.LOGS_DIR}/dlg_{lgname}.log"
            file_handler = logging.FileHandler(logfile)
            fmt = DlgFormatter(LOG_FMT)
            file_handler.setFormatter(fmt)
            logging.root.addHandler(file_handler)
            self.logger = logging.getLogger("dlg")
            self.logger.setLevel(self.DEFAULT_LOG_LEVEL)
        except OSError as e:
            raise RuntimeError(f"Failed to setup directories/logging: {e}")


    def _wait_for_completion(self, dim: DataIslandManager, session_id: str) -> bool:
        """Wait for all drops to complete execution.


        :param dim: Data island manager instance
        :param session_id: Current session ID

        :reurn bool: True if all drops completed successfully
        """
        start_time = time.time()
        while time.time() - start_time < self.MAX_WAIT_TIME:
            all_completed = all(
                status['status'] == DROPStates.COMPLETED
                for status in dim.getGraphStatus(session_id).values()
            )
            if all_completed:
                return True
            time.sleep(self.POLL_INTERVAL)
        self.logger.error("Failed to finish successfully.")
        return False

    def run(self) -> int:
        """Execute the logical graph using multiple node managers.
        :return:
            int: 0 if successful, 1 if any errors occurred
        """
        self.logger.info("Setting DLG root to %s", os.environ["DLG_ROOT"])

        # Start first node manager
        nm1_events_port, nm1_rpc_port = (5566, 6688)
        nm1_info = self.start_nm_in_thread(port=8000,
                                           events_port=nm1_events_port,
                                           rpc_port=nm1_rpc_port)

        nm1_hostname = create_full_hostname(nm1_info, nm1_events_port, nm1_rpc_port)

        # Start second node manager
        nm2_events_port, nm2_rpc_port = (5555, 6666)
        nm2_info = self.start_nm_in_thread(port=8999,
                                           events_port=constants.NODE_DEFAULT_EVENTS_PORT,
                                           rpc_port=constants.NODE_DEFAULT_RPC_PORT)
        nm2_hostname = create_full_hostname(nm2_info, nm2_events_port, nm2_rpc_port)

        manager_hostnames = [nm1_hostname, nm2_hostname]
        dim = DataIslandManager(manager_hostnames)

        # Prepare and partition graph
        logical_graph = prepare_lgt(self.lg_path, 0)
        partitioned_graph = unroll_and_partition_with_params(
            lgt=logical_graph,
            test=True,
            algorithm="metis",
            num_partitions=2,
            num_islands=1,
            par_label="Partition",
        )

        # Deploy graph
        dim_host = f"localhost:{constants.NODE_DEFAULT_REST_PORT}"
        pg_spec = partitioned_graph.to_pg_spec([dim_host] + manager_hostnames,
                                               ret_str=False)
        root_drops = common.get_roots(pg_spec)

        session_id = f"TestSession_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
        dim.createSession(session_id)
        dim.addGraphSpec(session_id, pg_spec)
        dim.deploySession(session_id, completedDrops=root_drops)

        # Monitor execution and cleanup
        execution_results = []
        execution_results.append(self._wait_for_completion(dim, session_id))

        def try_stop(manager):
            try:
                manager.stop()
                return True
            except AssertionError:
                return False

        for manager in [nm1_info, nm2_info]:
            execution_results.append(try_stop(manager))

        try:
            dim.shutdown()
            execution_results.append(True)
        except AssertionError:
            execution_results.append(False)

        return 0 if all(execution_results) else 1


    def __call__(self, *args, **kwargs) -> None:
        """Make the class callable, executing the run method.

        Args:
            *args: Positional arguments
            **kwargs: Keyword arguments
        """
        return self.run()


if __name__ == '__main__':
    runner = Runner(sys.argv[1])
    sys.exit(runner())

