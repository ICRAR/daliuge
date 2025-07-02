#
#    ICRAR - International Centre for Radio Astronomy Research 2024
#    (c) UWA - The University of Western Australia
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

import threading
import time
import datetime

from dlg.apps.app_base import BarrierAppDROP
from dlg.common import get_roots
from dlg.data.drops.data_base import logger
from dlg.ddap_protocol import AppDROPStates, DROPStates
from dlg.dropmake.pg_generator import unroll, partition
from dlg.manager.composite_manager import DataIslandManager
from dlg.manager.node_manager import NodeManager
from dlg.manager.drop_manager import DROPManager
from dlg.manager.rest import NMRestServer, CompositeManagerRestServer


def shutdownManager(managers: dict) -> None:
    """
    Helper function that iterates through a dictionary of managers and stops them and
    their respective servers.
    :param managers:dict of dicts, which contain DROPManager objects and information.
    :return:
    """
    for manager in managers.values():
        manager["manager"].shutdown()
        manager["server"].stop()


def startupManagersInThread(managers: dict) -> None:
    """
    Helper function that iterates through a dictionary of managers and starts
    them in separate threads.
    :param managers: dict of dicts, which contain DROPManager objects and information.
    :return: None
    """
    for manager in managers.values():
        thread = threading.Thread(
            target=manager["server"].start, args=(manager["host"], manager["port"])
        )
        thread.start()


##
# @brief SubGraphLocal
# @par EAGLE_START
# @param category DALiuGEApp
# @param tag daliuge
# @param log_level "NOTSET"/Select/ComponentParameter/NoPort/ReadWrite/NOTSET,DEBUG,INFO,WARNING,ERROR,CRITICAL/False/False/Set the log level for this drop
# @param dropclass dlg.apps.subgraph.SubGraphLocal/String/ComponentParameter/NoPort/ReadOnly//False/False/
# @param base_name subgraph/String/ComponentParameter/NoPort/ReadOnly//False/False/Base name of application class
# @param execution_time 5/Float/ConstraintParameter/NoPort/ReadOnly//False/False/Estimated execution time
# @param num_cpus 1/Integer/ConstraintParameter/NoPort/ReadOnly//False/False/Number of cores used
# @param group_start True/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Is this node the start of a group?
# @param node_manager_port 8080/Integer/ApplicationArgument/NoPort/ReadWrite//False/False/The port number of the SubGraph Node Manager
# @param island_manager_port 8081/Integer/ApplicationArgument/NoPort/ReadWrite//False/False/The port number of the SubGraph Island Manager
# @param node_manager_host localhost/string/ApplicationArgument/NoPort/ReadWrite//False/False/The node manager host address
# @param island_manager_host localhost/string/ApplicationArgument/NoPort/ReadWrite//False/False/The island manager host address
# @param partition_algorithm metis/string/ApplicationArgument/NoPort/ReadWrite//False/False/The island manager host address
# @par EAGLE_END
class SubGraphLocal(BarrierAppDROP):
    """
    InputApp for the SubGraph construct, designed to be used on a system with DALiuGE
    installed locally.
    """

    DEFAULT_SUBGRAPH_NODE_PORT = 8080
    DEFAULT_SUBGRAPH_ISLAND_PORT = 8081

    def initialize(self, **kwargs):
        super(SubGraphLocal, self).initialize(**kwargs)
        self._nm_port = self._popArg(
            kwargs, "node_manager_port", self.DEFAULT_SUBGRAPH_NODE_PORT
        )
        self._dim_port = self._popArg(
            kwargs, "island_manager_port", self.DEFAULT_SUBGRAPH_ISLAND_PORT
        )
        self._subgraph = self._popArg(kwargs, "subgraph", {})

        # List of nodes (IP's, str) that are used in
        self._nodeManagerHost = self._popArg(
            kwargs, "node_manager", "localhost"
        )  # :8080"])
        self._islandManagerHost = self._popArg(kwargs, "island_manager", "localhost")
        self._partition_algorithm = self._popArg(kwargs, "partition_algorithm", "metis")
        self._session_prefix = self._popArg(kwargs, "session_prefix", "subgraph")

    def _pollRESTInterface(
        self, manager: DROPManager, session: str, pollFrequencyInSeconds: int = 30
    ):
        """
        Check the completion status of each drop in the SubGraph.

        If not all have completed, sleep before returning the status

        :param manager: DropManager, the manager that is deploying the subgraph
        :param pollFrequencyInSeconds: number of times we poll the manager for
        :return:
        """
        finished = all(
            status["status"] in [DROPStates.ERROR, DROPStates.COMPLETED]
            for host, status in manager.getGraphStatus(session).items()
        )
        if not finished:
            time.sleep(pollFrequencyInSeconds)
        return finished

    def _translateSubGraph(self, node_list: list = None):
        """
        :param node_list: list of both data island and node managers
        ------

        Use a local install of the daliuge-translator to convert the LGT into a
        partitioned pg_spec.
        """
        if not list:
            return {}
        pgt = unroll(self._subgraph)
        pg = partition(
            pgt,
            algo="metis",
            num_partitions=1,
            num_islands=1,
            partition_label="Partition",
            show_gojs=True,
            max_cpu=8,
            max_load_imb=100,
        )
        return pg.to_pg_spec(node_list, ret_str=False)

    def run(self):
        """
        Start the required DROPManagers and deploy the subgraph

        This method will wait until confirmation from the DROPManager that the DROPs have
        either successfully completed or failed.
        """
        managers = {
            "node": {
                "manager": NodeManager(
                    self._nodeManagerHost, rpc_port=6667, events_port=5556
                ),
                "port": self._nm_port,
                "host": self._nodeManagerHost,
            },
            "dim": {
                "manager": DataIslandManager([self._nodeManagerHost]),
                "port": self._dim_port,
                "host": self._islandManagerHost,
            },
        }
        managers["node"]["server"] = NMRestServer(managers["node"])
        managers["dim"]["server"] = CompositeManagerRestServer(managers["dim"])
        startupManagersInThread(managers)

        nodes = [self._islandManagerHost, self._nodeManagerHost]
        try:
            session = self._translateAndDeploySubGraph(
                managers["dim"]["manager"], nodes
            )
            while not self._pollRESTInterface(
                manager=managers["dim"]["manager"],
                session=session,
                pollFrequencyInSeconds=10,
            ):
                logger.info("Running SubGraph externally")

        except RuntimeError as e:
            logger.debug("Exception when deploying subgraph: %s", e)
            shutdownManager(managers)
            self.execStatus = AppDROPStates.CANCELLED
            self.status = DROPStates.CANCELLED
            return self.status

        logger.info("Subgraph finished executing")
        shutdownManager(managers)
        return self.status

    def _translateAndDeploySubGraph(self, dim: DROPManager, nodes: list) -> str:
        """
        Translate the subgraph and prepare it for deployment.

        :param dim: Data Island Manager
        :param nodes: List of DropManagers used (DIM + NM)
        :return: None
        """
        session_id = self._generate_session_id()
        graphspec = self._translateSubGraph(nodes)
        dim.createSession(session_id)
        dim.addGraphSpec(session_id, graphspec)
        # Roots are required to 'kick off' the drop execution
        roots = get_roots(graphspec)
        dim.deploySession(session_id, completedDrops=roots)
        return session_id

    def _generate_session_id(self) -> str:
        """
        Create a session_id using a prefix + timestamp format

        :param prefix: str, an optionally-configurat session ID prefix
        :return: A string prefix
        """
        ts = time.time()
        return f"{self._session_prefix}_" + datetime.datetime.fromtimestamp(
            ts
        ).strftime("%Y-%m-%d_%H-%M-%S")
