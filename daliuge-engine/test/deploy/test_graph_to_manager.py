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
import time
import unittest
import pytest

from importlib.resources import files
from dlg import common
from dlg import constants

# Note this test will only run with a full installation of DALiuGE.
pexpect = pytest.importorskip("dlg.dropmake.web.translator_utils")

from dlg.dropmake.web.translator_utils import (unroll_and_partition_with_params,
                                               prepare_lgt)
from dlg.manager.composite_manager import DataIslandManager
from test.dlg_engine_testutils import NMTestsMixIn, DROPManagerUtils
from dlg.testutils import ManagerStarter
import daliuge_tests.dropmake as test_graphs

def create_full_hostname(server_info, event_port, rpc_port):

    return (f"{server_info.server._server.listen}:"
            f"{server_info.server._server.port}:{event_port}:{rpc_port}")

class GraphLoaderToNodeManager(NMTestsMixIn, ManagerStarter, unittest.TestCase):
    def test_input_in_remote_nm(self):
        """
        A test similar in spirit to TestDM.test_runGraphOneDOPerDom, but where
        application B is a PyFuncApp. This makes sure that PyFuncApp work fine
        across Node Managers.

        NM #1      NM #2
        =======    =============
        | A --|----|-> B --> C |
        =======    =============
        """
        # TODO go from logical_graph and partition the graph first,
        # then load it with the graph_loader

        # Partitioning currently requires the to_go_js + to_pg_spec approach
        # We will partition using METIS, as the base PGT class doesn't actually partition
        # anything.
        # lg_path = str(files(__package__)  / f"ArrayLoop.graph")
        lg_path = str(files(test_graphs) / "logical_graphs/ArrayLoop.graph")
        # lg = LG(fp)

        # drop_list = lg.unroll_to_tpl()
        lgt = prepare_lgt(lg_path, 0)
        pgt = unroll_and_partition_with_params(
            lgt=lgt,
            test=True,
            algorithm="metis",
            num_partitions=2,
            num_islands=1,
            par_label="Partition",
        )
        events_port, rpc_port = (5566, 6688)
        ms1_info = self.start_nm_in_thread(port=8000, events_port=5566, rpc_port=6688)
        ms1_hostname = create_full_hostname(ms1_info, events_port, rpc_port)

        events_port, rpc_port = (5555, 6666)
        ms2_info = self.start_nm_in_thread(port=8999,
                                           events_port=constants.NODE_DEFAULT_EVENTS_PORT,
                                           rpc_port=constants.NODE_DEFAULT_RPC_PORT)
        ms2_hostname = create_full_hostname(ms2_info, events_port, rpc_port)

        host_names = [ms1_hostname, ms2_hostname]
        dim = DataIslandManager(host_names)
        dim_host = f"localhost:{constants.NODE_DEFAULT_REST_PORT}"
        pg_spec = pgt.to_pg_spec([dim_host] + host_names, ret_str=False)
        roots = common.get_roots(pg_spec)
        dim.createSession("TestSession")
        dim.addGraphSpec("TestSession", pg_spec)
        dim.deploySession("TestSession", completedDrops=roots)

        from dlg.ddap_protocol import DROPStates
        time.sleep(25)
        for dropstatus in dim.getGraphStatus("TestSession").values():
            self.assertEqual(DROPStates.COMPLETED, dropstatus['status'])
        ms1_info.stop()
        ms2_info.stop()
        dim.shutdown()