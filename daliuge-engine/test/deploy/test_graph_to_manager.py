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
import unittest

from importlib.resources import files
from dlg import common
from dlg.dropmake.lg import LG
from dlg.dropmake.pgt import PGT, GPGTNoNeedMergeException
from dlg.dropmake.pg_generator import unroll, partition
from dlg.dropmake.web.translator_utils import unroll_and_partition_with_params, prepare_lgt
from dlg.manager.composite_manager import DataIslandManager
from dlg.manager.node_manager import NodeManager
from dlg.manager.drop_manager import DROPManager
from dlg.manager.rest import NMRestServer, CompositeManagerRestServer
from test.dlg_engine_testutils import NMTestsMixIn
from dlg.testutils import ManagerStarter


class GraphLoaderToNodeManager(NMTestsMixIn):
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
        lg_path = str(files(__package__)  / f"cont_img_mvp.graph")
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

        # TODO

        self.start_hosts(2)
        dim = DataIslandManager(
            self.host_names, dmCheckTimeout=5
        )

        pg_spec = pgt.to_pg_spec(self.host_names)
        roots = common.get_roots(pg_spec)
        dim.createSession("TestSession")
        # dim.addGraphSpec("TestSession", pg_spec)
        # dim.deploySession("TestSession", completedDrops=roots)

        #
        # c_data = ._test_runGraphInTwoNMs(g1, g2, rels, pickle.dumps(a_data), None)
        # self.assertEqual(a_data, pickle.loads(c_data))
