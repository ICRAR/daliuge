#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2024
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

import json
import pickle
import unittest

try:
    from importlib.resources import files, as_file
except (ImportError, ModuleNotFoundError):
    from importlib_resources import files

from dlg.common import CategoryType
from dlg.dropmake import path_utils
from dlg.dropmake.lg import LG


NODES = 'nodeDataArray'
LINKS = 'linkDataArray'
TEST_SSID = 'test_pg_gen'  # Needed to match output files generated in test_pg_gen.py


class TestLGNodeInit(unittest.TestCase):
    """
    Verify the correct data is stored in the LGNode class when a new LGNode is created
    during the LG constructor.

    This test is here to demonstrate what information is queryable at initial load time,
    which will facilitate backwards-compatibility testing if changes are made to the
    internal structure of the class.
    """
    graph_name = "eagle_gather_update.graph"
    graph_information = {"num_keys": 14}
    lg = LG(path_utils.get_lg_fpath('logical_graphs', graph_name), ssid="TEST")
    # Isolate (first) scatter node
    scatterKey = 0
    for key, lgn in lg._done_dict.items():
        if lgn.is_scatter:
            scatterKey = lgn.id
            break
    # Confirm releationship with children

    # TODO Get information on what the structure of the stored data looks like here
    # This includes the state of the LG class as a whole (e.g. what is in done_dict,
    # drop_dict, lg_links etc.)

    # TODO We should provide some tests that test things like querying a given LGNode's
    # children, or confirm the DOP of a given node. That way when we introduce new
    # representations, we have something to compare to.


class TestLGNToPGN(unittest.TestCase):
    """
    Verify any changes to the LGN to PGN method produce the same PGT structure.

    Intended as a regression testing to ensure backwards compatiblity with existing
    PGT behaviour.

    TODO Get information on what the structure of the stored data looks like here
    This includes the state of the LG class as a whole (e.g. what is in done_dict,
    drop_dict, lg_links etc.)
    """

    def test_simple_lgn_to_pgn(self):
        """
        Base-case Hello World application
        """

    def test_loop_graph(self):
        """
        More complex looping graph
        """
        graph_name = "testLoop.graph"
        graph_information = {"num_keys": 14}
        lg = LG(path_utils.get_lg_fpath('logical_graphs', graph_name), ssid="TEST")
        # TODO make this pick specifically the Loop node
        for lgn in lg._start_list:
            lg.lgn_to_pgn(lgn)

    def test_scatter_graph_graph(self):
        """
        Add scatter and gather constructs
        """

    def test_non_recursive(self):
        """
        Test non-recursive implementation of lgn_to_pgn
        """
        graph_name = "testLoop.graph"
        lg_recursive = LG(path_utils.get_lg_fpath("logical_graphs", graph_name),
                          ssid="TEST")
        for lgn in lg_recursive._start_list:
            lg_recursive.lgn_to_pgn(lgn)
        lg_non_recursive = LG(path_utils.get_lg_fpath("logical_graphs", graph_name), ssid="TEST")
        for lgn in lg_non_recursive._start_list:
            lg_non_recursive.lgn_to_pgn(lgn, recursive=False)
        for key in lg_recursive._drop_dict:
            self.assertEqual(lg_recursive._drop_dict[key],
                             lg_non_recursive._drop_dict[key])

        graph_name = "eagle_gather_update.graph"

        lg_recursive = LG(path_utils.get_lg_fpath("logical_graphs", graph_name), ssid="TEST")
        for lgn in lg_recursive._start_list:
            lg_recursive.lgn_to_pgn(lgn)
        lg_non_recursive = LG(path_utils.get_lg_fpath("logical_graphs", graph_name), ssid="TEST")
        for lgn in lg_non_recursive._start_list:
            lg_non_recursive.lgn_to_pgn(lgn, recursive=False)

        with open('lg_recursive.json', 'w') as fp:
            json.dump(lg_recursive._drop_dict, fp, indent=2)
        with open('lg_non_recursive.json', 'w') as fp:
            json.dump(lg_non_recursive._drop_dict, fp, indent=2)

        for key in lg_recursive._drop_dict:
            self.assertEqual(lg_recursive._drop_dict[key],
                             lg_non_recursive._drop_dict[key])


class TestLGInit(unittest.TestCase):
    """
    Verify the correct data is stored in the LG class when initially loading a logical
    graph from file.

    This test is here to demonstrate what information is queryable at initial load time,
    which will facilitate backwards-compatibility testing if changes are made to the
    internal structure of the class.

    Tests in this class will include verifying:
    - What construct something is
    - What children of that construct are
    - The relationships between LGNodes (i.e. links/edges)

    """

    def test_initial_relationships(self):
        """
        Confirm that self._lgn_list and self._lg_links are correct
        """

    lg_names = {
        "HelloWorld_simple.graph": {"num_keys": 2},
        "eagle_gather_empty_update.graph": {"num_keys": 11},
        "eagle_gather_simple_update.graph": {"num_keys": 18},
        "eagle_gather_update.graph": {"num_keys": 14},
        "testLoop.graph": {"num_keys": 4},
        "cont_img_mvp.graph": {"num_keys": 45},
        "test_grpby_gather.graph": {"num_keys": 21},
        "chiles_simple.graph": {"num_keys": 22},
        "Plasma_test.graph": {"num_keys": 6},
    }


class TestLGNodeLoading(unittest.TestCase):

    def test_LGNode_SubgraphData(self):
        """
        Test that when we initially construct the logical graph, the SubGraph data node
        that is added to the graph stores the sub-graph nodes and links.
        """
        fname = path_utils.get_lg_fpath("logical_graphs", "ExampleSubgraphSimple.graph")
        lg = LG(fname)
        subgraph_data_node_key = "bb9b78bc-b725-4b61-a12a-413bdcef7690"
        self.assertIsNotNone(lg._done_dict[subgraph_data_node_key].subgraph)


class TestLGUnroll(unittest.TestCase):
    """
    Test that the LG unrolls as expected

    Uses test/dropmake/pickles as test data

    Note: This is a regression testing class. These tests are based on graphs that were
    generated using the code they are testing. If the LG class and it's methods change
    in the future, test data may need to be re-generated (provided test failures are
    caused by known-breaking changes, as opposed to legitimate bugs!).
    """

    def test_lg_unroll(self):
        """
        Basic verification that we can unroll a list of dropdicts from a logical graph

        lg_names = { "logical_graph_file.graph": num_keys_in_drop_list, ...}
        """

        lg_names = {
            "HelloWorld_simple.graph": 2,
            "eagle_gather_empty_update.graph": 11,
            "eagle_gather_simple_update.graph": 18,
            "eagle_gather_update.graph": 14,
            "testLoop.graph": 4,
            "cont_img_mvp.graph": 45,
            "test_grpby_gather.graph": 21,
            "chiles_simple.graph": 22,
            "Plasma_test.graph": 6,
        }

        for lg_name, num_keys in lg_names.items():
            fp = path_utils.get_lg_fpath("logical_graphs", lg_name)
            lg = LG(fp, ssid=TEST_SSID)
            self.assertEqual(num_keys,
                             len(lg._done_dict.keys()),
                             f"Incorrect number of elements when constructing LG "
                             f"object using: {lg_name}")

            drop_list = lg.unroll_to_tpl()
            with open(path_utils.get_lg_fpath('pickle', lg_name), 'rb') as pk_fp:
                test_unroll = pickle.load(pk_fp)

            self.assertListEqual(test_unroll,
                                 drop_list,
                                 f"unroll_to_tpl failed for: {lg_name}")

    def test_lg_unroll_sharedmemory(self):
        """
        Confirm the SharedMemory data type is correctly unrolled.
        """
        lg_name = "SharedMemoryTest_update.graph"
        num_keys = 8
        fp = path_utils.get_lg_fpath("logical_graphs", lg_name)
        lg = LG(fp, ssid=TEST_SSID)
        self.assertEqual(num_keys,
                         len(lg._done_dict.keys()),
                         f"Incorrect number of elements when constructing LG "
                         f"object using: {lg_name}")

        drop_list = lg.unroll_to_tpl()
        for drop in drop_list:
            if drop["categoryType"] in [CategoryType.DATA, "data"]:
                self.assertEqual("SharedMemory", drop["category"])
