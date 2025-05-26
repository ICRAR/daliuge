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

import pickle
import unittest

from dlg.common import CategoryType, path_utils
from dlg.dropmake.lg import LG

NODES = 'nodeDataArray'
LINKS = 'linkDataArray'
TEST_SSID = 'test_pg_gen'  # Needed to match output files generated in test_pg_gen.py


class TestLGInit(unittest.TestCase):
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

    def test_lg_init(self):
        for lg_name, num_keys in self.lg_names.items():
            fp = path_utils.get_lg_fpath("logical_graphs", lg_name)
            lg = LG(fp, ssid=TEST_SSID)
            self.assertEqual(num_keys,
                             len(lg._done_dict.keys()),
                             f"Incorrect number of elements when constructing LG "
                             f"object using: {lg_name}")


def _calc_num_drops(drop_values):
    """
    Get the number of drops created during the lgn_to_pgn method.

    The drops are stored in dictionaries of the original LG node, so we
    iterate through them and get the number of Physical Graph Nodes for every one of
    the Logical Graph nodes.
    """
    return sum(len(drop_list) for drop_list in drop_values)


class TestLGNToPGN(unittest.TestCase):
    """
    Verify any changes to the LGN to PGN method produce the same PGT structure.

    Intended as a regression testing to ensure backwards compatiblity with existing
    PGT behaviour.

    # Note that currently, LGN to PGN creates all the physical graph nodes,
    but doesn't get rid of construct nodes (these are removed by the unroll_to_tpl)

    """

    def test_loop_graph(self):
        """
        More complex looping graph
        """
        graph_name = "testLoop.graph"
        graph_information = {"num_keys": 11}
        lg = LG(path_utils.get_lg_fpath('logical_graphs', graph_name), ssid="TEST")
        for lgn in lg._start_list:
            lg.lgn_to_pgn(lgn)

        self.assertEqual(graph_information["num_keys"],
                         _calc_num_drops(lg._drop_dict.values()))

    def test_scatter_graph_graph(self):
        """
        Test scatter gather constructs
        """

        graph_name = "eagle_gather_update.graph"
        graph_information = {"num_keys": 31}
        lg = LG(path_utils.get_lg_fpath('logical_graphs', graph_name), ssid="TEST")
        for lgn in lg._start_list:
            lg.lgn_to_pgn(lgn)

        self.assertEqual(graph_information["num_keys"],
                         _calc_num_drops(lg._drop_dict.values()))

    def test_non_recursive(self):
        """
        This tests that we can generate the correct number of expected 'unrolled' drops
        using a non-recursive implementation of the lgn_to_pgn translation method.


        We want to get the number of drops created during lgn_to_pgn, which is the
        intermediate representation of drops proir to calling 'unroll_to_tpl'.


        To test the call to lgn_to_pgn, we use the same structure as in unroll_to_tpl,
        and iterate through the _start_list identified during the LG class __init__.
        """

        lg_names = {"testLoop.graph": {"num_pgt_drops": 11},
                    "eagle_gather_update.graph": {"num_pgt_drops": 31}}

        for graph_name, test_dict in lg_names.items():
            expected_drops = test_dict['num_pgt_drops']

            lg_recursive = LG(path_utils.get_lg_fpath("logical_graphs", graph_name),
                              ssid="TEST")
            for lgn in lg_recursive._start_list:
                lg_recursive.lgn_to_pgn(lgn)
            self.assertEqual(
                expected_drops,
                _calc_num_drops(lg_recursive._drop_dict.values())
            )

            lg_non_recursive = LG(
                path_utils.get_lg_fpath("logical_graphs", graph_name), ssid="TEST"
            )
            for lgn in lg_non_recursive._start_list:
                lg_non_recursive.lgn_to_pgn(lgn, recursive=False)
            self.assertEqual(
                expected_drops,
                _calc_num_drops(lg_non_recursive._drop_dict.values())
            )

    def test_branch(self):
        """
        Test that the branch is getting the correct inputs and maps the output
        names to the correct drops.
        """

        lg_name = "branchTest.graph"

        lg = LG(path_utils.get_lg_fpath("logical_graphs", lg_name), ssid="TEST")
        outputPorts = lg._lgn_list[0].jd["outputPorts"]
        trueTargetId = [outputPorts[k]["target_id"] for k,v in outputPorts.items() if outputPorts[k]["name"]=="true"][0]
        trueTargetName = [n.name for n in lg._lgn_list if n.id == trueTargetId][0]
        lg.unroll_to_tpl()
        self.assertEqual("true",
                         trueTargetName)

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

    We no longer compare directly the output, as this causes errors with UIDs/OID
    conflicts. What we care about in this scenario is that twe have the correct nu

    """

    lg_names = {
        "HelloWorld_simple.graph": {"nodes": 2, "edges": 1},
        "eagle_gather_empty_update.graph": {"nodes": 22, "edges": 24},
        "eagle_gather_simple_update.graph": {"nodes": 42, "edges": 55},
        "eagle_gather_update.graph": {"nodes": 29, "edges": 30},
        "testLoop.graph": {"nodes": 11, "edges": 10},
        "cont_img_mvp.graph": {"nodes": 144, "edges": 188},
        "test_grpby_gather.graph": {"nodes": 15, "edges": 14},
        "chiles_simple.graph": {"nodes": 22, "edges": 21},
        "Plasma_test.graph": {"nodes": 6, "edges": 5},
        "SharedMemoryTest_update.graph": {"nodes": 8, "edges": 7},
        # "simpleMKN_update.graph", # Currently broken
    }

    def test_lg_unroll(self):
        """
        Basic verification that we can unroll a list of dropdicts from a logical graph

        lg_names = { "logical_graph_file.graph": num_keys_in_drop_list, ...}
        """
        # TODO These are number of logical graph nodes! Make this exclusive to LG init
        for lg_name, num_keys in self.lg_names.items():
            fp = path_utils.get_lg_fpath("logical_graphs", lg_name)
            lg = LG(fp, ssid=TEST_SSID)

            drop_list = lg.unroll_to_tpl()
            with open(path_utils.get_lg_fpath('pickle', lg_name), 'rb') as pk_fp:
                test_unroll = pickle.load(pk_fp)

            # It is worth mentioning that we do not get an accurate number of links
            # from the LG, as it is not tracked after the initial graph_loading.
            self.assertEqual(len(test_unroll), len(drop_list))
            self.assertEqual(num_keys['nodes'], len(drop_list))

            # Confirm number of output/consumers and inputs/producers are the same
            for i, drop in enumerate(drop_list):
                if 'outputs' in drop:
                    expected = test_unroll[i]['outputs']
                    actual = drop['outputs']
                    self.assertEqual(len(expected), len(actual))
                if 'inputs' in drop:
                    expected = test_unroll[i]['inputs']
                    actual = drop['inputs']
                    self.assertEqual(len(expected), len(actual))
                if 'producers' in drop:
                    expected = test_unroll[i]['producers']
                    actual = drop['producers']
                    self.assertEqual(len(expected), len(actual))
                if 'consumers' in drop:
                    expected = test_unroll[i]['consumers']
                    actual = drop['consumers']
                    self.assertEqual(len(expected), len(actual))

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
