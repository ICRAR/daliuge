#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2017
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

"""
Tests how the logical blockdag construction logic works.
This refers to how parent hashes and signatures are built.

Most of these tests will be asserting the obvious, with the exception of Reproducing behaviour.
"""

import json
import unittest

from dlg.common.reproducibility.constants import ReproducibilityFlags
from dlg.common.reproducibility.reproducibility import \
    init_lgt_repro_data, init_lg_repro_data, lg_build_blockdag


def _init_graph(filename):
    file = open(filename)
    lgt = json.load(file)
    file.close()
    for drop in lgt['nodeDataArray']:
        drop['reprodata'] = {}
        drop['reprodata']['lg_parenthashes'] = []
        drop['reprodata']['lgt_data'] = {'merkleroot': "1"}
        drop['reprodata']['lg_data'] = {}
    return lgt


class LogicalBlockdagRerunTests(unittest.TestCase):
    """
    Tests the logical blockdag construction behaviour when rerunning.
    In all cases all drops should be included at this stage.
    """

    rmode = ReproducibilityFlags.RERUN

    def test_single(self):
        """
        Tests a single drop
        A
        """
        lgt = _init_graph("topoGraphs/testSingle.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        leaves = lg_build_blockdag(lgt)[0]
        self.assertTrue(len(leaves) == 1)

    def test_twostart(self):
        """
        A graph with two starts
        A -->
             C
        B -->
        """
        lgt = _init_graph("topoGraphs/testTwoStart.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        leaves = lg_build_blockdag(lgt)[0]
        parenthashes = list(lgt['nodeDataArray'][1]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(len(leaves) == 1 and
                        len(parenthashes) == 2 and
                        parenthashes[0] == parenthashes[1])

    def test_twoend(self):
        """
        A graph with two ends
          --> B
        A
          --> C
        """
        lgt = _init_graph("topoGraphs/testTwoEnd.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        leaves = lg_build_blockdag(lgt)[0]
        self.assertTrue(leaves[0] == leaves[1])

    def test_twolines(self):
        """
        A graph with two starts and two ends
        A --> B
        C --> D
        """
        lgt = _init_graph("topoGraphs/testTwoLines.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        leaves = lg_build_blockdag(lgt)[0]
        self.assertTrue(leaves[0] == leaves[1])

    def test_data_fan(self):
        """
        Tests that a single data source scatters its signature to downstream data drops.
        """
        lgt = _init_graph("topoGraphs/dataFan.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        lg_build_blockdag(lgt)
        sourcehash = lgt['nodeDataArray'][0]['reprodata']['lg_blockhash']
        parenthash1 = list(lgt['nodeDataArray'][2]['reprodata']['lg_parenthashes'].values())
        parenthash2 = list(lgt['nodeDataArray'][3]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(parenthash1 == parenthash2 and parenthash1[0] == sourcehash)

    def test_data_funnel(self):
        """
        Tests that two data sources are collected in a single downstream data drop
        """
        lgt = _init_graph("topoGraphs/dataFunnel.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        lg_build_blockdag(lgt)
        sourcehash = lgt['nodeDataArray'][1]['reprodata']['lg_blockhash']
        parenthashes = list(lgt['nodeDataArray'][3]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(sourcehash == parenthashes[0] and len(parenthashes) == 1)

    def test_data_sandwich(self):
        """
        Tests two data drops with an interim computing drop
        :return:
        """
        lgt = _init_graph("topoGraphs/dataSandwich.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        lg_build_blockdag(lgt)
        sourcehash = lgt['nodeDataArray'][0]['reprodata']['lg_blockhash']
        parenthashes = list(lgt['nodeDataArray'][2]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(sourcehash == parenthashes[0] and len(parenthashes) == 1)

    def test_computation_sandwich(self):
        """
        Tests that an internal data drop surrounded by computing drops is handled correctly.
        """
        lgt = _init_graph("topoGraphs/computationSandwich.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        lg_build_blockdag(lgt)
        sourcehash = lgt['nodeDataArray'][1]['reprodata']['lg_blockhash']
        parenthashes = list(lgt['nodeDataArray'][2]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(sourcehash == parenthashes[0] and len(parenthashes) == 1)


class LogicalBlockdagRepeatTests(unittest.TestCase):
    """
    Tests the logical blockdag construction behaviour when rerunning.
    In all cases all drops should be included at this stage.
    """

    rmode = ReproducibilityFlags.REPEAT

    def test_single(self):
        """
        Tests a single drop
        A
        """
        lgt = _init_graph("topoGraphs/testSingle.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        leaves = lg_build_blockdag(lgt)[0]
        self.assertTrue(len(leaves) == 1)

    def test_twostart(self):
        """
        A graph with two starts
        A -->
             C
        B -->
        """
        lgt = _init_graph("topoGraphs/testTwoStart.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        leaves = lg_build_blockdag(lgt)[0]
        parenthashes = list(lgt['nodeDataArray'][1]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(len(leaves) == 1 and
                        len(parenthashes) == 2 and
                        parenthashes[0] == parenthashes[1])

    def test_twoend(self):
        """
        A graph with two ends
          --> B
        A
          --> C
        """
        lgt = _init_graph("topoGraphs/testTwoEnd.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        leaves = lg_build_blockdag(lgt)[0]
        self.assertTrue(leaves[0] == leaves[1])

    def test_twolines(self):
        """
        A graph with two starts and two ends
        A --> B
        C --> D
        """
        lgt = _init_graph("topoGraphs/testTwoLines.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        leaves = lg_build_blockdag(lgt)[0]
        self.assertTrue(leaves[0] == leaves[1])

    def test_data_fan(self):
        """
        Tests that a single data source scatters its signature to downstream data drops.
        """
        lgt = _init_graph("topoGraphs/dataFan.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        lg_build_blockdag(lgt)
        sourcehash = lgt['nodeDataArray'][0]['reprodata']['lg_blockhash']
        parenthash1 = list(lgt['nodeDataArray'][2]['reprodata']['lg_parenthashes'].values())
        parenthash2 = list(lgt['nodeDataArray'][3]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(parenthash1 == parenthash2 and parenthash1[0] == sourcehash)

    def test_data_funnel(self):
        """
        Tests that two data sources are collected in a single downstream data drop
        """
        lgt = _init_graph("topoGraphs/dataFunnel.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        lg_build_blockdag(lgt)
        sourcehash = lgt['nodeDataArray'][1]['reprodata']['lg_blockhash']
        parenthashes = list(lgt['nodeDataArray'][3]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(sourcehash == parenthashes[0] and len(parenthashes) == 1)

    def test_data_sandwich(self):
        """
        Tests two data drops with an interim computing drop
        :return:
        """
        lgt = _init_graph("topoGraphs/dataSandwich.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        lg_build_blockdag(lgt)
        sourcehash = lgt['nodeDataArray'][0]['reprodata']['lg_blockhash']
        parenthashes = list(lgt['nodeDataArray'][2]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(sourcehash == parenthashes[0] and len(parenthashes) == 1)

    def test_computation_sandwich(self):
        """
        Tests that an internal data drop surrounded by computing drops is handled correctly.
        """
        lgt = _init_graph("topoGraphs/computationSandwich.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        lg_build_blockdag(lgt)
        sourcehash = lgt['nodeDataArray'][1]['reprodata']['lg_blockhash']
        parenthashes = list(lgt['nodeDataArray'][2]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(sourcehash == parenthashes[0] and len(parenthashes) == 1)


class LogicalBlockdagRecomputeTests(unittest.TestCase):
    """
    Tests the logical blockdag construction behaviour when rerunning.
    In all cases all drops should be included at this stage.
    """

    rmode = ReproducibilityFlags.RECOMPUTE

    def test_single(self):
        """
        Tests a single drop
        A
        """
        lgt = _init_graph("topoGraphs/testSingle.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        leaves = lg_build_blockdag(lgt)[0]
        self.assertTrue(len(leaves) == 1)

    def test_twostart(self):
        """
        A graph with two starts
        A -->
             C
        B -->
        """
        lgt = _init_graph("topoGraphs/testTwoStart.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        leaves = lg_build_blockdag(lgt)[0]
        parenthashes = list(lgt['nodeDataArray'][1]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(len(leaves) == 1 and
                        len(parenthashes) == 2 and
                        parenthashes[0] == parenthashes[1])

    def test_twoend(self):
        """
        A graph with two ends
          --> B
        A
          --> C
        """
        lgt = _init_graph("topoGraphs/testTwoEnd.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        leaves = lg_build_blockdag(lgt)[0]
        self.assertTrue(leaves[0] == leaves[1])

    def test_twolines(self):
        """
        A graph with two starts and two ends
        A --> B
        C --> D
        """
        lgt = _init_graph("topoGraphs/testTwoLines.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        leaves = lg_build_blockdag(lgt)[0]
        self.assertTrue(leaves[0] == leaves[1])

    def test_data_fan(self):
        """
        Tests that a single data source scatters its signature to downstream data drops.
        """
        lgt = _init_graph("topoGraphs/dataFan.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        lg_build_blockdag(lgt)
        sourcehash = lgt['nodeDataArray'][0]['reprodata']['lg_blockhash']
        parenthash1 = list(lgt['nodeDataArray'][2]['reprodata']['lg_parenthashes'].values())
        parenthash2 = list(lgt['nodeDataArray'][3]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(parenthash1 == parenthash2 and parenthash1[0] == sourcehash)

    def test_data_funnel(self):
        """
        Tests that two data sources are collected in a single downstream data drop
        """
        lgt = _init_graph("topoGraphs/dataFunnel.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        lg_build_blockdag(lgt)
        sourcehash = lgt['nodeDataArray'][1]['reprodata']['lg_blockhash']
        parenthashes = list(lgt['nodeDataArray'][3]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(sourcehash == parenthashes[0] and len(parenthashes) == 1)

    def test_data_sandwich(self):
        """
        Tests two data drops with an interim computing drop
        :return:
        """
        lgt = _init_graph("topoGraphs/dataSandwich.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        lg_build_blockdag(lgt)
        sourcehash = lgt['nodeDataArray'][0]['reprodata']['lg_blockhash']
        parenthashes = list(lgt['nodeDataArray'][2]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(sourcehash == parenthashes[0] and len(parenthashes) == 1)

    def test_computation_sandwich(self):
        """
        Tests that an internal data drop surrounded by computing drops is handled correctly.
        """
        lgt = _init_graph("topoGraphs/computationSandwich.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        lg_build_blockdag(lgt)
        sourcehash = lgt['nodeDataArray'][1]['reprodata']['lg_blockhash']
        parenthashes = list(lgt['nodeDataArray'][2]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(sourcehash == parenthashes[0] and len(parenthashes) == 1)


class LogicalBlockdagReproduceTests(unittest.TestCase):
    """
    Tests the logical blockdag construction behaviour when rerunning.
    Computing drops should be truncated out of the blockdag construction.
    This means that the data tests will be very different.
    """

    rmode = ReproducibilityFlags.REPRODUCE

    def test_single(self):
        """
        Tests a single drop
        A
        """
        lgt = _init_graph("topoGraphs/testSingle.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        leaves = lg_build_blockdag(lgt)[0]
        self.assertTrue(len(leaves) == 1)

    def test_twostart(self):
        """
        A graph with two starts
        A -->
             C
        B -->
        """
        lgt = _init_graph("topoGraphs/testTwoStart.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        leaves = lg_build_blockdag(lgt)[0]
        parenthashes = list(lgt['nodeDataArray'][1]['reprodata']['lg_parenthashes'].values())
        sig0 = lgt['nodeDataArray'][0]['reprodata']['lg_blockhash']
        sig1 = lgt['nodeDataArray'][1]['reprodata']['lg_blockhash']
        sig2 = lgt['nodeDataArray'][2]['reprodata']['lg_blockhash']
        self.assertTrue(len(leaves) == 1 and
                        len(parenthashes) == 0 and
                        sig0 == sig1 and sig1 == sig2)

    def test_twoend(self):
        """
        A graph with two ends
          --> B
        A
          --> C
        """
        lgt = _init_graph("topoGraphs/testTwoEnd.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        leaves = lg_build_blockdag(lgt)[0]
        self.assertTrue(leaves[0] == leaves[1])

    def test_twolines(self):
        """
        A graph with two starts and two ends
        A --> B
        C --> D
        """
        lgt = _init_graph("topoGraphs/testTwoLines.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        leaves = lg_build_blockdag(lgt)[0]
        self.assertTrue(leaves[0] == leaves[1])

    def test_data_fan(self):
        """
        Tests that a single data source scatters its signature to downstream data drops.
        """
        lgt = _init_graph("topoGraphs/dataFan.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        lg_build_blockdag(lgt)
        sourcehash = lgt['nodeDataArray'][1]['reprodata']['lg_blockhash']
        parenthash1 = list(lgt['nodeDataArray'][2]['reprodata']['lg_parenthashes'].values())
        parenthash2 = list(lgt['nodeDataArray'][3]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(parenthash1 == parenthash2 and parenthash1[0] == sourcehash)

    def test_data_funnel(self):
        """
        Tests that two data sources are collected in a single downstream data drop
        """
        lgt = _init_graph("topoGraphs/dataFunnel.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        lg_build_blockdag(lgt)
        sourcehashes = [lgt['nodeDataArray'][0]['reprodata']['lg_blockhash'],
                        lgt['nodeDataArray'][2]['reprodata']['lg_blockhash']]
        parenthashes = list(lgt['nodeDataArray'][3]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(sourcehashes == parenthashes and len(parenthashes) == 2)

    def test_data_sandwich(self):
        """
        Tests two data drops with an interim computing drop
        :return:
        """
        lgt = _init_graph("topoGraphs/dataSandwich.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        lg_build_blockdag(lgt)
        sourcehash = lgt['nodeDataArray'][1]['reprodata']['lg_blockhash']
        parenthashes = list(lgt['nodeDataArray'][2]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(sourcehash == parenthashes[0] and len(parenthashes) == 1)

    def test_computation_sandwich(self):
        """
        Tests that an internal data drop surrounded by computing drops is handled correctly.
        """
        lgt = _init_graph("topoGraphs/computationSandwich.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        lg_build_blockdag(lgt)
        sourcehash = lgt['nodeDataArray'][1]['reprodata']['lg_blockhash']
        parenthashes = list(lgt['nodeDataArray'][2]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(sourcehash == parenthashes[0] and len(parenthashes) == 1)


class LogicalBlockdagReplicateSciTests(unittest.TestCase):
    """
    Tests the logical blockdag construction behaviour when rerunning.
    In all cases all drops should be included at this stage.
    """

    rmode = ReproducibilityFlags.REPLICATE_SCI

    def test_single(self):
        """
        Tests a single drop
        A
        """
        lgt = _init_graph("topoGraphs/testSingle.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        leaves = lg_build_blockdag(lgt)[0]
        self.assertTrue(len(leaves) == 1)

    def test_twostart(self):
        """
        A graph with two starts
        A -->
             C
        B -->
        """
        lgt = _init_graph("topoGraphs/testTwoStart.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        leaves = lg_build_blockdag(lgt)[0]
        parenthashes = list(lgt['nodeDataArray'][1]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(len(leaves) == 1 and
                        len(parenthashes) == 2 and
                        parenthashes[0] == parenthashes[1])

    def test_twoend(self):
        """
        A graph with two ends
          --> B
        A
          --> C
        """
        lgt = _init_graph("topoGraphs/testTwoEnd.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        leaves = lg_build_blockdag(lgt)[0]
        self.assertTrue(leaves[0] == leaves[1])

    def test_twolines(self):
        """
        A graph with two starts and two ends
        A --> B
        C --> D
        """
        lgt = _init_graph("topoGraphs/testTwoLines.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        leaves = lg_build_blockdag(lgt)[0]
        self.assertTrue(leaves[0] == leaves[1])

    def test_data_fan(self):
        """
        Tests that a single data source scatters its signature to downstream data drops.
        """
        lgt = _init_graph("topoGraphs/dataFan.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        lg_build_blockdag(lgt)
        sourcehash = lgt['nodeDataArray'][0]['reprodata']['lg_blockhash']
        parenthash1 = list(lgt['nodeDataArray'][2]['reprodata']['lg_parenthashes'].values())
        parenthash2 = list(lgt['nodeDataArray'][3]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(parenthash1 == parenthash2 and parenthash1[0] == sourcehash)

    def test_data_funnel(self):
        """
        Tests that two data sources are collected in a single downstream data drop
        """
        lgt = _init_graph("topoGraphs/dataFunnel.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        lg_build_blockdag(lgt)
        sourcehash = lgt['nodeDataArray'][1]['reprodata']['lg_blockhash']
        parenthashes = list(lgt['nodeDataArray'][3]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(sourcehash == parenthashes[0] and len(parenthashes) == 1)

    def test_data_sandwich(self):
        """
        Tests two data drops with an interim computing drop
        :return:
        """
        lgt = _init_graph("topoGraphs/dataSandwich.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        lg_build_blockdag(lgt)
        sourcehash = lgt['nodeDataArray'][0]['reprodata']['lg_blockhash']
        parenthashes = list(lgt['nodeDataArray'][2]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(sourcehash == parenthashes[0] and len(parenthashes) == 1)

    def test_computation_sandwich(self):
        """
        Tests that an internal data drop surrounded by computing drops is handled correctly.
        """
        lgt = _init_graph("topoGraphs/computationSandwich.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        lg_build_blockdag(lgt)
        sourcehash = lgt['nodeDataArray'][1]['reprodata']['lg_blockhash']
        parenthashes = list(lgt['nodeDataArray'][2]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(sourcehash == parenthashes[0] and len(parenthashes) == 1)


class LogicalBlockdagReplicateCompTests(unittest.TestCase):
    """
    Tests the logical blockdag construction behaviour when rerunning.
    In all cases all drops should be included at this stage.
    """

    rmode = ReproducibilityFlags.REPLICATE_COMP

    def test_single(self):
        """
        Tests a single drop
        A
        """
        lgt = _init_graph("topoGraphs/testSingle.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        leaves = lg_build_blockdag(lgt)[0]
        self.assertTrue(len(leaves) == 1)

    def test_twostart(self):
        """
        A graph with two starts
        A -->
             C
        B -->
        """
        lgt = _init_graph("topoGraphs/testTwoStart.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        leaves = lg_build_blockdag(lgt)[0]
        parenthashes = list(lgt['nodeDataArray'][1]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(len(leaves) == 1 and
                        len(parenthashes) == 2 and
                        parenthashes[0] == parenthashes[1])

    def test_twoend(self):
        """
        A graph with two ends
          --> B
        A
          --> C
        """
        lgt = _init_graph("topoGraphs/testTwoEnd.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        leaves = lg_build_blockdag(lgt)[0]
        self.assertTrue(leaves[0] == leaves[1])

    def test_twolines(self):
        """
        A graph with two starts and two ends
        A --> B
        C --> D
        """
        lgt = _init_graph("topoGraphs/testTwoLines.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        leaves = lg_build_blockdag(lgt)[0]
        self.assertTrue(leaves[0] == leaves[1])

    def test_data_fan(self):
        """
        Tests that a single data source scatters its signature to downstream data drops.
        """
        lgt = _init_graph("topoGraphs/dataFan.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        lg_build_blockdag(lgt)
        sourcehash = lgt['nodeDataArray'][0]['reprodata']['lg_blockhash']
        parenthash1 = list(lgt['nodeDataArray'][2]['reprodata']['lg_parenthashes'].values())
        parenthash2 = list(lgt['nodeDataArray'][3]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(parenthash1 == parenthash2 and parenthash1[0] == sourcehash)

    def test_data_funnel(self):
        """
        Tests that two data sources are collected in a single downstream data drop
        """
        lgt = _init_graph("topoGraphs/dataFunnel.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        lg_build_blockdag(lgt)
        sourcehash = lgt['nodeDataArray'][1]['reprodata']['lg_blockhash']
        parenthashes = list(lgt['nodeDataArray'][3]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(sourcehash == parenthashes[0] and len(parenthashes) == 1)

    def test_data_sandwich(self):
        """
        Tests two data drops with an interim computing drop
        :return:
        """
        lgt = _init_graph("topoGraphs/dataSandwich.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        lg_build_blockdag(lgt)
        sourcehash = lgt['nodeDataArray'][0]['reprodata']['lg_blockhash']
        parenthashes = list(lgt['nodeDataArray'][2]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(sourcehash == parenthashes[0] and len(parenthashes) == 1)

    def test_computation_sandwich(self):
        """
        Tests that an internal data drop surrounded by computing drops is handled correctly.
        """
        lgt = _init_graph("topoGraphs/computationSandwich.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        lg_build_blockdag(lgt)
        sourcehash = lgt['nodeDataArray'][1]['reprodata']['lg_blockhash']
        parenthashes = list(lgt['nodeDataArray'][2]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(sourcehash == parenthashes[0] and len(parenthashes) == 1)


class LogicalBlockdagReplicateTOTALTests(unittest.TestCase):
    """
    Tests the logical blockdag construction behaviour when rerunning.
    In all cases all drops should be included at this stage.
    """

    rmode = ReproducibilityFlags.REPLICATE_TOTAL

    def test_single(self):
        """
        Tests a single drop
        A
        """
        lgt = _init_graph("topoGraphs/testSingle.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        leaves = lg_build_blockdag(lgt)[0]
        self.assertTrue(len(leaves) == 1)

    def test_twostart(self):
        """
        A graph with two starts
        A -->
             C
        B -->
        """
        lgt = _init_graph("topoGraphs/testTwoStart.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        leaves = lg_build_blockdag(lgt)[0]
        parenthashes = list(lgt['nodeDataArray'][1]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(len(leaves) == 1 and
                        len(parenthashes) == 2 and
                        parenthashes[0] == parenthashes[1])

    def test_twoend(self):
        """
        A graph with two ends
          --> B
        A
          --> C
        """
        lgt = _init_graph("topoGraphs/testTwoEnd.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        leaves = lg_build_blockdag(lgt)[0]
        self.assertTrue(leaves[0] == leaves[1])

    def test_twolines(self):
        """
        A graph with two starts and two ends
        A --> B
        C --> D
        """
        lgt = _init_graph("topoGraphs/testTwoLines.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        leaves = lg_build_blockdag(lgt)[0]
        self.assertTrue(leaves[0] == leaves[1])

    def test_data_fan(self):
        """
        Tests that a single data source scatters its signature to downstream data drops.
        """
        lgt = _init_graph("topoGraphs/dataFan.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        lg_build_blockdag(lgt)
        sourcehash = lgt['nodeDataArray'][0]['reprodata']['lg_blockhash']
        parenthash1 = list(lgt['nodeDataArray'][2]['reprodata']['lg_parenthashes'].values())
        parenthash2 = list(lgt['nodeDataArray'][3]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(parenthash1 == parenthash2 and parenthash1[0] == sourcehash)

    def test_data_funnel(self):
        """
        Tests that two data sources are collected in a single downstream data drop
        """
        lgt = _init_graph("topoGraphs/dataFunnel.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        lg_build_blockdag(lgt)
        sourcehash = lgt['nodeDataArray'][1]['reprodata']['lg_blockhash']
        parenthashes = list(lgt['nodeDataArray'][3]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(sourcehash == parenthashes[0] and len(parenthashes) == 1)

    def test_data_sandwich(self):
        """
        Tests two data drops with an interim computing drop
        :return:
        """
        lgt = _init_graph("topoGraphs/dataSandwich.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        lg_build_blockdag(lgt)
        sourcehash = lgt['nodeDataArray'][0]['reprodata']['lg_blockhash']
        parenthashes = list(lgt['nodeDataArray'][2]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(sourcehash == parenthashes[0] and len(parenthashes) == 1)

    def test_computation_sandwich(self):
        """
        Tests that an internal data drop surrounded by computing drops is handled correctly.
        """
        lgt = _init_graph("topoGraphs/computationSandwich.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        lg_build_blockdag(lgt)
        sourcehash = lgt['nodeDataArray'][1]['reprodata']['lg_blockhash']
        parenthashes = list(lgt['nodeDataArray'][2]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(sourcehash == parenthashes[0] and len(parenthashes) == 1)


class LogicalBlockdagNothingTests(unittest.TestCase):
    """
    Tests the logical blockdag construction behaviour when rerunning.
    """

    rmode = ReproducibilityFlags.NOTHING

    def test_single(self):
        """
        Tests a single drop
        A
        """
        lgt = _init_graph("topoGraphs/testSingle.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        leaves = lg_build_blockdag(lgt)[0]
        self.assertTrue(len(leaves) == 1)

    def test_twostart(self):
        """
        A graph with two starts
        A -->
             C
        B -->
        """
        lgt = _init_graph("topoGraphs/testTwoStart.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        leaves = lg_build_blockdag(lgt)[0]
        parenthashes = list(lgt['nodeDataArray'][1]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(len(leaves) == 1 and
                        len(parenthashes) == 2 and
                        parenthashes[0] == parenthashes[1])

    def test_twoend(self):
        """
        A graph with two ends
          --> B
        A
          --> C
        """
        lgt = _init_graph("topoGraphs/testTwoEnd.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        leaves = lg_build_blockdag(lgt)[0]
        self.assertTrue(leaves[0] == leaves[1])

    def test_twolines(self):
        """
        A graph with two starts and two ends
        A --> B
        C --> D
        """
        lgt = _init_graph("topoGraphs/testTwoLines.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        leaves = lg_build_blockdag(lgt)[0]
        self.assertTrue(leaves[0] == leaves[1])

    def test_data_fan(self):
        """
        Tests that a single data source scatters its signature to downstream data drops.
        """
        lgt = _init_graph("topoGraphs/dataFan.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        lg_build_blockdag(lgt)
        sourcehash = lgt['nodeDataArray'][0]['reprodata']['lg_blockhash']
        parenthash1 = list(lgt['nodeDataArray'][2]['reprodata']['lg_parenthashes'].values())
        parenthash2 = list(lgt['nodeDataArray'][3]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(parenthash1 == parenthash2 and parenthash1[0] == sourcehash)

    def test_data_funnel(self):
        """
        Tests that two data sources are collected in a single downstream data drop
        """
        lgt = _init_graph("topoGraphs/dataFunnel.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        lg_build_blockdag(lgt)
        sourcehash = lgt['nodeDataArray'][1]['reprodata']['lg_blockhash']
        parenthashes = list(lgt['nodeDataArray'][3]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(sourcehash == parenthashes[0] and len(parenthashes) == 1)

    def test_data_sandwich(self):
        """
        Tests two data drops with an interim computing drop
        :return:
        """
        lgt = _init_graph("topoGraphs/dataSandwich.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        lg_build_blockdag(lgt)
        sourcehash = lgt['nodeDataArray'][0]['reprodata']['lg_blockhash']
        parenthashes = list(lgt['nodeDataArray'][2]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(sourcehash == parenthashes[0] and len(parenthashes) == 1)

    def test_computation_sandwich(self):
        """
        Tests that an internal data drop surrounded by computing drops is handled correctly.
        """
        lgt = _init_graph("topoGraphs/computationSandwich.graph")
        init_lgt_repro_data(lgt, rmode=str(self.rmode.value))
        init_lg_repro_data(lgt)
        lg_build_blockdag(lgt)
        sourcehash = lgt['nodeDataArray'][1]['reprodata']['lg_blockhash']
        parenthashes = list(lgt['nodeDataArray'][2]['reprodata']['lg_parenthashes'].values())
        self.assertTrue(sourcehash == parenthashes[0] and len(parenthashes) == 1)
