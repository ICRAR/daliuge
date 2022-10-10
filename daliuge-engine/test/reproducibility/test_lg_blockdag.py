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

import pkg_resources

from dlg.common.reproducibility.constants import ReproducibilityFlags, ALL_RMODES
from dlg.common.reproducibility.reproducibility import (
    init_lgt_repro_data,
    init_lg_repro_data,
    lg_build_blockdag,
)


def _init_graph(filename: str):
    with pkg_resources.resource_stream("test.reproducibility", filename) as file:
        lgt = json.load(file)
    return lgt


def _setup_lgt(filename: str, rmode: ReproducibilityFlags):
    lgt = _init_graph(filename)
    init_lgt_repro_data(lgt, rmode=str(rmode.value))
    init_lg_repro_data(lgt)
    leaves = lg_build_blockdag(lgt, rmode)[0]
    return lgt, leaves


class LogicalBlockdagTests(unittest.TestCase):

    def test_single(self):
        for rmode in ALL_RMODES:
            _, leaves = _setup_lgt("topoGraphs/testSingle.graph", rmode)
            self.assertTrue(len(leaves) == 1, rmode.name)

    def test_twostart(self):
        """
        A graph with two starts
        A -->
             C
        B -->
        """
        for rmode in ALL_RMODES:
            lgt, leaves = _setup_lgt("topoGraphs/testTwoStart.graph", rmode)
            parenthashes = list(
                lgt["nodeDataArray"][1]["reprodata"][rmode.name]["lg_parenthashes"].values()
            )
            if rmode == ReproducibilityFlags.REPRODUCE:
                parenthashes = list(
                    lgt["nodeDataArray"][1]["reprodata"][rmode.name][
                        "lg_parenthashes"].values()
                )
                sig0 = lgt["nodeDataArray"][0]["reprodata"][rmode.name]["lg_blockhash"]
                sig1 = lgt["nodeDataArray"][1]["reprodata"][rmode.name]["lg_blockhash"]
                sig2 = lgt["nodeDataArray"][2]["reprodata"][rmode.name]["lg_blockhash"]
                self.assertTrue(
                    len(leaves) == 1
                    and len(parenthashes) == 0
                    and sig0 == sig1
                    and sig1 == sig2
                    , rmode.name
                )
                self.assertTrue(len(parenthashes) == 0, rmode.name)
            else:
                self.assertTrue(
                    len(leaves) == 1
                    and len(parenthashes) == 2
                    and parenthashes[0] == parenthashes[1]
                    , rmode.name
                )

    def test_twoend(self):
        """
        A graph with two ends
          --> B
        A
          --> C
        """
        for rmode in ALL_RMODES:
            _, leaves = _setup_lgt("topoGraphs/testTwoEnd.graph", rmode)
            self.assertTrue(leaves[0] == leaves[1], rmode.name)

    def test_twolines(self):
        """
        A graph with two starts and two ends
        A --> B
        C --> D
        """
        for rmode in ALL_RMODES:
            _, leaves = _setup_lgt("topoGraphs/testTwoLines.graph", rmode)
            self.assertTrue(leaves[0] == leaves[1], rmode.name)

    def test_data_fan(self):
        """
        Tests that a single data source scatters its signature to downstream data drops.
        """
        for rmode in ALL_RMODES:
            lgt, leaves = _setup_lgt("topoGraphs/dataFan.graph", rmode)
            if rmode != ReproducibilityFlags.REPRODUCE:
                sourcehash = lgt["nodeDataArray"][0]["reprodata"][rmode.name]["lg_blockhash"]
                parenthash1 = list(
                    lgt["nodeDataArray"][2]["reprodata"][rmode.name]["lg_parenthashes"].values()
                )
                parenthash2 = list(
                    lgt["nodeDataArray"][3]["reprodata"][rmode.name]["lg_parenthashes"].values()
                )
                self.assertTrue(parenthash1 == parenthash2 and parenthash1[0] == sourcehash,
                                rmode.name)
            else:
                sourcehash = lgt["nodeDataArray"][1]["reprodata"][rmode.name]["lg_blockhash"]
                parenthash1 = list(
                    lgt["nodeDataArray"][2]["reprodata"][rmode.name][
                        "lg_parenthashes"].values()
                )
                parenthash2 = list(
                    lgt["nodeDataArray"][3]["reprodata"][rmode.name][
                        "lg_parenthashes"].values()
                )
                self.assertTrue(parenthash1 == parenthash2 and parenthash1[0] == sourcehash)

    def test_data_funnel(self):
        """
        Tests that two data sources are collected in a single downstream data drop
        """
        for rmode in ALL_RMODES:
            lgt, _ = _setup_lgt("topoGraphs/dataFunnel.graph", rmode)
            if rmode != ReproducibilityFlags.REPRODUCE:
                sourcehash = lgt["nodeDataArray"][1]["reprodata"][rmode.name]["lg_blockhash"]
                parenthashes = list(
                    lgt["nodeDataArray"][3]["reprodata"][rmode.name]["lg_parenthashes"].values()
                )
                self.assertTrue(sourcehash == parenthashes[0] and len(parenthashes) == 1,
                                rmode.name)
            else:
                sourcehashes = [
                    lgt["nodeDataArray"][0]["reprodata"][rmode.name]["lg_blockhash"],
                    lgt["nodeDataArray"][2]["reprodata"][rmode.name]["lg_blockhash"],
                ]
                parenthashes = list(
                    lgt["nodeDataArray"][3]["reprodata"][rmode.name][
                        "lg_parenthashes"].values()
                )
                self.assertTrue(sourcehashes == parenthashes and len(parenthashes) == 2, rmode.name)

    def test_data_sandwich(self):
        """
        Tests two data drops with an interim computing drop
        :return:
        """
        for rmode in ALL_RMODES:
            lgt, _ = _setup_lgt("topoGraphs/dataSandwich.graph", rmode)
            index = 0 if rmode != ReproducibilityFlags.REPRODUCE else 1
            sourcehash = lgt["nodeDataArray"][index]["reprodata"][rmode.name]["lg_blockhash"]
            parenthashes = list(
                lgt["nodeDataArray"][2]["reprodata"][rmode.name]["lg_parenthashes"].values()
            )
            self.assertTrue(sourcehash == parenthashes[0] and len(parenthashes) == 1, rmode.name)

    def test_computation_sandwich(self):
        """
        Tests that an internal data drop surrounded by computing drops is handled correctly.
        """
        for rmode in ALL_RMODES:
            lgt, _ = _setup_lgt("topoGraphs/computationSandwich.graph", rmode)
            sourcehash = lgt["nodeDataArray"][1]["reprodata"][rmode.name]["lg_blockhash"]
            parenthashes = list(
                lgt["nodeDataArray"][2]["reprodata"][rmode.name]["lg_parenthashes"].values()
            )
            if rmode != ReproducibilityFlags.REPRODUCE:
                self.assertTrue(sourcehash == parenthashes[0] and len(parenthashes) == 1,
                                rmode.name)
            else:
                self.assertEqual(0, len(parenthashes), rmode.name)


class LogicalBlockdagNothingTests(unittest.TestCase):
    """
    Tests the logical blockdag construction behaviour when asserting nothing.
    """

    rmode = ReproducibilityFlags.NOTHING

    def test_twostart(self):
        """
        A graph with two starts
        A -->
             C
        B -->
        """
        lgt, _ = _setup_lgt("topoGraphs/testTwoStart.graph", self.rmode)
        self.assertIn("reprodata", lgt["nodeDataArray"][1])

    def test_data_fan(self):
        """
        Tests that a single data source scatters its signature to downstream data drops.
        """
        lgt, _ = _setup_lgt("topoGraphs/dataFan.graph", self.rmode)
        for drop in lgt["nodeDataArray"]:
            self.assertIn("reprodata", drop)

    def test_data_funnel(self):
        """
        Tests that two data sources are collected in a single downstream data drop
        """
        lgt, _ = _setup_lgt("topoGraphs/dataFunnel.graph", self.rmode)
        for drop in lgt["nodeDataArray"]:
            self.assertIn("reprodata", drop)

    def test_data_sandwich(self):
        """
        Tests two data drops with an interim computing drop
        :return:
        """
        lgt, _ = _setup_lgt("topoGraphs/dataSandwich.graph", self.rmode)
        for drop in lgt["nodeDataArray"]:
            self.assertIn("reprodata", drop)

    def test_computation_sandwich(self):
        """
        Tests that an internal data drop surrounded by computing drops is handled correctly.
        """
        lgt, _ = _setup_lgt("topoGraphs/computationSandwich.graph", self.rmode)
        for drop in lgt["nodeDataArray"]:
            self.assertIn("reprodata", drop)
