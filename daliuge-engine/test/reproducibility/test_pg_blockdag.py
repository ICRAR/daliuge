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
Tests how the physical blockdag construction logic works.
This refers to how parent hashes and signatures are built.

Most of these tests will be asserting the obvious, with the exception of Reproducing behaviour.
"""

import unittest
import logging

from dlg.common.reproducibility.constants import (
    ReproducibilityFlags,
    ALL_RMODES,
)
from dlg.common.reproducibility.reproducibility import build_blockdag

logger = logging.getLogger("__name__")


def _generate_dummy_compute(rmode: ReproducibilityFlags):
    if rmode is not ReproducibilityFlags.ALL:
        return {
            "oid": 1,
            "reprodata": {
                "rmode": str(rmode.value),
                rmode.name: {
                    "lgt_data": {
                        "categoryType": "Application",
                        "category": "BashShellApp",
                    },
                    "lg_blockhash": "1",
                    "pgt_data": {"merkleroot": "2"},
                    "pgt_parenthashes": {},
                    "pgt_blockhash": "3",
                    "pg_data": {"merkleroot": "4"},
                    "pg_parenthashes": {},
                    "pg_blockhash": "5",
                    "rg_data": {"merkleroot": "6"},
                    "rg_parenthashes": {},
                },
            },
        }
    else:
        out_val = {"oid": 1, "reprodata": {}}
        out_val["reprodata"]["rmode"] = str(rmode.value)
        for level in ALL_RMODES:
            out_val["reprodata"][level.name] = {
                "lgt_data": {
                    "categoryType": "Application",
                    "category": "BashShellApp",
                },
                "lg_blockhash": "1",
                "pgt_data": {"merkleroot": "2"},
                "pgt_parenthashes": {},
                "pgt_blockhash": "3",
                "pg_data": {"merkleroot": "4"},
                "pg_parenthashes": {},
                "pg_blockhash": "5",
                "rg_data": {"merkleroot": "6"},
                "rg_parenthashes": {},
            }
        return out_val


def _generate_dummy_data(rmode: ReproducibilityFlags):
    if rmode is not ReproducibilityFlags.ALL:
        return {
            "oid": 1,
            "reprodata": {
                "rmode": str(rmode.value),
                rmode.name: {
                    "lgt_data": {
                        "categoryType": "Data",
                        "category": "File",
                    },
                    "lg_blockhash": "a",
                    "pgt_data": {"merkleroot": "b"},
                    "pgt_parenthashes": {},
                    "pgt_blockhash": "c",
                    "pg_data": {"merkleroot": "d"},
                    "pg_parenthashes": {},
                    "pg_blockhash": "e",
                    "rg_data": {"merkleroot": "f"},
                    "rg_parenthashes": {},
                },
            },
        }
    else:
        out_val = {"oid": 1, "reprodata": {}}
        out_val["reprodata"]["rmode"] = str(rmode.value)
        for level in ALL_RMODES:
            out_val["reprodata"][level.name] = {
                "lgt_data": {
                    "categoryType": "Data",
                    "category": "File",
                },
                "lg_blockhash": "a",
                "pgt_data": {"merkleroot": "b"},
                "pgt_parenthashes": {},
                "pgt_blockhash": "c",
                "pg_data": {"merkleroot": "d"},
                "pg_parenthashes": {},
                "pg_blockhash": "e",
                "rg_data": {"merkleroot": "f"},
                "rg_parenthashes": {},
            }
        return out_val


def _init_pgraph_single(rmode: ReproducibilityFlags):
    pgt = [_generate_dummy_compute(rmode)]
    return pgt


def _init_pgraph_twostart(rmode: ReproducibilityFlags):
    pgt = [
        _generate_dummy_compute(rmode),
        _generate_dummy_compute(rmode),
        _generate_dummy_compute(rmode),
    ]
    pgt[1]["oid"] = 2
    pgt[2]["oid"] = 3
    pgt[0]["outputs"] = [2]
    pgt[2]["outputs"] = [2]
    return pgt


def _init_pgraph_twoend(rmode: ReproducibilityFlags):
    pgt = [
        _generate_dummy_compute(rmode),
        _generate_dummy_compute(rmode),
        _generate_dummy_compute(rmode),
    ]
    pgt[1]["oid"] = 2
    pgt[2]["oid"] = 3
    pgt[0]["outputs"] = [2, 3]
    for drop in pgt:
        drop["reprodata"]["rmode"] = str(rmode.value)
    return pgt


def _init_pgraph_twolines(rmode: ReproducibilityFlags):
    pgt = [
        _generate_dummy_compute(rmode),
        _generate_dummy_compute(rmode),
        _generate_dummy_compute(rmode),
        _generate_dummy_compute(rmode),
    ]
    pgt[1]["oid"] = 2
    pgt[2]["oid"] = 3
    pgt[3]["oid"] = 4
    pgt[0]["outputs"] = [2]
    pgt[2]["outputs"] = [4]
    for drop in pgt:
        drop["reprodata"]["rmode"] = str(rmode.value)
    return pgt


def _init_pgraph_data_fan(rmode: ReproducibilityFlags):
    pgt = [
        _generate_dummy_data(rmode),
        _generate_dummy_compute(rmode),
        _generate_dummy_data(rmode),
        _generate_dummy_data(rmode),
    ]
    for i, drop in enumerate(pgt):
        pgt[i]["oid"] = i
        pgt[i]["reprodata"]["rmode"] = str(rmode.value)
    pgt[0]["outputs"] = [1]
    pgt[1]["outputs"] = [2, 3]
    return pgt


def _init_pgraph_data_funnel(rmode: ReproducibilityFlags):
    pgt = [
        _generate_dummy_data(rmode),
        _generate_dummy_data(rmode),
        _generate_dummy_compute(rmode),
        _generate_dummy_data(rmode),
    ]
    for i, drop in enumerate(pgt):
        pgt[i]["oid"] = i
        pgt[i]["reprodata"]["rmode"] = str(rmode.value)
    pgt[0]["outputs"] = [2]
    pgt[1]["outputs"] = [2]
    pgt[2]["outputs"] = [3]
    return pgt


def _init_pgraph_data_sandwich(rmode: ReproducibilityFlags):
    pgt = [
        _generate_dummy_data(rmode),
        _generate_dummy_compute(rmode),
        _generate_dummy_data(rmode),
    ]
    for i, drop in enumerate(pgt):
        pgt[i]["oid"] = i
        pgt[i]["reprodata"]["rmode"] = str(rmode.value)
    pgt[0]["outputs"] = [1]
    pgt[1]["outputs"] = [2]
    return pgt


def _init_pgraph_computation_sandwich(rmode: ReproducibilityFlags):
    pgt = [
        _generate_dummy_compute(rmode),
        _generate_dummy_data(rmode),
        _generate_dummy_compute(rmode),
    ]
    for i, drop in enumerate(pgt):
        pgt[i]["oid"] = i
        pgt[i]["reprodata"]["rmode"] = str(rmode.value)
    pgt[0]["outputs"] = [1]
    pgt[1]["outputs"] = [2]
    return pgt


class PhysicalBlockdagRerunTests(unittest.TestCase):
    """
    Tests physical blockdag construction when rerunning.
    This should be relatively straightforward, but expanded group nodes scatter/gather etc. are
    special cases for rerunning specifically.
    """

    rmode = ReproducibilityFlags.RERUN

    def test_pg_blockdag_single(self):
        """
        Tests a single drop
        """
        pgr = _init_pgraph_single(self.rmode)
        leaves = build_blockdag(pgr, "pg", self.rmode)[0]
        self.assertTrue(len(leaves) == 1)

    def test_pg_blockdag_twostart(self):
        """
        A graph with two starts
        1 -->
             3
        2 -->
        """
        pgr = _init_pgraph_twostart(self.rmode)
        leaves = build_blockdag(pgr, "pg", self.rmode)[0]
        parenthashes = list(
            pgr[1]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        self.assertTrue(
            len(leaves) == 1
            and len(parenthashes) == 2
            and parenthashes[0] == parenthashes[1]
        )

    def test_pg_blockdag_twoend(self):
        """
        A graph with two ends
          --> 2
        1
          --> 3
        """
        pgr = _init_pgraph_twoend(self.rmode)
        leaves = build_blockdag(pgr, "pg", self.rmode)[0]
        self.assertTrue(leaves[0] == leaves[1])

    def test_pg_blockdag_twolines(self):
        """
        A graph with two starts and two ends
        1 --> 2
        3 --> 4
        """
        pgr = _init_pgraph_twolines(self.rmode)
        leaves = build_blockdag(pgr, "pg", self.rmode)[0]
        self.assertTrue(leaves[0] == leaves[1])

    def test_data_fan(self):
        """
        Tests that a single data source scatters its signature to downstream data drops.
        """
        pgr = _init_pgraph_data_fan(self.rmode)
        build_blockdag(pgr, "pg", self.rmode)
        sourcehash = pgr[1]["reprodata"][self.rmode.name]["pg_blockhash"]
        parenthash1 = list(
            pgr[2]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        parenthash2 = list(
            pgr[3]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        self.assertTrue(
            parenthash1 == parenthash2 and parenthash1[0] == sourcehash
        )

    def test_data_funnel(self):
        """
        Tests that two data sources are collected in a single downstream data drop
        """
        pgr = _init_pgraph_data_funnel(self.rmode)
        build_blockdag(pgr, "pg", self.rmode)
        sourcehash = pgr[2]["reprodata"][self.rmode.name]["pg_blockhash"]
        parenthashes = list(
            pgr[3]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        self.assertTrue(
            sourcehash == parenthashes[0] and len(parenthashes) == 1
        )

    def test_data_sandwich(self):
        """
        Tests two data drops with an interim computing drop
        :return:
        """
        pgr = _init_pgraph_data_sandwich(self.rmode)
        build_blockdag(pgr, "pg", self.rmode)
        sourcehash = pgr[1]["reprodata"][self.rmode.name]["pg_blockhash"]
        parenthashes = list(
            pgr[2]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        self.assertTrue(
            sourcehash == parenthashes[0] and len(parenthashes) == 1
        )

    def test_computation_sandwich(self):
        """
        Tests that an internal data drop surrounded by computing drops is handled correctly.
        """
        pgr = _init_pgraph_computation_sandwich(self.rmode)
        build_blockdag(pgr, "pg", self.rmode)
        sourcehash = pgr[1]["reprodata"][self.rmode.name]["pg_blockhash"]
        parenthashes = list(
            pgr[2]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        self.assertTrue(
            sourcehash == parenthashes[0] and len(parenthashes) == 1
        )


class PhysicalBlockdagRepeatTests(unittest.TestCase):
    """
    Tests physical blockdag construction when repeating.
    This should be relatively straightforward.
    """

    rmode = ReproducibilityFlags.REPEAT

    def test_pg_blockdag_single(self):
        """
        Tests a single drop
        """
        pgr = _init_pgraph_single(self.rmode)
        leaves = build_blockdag(pgr, "pg", self.rmode)[0]
        self.assertTrue(len(leaves) == 1)

    def test_pg_blockdag_twostart(self):
        """
        A graph with two starts
        1 -->
             3
        2 -->
        """
        pgr = _init_pgraph_twostart(self.rmode)
        leaves = build_blockdag(pgr, "pg", self.rmode)[0]
        parenthashes = list(
            pgr[1]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        self.assertTrue(
            len(leaves) == 1
            and len(parenthashes) == 2
            and parenthashes[0] == parenthashes[1]
        )

    def test_pg_blockdag_twoend(self):
        """
        A graph with two ends
          --> 2
        1
          --> 3
        """
        pgr = _init_pgraph_twoend(self.rmode)
        leaves = build_blockdag(pgr, "pg", self.rmode)[0]
        self.assertTrue(leaves[0] == leaves[1])

    def test_pg_blockdag_twolines(self):
        """
        A graph with two starts and two ends
        1 --> 2
        3 --> 4
        """
        pgr = _init_pgraph_twolines(self.rmode)
        leaves = build_blockdag(pgr, "pg", self.rmode)[0]
        self.assertTrue(leaves[0] == leaves[1])

    def test_data_fan(self):
        """
        Tests that a single data source scatters its signature to downstream data drops.
        """
        pgr = _init_pgraph_data_fan(self.rmode)
        build_blockdag(pgr, "pg", self.rmode)
        sourcehash = pgr[1]["reprodata"][self.rmode.name]["pg_blockhash"]
        parenthash1 = list(
            pgr[2]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        parenthash2 = list(
            pgr[3]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        self.assertTrue(
            parenthash1 == parenthash2 and parenthash1[0] == sourcehash
        )

    def test_data_funnel(self):
        """
        Tests that two data sources are collected in a single downstream data drop
        """
        pgr = _init_pgraph_data_funnel(self.rmode)
        build_blockdag(pgr, "pg", self.rmode)
        sourcehash = pgr[2]["reprodata"][self.rmode.name]["pg_blockhash"]
        parenthashes = list(
            pgr[3]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        self.assertTrue(
            sourcehash == parenthashes[0] and len(parenthashes) == 1
        )

    def test_data_sandwich(self):
        """
        Tests two data drops with an interim computing drop
        :return:
        """
        pgr = _init_pgraph_data_sandwich(self.rmode)
        build_blockdag(pgr, "pg", self.rmode)
        sourcehash = pgr[1]["reprodata"][self.rmode.name]["pg_blockhash"]
        parenthashes = list(
            pgr[2]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        self.assertTrue(
            sourcehash == parenthashes[0] and len(parenthashes) == 1
        )

    def test_computation_sandwich(self):
        """
        Tests that an internal data drop surrounded by computing drops is handled correctly.
        """
        pgr = _init_pgraph_computation_sandwich(self.rmode)
        build_blockdag(pgr, "pg", self.rmode)
        sourcehash = pgr[1]["reprodata"][self.rmode.name]["pg_blockhash"]
        parenthashes = list(
            pgr[2]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        self.assertTrue(
            sourcehash == parenthashes[0] and len(parenthashes) == 1
        )


class PhysicalBlockdagRecomputeTests(unittest.TestCase):
    """
    Tests physical blockdag construction when recomputing.
    This should be relatively straightforward.
    """

    rmode = ReproducibilityFlags.RECOMPUTE

    def test_pg_blockdag_single(self):
        """
        Tests a single drop
        """
        pgr = _init_pgraph_single(self.rmode)
        leaves = build_blockdag(pgr, "pg", self.rmode)[0]
        self.assertTrue(len(leaves) == 1)

    def test_pg_blockdag_twostart(self):
        """
        A graph with two starts
        1 -->
             3
        2 -->
        """
        pgr = _init_pgraph_twostart(self.rmode)
        leaves = build_blockdag(pgr, "pg", self.rmode)[0]
        parenthashes = list(
            pgr[1]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        self.assertTrue(
            len(leaves) == 1
            and len(parenthashes) == 2
            and parenthashes[0] == parenthashes[1]
        )

    def test_pg_blockdag_twoend(self):
        """
        A graph with two ends
          --> 2
        1
          --> 3
        """
        pgr = _init_pgraph_twoend(self.rmode)
        leaves = build_blockdag(pgr, "pg", self.rmode)[0]
        self.assertTrue(leaves[0] == leaves[1])

    def test_pg_blockdag_twolines(self):
        """
        A graph with two starts and two ends
        1 --> 2
        3 --> 4
        """
        pgr = _init_pgraph_twolines(self.rmode)
        leaves = build_blockdag(pgr, "pg", self.rmode)[0]
        self.assertTrue(leaves[0] == leaves[1])

    def test_data_fan(self):
        """
        Tests that a single data source scatters its signature to downstream data drops.
        """
        pgr = _init_pgraph_data_fan(self.rmode)
        build_blockdag(pgr, "pg", self.rmode)
        sourcehash = pgr[1]["reprodata"][self.rmode.name]["pg_blockhash"]
        parenthash1 = list(
            pgr[2]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        parenthash2 = list(
            pgr[3]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        self.assertTrue(
            parenthash1 == parenthash2 and parenthash1[0] == sourcehash
        )

    def test_data_funnel(self):
        """
        Tests that two data sources are collected in a single downstream data drop
        """
        pgr = _init_pgraph_data_funnel(self.rmode)
        build_blockdag(pgr, "pg", self.rmode)
        sourcehash = pgr[2]["reprodata"][self.rmode.name]["pg_blockhash"]
        parenthashes = list(
            pgr[3]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        self.assertTrue(
            sourcehash == parenthashes[0] and len(parenthashes) == 1
        )

    def test_data_sandwich(self):
        """
        Tests two data drops with an interim computing drop
        :return:
        """
        pgr = _init_pgraph_data_sandwich(self.rmode)
        build_blockdag(pgr, "pg", self.rmode)
        sourcehash = pgr[1]["reprodata"][self.rmode.name]["pg_blockhash"]
        parenthashes = list(
            pgr[2]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        self.assertTrue(
            sourcehash == parenthashes[0] and len(parenthashes) == 1
        )

    def test_computation_sandwich(self):
        """
        Tests that an internal data drop surrounded by computing drops is handled correctly.
        """
        pgr = _init_pgraph_computation_sandwich(self.rmode)
        build_blockdag(pgr, "pg", self.rmode)
        sourcehash = pgr[1]["reprodata"][self.rmode.name]["pg_blockhash"]
        parenthashes = list(
            pgr[2]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        self.assertTrue(
            sourcehash == parenthashes[0] and len(parenthashes) == 1
        )


class PhysicalBlockdagReproduceTests(unittest.TestCase):
    """
    Tests physical blockdag construction when reproducing.
    Rerunning should bring about contracting behaviour when data drops are introduced.
    """

    rmode = ReproducibilityFlags.REPRODUCE

    def test_pg_blockdag_single(self):
        """
        Tests a single drop
        """
        pgr = _init_pgraph_single(self.rmode)
        leaves = build_blockdag(pgr, "pg", self.rmode)[0]
        self.assertTrue(len(leaves) == 1)

    def test_pg_blockdag_twostart(self):
        """
        A graph with two starts
        1 -->
             3
        2 -->
        """
        pgr = _init_pgraph_twostart(self.rmode)
        leaves = build_blockdag(pgr, "pg", self.rmode)[0]
        parenthashes = list(
            pgr[1]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        self.assertTrue(len(leaves) == 1 and len(parenthashes) == 0)

    def test_pg_blockdag_twoend(self):
        """
        A graph with two ends
          --> 2
        1
          --> 3
        """
        pgr = _init_pgraph_twoend(self.rmode)
        leaves = build_blockdag(pgr, "pg", self.rmode)[0]
        self.assertTrue(leaves[0] == leaves[1])

    def test_pg_blockdag_twolines(self):
        """
        A graph with two starts and two ends
        1 --> 2
        3 --> 4
        """
        pgr = _init_pgraph_twolines(self.rmode)
        leaves = build_blockdag(pgr, "pg", self.rmode)[0]
        self.assertTrue(leaves[0] == leaves[1])

    def test_data_fan(self):
        """
        Tests that a single data source scatters its signature to downstream data drops.
        """
        pgr = _init_pgraph_data_fan(self.rmode)
        build_blockdag(pgr, "pg", self.rmode)
        sourcehash = pgr[0]["reprodata"][self.rmode.name]["pg_blockhash"]
        parenthash1 = list(
            pgr[2]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        parenthash2 = list(
            pgr[3]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        self.assertTrue(
            parenthash1 == parenthash2 and parenthash1[0] == sourcehash
        )

    def test_data_funnel(self):
        """
        Tests that two data sources are collected in a single downstream data drop
        """
        pgr = _init_pgraph_data_funnel(self.rmode)
        build_blockdag(pgr, "pg", self.rmode)
        sourcehash = pgr[0]["reprodata"][self.rmode.name]["pg_blockhash"]
        parenthashes = list(
            pgr[3]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        self.assertTrue(
            sourcehash == parenthashes[0] and len(parenthashes) == 2
        )

    def test_data_sandwich(self):
        """
        Tests two data drops with an interim computing drop
        :return:
        """
        pgr = _init_pgraph_data_sandwich(self.rmode)
        build_blockdag(pgr, "pg", self.rmode)
        sourcehash = pgr[0]["reprodata"][self.rmode.name]["pg_blockhash"]
        parenthashes = list(
            pgr[2]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        self.assertTrue(
            sourcehash == parenthashes[0] and len(parenthashes) == 1
        )

    def test_computation_sandwich(self):
        """
        Tests that an internal data drop surrounded by computing drops is handled correctly.
        """
        pgr = _init_pgraph_computation_sandwich(self.rmode)
        build_blockdag(pgr, "pg", self.rmode)
        parenthashes = list(
            pgr[2]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        # Not going to get anything out of this, since reproduce only cares about terminal data.
        self.assertEqual(0, len(parenthashes))


class PhysicalBlockdagReplicateScientificTests(unittest.TestCase):
    """
    Tests physical blockdag construction when replicating scientifically.
    This should be relatively straightforward.
    """

    rmode = ReproducibilityFlags.REPLICATE_SCI

    def test_pg_blockdag_single(self):
        """
        Tests a single drop
        """
        pgr = _init_pgraph_single(self.rmode)
        leaves = build_blockdag(pgr, "pg", self.rmode)[0]
        self.assertTrue(len(leaves) == 1)

    def test_pg_blockdag_twostart(self):
        """
        A graph with two starts
        1 -->
             3
        2 -->
        """
        pgr = _init_pgraph_twostart(self.rmode)
        leaves = build_blockdag(pgr, "pg", self.rmode)[0]
        parenthashes = list(
            pgr[1]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        self.assertTrue(
            len(leaves) == 1
            and len(parenthashes) == 2
            and parenthashes[0] == parenthashes[1]
        )

    def test_pg_blockdag_twoend(self):
        """
        A graph with two ends
          --> 2
        1
          --> 3
        """
        pgr = _init_pgraph_twoend(self.rmode)
        leaves = build_blockdag(pgr, "pg", self.rmode)[0]
        self.assertTrue(leaves[0] == leaves[1])

    def test_pg_blockdag_twolines(self):
        """
        A graph with two starts and two ends
        1 --> 2
        3 --> 4
        """
        pgr = _init_pgraph_twolines(self.rmode)
        leaves = build_blockdag(pgr, "pg", self.rmode)[0]
        self.assertTrue(leaves[0] == leaves[1])

    def test_data_fan(self):
        """
        Tests that a single data source scatters its signature to downstream data drops.
        """
        pgr = _init_pgraph_data_fan(self.rmode)
        build_blockdag(pgr, "pg", self.rmode)
        sourcehash = pgr[1]["reprodata"][self.rmode.name]["pg_blockhash"]
        parenthash1 = list(
            pgr[2]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        parenthash2 = list(
            pgr[3]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        self.assertTrue(
            parenthash1 == parenthash2 and parenthash1[0] == sourcehash
        )

    def test_data_funnel(self):
        """
        Tests that two data sources are collected in a single downstream data drop
        """
        pgr = _init_pgraph_data_funnel(self.rmode)
        build_blockdag(pgr, "pg", self.rmode)
        sourcehash = pgr[2]["reprodata"][self.rmode.name]["pg_blockhash"]
        parenthashes = list(
            pgr[3]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        self.assertTrue(
            sourcehash == parenthashes[0] and len(parenthashes) == 1
        )

    def test_data_sandwich(self):
        """
        Tests two data drops with an interim computing drop
        :return:
        """
        pgr = _init_pgraph_data_sandwich(self.rmode)
        build_blockdag(pgr, "pg", self.rmode)
        sourcehash = pgr[1]["reprodata"][self.rmode.name]["pg_blockhash"]
        parenthashes = list(
            pgr[2]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        self.assertTrue(
            sourcehash == parenthashes[0] and len(parenthashes) == 1
        )

    def test_computation_sandwich(self):
        """
        Tests that an internal data drop surrounded by computing drops is handled correctly.
        """
        pgr = _init_pgraph_computation_sandwich(self.rmode)
        build_blockdag(pgr, "pg", self.rmode)
        sourcehash = pgr[1]["reprodata"][self.rmode.name]["pg_blockhash"]
        parenthashes = list(
            pgr[2]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        self.assertTrue(
            sourcehash == parenthashes[0] and len(parenthashes) == 1
        )


class PhysicalBlockdagReplicateComputationTests(unittest.TestCase):
    """
    Tests physical blockdag construction when replicating computationally.
    This should be relatively straightforward.
    """

    rmode = ReproducibilityFlags.REPLICATE_COMP

    def test_pg_blockdag_single(self):
        """
        Tests a single drop
        """
        pgr = _init_pgraph_single(self.rmode)
        leaves = build_blockdag(pgr, "pg", self.rmode)[0]
        self.assertTrue(len(leaves) == 1)

    def test_pg_blockdag_twostart(self):
        """
        A graph with two starts
        1 -->
             3
        2 -->
        """
        pgr = _init_pgraph_twostart(self.rmode)
        leaves = build_blockdag(pgr, "pg", self.rmode)[0]
        parenthashes = list(
            pgr[1]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        self.assertTrue(
            len(leaves) == 1
            and len(parenthashes) == 2
            and parenthashes[0] == parenthashes[1]
        )

    def test_pg_blockdag_twoend(self):
        """
        A graph with two ends
          --> 2
        1
          --> 3
        """
        pgr = _init_pgraph_twoend(self.rmode)
        leaves = build_blockdag(pgr, "pg", self.rmode)[0]
        self.assertTrue(leaves[0] == leaves[1])

    def test_pg_blockdag_twolines(self):
        """
        A graph with two starts and two ends
        1 --> 2
        3 --> 4
        """
        pgr = _init_pgraph_twolines(self.rmode)
        leaves = build_blockdag(pgr, "pg", self.rmode)[0]
        self.assertTrue(leaves[0] == leaves[1])

    def test_data_fan(self):
        """
        Tests that a single data source scatters its signature to downstream data drops.
        """
        pgr = _init_pgraph_data_fan(self.rmode)
        build_blockdag(pgr, "pg", self.rmode)
        sourcehash = pgr[1]["reprodata"][self.rmode.name]["pg_blockhash"]
        parenthash1 = list(
            pgr[2]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        parenthash2 = list(
            pgr[3]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        self.assertTrue(
            parenthash1 == parenthash2 and parenthash1[0] == sourcehash
        )

    def test_data_funnel(self):
        """
        Tests that two data sources are collected in a single downstream data drop
        """
        pgr = _init_pgraph_data_funnel(self.rmode)
        build_blockdag(pgr, "pg", self.rmode)
        sourcehash = pgr[2]["reprodata"][self.rmode.name]["pg_blockhash"]
        parenthashes = list(
            pgr[3]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        self.assertTrue(
            sourcehash == parenthashes[0] and len(parenthashes) == 1
        )

    def test_data_sandwich(self):
        """
        Tests two data drops with an interim computing drop
        :return:
        """
        pgr = _init_pgraph_data_sandwich(self.rmode)
        build_blockdag(pgr, "pg", self.rmode)
        sourcehash = pgr[1]["reprodata"][self.rmode.name]["pg_blockhash"]
        parenthashes = list(
            pgr[2]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        self.assertTrue(
            sourcehash == parenthashes[0] and len(parenthashes) == 1
        )

    def test_computation_sandwich(self):
        """
        Tests that an internal data drop surrounded by computing drops is handled correctly.
        """
        pgr = _init_pgraph_computation_sandwich(self.rmode)
        build_blockdag(pgr, "pg", self.rmode)
        sourcehash = pgr[1]["reprodata"][self.rmode.name]["pg_blockhash"]
        parenthashes = list(
            pgr[2]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        self.assertTrue(
            sourcehash == parenthashes[0] and len(parenthashes) == 1
        )


class PhysicalBlockdagReplicateTotalTests(unittest.TestCase):
    """
    Tests physical blockdag construction when replicating totally.
    This should be relatively straightforward.
    """

    rmode = ReproducibilityFlags.REPLICATE_TOTAL

    def test_pg_blockdag_single(self):
        """
        Tests a single drop
        """
        pgr = _init_pgraph_single(self.rmode)
        leaves = build_blockdag(pgr, "pg", self.rmode)[0]
        self.assertTrue(len(leaves) == 1)

    def test_pg_blockdag_twostart(self):
        """
        A graph with two starts
        1 -->
             3
        2 -->
        """
        pgr = _init_pgraph_twostart(self.rmode)
        leaves = build_blockdag(pgr, "pg", self.rmode)[0]
        parenthashes = list(
            pgr[1]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        self.assertTrue(
            len(leaves) == 1
            and len(parenthashes) == 2
            and parenthashes[0] == parenthashes[1]
        )

    def test_pg_blockdag_twoend(self):
        """
        A graph with two ends
          --> 2
        1
          --> 3
        """
        pgr = _init_pgraph_twoend(self.rmode)
        leaves = build_blockdag(pgr, "pg", self.rmode)[0]
        self.assertTrue(leaves[0] == leaves[1])

    def test_pg_blockdag_twolines(self):
        """
        A graph with two starts and two ends
        1 --> 2
        3 --> 4
        """
        pgr = _init_pgraph_twolines(self.rmode)
        leaves = build_blockdag(pgr, "pg", self.rmode)[0]
        self.assertTrue(leaves[0] == leaves[1])

    def test_data_fan(self):
        """
        Tests that a single data source scatters its signature to downstream data drops.
        """
        pgr = _init_pgraph_data_fan(self.rmode)
        build_blockdag(pgr, "pg", self.rmode)
        sourcehash = pgr[1]["reprodata"][self.rmode.name]["pg_blockhash"]
        parenthash1 = list(
            pgr[2]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        parenthash2 = list(
            pgr[3]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        self.assertTrue(
            parenthash1 == parenthash2 and parenthash1[0] == sourcehash
        )

    def test_data_funnel(self):
        """
        Tests that two data sources are collected in a single downstream data drop
        """
        pgr = _init_pgraph_data_funnel(self.rmode)
        build_blockdag(pgr, "pg", self.rmode)
        sourcehash = pgr[2]["reprodata"][self.rmode.name]["pg_blockhash"]
        parenthashes = list(
            pgr[3]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        self.assertTrue(
            sourcehash == parenthashes[0] and len(parenthashes) == 1
        )

    def test_data_sandwich(self):
        """
        Tests two data drops with an interim computing drop
        :return:
        """
        pgr = _init_pgraph_data_sandwich(self.rmode)
        build_blockdag(pgr, "pg", self.rmode)
        sourcehash = pgr[1]["reprodata"][self.rmode.name]["pg_blockhash"]
        parenthashes = list(
            pgr[2]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        self.assertTrue(
            sourcehash == parenthashes[0] and len(parenthashes) == 1
        )

    def test_computation_sandwich(self):
        """
        Tests that an internal data drop surrounded by computing drops is handled correctly.
        """
        pgr = _init_pgraph_computation_sandwich(self.rmode)
        build_blockdag(pgr, "pg", self.rmode)
        sourcehash = pgr[1]["reprodata"][self.rmode.name]["pg_blockhash"]
        parenthashes = list(
            pgr[2]["reprodata"][self.rmode.name]["pg_parenthashes"].values()
        )
        self.assertTrue(
            sourcehash == parenthashes[0] and len(parenthashes) == 1
        )


class PhysicalBlockdagAllTests(unittest.TestCase):
    """
    Tests physical blockdag construction when test ALL mode.
    This should be relatively straightforward, but expanded group nodes scatter/gather etc. are
    special cases for rerunning specifically.
    """

    rmode = ReproducibilityFlags.ALL

    def test_pg_blockdag_single(self):
        """
        Tests a single drop
        """
        pgr = _init_pgraph_single(self.rmode)
        for rmode in ALL_RMODES:
            leaves = build_blockdag(pgr, "pg", rmode)[0]
            self.assertTrue(len(leaves) == 1)

    def test_pg_blockdag_twostart(self):
        """
        A graph with two starts
        1 -->
             3
        2 -->
        """
        pgr = _init_pgraph_twostart(self.rmode)
        for rmode in ALL_RMODES:
            leaves = build_blockdag(pgr, "pg", rmode)[0]
            parenthashes = list(
                pgr[1]["reprodata"][rmode.name]["pg_parenthashes"].values()
            )
            if rmode is not ReproducibilityFlags.REPRODUCE:
                self.assertTrue(
                    len(leaves) == 1
                    and len(parenthashes) == 2
                    and parenthashes[0] == parenthashes[1]
                )
            else:
                self.assertTrue(len(leaves) == 1 and len(parenthashes) == 0)

    def test_pg_blockdag_twoend(self):
        """
        A graph with two ends
          --> 2
        1
          --> 3
        """
        pgr = _init_pgraph_twoend(self.rmode)
        for rmode in ALL_RMODES:
            leaves = build_blockdag(pgr, "pg", rmode)[0]
            self.assertTrue(leaves[0] == leaves[1])

    def test_pg_blockdag_twolines(self):
        """
        A graph with two starts and two ends
        1 --> 2
        3 --> 4
        """
        pgr = _init_pgraph_twolines(self.rmode)
        for rmode in ALL_RMODES:
            leaves = build_blockdag(pgr, "pg", rmode)[0]
            self.assertTrue(leaves[0] == leaves[1])

    def test_data_fan(self):
        """
        Tests that a single data source scatters its signature to downstream data drops.
        """
        pgr = _init_pgraph_data_fan(self.rmode)
        for rmode in ALL_RMODES:
            build_blockdag(pgr, "pg", rmode)
            if rmode is ReproducibilityFlags.REPRODUCE:
                sourcehash = pgr[0]["reprodata"][rmode.name]["pg_blockhash"]
            else:
                sourcehash = pgr[1]["reprodata"][rmode.name]["pg_blockhash"]
            parenthash1 = list(
                pgr[2]["reprodata"][rmode.name]["pg_parenthashes"].values()
            )
            parenthash2 = list(
                pgr[3]["reprodata"][rmode.name]["pg_parenthashes"].values()
            )
            self.assertTrue(
                parenthash1 == parenthash2 and parenthash1[0] == sourcehash
            )

    def test_data_funnel(self):
        """
        Tests that two data sources are collected in a single downstream data drop
        """
        pgr = _init_pgraph_data_funnel(self.rmode)
        for rmode in ALL_RMODES:
            build_blockdag(pgr, "pg", rmode)
            if rmode is ReproducibilityFlags.REPRODUCE:
                sourcehash = pgr[0]["reprodata"][rmode.name]["pg_blockhash"]
                parenthashes = list(
                    pgr[3]["reprodata"][rmode.name]["pg_parenthashes"].values()
                )
                self.assertTrue(
                    sourcehash == parenthashes[0] and len(parenthashes) == 2
                )
            else:
                sourcehash = pgr[2]["reprodata"][rmode.name]["pg_blockhash"]
                parenthashes = list(
                    pgr[3]["reprodata"][rmode.name]["pg_parenthashes"].values()
                )
                self.assertTrue(
                    sourcehash == parenthashes[0] and len(parenthashes) == 1
                )

    def test_data_sandwich(self):
        """
        Tests two data drops with an interim computing drop
        :return:
        """
        pgr = _init_pgraph_data_sandwich(self.rmode)
        for rmode in ALL_RMODES:
            build_blockdag(pgr, "pg", rmode)
            if rmode is ReproducibilityFlags.REPRODUCE:
                sourcehash = pgr[0]["reprodata"][rmode.name]["pg_blockhash"]
            else:
                sourcehash = pgr[1]["reprodata"][rmode.name]["pg_blockhash"]
            parenthashes = list(
                pgr[2]["reprodata"][rmode.name]["pg_parenthashes"].values()
            )
            self.assertTrue(
                sourcehash == parenthashes[0] and len(parenthashes) == 1
            )

    def test_computation_sandwich(self):
        """
        Tests that an internal data drop surrounded by computing drops is handled correctly.
        """
        pgr = _init_pgraph_computation_sandwich(self.rmode)
        for rmode in ALL_RMODES:
            build_blockdag(pgr, "pg", rmode)
            sourcehash = pgr[1]["reprodata"][rmode.name]["pg_blockhash"]
            parenthashes = list(
                pgr[2]["reprodata"][rmode.name]["pg_parenthashes"].values()
            )
            if rmode != ReproducibilityFlags.REPRODUCE:
                self.assertTrue(
                    sourcehash == parenthashes[0] and len(parenthashes) == 1
                )
            else:
                self.assertTrue(len(parenthashes) == 0)
