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
Tests the correctness of our topological sorting code.
This module does not test the final blockDAG generation.
Assumptions:
- The graphs tests are valid (not malformed)
"""

import json
import unittest
import daliuge_tests.engine.topoGraphs as test_graphs
try:
    from importlib.resources import files, as_file
except ModuleNotFoundError:
    from importlib_resources import files

from dlg.common.reproducibility.constants import ReproducibilityFlags
from dlg.common.reproducibility.reproducibility import (
    init_lgt_repro_data,
    init_lg_repro_data,
    lg_build_blockdag,
    build_blockdag,
)

_dummydrop = {
    "oid": 1,
    "reprodata": {
        "rmode": "1",
        "RERUN": {
            "lg_blockhash": "123",
            "pgt_data": {"merkleroot": "456"},
            "pgt_parenthashes": {},
            "pgt_blockhash": "135",
            "pg_data": {"merkleroot": "bogus"},
            "pg_parenthashes": {},
            "pg_blockhash": "246",
            "rg_data": {"merkleroot": "bogus2"},
            "rg_parenthashes": {},
        }
    },
}


def _init_graph(filename):
    f = files(test_graphs) / f"{filename}"
    with f.open('r') as file:
        lgt = json.load(file)
    for drop in lgt["nodeDataArray"]:
        drop["reprodata"] = {}
        drop["reprodata"]["lg_parenthashes"] = []
        drop["reprodata"]["lgt_data"] = {"merkleroot": "1"}
        drop["reprodata"]["lg_data"] = {}
    return lgt


def _init_pgraph_single():
    return [_dummydrop.copy()]


def _init_pgraph_twostart():
    pgt = [_dummydrop.copy(), _dummydrop.copy(), _dummydrop.copy()]
    pgt[1]["oid"] = 2
    pgt[2]["oid"] = 3
    pgt[0]["outputs"] = [2]
    pgt[2]["outputs"] = [2]
    return pgt


def _init_pgraph_twoend():
    pgt = [_dummydrop.copy(), _dummydrop.copy(), _dummydrop.copy()]
    pgt[1]["oid"] = 2
    pgt[2]["oid"] = 3
    pgt[0]["outputs"] = [2, 3]
    return pgt


def _init_pgraph_twolines():
    pgt = [_dummydrop.copy(), _dummydrop.copy(), _dummydrop.copy(), _dummydrop.copy()]
    pgt[1]["oid"] = 2
    pgt[2]["oid"] = 3
    pgt[3]["oid"] = 4
    pgt[0]["outputs"] = [2]
    pgt[2]["outputs"] = [4]
    return pgt


class ToposortTests(unittest.TestCase):
    """
    Reads test graphs from /topoGraphs, appends some dummy reprodata then runs through the
    traversal code.
    The goal is to make sure these routines execute correct topological sorts.
    """

    def test_lg_blockdag_single(self):
        """
        Tests a single drop
        A
        """
        lgt = _init_graph("testSingle.graph")
        init_lgt_repro_data(lgt, "1")
        init_lg_repro_data(lgt)
        visited = lg_build_blockdag(lgt, ReproducibilityFlags.RERUN)[1]

        self.assertTrue(visited == ["93bdccc6-c93e-49e9-8451-3f5aee6c3671"])

    def test_lg_blockdag_twostart(self):
        """
        A graph with two starts
        A -->
             C
        B -->
        """
        lgt = _init_graph("testTwoStart.graph")
        init_lgt_repro_data(lgt, "1")
        init_lg_repro_data(lgt)
        visited = lg_build_blockdag(lgt, ReproducibilityFlags.RERUN)[1]
        self.assertTrue(visited == [
            "4f57933d-a27c-4719-adf8-d1867c1800a4",
            "08c86565-cd63-4967-95ba-315f497c8835",
            "249dd6db-7fa4-478f-a4c9-2616446580f0"
        ])

    def test_lg_blockdag_twoend(self):
        """
        A graph with two ends
          --> B
        A
          --> C
        """
        lgt = _init_graph("testTwoEnd.graph")
        init_lgt_repro_data(lgt, "1")
        init_lg_repro_data(lgt)
        visited = lg_build_blockdag(lgt, ReproducibilityFlags.RERUN)[1]
        self.assertTrue(visited == [
            "ee1db22f-4977-4e84-9965-5cbbdb68c5e6",
            "23463b9a-57a7-4d96-a52f-e012c3fe0749",
            "542a2a53-628f-4f45-8ba0-76121ad42c64"])

    def test_lg_blockdag_twolines(self):
        """
        A graph with two starts and two ends
        A --> B
        C --> D
        """
        lgt = _init_graph("testTwoLines.graph")
        init_lgt_repro_data(lgt, "1")
        init_lg_repro_data(lgt)
        visited = lg_build_blockdag(lgt, ReproducibilityFlags.RERUN)[1]
        self.assertTrue(visited == [
            "2a33cae9-29cc-4133-be6d-f028584bf159",
            "82ac735a-ae5e-4eb3-b9b6-414b313214ef",
            "8a50de9c-762b-495c-be68-a61281ae1a3a",
            "aa607a29-0803-4319-9917-0516c4bbaa79"])

    def test_lg_blockdag_empty(self):
        """
        Tests an empty graph. Should fail gracefully.
        """
        lgt = _init_graph("testEmpty.graph")
        init_lgt_repro_data(lgt, "1")
        init_lg_repro_data(lgt)
        visited = lg_build_blockdag(lgt, ReproducibilityFlags.RERUN)[1]
        self.assertTrue(visited == [])

    def test_pgt_blockdag_single(self):
        """
        Tests a single drop
        1
        """
        pgt = _init_pgraph_single()
        visited = build_blockdag(pgt, "pgt", ReproducibilityFlags.RERUN)[1]
        self.assertTrue(visited == [1])

    def test_pgt_blockdag_twostart(self):
        """
        A graph with two starts
        1 -->
             3
        2 -->
        """
        pgt = _init_pgraph_twostart()
        visited = build_blockdag(pgt, "pgt", ReproducibilityFlags.RERUN)[1]
        self.assertTrue(visited == [3, 1, 2])

    def test_pgt_blockdag_twoend(self):
        """
        A graph with two ends
          --> 2
        1
          --> 3
        """
        pgt = _init_pgraph_twoend()
        visited = build_blockdag(pgt, "pgt", ReproducibilityFlags.RERUN)[1]
        self.assertTrue(visited == [1, 3, 2])

    def test_pgt_blockdag_twolines(self):
        """
        A graph with two starts and two ends
        1 --> 2
        3 --> 4
        """
        pgt = _init_pgraph_twolines()
        visited = build_blockdag(pgt, "pgt", ReproducibilityFlags.RERUN)[1]
        self.assertTrue(visited == [3, 4, 1, 2])

    def test_pgt_blockdag_empty(self):
        """
        Tests an empty graph. Should fail gracefully.
        """
        pgt = []
        visited = build_blockdag(pgt, "pgt", ReproducibilityFlags.RERUN)[1]
        self.assertTrue(visited == [])

    def test_pg_blockdag_single(self):
        """
        Tests a single drop
        """
        pgr = _init_pgraph_single()
        visited = build_blockdag(pgr, "pg", ReproducibilityFlags.RERUN)[1]
        self.assertTrue(visited == [1])

    def test_pg_blockdag_twostart(self):
        """
        A graph with two starts
        1 -->
             3
        2 -->
        """
        pgr = _init_pgraph_twostart()
        visited = build_blockdag(pgr, "pg", ReproducibilityFlags.RERUN)[1]
        self.assertTrue(visited == [3, 1, 2])

    def test_pg_blockdag_twoend(self):
        """
        A graph with two ends
          --> 2
        1
          --> 3
        """
        pgr = _init_pgraph_twoend()
        visited = build_blockdag(pgr, "pg", ReproducibilityFlags.RERUN)[1]
        self.assertTrue(visited == [1, 3, 2])

    def test_pg_blockdag_twolines(self):
        """
        A graph with two starts and two ends
        1 --> 2
        3 --> 4
        """
        pgr = _init_pgraph_twolines()
        visited = build_blockdag(pgr, "pgt", ReproducibilityFlags.RERUN)[1]
        self.assertTrue(visited == [3, 4, 1, 2])

    def test_pg_blockdag_empty(self):
        """
        Tests an empty graph. Should fail gracefully.
        """
        pgr = []
        visited = build_blockdag(pgr, "pg", ReproducibilityFlags.RERUN)[1]
        self.assertTrue(visited == [])

    def test_rg_blockdag_single(self):
        """
        Tests a single drop
        """
        rgr = _init_pgraph_single()
        visited = build_blockdag(rgr, "rg", ReproducibilityFlags.RERUN)[1]
        self.assertTrue(visited == [1])

    def test_rg_blockdag_twostart(self):
        """
        A graph with two starts
        1 -->
             3
        2 -->
        """
        rgr = _init_pgraph_twostart()
        visited = build_blockdag(rgr, "rg", ReproducibilityFlags.RERUN)[1]
        self.assertTrue(visited == [3, 1, 2])

    def test_rg_blockdag_twoend(self):
        """
        A graph with two ends
          --> 2
        1
          --> 3
        """
        rgr = _init_pgraph_twoend()
        visited = build_blockdag(rgr, "rg", ReproducibilityFlags.RERUN)[1]
        self.assertTrue(visited == [1, 3, 2])

    def test_rg_blockdag_twolines(self):
        """
        A graph with two starts and two ends
        1 --> 2
        3 --> 4
        """
        rgr = _init_pgraph_twolines()
        visited = build_blockdag(rgr, "rg", ReproducibilityFlags.RERUN)[1]
        self.assertTrue(visited == [3, 4, 1, 2])

    def test_rg_blockdag_empty(self):
        """
        Tests an empty graph. Should fail gracefully.
        """
        rgr = []
        visited = build_blockdag(rgr, "rg", ReproducibilityFlags.RERUN)[1]
        self.assertTrue(visited == [])
