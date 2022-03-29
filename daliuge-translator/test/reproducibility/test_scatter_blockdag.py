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
Tests how the logical and physical graphs handle a simple scatter; the behaviour of control nodes
are quite different to standard graphs.
"""

import json
import optparse
import tempfile
import unittest

from dlg.common.reproducibility.constants import ReproducibilityFlags
from dlg.common.reproducibility.reproducibility import (
    init_lgt_repro_data,
    init_lg_repro_data,
    lg_build_blockdag,
)
from dlg.translator.tool_commands import dlg_fill, dlg_partition, dlg_map, dlg_unroll


def _run_full_workflow(
    rmode: ReproducibilityFlags, workflow: str, workflow_loc="./", scratch_loc="./"
):
    lgt = workflow_loc + "/" + workflow + ".graph"
    lgr = scratch_loc + "/" + workflow + "LG.graph"
    pgs = scratch_loc + "/" + workflow + "PGS.graph"
    pgt = scratch_loc + "/" + workflow + "PGT.graph"
    pgr = scratch_loc + "/" + workflow + "PG.graph"

    rmodes = str(rmode.value)

    parser = optparse.OptionParser()
    dlg_fill(parser, ["-L", lgt, "-R", rmodes, "-o", lgr, "-f", "newline"])
    parser = optparse.OptionParser()
    dlg_unroll(parser, ["-L", lgr, "-o", pgs, "-f", "newline"])
    parser = optparse.OptionParser()
    dlg_partition(parser, ["-P", pgs, "-o", pgt, "-f", "newline"])
    parser = optparse.OptionParser()
    dlg_map(
        parser, ["-P", pgt, "-N", "127.0.0.1, 127.0.0.1", "-o", pgr, "-f", "newline"]
    )


def _read_graph(filename):
    file = open(filename)
    graph = json.load(file)
    file.close()
    return graph


def _init_graph(filename):
    file = open(filename)
    lgt = json.load(file)
    file.close()
    for drop in lgt["nodeDataArray"]:
        drop["reprodata"] = {}
        drop["reprodata"]["lg_parenthashes"] = []
        drop["reprodata"]["lgt_data"] = {"merkleroot": "1"}
        drop["reprodata"]["lg_data"] = {}
    return lgt


class ScatterTest(unittest.TestCase):
    """
    Tests a very simple scattered and gathered graph full of dummy files and bash scripts.
    See test/reproducibility/topoGraphs/simpleScatter.graph
    """

    temp_out = tempfile.TemporaryDirectory("out")

    def test_lg_scatter_rerun(self):
        """
        Tests how rerunning treats such a graph.
        Expected behaviour should be the same as any other type of graph - they are all logical
        components
        """
        lgt = _init_graph("test/reproducibility/reproGraphs/simpleScatter.graph")
        init_lgt_repro_data(lgt, rmode=ReproducibilityFlags.RERUN.value)
        init_lg_repro_data(lgt)
        visited = lg_build_blockdag(lgt)[1]
        scatter_drop = lgt["nodeDataArray"][1]
        app_drop = lgt["nodeDataArray"][2]
        scatter_inter_drop = lgt["nodeDataArray"][3]
        # Checks that the input app drop is the parent of the main application
        self.assertEqual(
            list(app_drop["reprodata"]["lg_parenthashes"].values())[0],
            scatter_inter_drop["reprodata"]["lg_blockhash"],
        )
        # Checks that the scatter drop is the parent of the input drop
        self.assertEqual(
            list(scatter_inter_drop["reprodata"]["lg_parenthashes"].values())[0],
            scatter_drop["reprodata"]["lg_blockhash"],
        )
        self.assertEqual(visited, [-1, -2, -5, -3, -6, -7, -9])

    def test_pg_scatter_rerun(self):
        """
        Tests how rerunning treats such a graph.
        Expected behaviour is as if there was no scattering or gathering - only the 'critical'
        path contributes to the hash value
        """
        scatter = "simpleScatter"
        noscatter = "simpleNoScatter"
        graph_loc = "test/reproducibility/reproGraphs/"
        _run_full_workflow(
            rmode=ReproducibilityFlags.RERUN,
            workflow=scatter,
            workflow_loc=graph_loc,
            scratch_loc=self.temp_out.name,
        )
        _run_full_workflow(
            rmode=ReproducibilityFlags.RERUN,
            workflow=noscatter,
            workflow_loc=graph_loc,
            scratch_loc=self.temp_out.name,
        )
        pgr_scatter = self.temp_out.name + "/" + scatter + "PG.graph"
        pgr_noscatter = self.temp_out.name + "/" + noscatter + "PG.graph"

        scatter_graph = _read_graph(pgr_scatter)
        scatter_graph = scatter_graph[0:-1]
        no_scatter_graph = _read_graph(pgr_noscatter)
        no_scatter_graph = no_scatter_graph[0:-1]
        # Correct number of drops unrolled
        self.assertEqual(len(scatter_graph), 10)
        # Correct number of drops unscattered
        self.assertEqual(len(no_scatter_graph), 7)
        # Their signatures should in principal be identicle
        self.assertEqual(
            scatter_graph[-1]["reprodata"]["pg_blockhash"],
            no_scatter_graph[-1]["reprodata"]["pg_blockhash"],
        )
