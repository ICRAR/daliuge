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
This module ensures that one can submit a test-graph with the NOTHING flag and have nothing
happen.
"""
import json
import optparse
import tempfile
import unittest

from dlg.common.reproducibility.constants import ReproducibilityFlags
from dlg.translator.tool_commands import dlg_fill, dlg_unroll, dlg_partition, dlg_map


def _run_full_workflow(rmode: ReproducibilityFlags, workflow: str, workflow_loc='./',
                       scratch_loc='./'):
    lgt = workflow_loc + workflow + ".graph"
    lgr = scratch_loc + workflow + "LG.graph"
    pgs = scratch_loc + workflow + "PGS.graph"
    pgt = scratch_loc + workflow + "PGT.graph"
    pgr = scratch_loc + workflow + "PG.graph"

    rmodes = str(rmode.value)

    parser = optparse.OptionParser()
    dlg_fill(parser, ['-L', lgt, '-R', rmodes, '-o', lgr, '-f', 'newline'])
    parser = optparse.OptionParser()
    dlg_unroll(parser, ['-L', lgr, '-o', pgs, '-f', 'newline'])
    parser = optparse.OptionParser()
    dlg_partition(parser, ['-P', pgs, '-o', pgt, '-f', 'newline'])
    parser = optparse.OptionParser()
    dlg_map(parser, ['-P', pgt, '-N', '127.0.0.1, 127.0.0.1', '-o', pgr, '-f', 'newline'])


def _read_graph(filename):
    file = open(filename)
    graph = json.load(file)
    file.close()
    return graph


class IntegrationNothingTest(unittest.TestCase):
    """
    Tests a simple graph up until submission for execution with the goal of asserting
    that running a graph with the NOTHING standard produces no reproducibility information.
    """
    temp_out = tempfile.TemporaryDirectory('out')

    def _cleanup(self):
        self.temp_out.cleanup()

    def test_computation_sandwich(self):
        """
        Opens a simple computationSandwich graph in a temporary directory
        No data should be present at any level of abstraction, reflected by a null merkleroot.
        """
        graph_name = 'computationSandwich'
        graph_loc = 'topoGraphs/'
        _run_full_workflow(rmode=ReproducibilityFlags.NOTHING, workflow=graph_name,
                           workflow_loc=graph_loc, scratch_loc=self.temp_out.name)
        pgr = self.temp_out.name + graph_name + "PG.graph"

        graph = _read_graph(pgr)
        graph = graph[0:-1]

        for drop in graph:
            self.assertIsNone(drop['reprodata']['lgt_data']['merkleroot'])
            self.assertIsNone(drop['reprodata']['lg_data']['merkleroot'])
            self.assertIsNone(drop['reprodata']['pgt_data']['merkleroot'])
            self.assertIsNone(drop['reprodata']['pg_data']['merkleroot'])
        self._cleanup()
