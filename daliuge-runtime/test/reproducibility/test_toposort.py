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
Tests the correctness of our topological sorting code. This module does not test the final blockDAG generation.
Assumptions:
- The graphs tests are valid (not malformed)
"""

import json
import unittest

from dlg.common.reproducibility.reproducibility import lg_build_blockdag


class ToposortTests(unittest.TestCase):

    def init_graph(self, filename):
        fp = open(filename)
        lgt = json.load(fp)
        fp.close()
        for drop in lgt['nodeDataArray']:
            drop['reprodata'] = {}
            drop['reprodata']['lg_parenthashes'] = []
            drop['reprodata']['lgt_data'] = {'merkleroot': "1"}
            drop['reprodata']['lg_data'] = {}
        return lgt

    def test_lg_blockdag_single(self):
        """
        Tests a single drop
        A
        """
        lgt = self.init_graph("topoGraphs/testSingle.graph")
        visited = lg_build_blockdag(lgt)
        assert visited == [-1]

    def test_lg_blockdag_twostart(self):
        """
        A graph with two starts
        A -->
             C
        B -->
        """
        lgt = self.init_graph("topoGraphs/testTwoStart.graph")
        visited = lg_build_blockdag(lgt)
        assert visited == [-3, -1, -2]

    def test_lg_blockdag_twoend(self):
        """
        A graph with two ends
          --> B
        A
          --> C
        """
        assert False

    def test_lg_blockdag_twolines(self):
        """
        A graph with two starts and two ends
        A --> B
        C --> D
        """
        assert False

    def test_lg_blockdag_notDAG(self):
        """
        A graph that is not a dag. This should fail.
        A --> B --> C --> B
        """
        assert False

    def test_lg_blockdag_cycle(self):
        """
        A graph that is one cycle. This should fail
        A --> B --> C --> A
        """

    def test_lg_blockdag_empty(self):
        """
        Tests an empty graph. Should fail gracefully.
        """
        assert False