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
import json
import subprocess
import unittest

import daliuge_tests.dropmake as test_graphs

from importlib.resources import files
from dlg import common
from dlg.common import tool



class TestTool(unittest.TestCase):
    def test_pipeline(self):
        """A pipeline from an LG all the way to a finished graph execution"""
        lg = str(files(test_graphs) / "logical_graphs/ArrayLoop.graph")

        # first fill the LGT parameters
        fill = tool.start_process(
            "fill",
            [
                "-vv",
                "-L",
                lg,
                "-p",
                "param1=hello",
                "-p",
                "param2=1",
                "-p",
                "param1.param2=hi",
                "-p",
                "param4.what=False",
            ],
            stdout=subprocess.PIPE,
        )

        # unroll the resulting LG
        unroll = tool.start_process(
            "unroll",
            ["-z", "--app", "1", "-vv"],
            stdin=fill.stdout,
            stdout=subprocess.PIPE,
        )

        # partition the PGT
        partition = tool.start_process(
            "partition", ["-vv"], stdin=unroll.stdout, stdout=subprocess.PIPE
        )

        # map PGT to resources to construct PG
        map_ = tool.start_process(
            "map",
            ["-vv", "-N", "localhost,localhost"],
            stdin=partition.stdout,
            stdout=subprocess.PIPE,
        )
        mapped_graph, _ = map_.communicate()

        for proc in fill, unroll, partition, map_:
            self.assertEqual(proc.wait(), 0)

        # It's valid JSON content, and actually a physical graph
        mapped_graph = json.loads(common.b2s(mapped_graph))
        # TODO: REPRODATA ATTACHED
        mapped_graph.pop()  # Get rid of reprodata
        self.assertTrue(list(common.get_roots(mapped_graph)))
        self.assertTrue(list(common.get_leaves(mapped_graph)))
