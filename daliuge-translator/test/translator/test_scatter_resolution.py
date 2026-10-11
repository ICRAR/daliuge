#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2026
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

import unittest
from pathlib import Path

from dlg.translator.stages.unroll.constructs.registry import is_construct
from dlg.translator.stages.unroll.lg import LG
from dlg.translator.vocabulary import Categories


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
SCATTER_GRAPH = (
    REPOSITORY_ROOT
    / "test"
    / "corpus"
    / "graphs"
    / "eagle-test-graphs"
    / "SuperBasicScatterGather.graph"
)


class TestScatterEdgeResolution(unittest.TestCase):
    def test_scatter_expands_input_link_to_each_child_drop(self):
        lg = LG(
            str(SCATTER_GRAPH),
            ssid="scatter-edge-resolution",
            apply_config=False,
        )
        scatter = next(
            node
            for node in lg._done_dict.values()
            if is_construct(node, Categories.SCATTER)
        )
        child_file = next(
            node for node in scatter.children if node.category == "File"
        )
        input_app = child_file.inputs[0]

        drops = lg.unroll_to_tpl()

        scatter_drops = lg._drop_dict[scatter.id]
        child_drops = lg._drop_dict[child_file.id]
        input_drop = lg._drop_dict[input_app.id][0]
        linked_drop_oids = [
            next(iter(output)) for output in input_drop["outputs"]
        ]

        self.assertGreater(scatter.dop, 1)
        self.assertEqual([], scatter_drops)
        self.assertEqual(scatter.dop, len(child_drops))
        self.assertEqual([drop["oid"] for drop in child_drops], linked_drop_oids)
        self.assertIn(input_drop, drops)


if __name__ == "__main__":
    unittest.main()
