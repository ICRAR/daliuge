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
from types import SimpleNamespace
from unittest.mock import Mock, patch

from dlg.common import CategoryType
import dlg.translator.stages.unroll.wire as wire_module
from dlg.translator.vocabulary import Categories


class TestSubgraphHandlerWiring(unittest.TestCase):

    @staticmethod
    def _node(node_id, category, is_group):
        node = SimpleNamespace(
            id=node_id,
            name=node_id,
            category=category,
            is_group=is_group,
            jd={
                "category": category,
                "categoryType": (
                    CategoryType.APPLICATION
                    if is_group
                    else CategoryType.DATA
                ),
            },
        )
        node.dop_diff = Mock(return_value=1)
        return node

    @staticmethod
    def _graph(source, target):
        return SimpleNamespace(
            _session_id="session-",
            _done_dict={
                source.id: source,
                target.id: target,
            },
            _drop_dict={
                source.id: [{"oid": "source-drop"}],
                target.id: [{"oid": "target-drop"}],
                "new_added": [],
            },
            _lg_links=[
                {
                    "from": source.id,
                    "to": target.id,
                }
            ],
            _loop_aware_set=set(),
        )

    def test_subgraph_source_routes_through_handler(self):
        source = self._node(
            "subgraph",
            Categories.SUBGRAPH,
            True,
        )
        target = self._node(
            "target",
            Categories.FILE,
            False,
        )
        graph = self._graph(source, target)

        with patch.object(
            wire_module,
            "_resolve_subgraph_edges",
        ) as resolve_subgraph:
            wire_module.wire(graph)

        resolve_subgraph.assert_called_once()
        args = resolve_subgraph.call_args.args

        self.assertIs(source, args[2])
        self.assertIs(target, args[3])

    def test_subgraph_target_routes_through_handler(self):
        source = self._node(
            "source",
            Categories.FILE,
            False,
        )
        target = self._node(
            "subgraph",
            Categories.SUBGRAPH,
            True,
        )
        graph = self._graph(source, target)

        with patch.object(
            wire_module,
            "_resolve_subgraph_edges",
        ) as resolve_subgraph:
            wire_module.wire(graph)

        resolve_subgraph.assert_called_once()
        args = resolve_subgraph.call_args.args

        self.assertIs(source, args[2])
        self.assertIs(target, args[3])


if __name__ == "__main__":
    unittest.main()
