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


class TestLeafHandlerWiring(unittest.TestCase):

    @staticmethod
    def _leaf_node(
        node_id,
        h_level=0,
        *,
        is_start_node=False,
        group=None,
        gid=None,
    ):
        node = SimpleNamespace(
            id=node_id,
            name=node_id,
            category=Categories.FILE,
            is_group=False,
            is_start_node=is_start_node,
            is_group_end=False,
            is_group_start=False,
            group=group,
            gid=gid if gid is not None else node_id,
            h_level=h_level,
            jd={
                "category": Categories.FILE,
                "categoryType": CategoryType.DATA,
            },
        )
        node.dop_diff = Mock(return_value=1)
        node.h_related = Mock(return_value=True)
        return node

    @staticmethod
    def _graph(source, target, source_drops=None, target_drops=None):
        if source_drops is None:
            source_drops = [{"oid": "source-drop"}]
        if target_drops is None:
            target_drops = [{"oid": "target-drop"}]

        return SimpleNamespace(
            _session_id="session-",
            _done_dict={
                source.id: source,
                target.id: target,
            },
            _drop_dict={
                source.id: source_drops,
                target.id: target_drops,
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

    def test_plain_leaf_fallback_routes_through_leaf_handler(self):
        source = self._leaf_node("source", h_level=1)
        target = self._leaf_node("target", h_level=0)
        graph = self._graph(source, target)

        with patch.object(
            wire_module,
            "_resolve_leaf_edges",
        ) as resolve_leaf:
            wire_module.wire(graph)

        resolve_leaf.assert_called_once()
        args, kwargs = resolve_leaf.call_args

        self.assertIs(source, args[2])
        self.assertIs(target, args[3])
        self.assertFalse(kwargs["loop_aware"])

    def test_start_node_skip_routes_through_leaf_handler(self):
        source = self._leaf_node(
            "source",
            is_start_node=True,
        )
        target = self._leaf_node("target")
        graph = self._graph(source, target)

        with patch.object(
            wire_module,
            "_resolve_leaf_edges",
        ) as resolve_leaf:
            wire_module.wire(graph)

        resolve_leaf.assert_called_once()
        self.assertFalse(
            resolve_leaf.call_args.kwargs["loop_aware"]
        )

    def test_loop_iteration_relink_routes_through_loop_handler(self):
        loop_group = SimpleNamespace(
            id="loop",
            name="loop",
            is_group=True,
            category=Categories.LOOP,
            jd={"category": Categories.LOOP},
            dop=2,
        )

        source = self._leaf_node(
            "source",
            group=loop_group,
            gid="loop",
        )
        source.is_group_end = True

        target = self._leaf_node(
            "target",
            group=loop_group,
            gid="loop",
        )
        target.is_group_start = True

        source_drops = [
            {"oid": "source-0"},
            {"oid": "source-1"},
        ]
        target_drops = [
            {"oid": "target-0"},
            {"oid": "target-1"},
        ]

        graph = self._graph(
            source,
            target,
            source_drops,
            target_drops,
        )

        with patch.object(
            wire_module,
            "get_handler",
            wraps=wire_module.get_handler,
        ) as get_handler, patch.object(
            wire_module,
            "_resolve_leaf_edges",
        ) as resolve_leaf, patch.object(
            wire_module,
            "_link_or_defer",
        ) as physical_link:
            wire_module.wire(graph)

        get_handler.assert_called_once_with(Categories.LOOP)
        resolve_leaf.assert_not_called()
        physical_link.assert_called_once()


if __name__ == "__main__":
    unittest.main()
