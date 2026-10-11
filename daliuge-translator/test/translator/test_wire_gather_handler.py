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

import dlg.translator.stages.unroll.wire as wire_module
from dlg.translator.vocabulary import Categories


class TestGatherHandlerWiring(unittest.TestCase):

    @staticmethod
    def _node(
        node_id,
        category,
        *,
        is_group,
        h_level=0,
        gid=None,
        group=None,
        gather_width=1,
    ):
        node = SimpleNamespace(
            id=node_id,
            name=node_id,
            category=category,
            is_group=is_group,
            h_level=h_level,
            gid=gid if gid is not None else node_id,
            group=group,
            gather_width=gather_width,
            jd={
                "category": category,
                "categoryType": "Data",
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

    def test_group_to_gather_routes_through_gather_handler(self):
        source = self._node(
            "groupby",
            Categories.GROUP_BY,
            is_group=True,
            h_level=1,
        )
        target = self._node(
            "gather",
            Categories.GATHER,
            is_group=True,
            h_level=0,
            gather_width=1,
        )

        graph = self._graph(source, target)

        with patch.object(
            wire_module,
            "_resolve_gather_edges",
        ) as resolve_gather:
            wire_module.wire(graph)

        resolve_gather.assert_called_once()

        args, _ = resolve_gather.call_args
        self.assertIs(source, args[2])
        self.assertIs(target, args[3])

    def test_leaf_to_gather_routes_through_gather_handler(self):
        source = self._node(
            "source",
            Categories.FILE,
            is_group=False,
            h_level=1,
        )
        target = self._node(
            "gather",
            Categories.GATHER,
            is_group=True,
            h_level=0,
            gather_width=1,
        )

        graph = self._graph(source, target)

        with patch.object(
            wire_module,
            "_resolve_gather_edges",
        ) as resolve_gather:
            wire_module.wire(graph)

        resolve_gather.assert_called_once()

        args, _ = resolve_gather.call_args
        self.assertIs(source, args[2])
        self.assertIs(target, args[3])

    def test_gather_sequentialisation_stays_on_legacy_cache_path(self):
        source = self._node(
            "gather",
            Categories.GATHER,
            is_group=True,
            gather_width=2,
        )
        target = self._node(
            "target",
            Categories.FILE,
            is_group=False,
            gid="other-group",
        )

        graph = self._graph(
            source,
            target,
            source_drops=[
                {"oid": "gather-0"},
                {"oid": "gather-1"},
            ],
            target_drops=[
                {"oid": "target-0"},
                {"oid": "target-1"},
            ],
        )

        with patch.object(
            wire_module,
            "_resolve_gather_edges",
        ) as resolve_gather:
            wire_module.wire(graph)

        resolve_gather.assert_not_called()


if __name__ == "__main__":
    unittest.main()
