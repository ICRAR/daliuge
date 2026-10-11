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

"""Tests for LoopHandler leaf-to-leaf edge resolution."""

import unittest
from types import SimpleNamespace

from dlg.common import dropdict
from dlg.translator.errors import GraphException
from dlg.translator.stages.unroll.constructs.loop import LoopHandler
from dlg.translator.stages.unroll.model import LogicalLink
from dlg.translator.vocabulary import Categories


class _WiringContext:
    """Minimal wiring context for direct LoopHandler tests."""

    session_id = "loop-handler-test"

    def __init__(self, chunk_size=1):
        self._chunk_size = chunk_size

    @staticmethod
    def node(node_id):
        raise AssertionError(f"Unexpected node lookup: {node_id}")

    def chunk_size(self, source, target):
        del source, target
        return self._chunk_size

    @staticmethod
    def split(drops, size):
        for index in range(0, len(drops), size):
            yield drops[index : index + size]


def _loop_group(name, dop):
    return SimpleNamespace(
        id=name,
        name=name,
        is_group=True,
        category=Categories.LOOP,
        jd={"category": Categories.LOOP},
        dop=dop,
    )


def _leaf(
    name,
    *,
    group=None,
    h_level=0,
    group_start=False,
    group_end=False,
    related=True,
):
    return SimpleNamespace(
        id=name,
        name=name,
        is_group=False,
        category=Categories.PYTHON_APP,
        jd={"category": Categories.PYTHON_APP},
        group=group,
        gid=group.id if group is not None else 0,
        h_level=h_level,
        is_group_start=group_start,
        is_group_end=group_end,
        h_related=lambda other: related,
    )


class TestLoopHandlerResolveEdges(unittest.TestCase):
    """Cover the four Loop-specific leaf-to-leaf edge cases."""

    def test_relinks_loop_end_to_next_iteration_start(self):
        loop = _loop_group("loop", 3)

        source = _leaf(
            "end",
            group=loop,
            h_level=1,
            group_end=True,
        )
        target = _leaf(
            "start",
            group=loop,
            h_level=1,
            group_start=True,
        )

        sources = [
            dropdict({"oid": f"source-{index}"})
            for index in range(6)
        ]
        targets = [
            dropdict({"oid": f"target-{index}"})
            for index in range(6)
        ]

        edges = LoopHandler().resolve_edges(
            LogicalLink(source, target),
            sources,
            targets,
            _WiringContext(),
        )

        self.assertEqual(
            [
                (sources[0], targets[1]),
                (sources[1], targets[2]),
                (sources[3], targets[4]),
                (sources[4], targets[5]),
            ],
            [(edge.source, edge.target) for edge in edges],
        )

    def test_relink_rejects_mismatched_drop_counts(self):
        loop = _loop_group("loop", 3)

        source = _leaf(
            "end",
            group=loop,
            h_level=1,
            group_end=True,
        )
        target = _leaf(
            "start",
            group=loop,
            h_level=1,
            group_start=True,
        )

        with self.assertRaisesRegex(
            GraphException,
            "# of sdrops",
        ):
            LoopHandler().resolve_edges(
                LogicalLink(source, target),
                [dropdict({"oid": "source-0"})],
                [
                    dropdict({"oid": "target-0"}),
                    dropdict({"oid": "target-1"}),
                ],
                _WiringContext(),
            )

    def test_stepwise_locks_independent_loops_by_loop_context(self):
        source_loop = _loop_group("source-loop", 2)
        target_loop = _loop_group("target-loop", 2)

        source = _leaf(
            "source",
            group=source_loop,
            h_level=1,
            related=False,
        )
        target = _leaf(
            "target",
            group=target_loop,
            h_level=1,
            related=False,
        )

        sources = [
            dropdict({"oid": "source-0", "loop_ctx": "0"}),
            dropdict({"oid": "source-1", "loop_ctx": "1"}),
        ]
        targets = [
            dropdict({"oid": "target-0", "loop_ctx": "0"}),
            dropdict({"oid": "target-1", "loop_ctx": "1"}),
        ]

        edges = LoopHandler().resolve_edges(
            LogicalLink(source, target),
            sources,
            targets,
            _WiringContext(),
        )

        self.assertEqual(
            [
                (sources[0], targets[0]),
                (sources[1], targets[1]),
            ],
            [(edge.source, edge.target) for edge in edges],
        )

    def test_loop_aware_edge_leaving_loop_uses_last_iteration(self):
        loop = _loop_group("loop", 3)

        source = _leaf(
            "source",
            group=loop,
            h_level=1,
        )
        target = _leaf(
            "target",
            h_level=0,
        )

        sources = [
            dropdict({"oid": f"source-{index}"})
            for index in range(6)
        ]
        targets = [
            dropdict({"oid": "target-0"}),
            dropdict({"oid": "target-1"}),
        ]

        edges = LoopHandler().resolve_edges(
            LogicalLink(
                source,
                target,
                loop_aware=True,
            ),
            sources,
            targets,
            _WiringContext(chunk_size=3),
        )

        self.assertEqual(
            [
                (sources[2], targets[0]),
                (sources[5], targets[1]),
            ],
            [(edge.source, edge.target) for edge in edges],
        )

    def test_loop_aware_edge_entering_loop_uses_first_iteration(self):
        loop = _loop_group("loop", 3)

        source = _leaf(
            "source",
            h_level=0,
        )
        target = _leaf(
            "target",
            group=loop,
            h_level=1,
        )

        sources = [
            dropdict({"oid": "source-0"}),
            dropdict({"oid": "source-1"}),
        ]
        targets = [
            dropdict({"oid": f"target-{index}"})
            for index in range(6)
        ]

        edges = LoopHandler().resolve_edges(
            LogicalLink(
                source,
                target,
                loop_aware=True,
            ),
            sources,
            targets,
            _WiringContext(chunk_size=3),
        )

        self.assertEqual(
            [
                (sources[0], targets[0]),
                (sources[1], targets[3]),
            ],
            [(edge.source, edge.target) for edge in edges],
        )

    def test_plain_leaf_case_stays_outside_loop_handler(self):
        source = _leaf("source")
        target = _leaf("target")

        edges = LoopHandler().resolve_edges(
            LogicalLink(source, target),
            [dropdict({"oid": "source"})],
            [dropdict({"oid": "target"})],
            _WiringContext(),
        )

        self.assertEqual([], edges)


if __name__ == "__main__":
    unittest.main()
