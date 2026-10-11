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
from collections import defaultdict
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock, patch

from dlg.common import CategoryType, dropdict
from dlg.translator.errors import GInvalidLink, GInvalidNode, GraphException
from dlg.translator.stages.unroll.constructs.branch import BranchHandler
from dlg.translator.stages.unroll.constructs.gather import GatherHandler
from dlg.translator.stages.unroll.constructs.groupby import GroupByHandler
from dlg.translator.stages.unroll.constructs.leaf import LeafHandler
from dlg.translator.stages.unroll.constructs.loop import LoopHandler
from dlg.translator.stages.unroll.constructs.mpi import MPIHandler
from dlg.translator.stages.unroll.constructs.registry import (
    get_handler_for_node,
    is_construct,
    register_handler,
)
from dlg.translator.stages.unroll.constructs.scatter import ScatterHandler
from dlg.translator.stages.unroll.constructs.service import ServiceHandler
from dlg.translator.stages.unroll.constructs.subgraph import SubgraphHandler
from dlg.translator.stages.unroll.coordinate import InstanceId
from dlg.translator.stages.unroll.instantiate import lgn_to_pgn
from dlg.translator.stages.unroll.lg_node import LGNode
from dlg.translator.stages.unroll.model import LogicalLink
from dlg.translator.vocabulary import Categories


class TestMPIHandlerInstantiate(unittest.TestCase):
    """Test MPI DROP instantiation through the handler."""

    def test_creates_one_drop_per_process_with_rank_and_loop_context(self):
        node = SimpleNamespace(
            dop=3,
            make_single_drop=Mock(),
        )
        node.make_single_drop.side_effect = (
            lambda coord, **kwargs: dropdict(
                {
                    "iid": str(coord),
                    **kwargs,
                }
            )
        )

        context = SimpleNamespace(
            loop_context="loop-1",
        )

        drops = MPIHandler().instantiate(
            node,
            InstanceId((0, 2)),
            context,
        )

        self.assertEqual(
            [
                {
                    "iid": "0-2-0",
                    "loop_ctx": "loop-1",
                    "proc_index": 0,
                },
                {
                    "iid": "0-2-1",
                    "loop_ctx": "loop-1",
                    "proc_index": 1,
                },
                {
                    "iid": "0-2-2",
                    "loop_ctx": "loop-1",
                    "proc_index": 2,
                },
            ],
            [
                {
                    key: drop[key]
                    for key in (
                        "iid",
                        "loop_ctx",
                        "proc_index",
                    )
                }
                for drop in drops
            ],
        )

    def test_instantiator_routes_mpi_nodes_through_handler(self):
        node = SimpleNamespace(
            id="mpi",
            category=Categories.MPI,
            is_group=False,
            dop=2,
            make_single_drop=Mock(
                side_effect=lambda coord, **kwargs: dropdict(
                    {
                        "iid": str(coord),
                        **kwargs,
                    }
                )
            ),
        )

        graph = SimpleNamespace(
            _session_id="test",
            _done_dict={
                node.id: node,
            },
            _drop_dict=defaultdict(list),
        )

        lgn_to_pgn(
            graph,
            node,
            InstanceId((0, 4)),
            "loop-2",
        )

        self.assertEqual(
            [
                {
                    "iid": "0-4-0",
                    "loop_ctx": "loop-2",
                    "proc_index": 0,
                },
                {
                    "iid": "0-4-1",
                    "loop_ctx": "loop-2",
                    "proc_index": 1,
                },
            ],
            [
                {
                    key: drop[key]
                    for key in (
                        "iid",
                        "loop_ctx",
                        "proc_index",
                    )
                }
                for drop in graph._drop_dict[node.id]
            ],
        )


class TestGroupByHandlerResolveEdges(unittest.TestCase):
    """Test GroupBy edge pairing for IID-derived keys and error cases."""

    class UnusedWiringContext:
        session_id = "test"

        @staticmethod
        def node(node_id):
            raise AssertionError(f"Unexpected node lookup: {node_id}")

        @staticmethod
        def chunk_size(source, target):
            del source, target
            raise AssertionError(
                "GroupBy edge resolution should not chunk drops"
            )

        @staticmethod
        def split(drops, size):
            del drops, size
            raise AssertionError(
                "GroupBy edge resolution should not split drops"
            )

    @staticmethod
    def _node(category, **attributes):
        node = SimpleNamespace(
            id=attributes.pop("id", category),
            name=attributes.pop("name", category),
            category=category,
            is_group=category
            in (
                Categories.GROUP_BY,
                Categories.SCATTER,
                Categories.LOOP,
            ),
            jd={"category": category},
            group=None,
            h_level=0,
        )

        for name, value in attributes.items():
            setattr(node, name, value)

        return cast(LGNode, node)

    @staticmethod
    def _drop(coord):
        source_drop = dropdict({"iid": str(coord)})
        source_drop.coord = coord
        return source_drop

    def _resolve(
        self,
        source_coords,
        target_count,
        **target_attributes,
    ):
        source_group = target_attributes.pop("source_group", None)

        source = self._node(
            Categories.PYTHON_APP,
            h_level=target_attributes.pop("source_h_level", 0),
        )
        source.group = source_group

        target = self._node(
            Categories.GROUP_BY,
            h_level=target_attributes.pop("target_h_level", 0),
            group_keys=target_attributes.pop("group_keys", None),
            group_by_scatter_layers=target_attributes.pop(
                "group_by_scatter_layers",
                (target_count, [], []),
            ),
        )

        link = LogicalLink(source=source, target=target)

        sources = [
            self._drop(coord)
            for coord in source_coords
        ]

        targets = [
            dropdict({"target": index})
            for index in range(target_count)
        ]

        edges = GroupByHandler().resolve_edges(
            link,
            sources,
            targets,
            self.UnusedWiringContext(),
        )

        return source, target, sources, targets, edges

    def test_buckets_source_drops_and_sorts_group_keys(self):
        _, _, sources, targets, edges = self._resolve(
            [
                InstanceId((2, 1)),
                InstanceId((0, 1)),
                InstanceId((0, 2)),
            ],
            2,
        )

        self.assertEqual(
            [
                (sources[0], targets[0]),
                (sources[1], targets[0]),
                (sources[2], targets[1]),
            ],
            [(edge.source, edge.target) for edge in edges],
        )

    def test_includes_outer_group_context_for_nested_scatter(self):
        _, _, sources, targets, edges = self._resolve(
            [
                InstanceId((4, 2, 1)),
                InstanceId((5, 2, 1)),
            ],
            2,
            source_h_level=3,
            target_h_level=1,
        )

        self.assertEqual(
            [
                (sources[0], targets[0]),
                (sources[1], targets[1]),
            ],
            [(edge.source, edge.target) for edge in edges],
        )

    def test_multi_key_groupby_reverses_iid_context(self):
        _, _, sources, targets, edges = self._resolve(
            [
                InstanceId((0, 1)),
                InstanceId((1, 0)),
            ],
            2,
            group_keys=("first", "second"),
            group_by_scatter_layers=(2, [0], []),
            source_group=self._node(Categories.SCATTER),
        )

        self.assertEqual(
            [
                (sources[1], targets[0]),
                (sources[0], targets[1]),
            ],
            [(edge.source, edge.target) for edge in edges],
        )

    def test_multi_key_groupby_sorts_indices_numerically_above_nine(
        self,
    ):
        keys = [
            (2, 0),
            (10, 0),
            (11, 0),
            (2, 10),
            (2, 11),
            (10, 2),
            (11, 10),
        ]
        source_coords = [
            InstanceId((second, first))
            for first, second in keys
        ]

        _, _, sources, targets, edges = self._resolve(
            source_coords,
            len(keys),
            group_keys=("first", "second"),
            group_by_scatter_layers=(
                len(keys),
                [0, 1],
                [],
            ),
            source_group=self._node(Categories.SCATTER),
        )

        self.assertEqual(
            [
                (sources[index], targets[target_index])
                for target_index, index in enumerate(
                    [0, 3, 4, 1, 5, 2, 6]
                )
            ],
            [(edge.source, edge.target) for edge in edges],
        )

    def test_chained_multi_key_groupby_reads_group_key_after_dollar(
        self,
    ):
        source = self._node(Categories.PYTHON_APP)
        source.group = self._node(
            Categories.GROUP_BY,
            name="outer-groupby",
        )

        target = self._node(
            Categories.GROUP_BY,
            group_keys=("first", "second"),
            group_by_scatter_layers=(2, [0, 1], []),
        )

        link = LogicalLink(source=source, target=target)

        sources = [
            self._drop(
                InstanceId((0, 1), group_key=(2, 3))
            ),
            self._drop(
                InstanceId((0, 1), group_key=(1, 3))
            ),
        ]

        targets = [
            dropdict({"target": 0}),
            dropdict({"target": 1}),
        ]

        edges = GroupByHandler().resolve_edges(
            link,
            sources,
            targets,
            self.UnusedWiringContext(),
        )

        self.assertEqual(
            [
                (sources[1], targets[0]),
                (sources[0], targets[1]),
            ],
            [(edge.source, edge.target) for edge in edges],
        )

    def test_chained_multi_key_groupby_requires_dollar_context(
        self,
    ):
        source = self._node(Categories.PYTHON_APP)
        source.group = self._node(Categories.GROUP_BY)

        target = self._node(
            Categories.GROUP_BY,
            group_keys=("first", "second"),
            group_by_scatter_layers=(1, [0], []),
        )

        link = LogicalLink(source=source, target=target)

        with self.assertRaisesRegex(
            GraphException,
            "hiearchy.*not specified",
        ):
            GroupByHandler().resolve_edges(
                link,
                [self._drop(InstanceId((0, 1)))],
                [dropdict({"target": 0})],
                self.UnusedWiringContext(),
            )

    def test_reads_structured_coordinate_instead_of_iid_string(self):
        source = self._node(Categories.PYTHON_APP)
        target = self._node(
            Categories.GROUP_BY,
            group_keys=None,
            group_by_scatter_layers=(2, [], []),
        )
        link = LogicalLink(source=source, target=target)

        sources = [
            self._drop(InstanceId((0, 1))),
            self._drop(InstanceId((0, 2))),
        ]

        # Deliberately make the serialized values disagree with the
        # coordinates. GroupBy resolution must use the structured values.
        sources[0]["iid"] = "0-2"
        sources[1]["iid"] = "0-1"

        targets = [
            dropdict({"target": 0}),
            dropdict({"target": 1}),
        ]

        edges = GroupByHandler().resolve_edges(
            link,
            sources,
            targets,
            self.UnusedWiringContext(),
        )

        self.assertEqual(
            [
                (sources[0], targets[0]),
                (sources[1], targets[1]),
            ],
            [(edge.source, edge.target) for edge in edges],
        )

    def test_rejects_mismatch_between_group_keys_and_target_drops(
        self,
    ):
        with self.assertRaisesRegex(
            GraphException,
            "# of Group keys 2 != # of Group Drops 1",
        ):
            self._resolve(
                [
                    InstanceId((0, 1)),
                    InstanceId((0, 2)),
                ],
                1,
            )


class TestConstructHandlerDoP(unittest.TestCase):

    def test_scatter_resolves_aligned_boundary_edges(self):
        source = SimpleNamespace(id="group", name="group")
        target = SimpleNamespace(id="component", name="component")
        link = LogicalLink(source, target)
        sources = [{"oid": "source-0"}, {"oid": "source-1"}]
        targets = [{"oid": "target-0"}, {"oid": "target-1"}]

        edges = ScatterHandler().resolve_edges(link, sources, targets, None)

        self.assertEqual(
            [
                (source_drop, target_drop)
                for source_drop, target_drop in zip(sources, targets)
            ],
            [(edge.source, edge.target) for edge in edges],
        )
        self.assertTrue(all(edge.link is link for edge in edges))

    def test_scatter_boundary_edge_resolution_preserves_length_error(self):
        link = LogicalLink(
            SimpleNamespace(id="group"),
            SimpleNamespace(id="component"),
        )

        with self.assertRaisesRegex(
            GraphException,
            r"# 1 Group Inputs group must be the same as # 2 of Component Outputs component",
        ):
            ScatterHandler().resolve_edges(
                link,
                [{"oid": "source"}],
                [{"oid": "target-0"}, {"oid": "target-1"}],
                None,
            )

    def test_gather_rejects_non_data_input(self):
        source = SimpleNamespace(
            id="source",
            category=Categories.PYTHON_APP,
            jd={
                "category": Categories.PYTHON_APP,
                "categoryType": "Application",
            },
        )
        target = SimpleNamespace(
            id="gather",
            category=Categories.GATHER,
            jd={"category": Categories.GATHER},
        )

        with self.assertRaises(GInvalidLink):
            GatherHandler().validate_link(source, target)

    def test_handler_dop_values(self):
        scatter = SimpleNamespace(
            jd={"num_of_copies": "4"},
            name="scatter",
            id="scatter",
        )
        self.assertEqual(
            4,
            ScatterHandler().degree_of_parallelism(scatter, None),
        )

        loop = SimpleNamespace(
            jd={"num_of_iter": "5"},
            name="loop",
            id="loop",
        )
        self.assertEqual(
            5,
            LoopHandler().degree_of_parallelism(loop, None),
        )

        mpi = SimpleNamespace(jd={"num_of_procs": "6"})
        self.assertEqual(
            6,
            MPIHandler().degree_of_parallelism(mpi, None),
        )

        groupby = SimpleNamespace(
            group_by_scatter_layers=(3, [], [])
        )
        self.assertEqual(
            3,
            GroupByHandler().degree_of_parallelism(groupby, None),
        )

        self.assertEqual(
            1,
            ServiceHandler().degree_of_parallelism(None, None),
        )
        self.assertEqual(
            1,
            SubgraphHandler().degree_of_parallelism(None, None),
        )
        self.assertEqual(
            1,
            LeafHandler().degree_of_parallelism(None, None),
        )

    def test_gather_dop(self):
        input_node = SimpleNamespace(
            category=Categories.GROUP_BY,
            jd={"category": Categories.GROUP_BY},
            dop=8,
        )
        gather = SimpleNamespace(
            inputs=[input_node],
            gather_width=2,
        )

        self.assertEqual(
            4,
            GatherHandler().degree_of_parallelism(gather, None),
        )

    @staticmethod
    def _group_node(category, **predicates):
        node = SimpleNamespace(
            is_group=True,
            category=category,
            jd={"category": category},
        )

        for name, value in predicates.items():
            setattr(node, name, value)

        return node

    def test_registry_dispatch(self):
        scatter = self._group_node(Categories.SCATTER)
        branch = SimpleNamespace(
            is_group=False,
            category=Categories.BRANCH,
            jd={"category": Categories.BRANCH},
        )
        mpi = SimpleNamespace(
            is_group=False,
            category=Categories.MPI,
            jd={"category": Categories.MPI},
        )
        leaf = SimpleNamespace(
            is_group=False,
            category="ordinary",
            jd={"category": "ordinary"},
        )

        self.assertIsInstance(
            get_handler_for_node(scatter),
            ScatterHandler,
        )
        self.assertIsInstance(get_handler_for_node(branch), BranchHandler)
        self.assertIsInstance(
            get_handler_for_node(mpi),
            MPIHandler,
        )
        self.assertIsInstance(
            get_handler_for_node(leaf),
            LeafHandler,
        )

    def test_group_handler_registration_adds_group_dispatch(self):
        handler = SimpleNamespace(
            construct_type="TestGroupConstruct",
            is_group_construct=True,
            edge_keys=(),
        )
        register_handler(handler)
        node = self._group_node(handler.construct_type)

        self.assertIs(get_handler_for_node(node), handler)

    def test_registry_rejects_group_node_matching_no_construct(self):
        for category in [Categories.MPI, Categories.BRANCH]:
            with self.subTest(category=category):
                with self.assertRaises(GInvalidNode):
                    get_handler_for_node(self._group_node(category))

    def test_registry_dispatch_prefers_construct_over_subgraph_flag(self):
        node = self._group_node(Categories.SCATTER)
        node.jd["isSubGraphApp"] = True

        self.assertIsInstance(get_handler_for_node(node), ScatterHandler)

    def test_registry_construct_checks_preserve_node_context(self):
        scatter = self._group_node(Categories.SCATTER)
        loop = self._group_node(Categories.LOOP)
        non_group_scatter = SimpleNamespace(
            is_group=False,
            category=Categories.SCATTER,
            jd={"category": Categories.SCATTER},
        )
        non_group_loop = SimpleNamespace(
            is_group=False,
            category=Categories.LOOP,
            jd={"category": Categories.LOOP},
        )
        subgraph_flagged = SimpleNamespace(
            is_group=False,
            category=Categories.PYTHON_APP,
            jd={"category": Categories.PYTHON_APP, "isSubGraphApp": True},
        )
        subgraph_disabled = SimpleNamespace(
            is_group=True,
            category=Categories.SUBGRAPH,
            jd={"category": Categories.SUBGRAPH, "isSubGraphApp": False},
        )

        self.assertTrue(is_construct(scatter, Categories.SCATTER))
        self.assertTrue(is_construct(loop, Categories.LOOP))
        self.assertFalse(is_construct(non_group_scatter, Categories.SCATTER))
        self.assertFalse(is_construct(non_group_loop, Categories.LOOP))
        self.assertTrue(is_construct(subgraph_flagged, Categories.SUBGRAPH))
        self.assertFalse(is_construct(subgraph_disabled, Categories.SUBGRAPH))

    def test_registry_rejects_group_node_without_category(self):
        node = self._group_node(Categories.SCATTER)
        node.category = "Unknown"
        node.jd = {}

        with self.assertRaises(GInvalidNode):
            get_handler_for_node(node)

    def test_lgnode_dop_uses_handler_and_caches_result(self):
        node = object.__new__(LGNode)
        node._dop = None

        handler = Mock()
        handler.degree_of_parallelism.return_value = 7

        with patch(
            "dlg.translator.stages.unroll.lg_node.get_handler_for_node",
            return_value=handler,
        ) as get_handler:
            self.assertEqual(7, node.dop)
            self.assertEqual(7, node.dop)

            get_handler.assert_called_once_with(node)
            handler.degree_of_parallelism.assert_called_once_with(node)


class TestSubgraphEdgeResolution(unittest.TestCase):

    def test_subgraph_edges_are_noop(self):
        subgraph = SimpleNamespace(name="subgraph")
        leaf = SimpleNamespace(name="leaf")

        handler = SubgraphHandler()

        with self.subTest(direction="subgraph-source"):
            link = LogicalLink(subgraph, leaf)
            self.assertEqual(
                [],
                handler.resolve_edges(
                    link,
                    [{"oid": "source"}],
                    [{"oid": "target"}],
                    Mock(),
                ),
            )

        with self.subTest(direction="subgraph-target"):
            link = LogicalLink(leaf, subgraph)
            self.assertEqual(
                [],
                handler.resolve_edges(
                    link,
                    [{"oid": "source"}],
                    [{"oid": "target"}],
                    Mock(),
                ),
            )


class TestServiceHandlerInstantiation(unittest.TestCase):

    def test_service_instantiation_creates_application_drop(self):
        coord = InstanceId((0,))
        drop = {"oid": "service-drop"}

        node = Mock()
        node.is_group = True
        node.is_data = False
        node.is_app = False
        node.jd = {"categoryType": "Construct"}
        node.make_single_drop.return_value = drop

        result = ServiceHandler().instantiate(node, coord, None)

        self.assertEqual([drop], result)
        self.assertEqual(
            CategoryType.APPLICATION,
            node.jd["categoryType"],
        )
        self.assertFalse(node.is_data)
        self.assertTrue(node.is_app)
        node.make_single_drop.assert_called_once_with(coord)

    def test_non_group_service_instantiation_is_noop(self):
        node = Mock()
        node.is_group = False

        result = ServiceHandler().instantiate(
            node,
            InstanceId((0,)),
            None,
        )

        self.assertEqual([], result)
        node.make_single_drop.assert_not_called()


class TestGatherEdgeResolution(unittest.TestCase):
    class FakeWiringContext:
        def __init__(self, chunk_size):
            self._chunk_size = chunk_size

        def chunk_size(self, source, target):
            del source, target
            return self._chunk_size

        @staticmethod
        def split(drops, size):
            for index in range(0, len(drops), size):
                yield drops[index:index + size]

    @staticmethod
    def _node(node_id, h_level):
        return SimpleNamespace(
            id=node_id,
            h_level=h_level,
        )

    def test_gather_groups_source_drops_by_chunk_size(self):
        source = self._node("source", 2)
        target = self._node("gather", 1)
        link = LogicalLink(source, target)

        sources = [
            {"oid": "s0"},
            {"oid": "s1"},
            {"oid": "s2"},
            {"oid": "s3"},
        ]
        targets = [
            {"oid": "g0"},
            {"oid": "g1"},
        ]

        edges = GatherHandler().resolve_edges(
            link,
            sources,
            targets,
            self.FakeWiringContext(2),
        )

        self.assertEqual(
            [
                ("s0", "g0"),
                ("s1", "g0"),
                ("s2", "g1"),
                ("s3", "g1"),
            ],
            [
                (edge.source["oid"], edge.target["oid"])
                for edge in edges
            ],
        )

    def test_gather_accepts_equal_h_level(self):
        source = self._node("source", 1)
        target = self._node("gather", 1)
        link = LogicalLink(source, target)

        edges = GatherHandler().resolve_edges(
            link,
            [{"oid": "s0"}],
            [{"oid": "g0"}],
            self.FakeWiringContext(1),
        )

        self.assertEqual(1, len(edges))
        self.assertEqual("s0", edges[0].source["oid"])
        self.assertEqual("g0", edges[0].target["oid"])

    def test_gather_rejects_target_with_higher_level(self):
        source = self._node("source", 1)
        target = self._node("gather", 2)
        link = LogicalLink(source, target)

        with self.assertRaises(GraphException):
            GatherHandler().resolve_edges(
                link,
                [{"oid": "s0"}],
                [{"oid": "g0"}],
                self.FakeWiringContext(1),
            )


class TestLeafEdgeResolution(unittest.TestCase):
    class FakeWiringContext:
        def __init__(self, chunk_size):
            self._chunk_size = chunk_size

        def chunk_size(self, source, target):
            del source, target
            return self._chunk_size

        @staticmethod
        def split(drops, size):
            for index in range(0, len(drops), size):
                yield drops[index:index + size]

    @staticmethod
    def _node(name, h_level, is_start_node=False):
        return SimpleNamespace(
            name=name,
            h_level=h_level,
            is_start_node=is_start_node,
        )

    def test_leaf_start_node_produces_no_edges(self):
        source = self._node("start", 0, is_start_node=True)
        target = self._node("target", 0)
        link = LogicalLink(source, target)

        edges = LeafHandler().resolve_edges(
            link,
            [{"oid": "source"}],
            [{"oid": "target"}],
            self.FakeWiringContext(1),
        )

        self.assertEqual([], edges)

    def test_leaf_source_higher_or_equal_distributes_sources_to_targets(self):
        source = self._node("source", 2)
        target = self._node("target", 1)
        link = LogicalLink(source, target)

        sources = [
            {"oid": "s0"},
            {"oid": "s1"},
            {"oid": "s2"},
            {"oid": "s3"},
        ]
        targets = [
            {"oid": "t0"},
            {"oid": "t1"},
        ]

        edges = LeafHandler().resolve_edges(
            link,
            sources,
            targets,
            self.FakeWiringContext(2),
        )

        self.assertEqual(
            [
                ("s0", "t0"),
                ("s1", "t0"),
                ("s2", "t1"),
                ("s3", "t1"),
            ],
            [
                (edge.source["oid"], edge.target["oid"])
                for edge in edges
            ],
        )

    def test_leaf_target_higher_distributes_targets_to_sources(self):
        source = self._node("source", 1)
        target = self._node("target", 2)
        link = LogicalLink(source, target)

        sources = [
            {"oid": "s0"},
            {"oid": "s1"},
        ]
        targets = [
            {"oid": "t0"},
            {"oid": "t1"},
            {"oid": "t2"},
            {"oid": "t3"},
        ]

        edges = LeafHandler().resolve_edges(
            link,
            sources,
            targets,
            self.FakeWiringContext(2),
        )

        self.assertEqual(
            [
                ("s0", "t0"),
                ("s0", "t1"),
                ("s1", "t2"),
                ("s1", "t3"),
            ],
            [
                (edge.source["oid"], edge.target["oid"])
                for edge in edges
            ],
        )


if __name__ == "__main__":
    unittest.main()
