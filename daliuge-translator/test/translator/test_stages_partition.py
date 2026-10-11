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
"""
Tests for PartitionStage (2-3, selected low-risk module migration).

PartitionStage is a thin wrapper: `run()` delegates to
`pg_generator.partition()` and `stamp()` delegates to
`init_pgt_partition_repro_data()`, converting to/from the typed artefact
envelopes on either side. These tests mock both delegate functions -- their
own behaviour is/should be tested where they're defined -- and check only
the glue: are they called with the right arguments, and is the result
wrapped in the right envelope type.

Mirrors the pattern established in test_stages_unroll.py (issue #40).
"""
import unittest
from unittest.mock import MagicMock, patch

from dlg.translator.artefacts import (
    PhysicalGraphTemplate,
    PhysicalGraphTemplatePartitioned,
)
from dlg.translator.stages.partition.stage import (
    PartitionStage,
    PartitionOptions,
    logger as partition_logger,
    partition,
)
from dlg.translator.stages.partition.algorithms.base import MetisOptions
from dlg.translator.stages.partition.algorithms.registry import (
    algorithm_code,
    algorithm_name,
    build_options,
    get_algorithm,
    known_algorithms,
)
from dlg.common import path_utils
from dlg.translator.stages.unroll.lg import LG
from dlg.translator.stages.partition.pgt import PGT
from dlg.translator.stages.partition.pgtp import MetisPGTP, MySarkarPGTP, MinNumPartsPGTP


TEST_SSID = "test_pg_gen"


def pgt_wire():
    # PhysicalArtefact's wire form always ends with a reprodata trailer
    # (possibly empty) -- see artefacts.py: `to_wire()` appends it
    # unconditionally, and `from_wire()` only treats the last element as
    # drops data if it has an 'oid'. Include the trailer here so this
    # round-trips exactly through from_wire()/to_wire().
    return [{"oid": "a"}, {"oid": "b"}, {"repro": "pgt-reprodata"}]


def pgtp_drops():
    # bare drop list as returned by pg_generator.partition() -- no
    # reprodata trailer, that's added back by PartitionStage.run() itself.
    return [{"oid": "a", "node": "#0"}, {"oid": "b", "node": "#1"}]


class TestPartitionAlgorithmRegistry(unittest.TestCase):
    ALGORITHMS = {
        "none": 0,
        "metis": 1,
        "mysarkar": 2,
        "min_num_parts": 3,
        "pso": 4,
    }

    def test_known_algorithm_wire_names_are_stable(self):
        self.assertEqual(
            known_algorithms(),
            list(self.ALGORITHMS),
        )

    def test_algorithm_names_map_to_stable_codes(self):
        for name, code in self.ALGORITHMS.items():
            self.assertEqual(algorithm_code(name), code)

    def test_algorithm_codes_map_to_stable_names(self):
        for name, code in self.ALGORITHMS.items():
            self.assertEqual(algorithm_name(code), name)

    def test_lookup_by_name_and_code_returns_same_plugin(self):
        for name, code in self.ALGORITHMS.items():
            by_name = get_algorithm(name)
            by_code = get_algorithm(code)

            self.assertIs(by_name, by_code)
            self.assertEqual(by_name.name, name)
            self.assertEqual(by_name.code, code)


class TestPartitionAlgorithmOptions(unittest.TestCase):
    def test_metis_accepts_metis_options(self):
        options = build_options(
            "metis",
            {
                "min_goal": 2,
                "ptype": 1,
                "max_load_imb": 75,
            },
        )

        self.assertEqual(options.min_goal, 2)
        self.assertEqual(options.ptype, 1)
        self.assertEqual(options.max_load_imb, 75)

    def test_pso_accepts_pso_options(self):
        options = build_options(
            "pso",
            {
                "max_cpu": 4,
                "max_mem": 512,
                "deadline": 100,
                "topk": 12,
                "swarm_size": 20,
            },
        )

        self.assertEqual(options.max_cpu, 4)
        self.assertEqual(options.max_mem, 512)
        self.assertEqual(options.deadline, 100)
        self.assertEqual(options.topk, 12)
        self.assertEqual(options.swarm_size, 20)

    def test_explicit_none_uses_default_value(self):
        options = build_options(
            "pso",
            {
                "topk": None,
                "swarm_size": None,
            },
        )

        self.assertEqual(options.topk, 30)
        self.assertEqual(options.swarm_size, 40)

    def test_explicit_none_for_other_algorithm_is_not_rejected(self):
        options = build_options(
            "metis",
            {
                "max_load_imb": 75,
                "topk": None,
            },
        )

        self.assertEqual(options, MetisOptions(max_load_imb=75))

    def test_all_none_legacy_params_build_defaults_for_every_algorithm(self):
        # The shape of the REST AlgoParams model: every key, all None.
        all_none = dict.fromkeys(
            [
                "min_goal",
                "ptype",
                "max_load_imb",
                "max_cpu",
                "max_mem",
                "time_greedy",
                "deadline",
                "topk",
                "swarm_size",
            ]
        )

        for name in known_algorithms():
            with self.subTest(algorithm=name):
                self.assertEqual(
                    build_options(name, all_none),
                    get_algorithm(name).options_type(),
                )

    def test_rejects_option_for_wrong_algorithm(self):
        with self.assertRaises(ValueError):
            build_options("metis", {"topk": 5})

    def test_none_algorithm_rejects_options(self):
        with self.assertRaises(ValueError):
            build_options("none", {"max_cpu": 8})


class TestPartitionPluginDispatch(unittest.TestCase):
    def test_partition_delegates_to_registered_plugin_by_name(self):
        algorithm = get_algorithm("none")
        graph = MagicMock()
        graph.to_pg_template.return_value = [{"oid": "result"}]
        source = [{"oid": "a"}]

        with patch.object(
            algorithm,
            "partition",
            return_value=graph,
        ) as mock_partition:
            result = partition(source, "none")

        mock_partition.assert_called_once()
        args, kwargs = mock_partition.call_args
        self.assertEqual(args[0], source)
        self.assertEqual(kwargs["num_partitions"], 1)
        self.assertEqual(kwargs["partition_label"], "partition")

        graph.to_gojs_json.assert_called_once_with(
            string_rep=False,
            visual=False,
        )
        graph.to_pg_template.assert_called_once_with(
            2,
            num_islands=1,
        )
        graph.to_pg_spec.assert_not_called()
        self.assertEqual(result, [{"oid": "result"}])

    def test_partition_accepts_numeric_algorithm_code(self):
        algorithm = get_algorithm(0)
        graph = MagicMock()
        source = [{"oid": "a"}]

        with patch.object(
            algorithm,
            "partition",
            return_value=graph,
        ) as mock_partition:
            result = partition(source, 0, show_gojs=True)

        mock_partition.assert_called_once()
        self.assertIs(result, graph)
        graph.to_gojs_json.assert_called_once_with(
            string_rep=False,
            visual=True,
        )
        graph.to_pg_template.assert_not_called()
        graph.to_pg_spec.assert_not_called()

    def test_partition_drops_unused_params_with_warning(self):
        # Mirrors the daliuge-engine callers: a param belonging to another
        # algorithm (apps/subgraph.py), a nested algo_params key
        # (deploy/create_dlg_job.py) and a misspelt keyword
        # (deploy/start_helm_cluster.py).
        algorithm = get_algorithm("metis")
        engine_params = {
            "max_load_imb": 100,
            "max_cpu": 8,
            "algo_params": {"ptype": 1},
            "num_partitons": 1,
        }

        with patch.object(
            algorithm,
            "partition",
            return_value=MagicMock(),
        ) as mock_partition, self.assertLogs(
            partition_logger, "WARNING"
        ) as logs:
            partition([], "metis", show_gojs=True, **engine_params)

        _, kwargs = mock_partition.call_args
        self.assertEqual(kwargs["options"], MetisOptions(max_load_imb=100))
        self.assertIn("algo_params, max_cpu, num_partitons", logs.output[0])

    def test_partition_drops_none_params_without_warning(self):
        algorithm = get_algorithm("metis")

        with patch.object(
            algorithm,
            "partition",
            return_value=MagicMock(),
        ) as mock_partition, self.assertNoLogs(partition_logger, "WARNING"):
            partition([], "metis", show_gojs=True, topk=None)

        _, kwargs = mock_partition.call_args
        self.assertEqual(kwargs["options"], MetisOptions())

    def test_strict_partition_rejects_options_for_wrong_plugin(self):
        with self.assertRaises(ValueError):
            partition([], "metis", strict=True, topk=5)


class TestPartitionStageRun(unittest.TestCase):
    @patch("dlg.translator.stages.partition.stage.partition")
    def test_run_delegates_to_pg_generator_partition(self, mock_partition):
        mock_partition.return_value = pgtp_drops()
        pgt = PhysicalGraphTemplate.from_wire(pgt_wire())

        result = PartitionStage().run(pgt)

        mock_partition.assert_called_once()
        _, kwargs = mock_partition.call_args
        self.assertEqual(kwargs["pgt"], pgt.drops)
        self.assertEqual(kwargs["algo"], "metis")
        self.assertEqual(kwargs["num_partitions"], 1)
        self.assertEqual(kwargs["num_islands"], 1)
        self.assertEqual(kwargs["partition_label"], "partition")
        self.assertIs(kwargs["strict"], True)

        self.assertIsInstance(result, PhysicalGraphTemplatePartitioned)
        self.assertEqual(result.drops, pgtp_drops())
        self.assertEqual(result.reprodata, pgt.reprodata)

    @patch("dlg.translator.stages.partition.stage.partition")
    def test_run_passes_through_partition_options(self, mock_partition):
        mock_partition.return_value = pgtp_drops()
        pgt = PhysicalGraphTemplate.from_wire(pgt_wire())
        opts = PartitionOptions(
            algo="mysarkar",
            num_partitions=4,
            num_islands=2,
            partition_label="island",
            algo_params={"max_load_imb": 0.5},
        )

        PartitionStage(opts).run(pgt)

        _, kwargs = mock_partition.call_args
        self.assertEqual(kwargs["algo"], "mysarkar")
        self.assertEqual(kwargs["num_partitions"], 4)
        self.assertEqual(kwargs["num_islands"], 2)
        self.assertEqual(kwargs["partition_label"], "island")
        self.assertEqual(kwargs["max_load_imb"], 0.5)

    @patch("dlg.translator.stages.partition.stage.partition")
    def test_run_hands_partition_a_copy_not_the_original(self, mock_partition):
        # partition() is documented (via resource_map's sibling) to mutate
        # its pgt argument in place -- run() must pass a copy, never the
        # PhysicalGraphTemplate's own drops list.
        mock_partition.side_effect = lambda pgt, **_: pgt
        pgt = PhysicalGraphTemplate.from_wire(pgt_wire())

        PartitionStage().run(pgt)

        _, kwargs = mock_partition.call_args
        self.assertIsNot(kwargs["pgt"], pgt.drops)

    def test_name_is_partition(self):
        self.assertEqual(PartitionStage.name, "partition")


class TestPartitionStageStamp(unittest.TestCase):
    @patch("dlg.translator.stages.partition.stage.init_pgt_partition_repro_data")
    def test_stamp_delegates_to_reproducibility_hook(self, mock_stamp):
        stamped_wire = [{"oid": "a", "reprodata": {"stamped": True}}, {}]
        mock_stamp.return_value = stamped_wire
        pgtp = PhysicalGraphTemplatePartitioned.from_wire(
            [*pgtp_drops(), {"repro": "pgtp-reprodata"}]
        )

        result = PartitionStage().stamp(pgtp)

        mock_stamp.assert_called_once_with(pgtp.to_wire())
        self.assertIsInstance(result, PhysicalGraphTemplatePartitioned)
        self.assertEqual(result.to_wire(), stamped_wire)

    @patch("dlg.translator.stages.partition.stage.init_pgt_partition_repro_data")
    def test_stamp_hands_the_hook_a_copy_not_the_original(self, mock_stamp):
        # The hook mutates its argument in place -- stamp() must round-trip
        # through to_wire() so the PGT-P passed in is never the same object
        # the hook mutates.
        mock_stamp.side_effect = lambda wire: wire
        pgtp = PhysicalGraphTemplatePartitioned.from_wire(
            [*pgtp_drops(), {"repro": "pgtp-reprodata"}]
        )

        PartitionStage().stamp(pgtp)

        (passed_wire,), _ = mock_stamp.call_args
        self.assertIsNot(passed_wire, pgtp.drops)


class TestPGGen(unittest.TestCase):
    """
    Test that the PhysicalGraph template constructor and supporting methods work.

    Uses test/dropmake/pg_spec as test data

    Note: This is a regression testing class. These tests are based on graphs that were
    generated using the code they are testing. If the PGT (sub)class and it's methods
    change in the future, test data may need to be re-generated (provided test
    failures are caused by known-breaking changes, as opposed to legitimate bugs!).
    """
    lgnames = {
        "HelloWorld_simple.graph": {"nodes": 2, "edges": 1},
        "eagle_gather_empty_update.graph": {"nodes": 22, "edges": 24},
        "eagle_gather_simple_update.graph": {"nodes": 42, "edges": 55},
        "eagle_gather_update.graph": {"nodes": 29, "edges": 30},
        "testLoop.graph": {"nodes": 11, "edges": 10},
        "cont_img_mvp.graph": {"nodes": 144, "edges": 188},
        "test_grpby_gather.graph": {"nodes": 15, "edges": 14},
        "chiles_simple.graph": {"nodes": 22, "edges": 21},
        "Plasma_test.graph": {"nodes": 6, "edges": 5},
        "SharedMemoryTest_update.graph": {"nodes": 8, "edges": 7},
        # "simpleMKN_update.graph", # Currently broken
    }

    def _create_pgt(self, lg_name):
        fp = path_utils.get_lg_fpath('logical_graphs', lg_name)
        lg = LG(fp, ssid=TEST_SSID)
        drop_list = lg.unroll_to_tpl()
        return PGT(drop_list)

    def test_pgt_init(self):
        """
        Confirm that the PGT DAG correctly establishes the right number of nodes and edges
        """

        for lg_name, lg_expected in self.lgnames.items():
            pgt = self._create_pgt(lg_name)
            num_nodes = len(pgt.dag.nodes)
            num_edges = len(pgt.dag.edges)
            self.assertEqual(lg_expected['nodes'], num_nodes)
            self.assertEqual(lg_expected['edges'], num_edges)

    def test_pgt_to_json(self):
        """
        Verify that the expeceted output of the PGT to_gojs_json method is correct.

        Note that the to_gojs_json is not _just_ producing the go_js representation; it
        is also performing transformations on the PGT in the child classes.

        This confirms that the number of nodes and edges is a) consistent with the
        expected numbers, and b) is self-consistent between the drops and the networkx
        graph that are used interchangeably in the PGT class.
        """

        for lg_name, pg_expected in self.lgnames.items():
            pgt = self._create_pgt(lg_name)
            pgt.to_gojs_json(visual=False, string_rep=False)
            self.assertEqual(len(pgt.dag.edges), len(pgt.links))
            self.assertEqual(len(pgt.dag.nodes), len(pgt.drops))
            self.assertEqual(pg_expected['nodes'], len(pgt.drops))
            self.assertEqual(pg_expected['edges'], len(pgt.dag.edges))


class TestPGPartitionRegression(unittest.TestCase):
    SARKAR_PARTITION_RESULTS = {
        "testLoop.graph": {
            'algo': 'Edge Zero', 'min_exec_time': 30, 'total_data_movement': 50,
            'exec_time': 80, 'num_parts': 0
        },
        "cont_img_mvp.graph": {
            'algo': 'Edge Zero', 'min_exec_time': 144, 'total_data_movement': 932,
            'exec_time': 444, 'num_parts': 0
        },
        "test_grpby_gather.graph": {
            'algo': 'Edge Zero', 'min_exec_time': 16, 'total_data_movement': 70,
            'exec_time': 51, 'num_parts': 0
        },
        "chiles_simple.graph": {
            'algo': 'Edge Zero', 'min_exec_time': 45, 'total_data_movement': 1080,
            'exec_time': 285, 'num_parts': 0
        }
    }

    MINPARTS_RESULTS = {
        "testLoop.graph": {
            'algo': 'Lookahead', 'min_exec_time': 30, 'total_data_movement': 50,
            'exec_time': 80, 'num_parts': 0
        },
        "cont_img_mvp.graph": {
            'algo': 'Lookahead', 'min_exec_time': 144,
            'total_data_movement': 932, 'exec_time': 444, 'num_parts': 0
        },
        "test_grpby_gather.graph": {
            'algo': 'Lookahead', 'min_exec_time': 16,
            'total_data_movement': 70, 'exec_time': 51, 'num_parts': 0
        },
        "chiles_simple.graph": {
            'algo': 'Lookahead', 'min_exec_time': 45, 'total_data_movement': 1080,
            'exec_time': 285, 'num_parts': 0
        }
    }

    def setUp(self):
        self.partitionMethodLGs = [
            "testLoop.graph",
            "cont_img_mvp.graph",
            "test_grpby_gather.graph",
            "chiles_simple.graph",
            # "simpleMKN.graph", # Broken
        ]

    def test_metis_pgtp(self):
        """
        Confirm that basic Sarkar paritioning has not regressed
        """
        expected = {'algo': 'METIS_LB91',
                    'min_exec_time': None,
                    'total_data_movement': None, 'exec_time': None,
                    'num_parts': 1}

        for lg_names in self.partitionMethodLGs:
            fp = path_utils.get_lg_fpath('logical_graphs', lg_names)
            lg = LG(fp)
            drop_list = lg.unroll_to_tpl()
            pgtp = MetisPGTP(drop_list)
            self.assertEqual(expected, pgtp.result())

    def test_mysarkar_pgtp(self):
        """
        Confirm that basic Sarkar paritioning has not regressed
        """

        for lg_name in self.partitionMethodLGs:
            fp = path_utils.get_lg_fpath('logical_graphs', lg_name)
            lg = LG(fp)
            drop_list = lg.unroll_to_tpl()
            pgtp = MySarkarPGTP(drop_list)
            self.assertEqual(
                self.SARKAR_PARTITION_RESULTS[lg_name],
                pgtp.result(),
                f"Partition results do not match test case for: {lg_name}")

    def test_minnumparts_pgtp(self):
        tgt_deadline = [200, 300, 90, 80, 160]
        for i, lg_name in enumerate(self.partitionMethodLGs):
            fp = path_utils.get_lg_fpath('logical_graphs', lg_name)
            lg = LG(fp)
            drop_list = lg.unroll_to_tpl()
            pgtp = MinNumPartsPGTP(drop_list, tgt_deadline[i])
            self.assertEqual(self.MINPARTS_RESULTS[lg_name],
                             pgtp.result(),
                             f"Incorrect partition results for: {lg_name}")


if __name__ == "__main__":
    unittest.main()
