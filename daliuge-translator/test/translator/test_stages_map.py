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
Tests for MapStage (2-3, selected low-risk module migration).

MapStage is a thin wrapper: `run()` delegates to `pg_generator.resource_map()`
and `stamp()` delegates to `init_pg_repro_data()`, converting to/from the
typed artefact envelopes on either side. These tests mock both delegate
functions -- their own behaviour is/should be tested where they're defined --
and check only the glue: are they called with the right arguments, and is
the result wrapped in the right envelope type.

Mirrors the pattern established in test_stages_unroll.py (issue #40).
"""
import unittest
from unittest.mock import patch

from dlg.translator.artefacts import PhysicalGraphTemplatePartitioned, PhysicalGraph
from dlg.translator.stages.map.stage import MapStage, MapOptions
from dlg.common import path_utils
from dlg.translator.stages.unroll.lg import LG
from dlg.translator.stages.partition.pgt import GPGTNoNeedMergeException
from dlg.translator.stages.partition.pgtp import MetisPGTP, MySarkarPGTP


TEST_SSID = "test_pg_gen"


def pgtp_wire():
    # PhysicalArtefact's wire form always ends with a reprodata trailer
    # (possibly empty) -- see artefacts.py: `to_wire()` appends it
    # unconditionally, and `from_wire()` only treats the last element as
    # drops data if it has an 'oid'. Include the trailer here so this
    # round-trips exactly through from_wire()/to_wire().
    return [
        {"oid": "a", "node": "#0"},
        {"oid": "b", "node": "#1"},
        {"repro": "pgtp-reprodata"},
    ]


def pg_drops():
    # bare drop list as returned by pg_generator.resource_map() -- no
    # reprodata trailer, that's added back by MapStage.run() itself.
    return [{"oid": "a", "node": "10.0.0.1"}, {"oid": "b", "node": "10.0.0.2"}]


class TestMapStageRun(unittest.TestCase):
    @patch("dlg.translator.stages.map.stage.resource_map")
    def test_run_delegates_to_pg_generator_resource_map(self, mock_resource_map):
        mock_resource_map.return_value = pg_drops()
        pgtp = PhysicalGraphTemplatePartitioned.from_wire(pgtp_wire())

        result = MapStage(MapOptions(nodes=["10.0.0.1", "10.0.0.2"])).run(pgtp)

        mock_resource_map.assert_called_once()
        _, kwargs = mock_resource_map.call_args
        self.assertEqual(kwargs["pgt"], pgtp.drops)
        self.assertEqual(kwargs["nodes"], ["10.0.0.1", "10.0.0.2"])
        self.assertEqual(kwargs["num_islands"], 1)
        self.assertTrue(kwargs["co_host_dim"])

        self.assertIsInstance(result, PhysicalGraph)
        self.assertEqual(result.drops, pg_drops())
        self.assertEqual(result.reprodata, pgtp.reprodata)

    @patch("dlg.translator.stages.map.stage.resource_map")
    def test_run_passes_through_map_options(self, mock_resource_map):
        mock_resource_map.return_value = pg_drops()
        pgtp = PhysicalGraphTemplatePartitioned.from_wire(pgtp_wire())
        opts = MapOptions(nodes=["10.0.0.1"], num_islands=3, co_host_dim=False)

        MapStage(opts).run(pgtp)

        _, kwargs = mock_resource_map.call_args
        self.assertEqual(kwargs["nodes"], ["10.0.0.1"])
        self.assertEqual(kwargs["num_islands"], 3)
        self.assertFalse(kwargs["co_host_dim"])

    @patch("dlg.translator.stages.map.stage.resource_map")
    def test_run_hands_resource_map_a_copy_not_the_original(self, mock_resource_map):
        # resource_map() is documented as mutating its pgt argument in place
        # (stage.py wraps it in deepcopy for exactly this reason) -- run()
        # must never pass the PhysicalGraphTemplatePartitioned's own drops.
        mock_resource_map.side_effect = lambda pgt, **_: pgt
        pgtp = PhysicalGraphTemplatePartitioned.from_wire(pgtp_wire())

        MapStage(MapOptions(nodes=["10.0.0.1"])).run(pgtp)

        _, kwargs = mock_resource_map.call_args
        self.assertIsNot(kwargs["pgt"], pgtp.drops)

    def test_name_is_map(self):
        self.assertEqual(MapStage.name, "map")


class TestMapStageStamp(unittest.TestCase):
    @patch("dlg.translator.stages.map.stage.init_pg_repro_data")
    def test_stamp_delegates_to_reproducibility_hook(self, mock_stamp):
        stamped_wire = [{"oid": "a", "reprodata": {"stamped": True}}, {}]
        mock_stamp.return_value = stamped_wire
        pg = PhysicalGraph.from_wire([*pg_drops(), {"repro": "pg-reprodata"}])

        result = MapStage(MapOptions(nodes=["10.0.0.1"])).stamp(pg)

        mock_stamp.assert_called_once_with(pg.to_wire())
        self.assertIsInstance(result, PhysicalGraph)
        self.assertEqual(result.to_wire(), stamped_wire)

    @patch("dlg.translator.stages.map.stage.init_pg_repro_data")
    def test_stamp_hands_the_hook_a_copy_not_the_original(self, mock_stamp):
        # The hook mutates its argument in place -- stamp() must round-trip
        # through to_wire() so the PG passed in is never the same object
        # the hook mutates.
        mock_stamp.side_effect = lambda wire: wire
        pg = PhysicalGraph.from_wire([*pg_drops(), {"repro": "pg-reprodata"}])

        MapStage(MapOptions(nodes=["10.0.0.1"])).stamp(pg)

        (passed_wire,), _ = mock_stamp.call_args
        self.assertIsNot(passed_wire, pg.drops)


class TestPGMapRegression(unittest.TestCase):
    SARKAR_PARTITION_RESULTS_GEN_ISLAND = {
        "testLoop.graph": {
            'algo': 'Edge Zero',
            'min_exec_time': 30,
            'total_data_movement': 0,
            'exec_time': 30, 'num_parts': 1
        },
        "cont_img_mvp.graph": {
            'algo': 'Edge Zero', 'min_exec_time': 144, 'total_data_movement': 135,
            'exec_time': 179, 'num_islands': 2, 'num_parts': 4
        },
        "test_grpby_gather.graph": {
            'algo': 'Edge Zero', 'min_exec_time': 16,
            'total_data_movement': 0, 'exec_time': 16, 'num_parts': 1
        },
        "chiles_simple.graph": {
            'algo': 'Edge Zero', 'min_exec_time': 45, 'total_data_movement': 0,
            'exec_time': 45, 'num_parts': 1
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

    def test_metis_pgtp_gen_pg(self):
        """
        Regression testing to confirm that basic METIS partitioning works,
        then generating a PGT spec works when using multiple nodes.

        We check that the partition result before differs from the result achieved
        after translating to the pg_spec, as this involves partitioning and should
        result in speed up.
        """
        node_list = ["10.128.0.11", "10.128.0.11", "10.128.0.12", "10.128.0.13"]
        total_data_movement_pgspec = {
            "testLoop.graph": 10,
            "cont_img_mvp.graph": 45,
            "test_grpby_gather.graph": 20,
            "chiles_simple.graph": 20,
        }
        for lg_name in self.partitionMethodLGs:
            fp = path_utils.get_lg_fpath('logical_graphs', lg_name)
            lg = LG(fp)
            drop_list = lg.unroll_to_tpl()
            pgtp = MetisPGTP(drop_list, 3, merge_parts=True)
            result = pgtp.result()
            self.assertIsNone(result['total_data_movement'],
                              f"Incorrect partitioning for: {lg_name}")
            self.assertEqual(3, result['num_parts'])
            pgtp.to_gojs_json(visual=False)
            pgtp.to_pg_spec(node_list)
            result = pgtp.result()
            self.assertEqual(total_data_movement_pgspec[lg_name],
                             result['total_data_movement'],
                             f"Incorrect partitioning for: {lg_name}")

    def test_metis_pgtp_gen_pg_island(self):
        """
        Regression testing to confirm that partitioning, then generating a PGT spec works
        when using multiple nodes and 2 data islands.
        """
        node_list = [
            "10.128.0.11",
            "10.128.0.12",
            "10.128.0.13",
            "10.128.0.14",
            "10.128.0.15",
            "10.128.0.16",
        ]
        nb_islands = 2
        nb_nodes = len(node_list) - nb_islands
        for lg_name in self.partitionMethodLGs:
            fp = path_utils.get_lg_fpath('logical_graphs', lg_name)
            lg = LG(fp)
            drop_list = lg.unroll_to_tpl()
            pgtp = MetisPGTP(drop_list, nb_nodes, merge_parts=True)
            self.assertFalse('num_islands' in pgtp.result())
            pgtp.to_gojs_json(visual=False)
            pgtp.to_pg_spec(node_list, num_islands=nb_islands)
            self.assertTrue('num_islands' in pgtp.result(),
                            f"No islands in PG spec for: {lg_name}")
            self.assertEqual(2, pgtp.result()['num_islands'],
                             f"Incorrect number of islands in PG spec for: {lg_name}")

    def test_mysarkar_pgtp_gen_pg(self):
        """
        Regression testing to confirm that basic Sarkar partitioning, then generating a
        PGT spec works when using multiple nodes.

        We check that the partition result before differs from the result achieved
        after translating to the pg_spec, as this involves partitioning and should
        result in speed up.
        """
        node_list = ["10.128.0.11", "10.128.0.12", "10.128.0.13"]
        for lg_name in self.partitionMethodLGs:
            fp = path_utils.get_lg_fpath('logical_graphs', lg_name)
            lg = LG(fp)
            drop_list = lg.unroll_to_tpl()
            pgtp = MySarkarPGTP(drop_list, 3, merge_parts=True)
            pre_spec_result = pgtp.result()
            pgtp.to_gojs_json(visual=False)
            pgtp.to_pg_spec(node_list)
            # Confirm that partitioning improves the execution speed.
            self.assertGreater(
                pre_spec_result['exec_time'], pgtp.result()['exec_time'],
                f"Partition of {lg_name} should cause speed up, but this did not occur.")

    def test_mysarkar_pgtp_gen_pg_island(self):
        """
        Regression testing to confirm that partitioning, then generating a PGT spec works
        when using multiple nodes and 2 data islands.
        """
        node_list = [
            "10.128.0.11",
            "10.128.0.12",
            "10.128.0.13",
            "10.128.0.14",
            "10.128.0.15",
            "10.128.0.16",
        ]
        nb_islands = 2
        new_num_parts = len(node_list) - nb_islands
        for lg_name in self.partitionMethodLGs:
            fp = path_utils.get_lg_fpath('logical_graphs', lg_name)
            lg = LG(fp, ssid=TEST_SSID)
            drop_list = lg.unroll_to_tpl()
            pgtp = MySarkarPGTP(drop_list, None, merge_parts=True)
            pgtp.to_gojs_json(visual=True, string_rep=False)

            if lg_name != "cont_img_mvp.graph":
                self.assertRaises(
                    GPGTNoNeedMergeException,
                    pgtp.merge_partitions,
                    new_num_parts,
                    False,
                    f"Exception was not raised for: {lg_name}")
                partition_results = pgtp.result()
                self.assertEqual(self.SARKAR_PARTITION_RESULTS_GEN_ISLAND[lg_name],
                                 partition_results)
            else:
                pgtp.merge_partitions(len(node_list) - nb_islands, form_island=False)
                pgtp.to_pg_spec(node_list, num_islands=nb_islands)
                self.assertEqual(self.SARKAR_PARTITION_RESULTS_GEN_ISLAND[lg_name],
                                 pgtp.result(),
                                 f"Incorrect partition results for: {lg_name}")


if __name__ == "__main__":
    unittest.main()
