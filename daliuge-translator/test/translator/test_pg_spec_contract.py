"""Contract tests for the legacy PGT.to_pg_spec facade."""

import json
import unittest
from unittest.mock import patch

from dlg.translator.stages.partition.pgt import PGT


def _pgt():
    drops = [
        {
            "oid": "a",
            "name": "a",
            "categoryType": "Application",
            "iid": 0,
        },
        {
            "oid": "b",
            "name": "b",
            "categoryType": "Data",
            "iid": 0,
        },
    ]

    pgt = PGT(drops, build_dag=False)
    pgt._num_parts_done = 2
    pgt._oid_gid_map = {
        "a": 0,
        "b": 1,
    }
    pgt._gid_island_id_map = {
        0: 0,
        1: 0,
    }
    return pgt


class TestPGSpecContract(unittest.TestCase):
    def test_template_mapping_preserves_placeholder_convention(self):
        pgt = _pgt()

        result = pgt.to_pg_spec(
            [],
            ret_str=False,
            num_islands=1,
            tpl_nodes_len=2,
        )

        self.assertEqual(
            result,
            [
                {
                    "oid": "a",
                    "name": "a",
                    "categoryType": "Application",
                    "iid": 0,
                    "node": "#0",
                    "island": "#0",
                },
                {
                    "oid": "b",
                    "name": "b",
                    "categoryType": "Data",
                    "iid": 0,
                    "node": "#1",
                    "island": "#0",
                },
            ],
        )

    def test_mapping_preserves_real_node_and_island_names(self):
        pgt = _pgt()

        result = pgt.to_pg_spec(
            [
                "dim.example",
                "node-a.example",
                "node-b.example",
            ],
            ret_str=False,
            num_islands=1,
        )

        self.assertEqual(
            [drop["node"] for drop in result],
            [
                "node-a.example",
                "node-b.example",
            ],
        )
        self.assertEqual(
            [drop["island"] for drop in result],
            [
                "dim.example",
                "dim.example",
            ],
        )

    def test_island_formation_delegates_to_partition_merging(self):
        pgt = _pgt()

        def form_islands(new_num_parts, form_island=False):
            self.assertEqual(new_num_parts, 2)
            self.assertTrue(form_island)
            pgt._gid_island_id_map = {
                0: 0,
                1: 1,
            }

        with patch.object(
            pgt,
            "merge_partitions",
            side_effect=form_islands,
        ) as merge_partitions:
            result = pgt.to_pg_spec(
                [
                    "island-a.example",
                    "island-b.example",
                    "node-a.example",
                    "node-b.example",
                ],
                ret_str=False,
                num_islands=2,
            )

        merge_partitions.assert_called_once_with(
            2,
            form_island=True,
        )
        self.assertEqual(
            [drop["island"] for drop in result],
            [
                "island-a.example",
                "island-b.example",
            ],
        )

    def test_insufficient_nodes_delegates_to_physical_partition_merge(self):
        pgt = _pgt()

        def merge_nodes(new_num_parts, form_island=False):
            self.assertEqual(new_num_parts, 1)
            self.assertFalse(form_island)
            pgt._oid_gid_map = {
                "a": 0,
                "b": 0,
            }

        with patch.object(
            pgt,
            "merge_partitions",
            side_effect=merge_nodes,
        ) as merge_partitions:
            result = pgt.to_pg_spec(
                [
                    "dim.example",
                    "node.example",
                ],
                ret_str=False,
                num_islands=1,
            )

        merge_partitions.assert_called_once_with(
            1,
            form_island=False,
        )
        self.assertEqual(
            [drop["node"] for drop in result],
            [
                "node.example",
                "node.example",
            ],
        )

    def test_string_output_matches_drop_payload(self):
        pgt = _pgt()

        result = pgt.to_pg_spec(
            [],
            ret_str=True,
            num_islands=1,
            tpl_nodes_len=2,
        )

        self.assertEqual(
            json.loads(result),
            pgt.drops,
        )
        self.assertEqual(
            result,
            json.dumps(pgt.drops, indent=2),
        )


if __name__ == "__main__":
    unittest.main()
