"""Tests for partition placeholder generation."""

import unittest

from dlg.translator.stages.partition.placeholders import (
    placeholder_list,
    prepare_node_list,
    stamp_placeholders,
)


class TestPartitionPlaceholders(unittest.TestCase):
    def test_empty_node_list_builds_temporary_mapping_space(self):
        node_list, is_template = prepare_node_list(
            [],
            tpl_nodes_len=3,
            num_islands=2,
        )

        self.assertEqual(
            node_list,
            [0, 1, 2, 3, 4],
        )
        self.assertTrue(is_template)

    def test_existing_node_list_is_preserved(self):
        original = [
            "island.example",
            "node.example",
        ]

        node_list, is_template = prepare_node_list(
            original,
            tpl_nodes_len=3,
            num_islands=1,
        )

        self.assertIs(node_list, original)
        self.assertFalse(is_template)

    def test_zero_template_length_does_not_create_placeholders(self):
        node_list, is_template = prepare_node_list(
            [],
            tpl_nodes_len=0,
            num_islands=1,
        )

        self.assertEqual(node_list, [])
        self.assertFalse(is_template)

    def test_placeholder_format_is_preserved_verbatim(self):
        self.assertEqual(
            placeholder_list(4),
            [
                "#0",
                "#1",
                "#2",
                "#3",
            ],
        )


class TestStampPlaceholders(unittest.TestCase):
    def test_stamps_nodes_and_islands_from_partition_maps(self):
        drops = [
            {"oid": "a", "iid": 0},
            {"oid": "b", "iid": 1},
        ]

        stamp_placeholders(
            drops,
            num_node_managers=2,
            num_island_managers=2,
            oid_gid_map={
                "a": 0,
                "b": 1,
            },
            gid_island_id_map={
                0: 0,
                1: 3,
            },
            num_islands=2,
            form_island=True,
        )

        self.assertEqual(
            [drop["node"] for drop in drops],
            ["#0", "#1"],
        )
        self.assertEqual(
            [drop["island"] for drop in drops],
            ["#0", "#1"],
        )

    def test_single_island_uses_first_island_placeholder(self):
        drops = [
            {"oid": "a", "iid": 0},
            {"oid": "b", "iid": 0},
        ]

        stamp_placeholders(
            drops,
            num_node_managers=2,
            num_island_managers=1,
            oid_gid_map={
                "a": 0,
                "b": 1,
            },
            gid_island_id_map={
                0: 4,
                1: 9,
            },
            num_islands=1,
            form_island=False,
        )

        self.assertEqual(
            [drop["island"] for drop in drops],
            ["#0", "#0"],
        )

    def test_partition_beyond_node_count_raises(self):
        drops = [{"oid": "a", "iid": 0}]

        with self.assertRaises(IndexError):
            stamp_placeholders(
                drops,
                num_node_managers=1,
                num_island_managers=1,
                oid_gid_map={"a": 1},
                gid_island_id_map={1: 0},
                num_islands=1,
                form_island=False,
            )


if __name__ == "__main__":
    unittest.main()
