"""Tests for partition placeholder generation."""

import unittest

from dlg.translator.stages.partition.placeholders import (
    placeholder_list,
    prepare_node_list,
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


if __name__ == "__main__":
    unittest.main()
