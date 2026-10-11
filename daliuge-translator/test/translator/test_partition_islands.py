"""Tests for partition island preparation."""

import unittest
from unittest.mock import Mock

from dlg.translator.stages.partition.islands import (
    merge_for_islands,
    split_island_resources,
)


class TestPartitionIslands(unittest.TestCase):
    def test_resources_are_split_between_islands_and_nodes(self):
        island_managers, node_managers, form_island = (
            split_island_resources(
                [
                    "island-a",
                    "island-b",
                    "node-a",
                    "node-b",
                ],
                2,
            )
        )

        self.assertEqual(
            island_managers,
            [
                "island-a",
                "island-b",
            ],
        )
        self.assertEqual(
            node_managers,
            [
                "node-a",
                "node-b",
            ],
        )
        self.assertTrue(form_island)

    def test_single_island_does_not_form_island_partitions(self):
        island_managers, node_managers, form_island = (
            split_island_resources(
                [
                    "island",
                    "node-a",
                    "node-b",
                ],
                1,
            )
        )

        self.assertEqual(
            island_managers,
            ["island"],
        )
        self.assertEqual(
            node_managers,
            [
                "node-a",
                "node-b",
            ],
        )
        self.assertFalse(form_island)

    def test_multiple_islands_merge_partitions_by_island_count(self):
        merge_partitions = Mock()

        merge_for_islands(
            num_islands=2,
            num_parts_done=4,
            node_managers=[
                "node-a",
                "node-b",
                "node-c",
                "node-d",
            ],
            merge_partitions=merge_partitions,
        )

        merge_partitions.assert_called_once_with(
            2,
            form_island=True,
        )

    def test_single_island_merges_when_nodes_are_insufficient(self):
        merge_partitions = Mock()

        merge_for_islands(
            num_islands=1,
            num_parts_done=3,
            node_managers=[
                "node-a",
                "node-b",
            ],
            merge_partitions=merge_partitions,
        )

        merge_partitions.assert_called_once_with(
            2,
            form_island=False,
        )

    def test_single_island_does_not_merge_when_nodes_are_sufficient(self):
        merge_partitions = Mock()

        merge_for_islands(
            num_islands=1,
            num_parts_done=2,
            node_managers=[
                "node-a",
                "node-b",
            ],
            merge_partitions=merge_partitions,
        )

        merge_partitions.assert_not_called()


if __name__ == "__main__":
    unittest.main()
