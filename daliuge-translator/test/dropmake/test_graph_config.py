#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2015
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

import json
import unittest
from dlg.dropmake.graph_config import apply_active_configuration, get_key_idx_from_list
import daliuge_tests.dropmake as test_graphs

try:
    from importlib.resources import files, as_file
except ModuleNotFoundError:
    from importlib_resources import files

LOG_PRFIX = "WARNING:dlg.dlg.dropmake.graph_config:"


def get_lg_from_fname(lg_name: str) -> dict:
    """
    Return the logical graph from the graph_config files in the test graph repository
    """
    fname = str(files(test_graphs) / f"graph_config/{lg_name}")
    with open(fname, "r") as fp:
        return json.load(fp)


def get_value_from_lgnode_field(
    node_id: str, field_id: str, logical_graph: dict
) -> str:
    """
    Helper function that retrieves the value of a specified field from a given node

    Returns: str representation of the value
    """
    idx = get_key_idx_from_list(node_id, logical_graph["nodeDataArray"])
    field_idx = get_key_idx_from_list(
        field_id, logical_graph["nodeDataArray"][idx]["fields"]
    )
    return logical_graph["nodeDataArray"][idx]["fields"][field_idx]["value"]


class TestGraphConfig(unittest.TestCase):
    """ """

    def test_apply_with_empty_config(self):
        """
        Exhaust the following possibilities that may exist when attempting to
        apply graph config:
            - "activeGraphConfigId" is not in graph
            - "activeGraphConfigId" is empty
            - "graphConfigurations" is not in graph
            - "graphConfigurations" is empty
            - Keys from the activeGraphConfigId or graphConfigurations are not found in
            the logical graph.
        """
        lg = get_lg_from_fname("ArrayLoopNoActiveID.graph")
        with self.assertLogs("root", level="WARNING") as cm:
            alt_lg = apply_active_configuration(lg)
        self.assertEqual(
            [
                f"{LOG_PRFIX}No activeGraphConfigId data available in Logical Graph.",
                f"{LOG_PRFIX}No graphConfigurations data available in Logical Graph.",
            ],
            cm.output,
        )

        lg["activeGraphConfigId"] = ""
        with self.assertLogs("root", level="WARNING") as cm:
            alt_lg = apply_active_configuration(lg)
        self.assertEqual(
            [
                f"{LOG_PRFIX}No activeGraphConfigId data available in Logical Graph.",
                f"{LOG_PRFIX}No graphConfigurations data available in Logical Graph.",
            ],
            cm.output,
        )

        lg["activeGraphConfigId"] = "temporaryString"

        with self.assertLogs("root", level="WARNING") as cm:
            alt_lg = apply_active_configuration(lg)
        self.assertEqual(
            [f"{LOG_PRFIX}No graphConfigurations data available in Logical Graph."],
            cm.output,
        )

        lg["graphConfigurations"] = {"key": "value"}

        with self.assertLogs("root", level="WARNING") as cm:
            alt_lg = apply_active_configuration(lg)
            self.assertEqual(
                [
                    f"{LOG_PRFIX}Graph config key does not exist in logical graph. "
                    "Using base field values."
                ],
                cm.output,
            )

    def test_apply_with_loop(self):
        """
        ArrayLoopLoop.graph has a GraphConfig with modified "num_of_iter" field in the
        "Loop" node (construct).
        """
        lg = get_lg_from_fname("ArrayLoopLoop.graph")
        node_id = "732df21b-f714-4d25-9773-b4169db270a0"
        field_id = "3d25fcc9-50bb-4bbc-9b19-2eceabc238f2"
        value = get_value_from_lgnode_field(node_id, field_id, lg)
        self.assertEqual(1, int(value))
        lg = apply_active_configuration(lg)
        value = get_value_from_lgnode_field(node_id, field_id, lg)
        self.assertEqual(5, int(value))

    def test_apply_with_scatter(self):
        """
        ArrayLoopLoop.graph has a GraphConfig with modified "num_of_copies" field in the
        "Scatter" node (construct).
        """
        lg = get_lg_from_fname("ArrayLoopScatter.graph")
        node_id = "5560c56a-10f3-4d42-a436-404816dddf5f"
        field_id = "0057b318-2405-4b5b-ac59-06c0068f91b7"
        value = get_value_from_lgnode_field(node_id, field_id, lg)
        self.assertEqual(1, int(value))
        lg = apply_active_configuration(lg)
        value = get_value_from_lgnode_field(node_id, field_id, lg)
        self.assertEqual(5, int(value))
