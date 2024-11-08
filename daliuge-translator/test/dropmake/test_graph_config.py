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


def get_graphconfig_fname(lg_name: str):
    """
    Return the graph_config from the test repository 
    """
    return str(files(test_graphs) / f"graph_config/{lg_name}")

def get_value_from_lgnode_field(node_id: str, field_id: str, logical_graph: dict) -> str:
    """
    Helper function that retrieves the value of a specified field from a given node

    Returns: str representation of the value
    """
    idx = get_key_idx_from_list(node_id, logical_graph["nodeDataArray"])
    field_idx = get_key_idx_from_list(field_id, logical_graph["nodeDataArray"][idx]["fields"])
    return logical_graph["nodeDataArray"][idx]["fields"][field_idx]["value"]

class TestGraphConfig(unittest.TestCase):
    """
    """

    def test_apply_with_no_config(self):
        pass

    def test_apply_with_loop(self):
        """
        ArrayLoopLoop.graph has a GraphConfig with modified "num_of_iter" field in the 
        "Loop" node (construct). 
        """
        fname = get_graphconfig_fname("ArrayLoopLoop.graph")
        with open(fname, "r") as fp:
            lg = json.load(fp)
        node_id = "732df21b-f714-4d25-9773-b4169db270a0"
        field_id = "3d25fcc9-50bb-4bbc-9b19-2eceabc238f2"
        value = get_value_from_lgnode_field(node_id, field_id, lg)
        self.assertEqual(1, int(value))
        lg = apply_active_configuration(lg)
        value = get_value_from_lgnode_field(node_id, field_id, lg)
        self.assertEqual(5, int(value))

    def test_apply_with_scatter(self):
        fname = get_graphconfig_fname("ArrayLoopScatter.graph")
        with open(fname, "r") as fp:
            lg = json.load(fp)
        node_id = "732df21b-f714-4d25-9773-b4169db270a0"
        field_id = "5560c56a-10f3-4d42-a436-404816dddf5f"
        value = get_value_from_lgnode_field(node_id, field_id, lg)
        self.assertEqual(1, int(value))
        lg = apply_active_configuration(lg)
        value = get_value_from_lgnode_field(node_id, field_id, lg)
        self.assertEqual(5, int(value))

class TestGraphUnrollWithconfig(unittest.TestCase):
    """
    """