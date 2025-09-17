#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2025
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
Test that we correctly convert from OJS to V4
"""

import json
import jsonschema
import os
import unittest

import dlg.dropmake.schema as sc
from dlg.dropmake.ojs_converter import (field_list_to_dict, node_list_to_dict,
                                        convert_ojs_to_v4, edge_list_to_dict)

fileA_OJS = os.path.expandvars("$HOME/github/lg-dict/HelloWorld_simple.graph")
fileB_OJS = os.path.expandvars("$HOME/github/lg-dict/ArrayLoop.graph")

fileA_v4 = os.path.expandvars("$HOME/github/lg-dict/HelloWorld_simple_dict.graph")
fileB_v4 = os.path.expandvars("$HOME/github/lg-dict/ArrayLoop_dict.graph")

lgschema = os.path.expandvars("$HOME/github/EAGLE/static/lg.graph.v4.schema")

def load_json(path: str):
    with open(path) as fp:
        return json.load(fp)

class TestOJStoV4ConverstionSimple(unittest.TestCase):

    def setUp(self):
        self.ojs = load_json(fileA_OJS)
        self.actual_v4 = load_json(fileA_v4)

    def test_field_conversion(self):
        node = self.ojs[sc.OJS_NODES].pop()
        fields = field_list_to_dict(node[sc.FIELDS])
        fields_v4 = self.actual_v4[sc.V4_NODES][node['id']][sc.FIELDS]
        for f, d in fields.items():
            self.assertEqual(fields_v4[f], d)

    def test_node_conversion(self):
        v4_nodes = node_list_to_dict(self.ojs[sc.OJS_NODES])
        self.assertEqual(self.actual_v4[sc.V4_NODES], v4_nodes)


    def test_edge_conversion(self):
        v4_edges = edge_list_to_dict(self.ojs[sc.OJS_EDGES])
        self.assertEqual(len(self.actual_v4[sc.V4_EDGES]), len(v4_edges))


class TestOJStoV4ConverstionComplex(unittest.TestCase):
    """
    Performs the same sequence of tests as Simple test above, but uses a more complex
    logical graph that uses Loops and Scatters.
    """

    def setUp(self):
        self.ojs = load_json(fileB_OJS)
        self.actual_v4 = load_json(fileB_v4)

    def test_field_conversion(self):
        node = self.ojs[sc.OJS_NODES].pop()
        fields = field_list_to_dict(node[sc.FIELDS])
        fields_v4 = self.actual_v4[sc.V4_NODES][node['id']][sc.FIELDS]
        for f, d in fields.items():
            self.assertEqual(fields_v4[f], d)

    def test_node_conversion(self):
        v4_nodes = node_list_to_dict(self.ojs[sc.OJS_NODES])
        self.assertEqual(self.actual_v4[sc.V4_NODES], v4_nodes)


    def test_edge_conversion(self):
        v4_edges = edge_list_to_dict(self.ojs[sc.OJS_EDGES])
        self.assertEqual(len(self.actual_v4[sc.V4_EDGES]), len(v4_edges))


