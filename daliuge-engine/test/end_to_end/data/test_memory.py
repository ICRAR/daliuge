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
Test runtime behaviour of MemoryDROP

This acts as both a feature and regression test suite.
"""
import dill
import pytest
import unittest
pexpect = pytest.importorskip("dlg.dropmake")

from importlib.resources import files
from test.dlg_end_to_end_utils import create_and_run_graph_spec, translate_graph

from daliuge_tests.engine import graphs


def helloDALiuGE():
    return "Hello DALiuGE"

TYPE_MAP = {
    "int": {"type": int, "value":11},
    "string": {"type": str, "value":'11'},
    "list": {"type": list, "value":[11, 12, 13]},
    "dict": {"type": dict, "value":{"a": 1, "b": "test", "c": 0.0}},
    "float": {"type": float, "value":11.1},
    "object": {"type": type(helloDALiuGE), "value": helloDALiuGE()},
    "true": {"type": bool, "value":True},
    "false": {"type": bool, "value":False}
}

class TestMemory(unittest.TestCase):
    """
    Given a dropspec, make sure the ports are loaded correctly.
    """

    def test_memory_types(self):
        """
        We run the pydata_types.graph and confirm:
            - The types are as expected (derived from the filename)
            - The values are what we expected
        """
        f = files(graphs)/'pydata_types.graph'
        g = translate_graph(str(f), 'pydata_types')

        roots, leafs = create_and_run_graph_spec(self, g)
        for l in leafs:
            expected_type = l.filename.split(".")[0]
            if expected_type == "string":
                with open(l.path, "r") as fp:
                    value = fp.read()
                    self.assertEqual(TYPE_MAP[expected_type]['type'], type(value))
                    self.assertEqual(TYPE_MAP[expected_type]["value"], value)
            else:
                with open(l.path, "rb") as fp:
                    value = dill.load(fp)
                    if expected_type != "object":
                        self.assertEqual(TYPE_MAP[expected_type]["type"], type(value))
                        self.assertEqual(TYPE_MAP[expected_type]["value"], value)
                    else:
                        self.assertEqual(TYPE_MAP[expected_type]["type"], type(value))
                        self.assertEqual(TYPE_MAP[expected_type]["value"], value())
