#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2019
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
import os
import unittest
import daliuge_tests.dropmake as test_graphs
from dlg.dropmake import pg_generator

from importlib.resources import files

lg_dir = str(files(test_graphs) / "logical_graphs")


# Test LGT to LG method: Filling parameter values in LG.
class LGFillTest(unittest.TestCase):
    def test_fill_lg(self):
        params = {
            "param1": 1,
            "param2": "2",
            "param1.param2": True,
            "param4": {"what": "hi"},
        }
        with open(os.path.join(lg_dir, "cont_img_mvp.graph")) as f:
            lg = pg_generator.fill(json.load(f), params)
        for node_idx, value in zip((5, 12, 26, 34), ("1", "2", "True", "hi")):
            print(node_idx)
            node = lg["nodeDataArray"][node_idx]
            found = None
            for field in node["fields"]:
                if field["name"] == "dummy":
                    found = field["value"]
            self.assertEqual(found, value)
