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
import os
import unittest

from dlg.dropmake.ojs_converter import convert_ojs_to_v4

fileA_OJS = os.path.expandvars("$HOME/github/lg-dict/HelloWorld_simple_dict.graph")
fileB_OJS = os.path.expandvars("$HOME/github/lg-dict/ArrayLoop_dict.graph")

fileA_v4 = os.path.expandvars("$HOME/github/lg-dict/HelloWorld_simple_dict.graph")
fileB_v4 = os.path.expandvars("$HOME/github/lg-dict/ArrayLoop_dict.graph")

def load_json(path: str):
    with open(path) as fp:
        return json.load(fp)

class TestOJStoV4Converstion(unittest.TestCase):

    def setUp(self):
        self.ojs = load_json(fileA_OJS)
        self.actual_v4 = load_json(fileA_v4)


    def test_node_conversion(self):


    def test_full_conversion(self):
        """
        Use HelloWorldApp to confirm that the structure is successfully converted
        :return:
        """

        v4 = convert_ojs_to_v4(self.ojs)
        self.assertEqual(self.actual_v4, v4)