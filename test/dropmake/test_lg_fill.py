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

import unittest
import os
import pkg_resources

from dlg.dropmake import pg_generator
import json

lg_dir = pkg_resources.resource_filename(__name__, 'logical_graphs')  # @UndefinedVariable

class LGFillTest(unittest.TestCase):

    def test_fill_lg(self):
        params = {
            'param1': 1,
            'param2': '2',
            'param1.param2': True,
            'param4': {
                'what': 'hi'
            }
        }
        with open(os.path.join(lg_dir, 'chiles_simple.json')) as f:
            lg = pg_generator.fill(json.load(f), params)
        for node_idx, value in zip((20, 21, 22, 23), ('1', '2', 'True', 'hi')):
            self.assertEqual(lg['nodeDataArray'][node_idx]['Arg10'], value)