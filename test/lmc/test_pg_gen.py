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

import unittest, os, pkg_resources
import pprint

from dfms.lmc.pg_generator import LGNode, LG
from collections import defaultdict

class TestPGGen(unittest.TestCase):

    def test_pg_generator(self):
        fp = pkg_resources.resource_filename('dfms.lg', 'web/lofar_std.json')
        #fp = '/Users/Chen/proj/dfms/dfms/lg/web/lofar_std.json'
        lg = LG(fp)
        self.assertEquals(len(lg._done_dict.keys()), 35)
        lg.unroll_to_tpl()
        #pprint.pprint(dict(lg._drop_dict))
        #input_dict = defaultdict(list)
        #lg.to_pg_tpl(input_dict)

    def test_pg_test(self):
        fp = pkg_resources.resource_filename('dfms.lg', 'web/lofar_cal.json')
        #fp = '/Users/Chen/proj/dfms/dfms/lg/web/lofar_cal.json'
        lg = LG(fp)
        lg.unroll_to_tpl()
        #input_dict = defaultdict(list)
        #lg.to_pg_tpl(input_dict)
        pprint.pprint(dict(lg._drop_dict))
