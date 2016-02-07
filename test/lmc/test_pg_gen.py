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

import unittest, os, pkg_resources, json
import pprint
import networkx as nx

from dfms.lmc.pg_generator import LGNode, LG, PGT, MetisPGTP, PyrrosPGTP, MySarkarPGTP
from dfms.lmc.scheduler import Scheduler, MySarkarScheduler, DAGUtil, Partition
from collections import defaultdict

class TestPGGen(unittest.TestCase):

    def test_pg_generator(self):
        fp = pkg_resources.resource_filename('dfms.lg', 'web/lofar_std.json')
        #fp = '/Users/Chen/proj/dfms/dfms/lg/web/lofar_std.json'
        lg = LG(fp)
        self.assertEquals(len(lg._done_dict.keys()), 33)
        drop_list = lg.unroll_to_tpl()
        #print json.dumps(drop_list, indent=2)
        #pprint.pprint(drop_list)
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
        #pprint.pprint(dict(lg._drop_dict))

    def test_pgt_to_json(self):
        fp = pkg_resources.resource_filename('dfms.lg', 'web/lofar_std.json')
        lg = LG(fp)
        drop_list = lg.unroll_to_tpl()
        pgt = PGT(drop_list)
        #print pgt.to_gojs_json()

    def test_metis_pgtp(self):
        lgnames = ['lofar_std.json', 'chiles_two.json', 'lofar_cal.json', 'chiles_two_dev1.json', 'chiles_simple.json']
        tgt_partnum = [15, 15, 10, 10, 5]
        for i, lgn in enumerate(lgnames):
            fp = pkg_resources.resource_filename('dfms.lg', 'web/{0}'.format(lgn))
            lg = LG(fp)
            drop_list = lg.unroll_to_tpl()
            pgtp = MetisPGTP(drop_list)
            pgtp.json

    def test_mysarkar_pgtp(self):
        lgnames = ['lofar_std.json', 'chiles_two.json', 'lofar_cal.json', 'chiles_two_dev1.json', 'chiles_simple.json']
        tgt_partnum = [15, 15, 10, 10, 5]
        for i, lgn in enumerate(lgnames):
            fp = pkg_resources.resource_filename('dfms.lg', 'web/{0}'.format(lgn))
            lg = LG(fp)
            drop_list = lg.unroll_to_tpl()
            pgtp = MySarkarPGTP(drop_list)
            pgtp.json
        #print pgtp.to_gojs_json()
