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

import unittest, pkg_resources

from dlg.dropmake.pg_generator import LG, PGT, MetisPGTP, MySarkarPGTP,\
 MinNumPartsPGTP, GPGTNoNeedMergeException

"""
python -m unittest test.dropmake.test_pg_gen
"""

def get_lg_fname(lg_name):
    return pkg_resources.resource_filename(__name__, 'logical_graphs/{0}'.format(lg_name))  # @UndefinedVariable

class TestPGGen(unittest.TestCase):

    def test_pg_generator(self):
        fp = get_lg_fname('lofar_std.json')
        #fp = '/Users/Chen/proj/dfms/dfms/lg/web/lofar_std.json'
        lg = LG(fp)
        self.assertEqual(len(lg._done_dict.keys()), 36)
        drop_list = lg.unroll_to_tpl()
        #print json.dumps(drop_list, indent=2)
        #pprint.pprint(drop_list)
        #pprint.pprint(dict(lg._drop_dict))
        #input_dict = defaultdict(list)
        #lg.to_pg_tpl(input_dict)

    def test_pg_test(self):
        fp = get_lg_fname('test_grpby_gather.json')
        lg = LG(fp)
        lg.unroll_to_tpl()
        #input_dict = defaultdict(list)
        #lg.to_pg_tpl(input_dict)
        #pprint.pprint(dict(lg._drop_dict))

    def test_pgt_to_json(self):
        fp = get_lg_fname('lofar_std.json')
        lg = LG(fp)
        drop_list = lg.unroll_to_tpl()
        pgt = PGT(drop_list)
        #print pgt.to_gojs_json()

    def test_metis_pgtp(self):
        lgnames = ['lofar_std.json', 'test_grpby_gather.json', 'chiles_simple.json']
        tgt_partnum = [15, 15, 10, 10, 5]
        for i, lgn in enumerate(lgnames):
            fp = get_lg_fname(lgn)
            lg = LG(fp)
            drop_list = lg.unroll_to_tpl()
            pgtp = MetisPGTP(drop_list)
            pgtp.json

    def test_metis_pgtp_gen_pg(self):
        lgnames = ['lofar_std.json', 'test_grpby_gather.json', 'chiles_simple.json']
        tgt_partnum = [15, 15, 10, 10, 5]
        node_list = ['10.128.0.11', '10.128.0.12', '10.128.0.13']
        for i, lgn in enumerate(lgnames):
            fp = get_lg_fname(lgn)
            lg = LG(fp)
            drop_list = lg.unroll_to_tpl()
            pgtp = MetisPGTP(drop_list, 3, merge_parts=True)
            #pgtp.json
            pgtp.to_gojs_json(visual=False)
            pg_spec = pgtp.to_pg_spec(node_list)
            # with open('/tmp/met_{0}_pgspec.json'.format(lgn.split('.')[0]), 'w') as f:
            #     f.write(pg_spec)

    def test_metis_pgtp_gen_pg_island(self):
        lgnames = ['lofar_std.json', 'test_grpby_gather.json', 'chiles_simple.json']
        tgt_partnum = [15, 15, 10, 10, 5]
        node_list = ['10.128.0.11', '10.128.0.12',
                     '10.128.0.13', '10.128.0.14',
                     '10.128.0.15', '10.128.0.16']
        nb_islands = 2
        nb_nodes = len(node_list) - nb_islands
        for i, lgn in enumerate(lgnames):
            fp = get_lg_fname(lgn)
            lg = LG(fp)
            drop_list = lg.unroll_to_tpl()
            pgtp = MetisPGTP(drop_list, nb_nodes, merge_parts=True)
            pgtp.to_gojs_json(visual=False)
            pg_spec = pgtp.to_pg_spec(node_list, num_islands=nb_islands)
            pgtp.result(lazy=False)

    def test_mysarkar_pgtp(self):
        lgnames = ['lofar_std.json', 'test_grpby_gather.json', 'chiles_simple.json']
        tgt_partnum = [15, 15, 10, 10, 5]
        for i, lgn in enumerate(lgnames):
            fp = get_lg_fname(lgn)
            lg = LG(fp)
            drop_list = lg.unroll_to_tpl()
            pgtp = MySarkarPGTP(drop_list)
            pgtp.json

    def test_mysarkar_pgtp_gen_pg(self):
        lgnames = ['lofar_std.json', 'test_grpby_gather.json', 'chiles_simple.json']
        tgt_partnum = [15, 15, 10, 10, 5]
        node_list = ['10.128.0.11', '10.128.0.12', '10.128.0.13']
        for i, lgn in enumerate(lgnames):
            fp = get_lg_fname(lgn)
            lg = LG(fp)
            drop_list = lg.unroll_to_tpl()
            pgtp = MySarkarPGTP(drop_list, 3, merge_parts=True)
            #pgtp.json
            pgtp.to_gojs_json(visual=False)
            pg_spec = pgtp.to_pg_spec(node_list)

    def test_mysarkar_pgtp_gen_pg_island(self):
        lgnames = ['lofar_std.json', 'test_grpby_gather.json', 'chiles_simple.json']
        node_list = ['10.128.0.11', '10.128.0.12',
                     '10.128.0.13', '10.128.0.14',
                     '10.128.0.15', '10.128.0.16']
        for i, lgn in enumerate(lgnames):
            fp = get_lg_fname(lgn)
            lg = LG(fp)
            drop_list = lg.unroll_to_tpl()
            pgtp = MySarkarPGTP(drop_list, None, merge_parts=True)
            pgtp.to_gojs_json(visual=False)
            nb_islands = 2
            #print(lgn)
            try:
                pgtp.merge_partitions(len(node_list) - nb_islands, form_island=False)
            except GPGTNoNeedMergeException as ge:
                continue
            pg_spec = pgtp.to_pg_spec(node_list, num_islands=nb_islands)
            pgtp.result()

    def test_minnumparts_pgtp(self):
        lgnames = ['lofar_std.json', 'test_grpby_gather.json', 'chiles_simple.json']
        #tgt_partnum = [15, 15, 10, 10, 5]
        tgt_deadline = [200, 300, 90, 80, 160]
        for i, lgn in enumerate(lgnames):
            fp = get_lg_fname(lgn)
            lg = LG(fp)
            drop_list = lg.unroll_to_tpl()
            pgtp = MinNumPartsPGTP(drop_list, tgt_deadline[i])
            pgtp.json

    def test_pg_eagle(self):
        lgs = ['eagle_gather_simple.json', 'eagle_gather_empty.json', 'eagle_gather.json']
        for lg in lgs:
            fp = get_lg_fname(lg)
            lg = LG(fp)
            lg.unroll_to_tpl()
