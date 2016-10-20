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

from dfms.dropmake.pg_generator import LG, PGT, MetisPGTP, MySarkarPGTP, MinNumPartsPGTP

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
            pgtp = MetisPGTP(drop_list, 3)
            pgtp.json
            pg_spec = pgtp.to_pg_spec(node_list)
            # with open('/tmp/met_{0}_pgspec.json'.format(lgn.split('.')[0]), 'w') as f:
            #     f.write(pg_spec)

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
            pgtp.json
            pg_spec = pgtp.to_pg_spec(node_list)
            # with open('/tmp/sar_{0}_pgspec.json'.format(lgn.split('.')[0]), 'w') as f:
            #     f.write(pg_spec)
            # if (i == 0):
            #     from dfms.manager.client import DataIslandManagerClient
            #     dmc = DataIslandManagerClient(host='sdp-dfms.ddns.net', port=8097)
            #     ssid = 'ChenICRAR'
            #     dmc.create_session(ssid)
            #     dmc.append_graph(ssid, pg_spec)
            #     ret = dmc.deploy_session(ssid)
            #     print ret

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