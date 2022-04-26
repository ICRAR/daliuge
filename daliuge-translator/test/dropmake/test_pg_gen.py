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

import unittest

import pkg_resources
from dlg.dropmake.lg import LG
from dlg.dropmake.pgt import PGT, GPGTNoNeedMergeException
from dlg.dropmake.pgtp import MetisPGTP, MySarkarPGTP, MinNumPartsPGTP

"""
python -m unittest test.dropmake.test_pg_gen
"""


def get_lg_fname(lg_name):
    return pkg_resources.resource_filename(
        __name__, "logical_graphs/{0}".format(lg_name)
    )  # @UndefinedVariable


class TestPGGen(unittest.TestCase):
    def test_pg_generator(self):
        fp = get_lg_fname("cont_img.graph")
        #        fp = get_lg_fname('testScatter.graph')
        lg = LG(fp)
        self.assertEqual(len(lg._done_dict.keys()), 46)
        drop_list = lg.unroll_to_tpl()
        # print json.dumps(drop_list, indent=2)
        # pprint.pprint(drop_list)
        # pprint.pprint(dict(lg._drop_dict))
        # input_dict = defaultdict(list)
        # lg.to_pg_tpl(input_dict)

    def test_pg_test(self):
        fp = get_lg_fname("test_grpby_gather.graph")
        lg = LG(fp)
        lg.unroll_to_tpl()
        # input_dict = defaultdict(list)
        # lg.to_pg_tpl(input_dict)
        # pprint.pprint(dict(lg._drop_dict))

    def test_pgt_to_json(self):
        fp = get_lg_fname("HelloWorld_simple.graph")
        lg = LG(fp)
        drop_list = lg.unroll_to_tpl()
        pgt = PGT(drop_list)
        pg_json = pgt.to_gojs_json()
        _dum = pg_json
        # we should really check the output here

    def test_metis_pgtp(self):
        lgnames = [
            "HelloWorld_simple.graph",
            "simpleMKN.graph",
            "testLoop.graph",
            "cont_img.graph",
            "test_grpby_gather.graph",
            "chiles_simple.graph",
        ]
        tgt_partnum = [15, 15, 10, 10, 5]
        for i, lgn in enumerate(lgnames):
            fp = get_lg_fname(lgn)
            lg = LG(fp)
            drop_list = lg.unroll_to_tpl()
            pgtp = MetisPGTP(drop_list)
            pgtp.json

    def test_metis_pgtp_gen_pg(self):
        lgnames = [
            "HelloWorld_simple.graph",
            "testLoop.graph",
            "cont_img.graph",
            "test_grpby_gather.graph",
            "chiles_simple.graph",
        ]
        tgt_partnum = [15, 15, 10, 10, 5]
        node_list = ["10.128.0.11", "10.128.0.12", "10.128.0.13"]
        for i, lgn in enumerate(lgnames):
            fp = get_lg_fname(lgn)
            lg = LG(fp)
            drop_list = lg.unroll_to_tpl()
            pgtp = MetisPGTP(drop_list, 3, merge_parts=True)
            # pgtp.json
            pgtp.to_gojs_json(visual=False)
            pg_spec = pgtp.to_pg_spec(node_list)
            # with open('/tmp/met_{0}_pgspec.graph'.format(lgn.split('.')[0]), 'w') as f:
            #     f.write(pg_spec)

    def test_metis_pgtp_gen_pg_island(self):
        lgnames = [
            "testLoop.graph",
            "cont_img.graph",
            "test_grpby_gather.graph",
            "chiles_simple.graph",
        ]
        tgt_partnum = [15, 15, 10, 10, 5]
        node_list = [
            "10.128.0.11",
            "10.128.0.12",
            "10.128.0.13",
            "10.128.0.14",
            "10.128.0.15",
            "10.128.0.16",
        ]
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
        lgnames = [
            "testLoop.graph",
            "cont_img.graph",
            "test_grpby_gather.graph",
            "chiles_simple.graph",
        ]
        tgt_partnum = [15, 15, 10, 10, 5]
        for i, lgn in enumerate(lgnames):
            fp = get_lg_fname(lgn)
            lg = LG(fp)
            drop_list = lg.unroll_to_tpl()
            pgtp = MySarkarPGTP(drop_list)
            pgtp.json

    def test_mysarkar_pgtp_gen_pg(self):
        # TODO: cont_img.graph causes random failures in this test.
        # ERROR: dlg.dropmake.scheduler.SchedulerException: Cannot find a idle PID, max_dop provided: 8

        # lgnames = ['testLoop.graph', 'cont_img.graph', 'test_grpby_gather.graph', 'chiles_simple.graph']
        lgnames = ["testLoop.graph", "test_grpby_gather.graph", "chiles_simple.graph"]
        tgt_partnum = [15, 15, 10, 10, 5]
        node_list = ["10.128.0.11", "10.128.0.12", "10.128.0.13"]
        for i, lgn in enumerate(lgnames):
            fp = get_lg_fname(lgn)
            lg = LG(fp)
            drop_list = lg.unroll_to_tpl()
            pgtp = MySarkarPGTP(drop_list, 3, merge_parts=True)
            # pgtp.json
            pgtp.to_gojs_json(visual=False)
            pg_spec = pgtp.to_pg_spec(node_list)

    def test_mysarkar_pgtp_gen_pg_island(self):
        lgnames = [
            "testLoop.graph",
            "cont_img.graph",
            "test_grpby_gather.graph",
            "chiles_simple.graph",
        ]
        node_list = [
            "10.128.0.11",
            "10.128.0.12",
            "10.128.0.13",
            "10.128.0.14",
            "10.128.0.15",
            "10.128.0.16",
        ]
        for i, lgn in enumerate(lgnames):
            fp = get_lg_fname(lgn)
            lg = LG(fp)
            drop_list = lg.unroll_to_tpl()
            pgtp = MySarkarPGTP(drop_list, None, merge_parts=True)
            pgtp.to_gojs_json(visual=False)
            nb_islands = 2
            # print(lgn)
            try:
                pgtp.merge_partitions(len(node_list) - nb_islands, form_island=False)
            except GPGTNoNeedMergeException as ge:
                continue
            pg_spec = pgtp.to_pg_spec(node_list, num_islands=nb_islands)
            pgtp.result()

    def test_minnumparts_pgtp(self):
        lgnames = [
            "testLoop.graph",
            "cont_img.graph",
            "test_grpby_gather.graph",
            "chiles_simple.graph",
        ]
        # tgt_partnum = [15, 15, 10, 10, 5]
        tgt_deadline = [200, 300, 90, 80, 160]
        for i, lgn in enumerate(lgnames):
            fp = get_lg_fname(lgn)
            lg = LG(fp)
            drop_list = lg.unroll_to_tpl()
            pgtp = MinNumPartsPGTP(drop_list, tgt_deadline[i])
            pgtp.json

    def test_pg_eagle(self):
        lgs = [
            "eagle_gather_simple.graph",
            "eagle_gather_empty.graph",
            "eagle_gather.graph",
        ]
        for lg in lgs:
            fp = get_lg_fname(lg)
            lg = LG(fp)
            lg.unroll_to_tpl()

    def test_plasma_graph(self):
        # test loading of Plasma graph
        lgs = ["Plasma_test.graph"]
        for lg in lgs:
            fp = get_lg_fname(lg)
            lg = LG(fp)
            lg.unroll_to_tpl()

    def test_shmem_graph(self):
        # Test loading of shared memory graph
        lgs = ["SharedMemoryTest.graph"]
        for lg in lgs:
            fp = get_lg_fname(lg)
            lg = LG(fp)
            out = lg.unroll_to_tpl()
            for drop in out:
                if drop["type"] == "plain":
                    self.assertEqual("SharedMemory", drop["storage"])
