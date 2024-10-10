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

import sys
import unittest
import json
import pickle

<<<<<<< HEAD
import pkg_resources
from dlg.common import CategoryType
from dlg.dropmake.lg import LG
from dlg.dropmake.pgt import PGT, GPGTNoNeedMergeException
from dlg.dropmake.pgtp import MetisPGTP, MySarkarPGTP, MinNumPartsPGTP
=======

from dlg.dropmake.lg import LG
from dlg.dropmake.pgt import PGT, GPGTNoNeedMergeException
from dlg.dropmake.pgtp import MetisPGTP, MySarkarPGTP, MinNumPartsPGTP
from dlg.dropmake import path_utils
>>>>>>> master

"""
python -m unittest test.dropmake.test_pg_gen
"""
TEST_SSID = 'test_pg_gen'


<<<<<<< HEAD
def get_lg_fpath(type, f_name):
    """
    Get the test data file path based on the logical graph name and type of file we want
    :param type: str, type of test data (logical_graph, pickle, pg_spec) we are comparing
    :param f_name: name of the original logical graph created in Eagle
    :return: str, full path of the file
    """
    f_dir = 'logical_graphs'
    if type == 'pickle':
        f_name = f_name.split('.')[0] + '.pkl'
        f_dir = type
    if type == 'pg_spec':
        f_name = f_name.split('.')[0] + '.json'
        f_dir = type

    return pkg_resources.resource_filename(
        __name__, f"{f_dir}/{f_name}"
    )


class TestLGUnroll(unittest.TestCase):
    """
    Test that the LG unrolls as expected

    Uses test/dropmake/pickles as test data

    Note: This is a regression testing class. These tests are based on graphs that were
    generated using the code they are testing. If the LG class and it's methods change
    in the future, test data may need to be re-generated (provided test failures are
    caused by known-breaking changes, as opposed to legitimate bugs!).
    """

    def test_lg_unroll(self):
        """
        Basic verification that we can unroll a list of dropdicts from a logical graph

        lg_names = { "logical_graph_file.graph": num_keys_in_drop_list, ...}
        """

        lg_names = {
            "HelloWorld_simple.graph": 2,
            "eagle_gather_empty_update.graph": 11,
            "eagle_gather_simple_update.graph": 18,
            "eagle_gather_update.graph": 14,
            "testLoop.graph": 4,
            "cont_img_mvp.graph": 45,
            "test_grpby_gather.graph": 21,
            "chiles_simple.graph": 22,
            "Plasma_test.graph": 6,
        }

        for lg_name, num_keys in lg_names.items():
            fp = get_lg_fpath("logical_graphs", lg_name)
            lg = LG(fp, ssid=TEST_SSID)
            self.assertEqual(num_keys,
                             len(lg._done_dict.keys()),
                             f"Incorrect number of elements when constructing LG "
                             f"object using: {lg_name}")

            drop_list = lg.unroll_to_tpl()
            with open(get_lg_fpath('pickle', lg_name), 'rb') as pk_fp:
                test_unroll = pickle.load(pk_fp)

            self.assertListEqual(test_unroll,
                                 drop_list,
                                 f"unroll_to_tpl failed for: {lg_name}")

    def test_lg_unroll_sharedmemory(self):
        """
        Confirm the SharedMemory data type is correctly unrolled.
        """
        lg_name = "SharedMemoryTest_update.graph"
        num_keys = 8
        fp = get_lg_fpath("logical_graphs", lg_name)
        lg = LG(fp, ssid=TEST_SSID)
        self.assertEqual(num_keys,
                         len(lg._done_dict.keys()),
                         f"Incorrect number of elements when constructing LG "
                         f"object using: {lg_name}")

        drop_list = lg.unroll_to_tpl()
        for drop in drop_list:
            if drop["categoryType"] in [CategoryType.DATA, "data"]:
                self.assertEqual("SharedMemory", drop["category"])

=======
>>>>>>> master

class TestPGGen(unittest.TestCase):
    """
    Test that the PhysicalGraph template constructor and supporting methods work.

    Uses test/dropmake/pg_spec as test data

    Note: This is a regression testing class. These tests are based on graphs that were
    generated using the code they are testing. If the PGT (sub)class and it's methods
    change in the future, test data may need to be re-generated (provided test
    failures are caused by known-breaking changes, as opposed to legitimate bugs!).
    """
<<<<<<< HEAD

    def test_pgt_to_json(self):
        lgnames = [
            "HelloWorld_simple.graph",
            "eagle_gather_empty_update.graph",
            "eagle_gather_simple_update.graph",
            "eagle_gather_update.graph",
            "testLoop.graph",
            "cont_img_mvp.graph",
            "test_grpby_gather.graph",
            "chiles_simple.graph",
            "Plasma_test.graph",
            "SharedMemoryTest_update.graph",
            # "simpleMKN_update.graph", # Currently broken
        ]

        for lg_name in lgnames:
            fp = get_lg_fpath('logical_graphs', lg_name)
            lg = LG(fp, ssid=TEST_SSID)
            drop_list = lg.unroll_to_tpl()
            pgt = PGT(drop_list)
            pg_json = pgt.to_gojs_json(visual=True, string_rep=False)
            with open(get_lg_fpath('pg_spec', lg_name), 'r') as json_fp:
                test_json = json.load(json_fp)
            self.assertDictEqual(test_json, pg_json,
                             f"pgt.to_gojs_json failed for: {lg_name}")

=======
    lgnames = {
        "HelloWorld_simple.graph": {"nodes": 2, "edges": 1},
        "eagle_gather_empty_update.graph": {"nodes": 22, "edges": 24},
        "eagle_gather_simple_update.graph": {"nodes": 42, "edges": 55},
        "eagle_gather_update.graph": {"nodes": 29, "edges": 30},
        "testLoop.graph": {"nodes": 11, "edges": 10},
        "cont_img_mvp.graph": {"nodes": 144, "edges": 188},
        "test_grpby_gather.graph": {"nodes": 15, "edges": 14},
        "chiles_simple.graph": {"nodes": 22, "edges": 21},
        "Plasma_test.graph": {"nodes": 6, "edges": 5},
        "SharedMemoryTest_update.graph": {"nodes": 8, "edges": 7},
        # "simpleMKN_update.graph", # Currently broken
    }

    def _create_pgt(self, lg_name):
        fp = path_utils.get_lg_fpath('logical_graphs', lg_name)
        lg = LG(fp, ssid=TEST_SSID)
        drop_list = lg.unroll_to_tpl()
        return PGT(drop_list)

    def test_pgt_init(self):
        """
        Confirm that the PGT DAG correctly establishes the right number of nodes and edges
        """

        for lg_name, lg_expected in self.lgnames.items():
            pgt = self._create_pgt(lg_name)
            num_nodes = len(pgt.dag.nodes)
            num_edges = len(pgt.dag.edges)
            self.assertEqual(lg_expected['nodes'], num_nodes)
            self.assertEqual(lg_expected['edges'], num_edges)

    def test_pgt_to_json(self):
        """
        Verify that the expeceted output of the PGT to_gojs_json method is correct.

        Note that the to_gojs_json is not _just_ producing the go_js representation; it
        is also performing transformations on the PGT in the child classes.

        This confirms that the number of nodes and edges is a) consistent with the
        expected numbers, and b) is self-consistent between the drops and the networkx
        graph that are used interchangeably in the PGT class.
        """

        for lg_name, pg_expected in self.lgnames.items():
            pgt = self._create_pgt(lg_name)
            pgt.to_gojs_json(visual=False, string_rep=False)
            self.assertEqual(len(pgt.dag.edges), len(pgt.links))
            self.assertEqual(len(pgt.dag.nodes), len(pgt.drops))
            self.assertEqual(pg_expected['nodes'], len(pgt.drops))
            self.assertEqual(pg_expected['edges'], len(pgt.dag.edges))
>>>>>>> master

class TestPGPartition(unittest.TestCase):
    """
    Test that the PhysicalGraph subclass partitioning methods work, and that there is
    support for

    Uses test.dropmake.__init__ as reference partition result test data.
    Files in test/dropmake/pg_spec are not used as test data.

    Note: This is a regression testing class. These tests are based on graphs that were
    generated using the code they are testing. If the PGT (sub)class and it's methods
    change in the future, test data may need to be re-generated (provided test
    failures are caused by known-breaking changes, as opposed to legitimate bugs!).
    """

    SARKAR_PARTITION_RESULTS = {
        "testLoop.graph": {
            'algo': 'Edge Zero', 'min_exec_time': 30, 'total_data_movement': 50,
            'exec_time': 80, 'num_parts': 0
        },
        "cont_img_mvp.graph": {
            'algo': 'Edge Zero', 'min_exec_time': 144, 'total_data_movement': 932,
            'exec_time': 444, 'num_parts': 0
        },
        "test_grpby_gather.graph": {
<<<<<<< HEAD
            'algo': 'Edge Zero', 'min_exec_time': 12, 'total_data_movement': 70,
            'exec_time': 47, 'num_parts': 0
=======
            'algo': 'Edge Zero', 'min_exec_time': 16, 'total_data_movement': 70,
            'exec_time': 51, 'num_parts': 0
>>>>>>> master
        },
        "chiles_simple.graph": {
            'algo': 'Edge Zero', 'min_exec_time': 45, 'total_data_movement': 1080,
            'exec_time': 285, 'num_parts': 0
        }
    }

    SARKAR_PARTITION_RESULTS_GEN_ISLAND = {
        "testLoop.graph": {
            'algo': 'Edge Zero',
            'min_exec_time': 30,
            'total_data_movement': 0,
            'exec_time': 30, 'num_parts': 1
        },
        "cont_img_mvp.graph": {
            'algo': 'Edge Zero', 'min_exec_time': 144, 'total_data_movement': 135,
            'exec_time': 179, 'num_islands': 2, 'num_parts': 4
        },
        "test_grpby_gather.graph": {
<<<<<<< HEAD
            'algo': 'Edge Zero', 'min_exec_time': 12,
            'total_data_movement': 0, 'exec_time': 12, 'num_parts': 1
=======
            'algo': 'Edge Zero', 'min_exec_time': 16,
            'total_data_movement': 0, 'exec_time': 16, 'num_parts': 1
>>>>>>> master
        },
        "chiles_simple.graph": {
            'algo': 'Edge Zero', 'min_exec_time': 45, 'total_data_movement': 0,
            'exec_time': 45, 'num_parts': 1
        }
    }

    MINPARTS_RESULTS = {
        "testLoop.graph": {
            'algo': 'Lookahead', 'min_exec_time': 30, 'total_data_movement': 50,
            'exec_time': 80, 'num_parts': 0
        },
        "cont_img_mvp.graph": {
            'algo': 'Lookahead', 'min_exec_time': 144,
            'total_data_movement': 932, 'exec_time': 444, 'num_parts': 0
        },
        "test_grpby_gather.graph": {
<<<<<<< HEAD
            'algo': 'Lookahead', 'min_exec_time': 12,
            'total_data_movement': 70, 'exec_time': 47, 'num_parts': 0
=======
            'algo': 'Lookahead', 'min_exec_time': 16,
            'total_data_movement': 70, 'exec_time': 51, 'num_parts': 0
>>>>>>> master
        },
        "chiles_simple.graph": {
            'algo': 'Lookahead', 'min_exec_time': 45, 'total_data_movement': 1080,
            'exec_time': 285, 'num_parts': 0
        }
    }

    def setUp(self):
        self.partitionMethodLGs = [
            "testLoop.graph",
            "cont_img_mvp.graph",
            "test_grpby_gather.graph",
            "chiles_simple.graph",
            # "simpleMKN.graph", # Broken
        ]

    def test_metis_pgtp(self):
        """
        Confirm that basic Sarkar paritioning has not regressed
        """
        expected = {'algo': 'METIS_LB91',
                    'min_exec_time': None,
                    'total_data_movement': None, 'exec_time': None,
                    'num_parts': 1}

        for lg_names in self.partitionMethodLGs:
<<<<<<< HEAD
            fp = get_lg_fpath('logical_graphs', lg_names)
=======
            fp = path_utils.get_lg_fpath('logical_graphs', lg_names)
>>>>>>> master
            lg = LG(fp)
            drop_list = lg.unroll_to_tpl()
            pgtp = MetisPGTP(drop_list)
            self.assertEqual(expected, pgtp.result())

    def test_metis_pgtp_gen_pg(self):
        """
        Regression testing to confirm that basic METIS partitioning works,
        then generating a PGT spec works when using multiple nodes.

        We check that the partition result before differs from the result achieved
        after translating to the pg_spec, as this involves partitioning and should
        result in speed up.
        """
        node_list = ["10.128.0.11", "10.128.0.11", "10.128.0.12", "10.128.0.13"]
        total_data_movement_pgspec = {
            "testLoop.graph": 10,
            "cont_img_mvp.graph": 45,
            "test_grpby_gather.graph": 20,
            "chiles_simple.graph": 20,
        }
        for lg_name in self.partitionMethodLGs:
<<<<<<< HEAD
            fp = get_lg_fpath('logical_graphs', lg_name)
=======
            fp = path_utils.get_lg_fpath('logical_graphs', lg_name)
>>>>>>> master
            lg = LG(fp)
            drop_list = lg.unroll_to_tpl()
            pgtp = MetisPGTP(drop_list, 3, merge_parts=True)
            result = pgtp.result()
            self.assertIsNone(result['total_data_movement'],
                              f"Incorrect partitioning for: {lg_name}")
            self.assertEqual(3, result['num_parts'])
            pgtp.to_gojs_json(visual=False)
            pgtp.to_pg_spec(node_list)
            result = pgtp.result()
            self.assertEqual(total_data_movement_pgspec[lg_name],
                             result['total_data_movement'],
                             f"Incorrect partitioning for: {lg_name}")
<<<<<<< HEAD
=======

>>>>>>> master
    def test_metis_pgtp_gen_pg_island(self):
        """
        Regression testing to confirm that partitioning, then generating a PGT spec works
        when using multiple nodes and 2 data islands.
        """
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
        for lg_name in self.partitionMethodLGs:
<<<<<<< HEAD
            fp = get_lg_fpath('logical_graphs', lg_name)
=======
            fp = path_utils.get_lg_fpath('logical_graphs', lg_name)
>>>>>>> master
            lg = LG(fp)
            drop_list = lg.unroll_to_tpl()
            pgtp = MetisPGTP(drop_list, nb_nodes, merge_parts=True)
            self.assertFalse('num_islands' in pgtp.result())
            pgtp.to_gojs_json(visual=False)
            pgtp.to_pg_spec(node_list, num_islands=nb_islands)
            self.assertTrue('num_islands' in pgtp.result(),
                            f"No islands in PG spec for: {lg_name}")
            self.assertEqual(2, pgtp.result()['num_islands'],
                             f"Incorrect number of islands in PG spec for: {lg_name}")

    def test_mysarkar_pgtp(self):
        """
        Confirm that basic Sarkar paritioning has not regressed
        """

        for lg_name in self.partitionMethodLGs:
<<<<<<< HEAD
            fp = get_lg_fpath('logical_graphs', lg_name)
=======
            fp = path_utils.get_lg_fpath('logical_graphs', lg_name)
>>>>>>> master
            lg = LG(fp)
            drop_list = lg.unroll_to_tpl()
            pgtp = MySarkarPGTP(drop_list)
            self.assertEqual(
                self.SARKAR_PARTITION_RESULTS[lg_name],
                pgtp.result(),
                f"Partition results do not match test case for: {lg_name}")

    def test_mysarkar_pgtp_gen_pg(self):
        """
        Regression testing to confirm that basic Sarkar partitioning, then generating a
        PGT spec works when using multiple nodes.

        We check that the partition result before differs from the result achieved
        after translating to the pg_spec, as this involves partitioning and should
        result in speed up.
        """
        node_list = ["10.128.0.11", "10.128.0.12", "10.128.0.13"]
        for lg_name in self.partitionMethodLGs:
<<<<<<< HEAD
            fp = get_lg_fpath('logical_graphs', lg_name)
=======
            fp = path_utils.get_lg_fpath('logical_graphs', lg_name)
>>>>>>> master
            lg = LG(fp)
            drop_list = lg.unroll_to_tpl()
            pgtp = MySarkarPGTP(drop_list, 3, merge_parts=True)
            pre_spec_result = pgtp.result()
            pgtp.to_gojs_json(visual=False)
            pgtp.to_pg_spec(node_list)
            # Confirm that partitioning improves the execution speed.
            self.assertGreater(
                pre_spec_result['exec_time'], pgtp.result()['exec_time'],
                f"Partition of {lg_name} should cause speed up, but this did not occur.")

    def test_mysarkar_pgtp_gen_pg_island(self):
        """
        Regression testing to confirm that partitioning, then generating a PGT spec works
        when using multiple nodes and 2 data islands.
        """
        node_list = [
            "10.128.0.11",
            "10.128.0.12",
            "10.128.0.13",
            "10.128.0.14",
            "10.128.0.15",
            "10.128.0.16",
        ]
        nb_islands = 2
        new_num_parts = len(node_list) - nb_islands
        for lg_name in self.partitionMethodLGs:
<<<<<<< HEAD
            fp = get_lg_fpath('logical_graphs', lg_name)
=======
            fp = path_utils.get_lg_fpath('logical_graphs', lg_name)
>>>>>>> master
            lg = LG(fp, ssid=TEST_SSID)
            drop_list = lg.unroll_to_tpl()
            pgtp = MySarkarPGTP(drop_list, None, merge_parts=True)
            pgtp.to_gojs_json(visual=True, string_rep=False)

            if lg_name != "cont_img_mvp.graph":
                self.assertRaises(
                    GPGTNoNeedMergeException,
                    pgtp.merge_partitions,
                    new_num_parts,
                    False,
                    f"Exception was not raised for: {lg_name}")
                partition_results = pgtp.result()
                self.assertEqual(self.SARKAR_PARTITION_RESULTS_GEN_ISLAND[lg_name],
                                 partition_results)
            else:
                pgtp.merge_partitions(len(node_list) - nb_islands, form_island=False)
                pgtp.to_pg_spec(node_list, num_islands=nb_islands)
                self.assertEqual(self.SARKAR_PARTITION_RESULTS_GEN_ISLAND[lg_name],
<<<<<<< HEAD
                                pgtp.result(),
=======
                                 pgtp.result(),
>>>>>>> master
                                 f"Incorrect partition results for: {lg_name}")

    def test_minnumparts_pgtp(self):
        tgt_deadline = [200, 300, 90, 80, 160]
        for i, lg_name in enumerate(self.partitionMethodLGs):
<<<<<<< HEAD
            fp = get_lg_fpath('logical_graphs', lg_name)
=======
            fp = path_utils.get_lg_fpath('logical_graphs', lg_name)
>>>>>>> master
            lg = LG(fp)
            drop_list = lg.unroll_to_tpl()
            pgtp = MinNumPartsPGTP(drop_list, tgt_deadline[i])
            self.assertEqual(self.MINPARTS_RESULTS[lg_name],
                             pgtp.result(),
                             f"Incorrect partition results for: {lg_name}")


if __name__ == '__main__':
    """
    Used to generate the pickle and logical graph files used for testing.

    IMPORTANT: Run this _only_ when the expected output of unroll_to_tpl has been 
    _knowingly_ changed. 
    """
    try:
        arg = sys.argv[1]
        if arg.lower() == "test-gen":
            print("\nRunning test dataset generator on following logical graphs:")
    except IndexError:
<<<<<<< HEAD
        print("You have run the test dataset generator for this test suite.\n"
=======
        print("You have run the dataset generator for this test suite.\n"
>>>>>>> master
              "\n"
              "This may have been done by accident: if so, double check the unitttest "
              "directive is used when running the file.\n"
              "\n"
              "If this was a deliberate effort to update the test cases due to a known "
              "change in the translator, please use the 'test-gen' argument. "
<<<<<<< HEAD
              "Ensure that the changes are necessary, as this suite provides essential "
              "regression testing for translator behaviour.")
=======
              "Please ensure that the changes are necessary, as this suite provides"
              "essential regression testing for translator behaviour.")
>>>>>>> master
        exit()

    pickle_dir = "pickle"
    physical_graph_spec = "pg_spec"
    lgnames = [
        "HelloWorld_simple.graph",
        "eagle_gather_empty_update.graph",
        "eagle_gather_simple_update.graph",
        "eagle_gather_update.graph",
        "testLoop.graph",
        "cont_img_mvp.graph",
        "test_grpby_gather.graph",
        "chiles_simple.graph",
        "Plasma_test.graph",
        "SharedMemoryTest_update.graph",
        # "simpleMKN_update.graph", # Currently broken
    ]

    for lg_name in lgnames:
        print('\t', lg_name)
<<<<<<< HEAD
        fp = get_lg_fpath('logical_graphs', lg_name)
=======
        fp = path_utils.get_lg_fpath('logical_graphs', lg_name)
>>>>>>> master
        lg = LG(fp, ssid=TEST_SSID)

        lg_unroll = lg.unroll_to_tpl()
        fn_pkl = lg_name.split('.')[0] + '.pkl'
<<<<<<< HEAD
        pkl_path = pkg_resources.resource_filename(
            __name__, f"{pickle_dir}/{fn_pkl}"
        )
=======
        pkl_path = path_utils.get_lg_fpath("pickle", lg_name)
>>>>>>> master
        with open(pkl_path, 'wb') as fp:
            pickle.dump(lg_unroll, fp)

        pgt = PGT(lg_unroll)
        pg_json = pgt.to_gojs_json(visual=True, string_rep=False)
        fn_json = lg_name.split('.')[0] + '.json'
<<<<<<< HEAD
        pg_path = pkg_resources.resource_filename(
            __name__, f"{physical_graph_spec}/{fn_json}"
        )
        with open(pg_path, 'w') as fp:
            json.dump(pg_json, fp)
=======
        pg_path = path_utils.get_lg_fpath("pg_spec", lg_name)

        with open(pg_path, 'w') as fp:
            json.dump(pg_json, fp, indent=2)
>>>>>>> master
