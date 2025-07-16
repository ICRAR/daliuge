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


from dlg.dropmake.lg import LG
from dlg.dropmake.pgt import PGT, GPGTNoNeedMergeException
from dlg.dropmake.pgtp import MetisPGTP, MySarkarPGTP, MinNumPartsPGTP
from dlg.common import path_utils

"""
python -m unittest test.dropmake.test_pg_gen
"""
TEST_SSID = 'test_pg_gen'


class TestPGGen(unittest.TestCase):
    """
    Test that the PhysicalGraph template constructor and supporting methods work.

    Uses test/dropmake/pg_spec as test data

    Note: This is a regression testing class. These tests are based on graphs that were
    generated using the code they are testing. If the PGT (sub)class and it's methods
    change in the future, test data may need to be re-generated (provided test
    failures are caused by known-breaking changes, as opposed to legitimate bugs!).
    """
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
            'algo': 'Edge Zero', 'min_exec_time': 16, 'total_data_movement': 70,
            'exec_time': 51, 'num_parts': 0
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
            'algo': 'Edge Zero', 'min_exec_time': 16,
            'total_data_movement': 0, 'exec_time': 16, 'num_parts': 1
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
            'algo': 'Lookahead', 'min_exec_time': 16,
            'total_data_movement': 70, 'exec_time': 51, 'num_parts': 0
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
            fp = path_utils.get_lg_fpath('logical_graphs', lg_names)
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
            fp = path_utils.get_lg_fpath('logical_graphs', lg_name)
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
            fp = path_utils.get_lg_fpath('logical_graphs', lg_name)
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
            fp = path_utils.get_lg_fpath('logical_graphs', lg_name)
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
            fp = path_utils.get_lg_fpath('logical_graphs', lg_name)
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
            fp = path_utils.get_lg_fpath('logical_graphs', lg_name)
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
                                 pgtp.result(),
                                 f"Incorrect partition results for: {lg_name}")

    def test_minnumparts_pgtp(self):
        tgt_deadline = [200, 300, 90, 80, 160]
        for i, lg_name in enumerate(self.partitionMethodLGs):
            fp = path_utils.get_lg_fpath('logical_graphs', lg_name)
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
        print("You have run the dataset generator for this test suite.\n"
              "\n"
              "This may have been done by accident: if so, double check the unitttest "
              "directive is used when running the file.\n"
              "\n"
              "If this was a deliberate effort to update the test cases due to a known "
              "change in the translator, please use the 'test-gen' argument. "
              "Please ensure that the changes are necessary, as this suite provides"
              "essential regression testing for translator behaviour.")
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
        "test_ports.graph",
        "pyfunc_glob_shell_test.graph"
        # "simpleMKN_update.graph", # Currently broken
    ]

    for lg_name in lgnames:
        print('\t', lg_name)
        fp = path_utils.get_lg_fpath('logical_graphs', lg_name)
        lg = LG(fp, ssid=TEST_SSID)

        lg_unroll = lg.unroll_to_tpl()
        pkl_path = path_utils.get_lg_fpath("pickle", lg_name)
        with open(pkl_path, 'wb') as fp:
            pickle.dump(lg_unroll, fp)

        pgt = PGT(lg_unroll)
        pg_json = pgt.to_gojs_json(visual=True, string_rep=False)
        pg_path = path_utils.get_lg_fpath("go_js_json", lg_name)

        with open(pg_path, 'w') as fp:
            json.dump(pg_json, fp, indent=2)

        node_list = [
            "10.128.0.11",
            "10.128.0.12",
            "10.128.0.13",
            "10.128.0.14",
            "10.128.0.15",
            "10.128.0.16",
        ]
        pgtp = MetisPGTP(lg_unroll,merge_parts=True)
        pgtp.to_gojs_json(visual=True, string_rep=False)
        pg_spec = pgtp.to_pg_spec(node_list=node_list, num_islands=1, ret_str=False)
        fn_spec = lg_name.split('.')[0] + '.spec'
        drop_spec_path = path_utils.get_lg_fpath("drop_spec", lg_name)
        with open(drop_spec_path, 'w') as fp:
            json.dump(pg_spec, fp, indent=2)