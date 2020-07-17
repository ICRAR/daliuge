import json
import unittest

from dlg.common.reproducibility.constants import ReproducibilityFlags
from dlg.common.reproducibility.reproducibility import accumulate_lgt_drop_data


class AccumulateLGTRerunData(unittest.TestCase):
    # Locations in apps.graph for various application types
    bash_app = 0
    dyn_lib = 1
    mpi = 2
    docker = 3
    python = 4

    def test_app_accumulate(self):
        fp = open('reproGraphs/apps.graph')
        node_data = json.load(fp)['nodeDataArray']
        app_hashes = []
        for i in range(5):
            app = node_data[i]
            hash_data = accumulate_lgt_drop_data(app, ReproducibilityFlags.RERUN)
            print(hash_data)
            #app_hashes.append(HASHING_ALG(hash_data).hexdigest()))
        fp.close()
        assert True

    def test_data_accumulate(self):
        self.assertEqual(True, False)

    def test_group_accumulate(self):
        self.assertEqual(True, False)

    def test_control_accumulate(self):
        self.assertEqual(True, False)

    def test_other_accumulate(self):
        self.assertEqual(True, False)


class AccumulateLGRerunData(unittest.TestCase):
    def test_all_accumulate(self):
        self.assertEqual(True, False)


class AccumulatePGTUnrollRerunData(unittest.TestCase):
    def test_app_accumulate(self):
        self.assertEqual(True, False)

    def test_data_accumulate(self):
        self.assertEqual(True, False)

    def test_group_accumulate(self):
        self.assertEqual(True, False)

    def test_control_accumulate(self):
        self.assertEqual(True, False)

    def test_other_accumulate(self):
        self.assertEqual(True, False)


class AccumulatePGTPartitionRerunData(unittest.TestCase):
    def test_app_accumulate(self):
        self.assertEqual(True, False)

    def test_data_accumulate(self):
        self.assertEqual(True, False)

    def test_group_accumulate(self):
        self.assertEqual(True, False)

    def test_control_accumulate(self):
        self.assertEqual(True, False)

    def test_other_accumulate(self):
        self.assertEqual(True, False)


class AccumulatePGRerunData(unittest.TestCase):
    def test_app_accumulate(self):
        self.assertEqual(True, False)

    def test_data_accumulate(self):
        self.assertEqual(True, False)

    def test_group_accumulate(self):
        self.assertEqual(True, False)

    def test_control_accumulate(self):
        self.assertEqual(True, False)

    def test_other_accumulate(self):
        self.assertEqual(True, False)


if __name__ == '__main__':
    unittest.main()
