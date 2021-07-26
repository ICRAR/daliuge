#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2017
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
Tests the routine generating a drop's reproducibility data.
Ideally, one would have a pre-existing standard for each drop-type to be tested individually.
For now, this will suffice.
"""

import json
import optparse
import tempfile
import unittest

from dlg.common.reproducibility.constants import ReproducibilityFlags
from dlg.common.reproducibility.reproducibility import accumulate_lgt_drop_data, \
    accumulate_lg_drop_data
from dlg.translator.tool_commands import dlg_fill, dlg_unroll, dlg_partition, dlg_map


def _fill_workflow(rmode: ReproducibilityFlags, workflow: str, workflow_loc='./', scratch_loc='./'):
    lgt = workflow_loc + workflow + ".graph"
    lgr = scratch_loc + workflow + "LG.graph"

    rmodes = str(rmode.value)

    parser = optparse.OptionParser()
    dlg_fill(parser, ['-L', lgt, '-R', rmodes, '-o', lgr, '-f', 'newline'])


def _run_full_workflow(rmode: ReproducibilityFlags, workflow: str, workflow_loc='./',
                       scratch_loc='./'):
    lgr = scratch_loc + workflow + "LG.graph"
    pgs = scratch_loc + workflow + "PGS.graph"
    pgt = scratch_loc + workflow + "PGT.graph"
    pgr = scratch_loc + workflow + "PG.graph"

    _fill_workflow(rmode, workflow, workflow_loc, scratch_loc)
    parser = optparse.OptionParser()
    dlg_unroll(parser, ['-L', lgr, '-o', pgs, '-f', 'newline'])
    parser = optparse.OptionParser()
    dlg_partition(parser, ['-P', pgs, '-o', pgt, '-f', 'newline'])
    parser = optparse.OptionParser()
    dlg_map(parser, ['-P', pgt, '-N', '127.0.0.1, 127.0.0.1', '-o', pgr, '-f', 'newline'])


class AccumulateLGTRerunData(unittest.TestCase):
    """
    Tests the rerun standard at the logical graph template level.
    """
    # Locations in apps.graph for various application types
    bash_app = 0
    dyn_lib = 1
    mpi = 2
    docker = 3
    python = 4

    expected = ['category_type', 'category', 'numInputPorts', 'numOutputPorts', 'streaming']

    file = open('reproGraphs/apps.graph')
    lgt_node_data = json.load(file)['nodeDataArray']
    file.close()
    file = open('reproGraphs/files.graph')
    lgt_files_data = json.load(file)['nodeDataArray']
    file.close()
    file = open('reproGraphs/groups.graph')
    lgt_groups_data = json.load(file)['nodeDataArray']
    file.close()
    file = open('reproGraphs/misc.graph')
    lgt_misc_data = json.load(file)['nodeDataArray']
    file.close()

    def test_app_accumulate(self):
        """
        Tests that lgt rerun data is collected for application types
        """
        for drop in enumerate(self.lgt_node_data):
            hash_data = accumulate_lgt_drop_data(drop[1], ReproducibilityFlags.RERUN)
            self.assertEqual(self.expected, list(hash_data.keys()))

    def test_data_accumulate(self):
        """
        Tests that lgt rerun data is collected for file types
        """
        for drop in enumerate(self.lgt_files_data):
            hash_data = accumulate_lgt_drop_data(drop[1], ReproducibilityFlags.RERUN)
            self.assertEqual(self.expected, list(hash_data.keys()))

    def test_group_accumulate(self):
        """
        Tests that lgt rerun data is collected for group types
        """
        for drop in enumerate(self.lgt_groups_data):
            hash_data = accumulate_lgt_drop_data(drop[1], ReproducibilityFlags.RERUN)
            self.assertEqual(self.expected, list(hash_data.keys()))

    def test_other_accumulate(self):
        """
        Tests that lgt rerun data is collected for other types
        """
        for drop in enumerate(self.lgt_misc_data):
            hash_data = accumulate_lgt_drop_data(drop[1], ReproducibilityFlags.RERUN)
            self.assertEqual(self.expected, list(hash_data.keys()))


class AccumulateLGRerunData(unittest.TestCase):
    """
    Tests the rerun standard at the logical graph level.
    """
    expected = []
    temp_out = tempfile.TemporaryDirectory('out')

    def _cleanup(self):
        self.temp_out.cleanup()

    def _setup(self):
        _fill_workflow(ReproducibilityFlags.RERUN, 'apps', 'reproGraphs/', self.temp_out.name)
        _fill_workflow(ReproducibilityFlags.RERUN, 'files', 'reproGraphs/', self.temp_out.name)
        _fill_workflow(ReproducibilityFlags.RERUN, 'groups', 'reproGraphs/', self.temp_out.name)
        _fill_workflow(ReproducibilityFlags.RERUN, 'misc', 'reproGraphs/', self.temp_out.name)

        file = open(self.temp_out.name + 'apps' + 'LG.graph')
        self.lg_node_data = json.load(file)['nodeDataArray']
        file.close()
        file = open(self.temp_out.name + 'files' + 'LG.graph')
        self.lg_files_data = json.load(file)['nodeDataArray']
        file.close()
        file = open(self.temp_out.name + 'groups' + 'LG.graph')
        self.lg_group_data = json.load(file)['nodeDataArray']
        file.close()
        file = open(self.temp_out.name + 'misc' + 'LG.graph')
        self.lg_misc_data = json.load(file)['nodeDataArray']
        file.close()

    def test_all_accumulate(self):
        """
        Tests that lg rerun data is collected correctly (should not contain any information)
        """
        self._setup()
        for drop in enumerate(
                self.lg_node_data + self.lg_files_data + self.lg_group_data + self.lg_misc_data):
            hash_data = accumulate_lg_drop_data(drop[1], ReproducibilityFlags.RERUN)
            self.assertEqual(self.expected, list(hash_data.keys()))


class AccumulatePGTUnrollRerunData(unittest.TestCase):
    """
    Tests the rerun standard at the physical graph template level.
    Can currently only test apps.graph and files.graph, the translator cannot deal with groups
    or comments easily.
    """
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
