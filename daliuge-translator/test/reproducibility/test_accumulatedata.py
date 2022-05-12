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
from dlg.common.reproducibility.reproducibility import (
    accumulate_lgt_drop_data,
    accumulate_lg_drop_data,
    accumulate_pgt_unroll_drop_data,
    accumulate_pgt_partition_drop_data,
    accumulate_pg_drop_data,
)
from dlg.translator.tool_commands import dlg_fill, dlg_unroll, dlg_partition, dlg_map


def _fill_workflow(
    rmode: ReproducibilityFlags, workflow: str, workflow_loc="./", scratch_loc="./"
):
    lgt = workflow_loc + workflow + ".graph"
    lgr = scratch_loc + "/" + workflow + "LG.graph"

    rmodes = str(rmode.value)

    parser = optparse.OptionParser()
    dlg_fill(parser, ["-L", lgt, "-R", rmodes, "-o", lgr, "-f", "newline"])


def _run_full_workflow(
    rmode: ReproducibilityFlags, workflow: str, workflow_loc="./", scratch_loc="./"
):
    lgr = scratch_loc + "/" + workflow + "LG.graph"
    pgs = scratch_loc + "/" + workflow + "PGS.graph"
    pgt = scratch_loc + "/" + workflow + "PGT.graph"
    pgr = scratch_loc + "/" + workflow + "PG.graph"

    _fill_workflow(rmode, workflow, workflow_loc, scratch_loc)
    parser = optparse.OptionParser()
    dlg_unroll(parser, ["-L", lgr, "-o", pgs, "-f", "newline"])
    parser = optparse.OptionParser()
    dlg_partition(parser, ["-P", pgs, "-o", pgt, "-f", "newline"])
    parser = optparse.OptionParser()
    dlg_map(
        parser, ["-P", pgt, "-N", "127.0.0.1, 127.0.0.1", "-o", pgr, "-f", "newline"]
    )


class AccumulateLGTRerunData(unittest.TestCase):
    """
    Tests the rerun standard at the logical graph template level.
    """

    rmode = ReproducibilityFlags.RERUN
    expected = {
        "categoryType",
        "category",
        "inputPorts",
        "inputLocalPorts",
        "outputLocalPorts",
        "outputPorts",
        "streaming",
    }

    file = open("test/reproducibility/reproGraphs/apps.graph")
    lgt_node_data = json.load(file)["nodeDataArray"]
    file.close()
    file = open("test/reproducibility/reproGraphs/files.graph")
    lgt_files_data = json.load(file)["nodeDataArray"]
    file.close()
    file = open("test/reproducibility/reproGraphs/groups.graph")
    lgt_groups_data = json.load(file)["nodeDataArray"]
    file.close()
    file = open("test/reproducibility/reproGraphs/misc.graph")
    lgt_misc_data = json.load(file)["nodeDataArray"]
    file.close()

    def test_app_accumulate(self):
        """
        Tests that lgt rerun data is collected for application types
        """
        for drop in enumerate(self.lgt_node_data):
            hash_data = accumulate_lgt_drop_data(drop[1], self.rmode)
            self.assertEqual(self.expected, hash_data.keys())

    def test_data_accumulate(self):
        """
        Tests that lgt rerun data is collected for file types
        """
        for drop in enumerate(self.lgt_files_data):
            hash_data = accumulate_lgt_drop_data(drop[1], self.rmode)
            self.assertEqual(self.expected, hash_data.keys())

    def test_group_accumulate(self):
        """
        Tests that lgt rerun data is collected for group types
        """
        for drop in enumerate(self.lgt_groups_data):
            hash_data = accumulate_lgt_drop_data(drop[1], self.rmode)
            self.assertEqual(self.expected, hash_data.keys())

    def test_other_accumulate(self):
        """
        Tests that lgt rerun data is collected for other types
        """
        for drop in enumerate(self.lgt_misc_data):
            hash_data = accumulate_lgt_drop_data(drop[1], self.rmode)
            self.assertEqual(self.expected, hash_data.keys())


class AccumulateLGRerunData(unittest.TestCase):
    """
    Tests the rerun standard at the logical graph level.
    """

    rmode = ReproducibilityFlags.RERUN
    expected = {}
    temp_out = tempfile.TemporaryDirectory()
    lg_node_data = None
    lg_files_data = None
    lg_group_data = None
    lg_misc_data = None

    def _setup(self):
        _fill_workflow(
            self.rmode, "apps", "test/reproducibility/reproGraphs/", self.temp_out.name
        )
        _fill_workflow(
            self.rmode, "files", "test/reproducibility/reproGraphs/", self.temp_out.name
        )
        _fill_workflow(
            self.rmode,
            "groups",
            "test/reproducibility/reproGraphs/",
            self.temp_out.name,
        )
        _fill_workflow(
            self.rmode, "misc", "test/reproducibility/reproGraphs/", self.temp_out.name
        )

        file = open(self.temp_out.name + "/" + "apps" + "LG.graph")
        self.lg_node_data = json.load(file)["nodeDataArray"]
        file.close()
        file = open(self.temp_out.name + "/" + "files" + "LG.graph")
        self.lg_files_data = json.load(file)["nodeDataArray"]
        file.close()
        file = open(self.temp_out.name + "/" + "groups" + "LG.graph")
        self.lg_group_data = json.load(file)["nodeDataArray"]
        file.close()
        file = open(self.temp_out.name + "/" + "misc" + "LG.graph")
        self.lg_misc_data = json.load(file)["nodeDataArray"]
        file.close()

    def test_all_accumulate(self):
        """
        Tests that lg rerun data is collected correctly (should not contain any information)
        """
        self._setup()
        for drop in enumerate(
            self.lg_node_data
            + self.lg_files_data
            + self.lg_group_data
            + self.lg_misc_data
        ):
            hash_data = accumulate_lg_drop_data(drop[1], self.rmode)
            self.assertEqual(self.expected, dict(hash_data.keys()))


class AccumulatePGTUnrollRerunData(unittest.TestCase):
    """
    Tests the rerun standard at the physical graph template level.
    Can currently only test apps.graph and files.graph, the translator cannot deal with groups
    or comments easily.
    """

    rmode = ReproducibilityFlags.RERUN
    temp_out = tempfile.TemporaryDirectory()
    pgs_node_data = None
    pgs_file_data = None

    def _setup(self):
        _run_full_workflow(
            self.rmode, "apps", "test/reproducibility/reproGraphs/", self.temp_out.name
        )
        _run_full_workflow(
            self.rmode, "files", "test/reproducibility/reproGraphs/", self.temp_out.name
        )

        file = open(self.temp_out.name + "/" + "apps" + "PGS.graph")
        self.pgs_node_data = json.load(file)[0:-1]
        file.close()
        file = open(self.temp_out.name + "/" + "files" + "PGS.graph")
        self.pgs_file_data = json.load(file)[0:-1]
        file.close()

    def test_app_accumulate(self):
        """
        The the application type matters for rerunning
        """
        expected = {"type", "dt"}
        self._setup()
        for drop in enumerate(self.pgs_node_data):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            self.assertEqual(expected, hash_data.keys())

    def test_data_accumulate(self):
        """
        Only the storage type matters for rerunning
        """
        expected = {"type", "storage"}
        self._setup()
        for drop in enumerate(self.pgs_file_data):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            self.assertEqual(expected, hash_data.keys())

    @unittest.skip("pg_generator does not like sample graphs")
    def test_group_accumulate(self):
        """
        Only the drop type and input app type matters for rerunning
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Only the drop type matters for rerunning
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_other_accumulate(self):
        """
        Nothing should matter for rerunning
        """
        self.assertEqual(True, False)


class AccumulatePGTPartitionRerunData(unittest.TestCase):
    """
    Tests the rerun standard at the physical graph template level.
    """

    rmode = ReproducibilityFlags.RERUN
    temp_out = tempfile.TemporaryDirectory()
    pgt_node_data = None
    pgt_file_data = None

    def _setup(self):
        _run_full_workflow(
            self.rmode, "apps", "test/reproducibility/reproGraphs/", self.temp_out.name
        )
        _run_full_workflow(
            self.rmode, "files", "test/reproducibility/reproGraphs/", self.temp_out.name
        )

        file = open(self.temp_out.name + "/" + "apps" + "PGT.graph")
        self.pgt_node_data = json.load(file)[0:-1]
        file.close()
        file = open(self.temp_out.name + "/" + "files" + "PGT.graph")
        self.pgt_file_data = json.load(file)[0:-1]
        file.close()

    def test_app_accumulate(self):
        """
        Only application type matters for rerunning.
        """
        expected = {"type", "dt"}
        self._setup()
        for drop in enumerate(self.pgt_node_data):
            hash_data = accumulate_pgt_partition_drop_data(drop[1])
            self.assertEqual(expected, hash_data.keys())

    def test_data_accumulate(self):
        """
        Only storage type matters for rerunning.
        """
        expected = {"type", "storage"}
        self._setup()
        for drop in enumerate(self.pgt_file_data):
            hash_data = accumulate_pgt_partition_drop_data(drop[1])
            self.assertEqual(expected, hash_data.keys())

    @unittest.skip("pg_generator does not like sample graphs")
    def test_group_accumulate(self):
        """
        Only drop type matters for rerunning.
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Only drop type matters for rerunning.
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_other_accumulate(self):
        """
        Should not  matter for rerunning.
        """
        self.assertEqual(True, False)


class AccumulatePGRerunData(unittest.TestCase):
    """
    Tests the rerun standard at the physical graph level
    """

    rmode = ReproducibilityFlags.RERUN
    temp_out = tempfile.TemporaryDirectory()
    pg_node_data = None
    pg_file_data = None

    def _setup(self):
        _run_full_workflow(
            self.rmode, "apps", "test/reproducibility/reproGraphs/", self.temp_out.name
        )
        _run_full_workflow(
            self.rmode, "files", "test/reproducibility/reproGraphs/", self.temp_out.name
        )

        file = open(self.temp_out.name + "/" + "apps" + "PG.graph")
        self.pg_node_data = json.load(file)[0:-1]
        file.close()
        file = open(self.temp_out.name + "/" + "files" + "PG.graph")
        self.pg_file_data = json.load(file)[0:-1]
        file.close()

    def test_app_accumulate(self):
        """
        Nothing matters for rerunning.
        """
        expected = {}
        self._setup()
        for drop in enumerate(self.pg_node_data):
            hash_data = accumulate_pg_drop_data(drop[1])
            self.assertEqual(expected, dict(hash_data.keys()))

    def test_data_accumulate(self):
        """
        Nothing matters for rerunning.
        """
        expected = {}
        self._setup()
        for drop in enumerate(self.pg_file_data):
            hash_data = accumulate_pg_drop_data(drop[1])
            self.assertEqual(expected, dict(hash_data.keys()))

    @unittest.skip("pg_generator does not like sample graphs")
    def test_group_accumulate(self):
        """
        Nothing matters for rerunning.
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Nothing matters for rerunning.
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_other_accumulate(self):
        """
        Nothing matters for rerunning.
        """
        self.assertEqual(True, False)


class AccumulateLGTRepeatData(unittest.TestCase):
    """
    Tests the repeat standard at the logical graph template level.
    """

    rmode = ReproducibilityFlags.REPEAT
    expected = {
        "categoryType",
        "category",
        "inputPorts",
        "inputLocalPorts",
        "outputPorts",
        "outputLocalPorts",
        "streaming",
    }

    file = open("test/reproducibility/reproGraphs/apps.graph")
    lgt_node_data = json.load(file)["nodeDataArray"]
    file.close()
    file = open("test/reproducibility/reproGraphs/files.graph")
    lgt_files_data = json.load(file)["nodeDataArray"]
    file.close()
    file = open("test/reproducibility/reproGraphs/groups.graph")
    lgt_groups_data = json.load(file)["nodeDataArray"]
    file.close()
    file = open("test/reproducibility/reproGraphs/misc.graph")
    lgt_misc_data = json.load(file)["nodeDataArray"]
    file.close()

    def test_app_accumulate(self):
        """
        Tests that lgt repeat data is collected for application types
        """
        for drop in enumerate(self.lgt_node_data):
            hash_data = accumulate_lgt_drop_data(drop[1], self.rmode)
            self.assertEqual(self.expected, hash_data.keys())

    def test_data_accumulate(self):
        """
        Tests that lgt repeat data is collected for file types
        """
        for drop in enumerate(self.lgt_files_data):
            hash_data = accumulate_lgt_drop_data(drop[1], self.rmode)
            self.assertEqual(self.expected, hash_data.keys())

    def test_group_accumulate(self):
        """
        Tests that lgt repeat data is collected for group types
        """
        for drop in enumerate(self.lgt_groups_data):
            hash_data = accumulate_lgt_drop_data(drop[1], self.rmode)
            self.assertEqual(self.expected, hash_data.keys())

    def test_other_accumulate(self):
        """
        Tests that lgt repeat data is collected for other types
        """
        for drop in enumerate(self.lgt_misc_data):
            hash_data = accumulate_lgt_drop_data(drop[1], self.rmode)
            self.assertEqual(self.expected, hash_data.keys())


class AccumulateLGRepeatData(unittest.TestCase):
    """
    Tests the repeat standard at the logical graph level.
    """

    rmode = ReproducibilityFlags.REPEAT
    expected = []
    temp_out = tempfile.TemporaryDirectory()
    lg_node_data = None
    lg_files_data = None
    lg_group_data = None
    lg_misc_data = None

    def _setup(self):
        _fill_workflow(
            self.rmode, "apps", "test/reproducibility/reproGraphs/", self.temp_out.name
        )
        _fill_workflow(
            self.rmode, "files", "test/reproducibility/reproGraphs/", self.temp_out.name
        )
        _fill_workflow(
            self.rmode,
            "groups",
            "test/reproducibility/reproGraphs/",
            self.temp_out.name,
        )
        _fill_workflow(
            self.rmode, "misc", "test/reproducibility/reproGraphs/", self.temp_out.name
        )

        file = open(self.temp_out.name + "/" + "apps" + "LG.graph")
        self.lg_node_data = json.load(file)["nodeDataArray"]
        file.close()
        file = open(self.temp_out.name + "/" + "files" + "LG.graph")
        self.lg_files_data = json.load(file)["nodeDataArray"]
        file.close()
        file = open(self.temp_out.name + "/" + "groups" + "LG.graph")
        self.lg_group_data = json.load(file)["nodeDataArray"]
        file.close()
        file = open(self.temp_out.name + "/" + "misc" + "LG.graph")
        self.lg_misc_data = json.load(file)["nodeDataArray"]
        file.close()

    def _bash(self, drop):
        expected = {"execution_time", "num_cpus", "Arg01"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _dynlib(self, drop):
        expected = {"execution_time", "num_cpus", "libpath"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _mpi(self, drop):
        expected = {"execution_time", "num_cpus", "num_of_procs"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _docker(self, drop):
        expected = {
            "execution_time",
            "num_cpus",
            "image",
            "command",
            "user",
            "ensureUserAndSwitch",
            "removeContainer",
            "additionalBindings",
        }
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _component(self, drop):
        expected = {"execution_time", "num_cpus", "appclass"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _memory(self, drop):
        expected = {"data_volume"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _file(self, drop):
        expected = {"data_volume", "check_filepath_exists"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _ngas(self, drop):
        expected = {"data_volume"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _groupby(self, drop):
        expected = {
            "inputApplicationName",
            "inputApplicationType",
            "group_key",
            "group_axis",
        }
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data)

    def _scatter(self, drop):
        expected = {
            "inputApplicationName",
            "inputApplicationType",
            "num_of_copies",
            "scatter_axis",
        }
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data)

    def _loop(self, drop):
        expected = {"inputApplicationName", "inputApplicationType", "num_of_iter"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data)

    def test_all_accumulate(self):
        """
        We make the data expected for each accounted for drop type explicit.
        """
        self._setup()
        self._bash(self.lg_node_data[0])
        self._dynlib(self.lg_node_data[1])
        self._mpi(self.lg_node_data[2])
        self._docker(self.lg_node_data[3])
        self._component(self.lg_node_data[4])
        self._memory(self.lg_files_data[0])
        self._file(self.lg_files_data[1])
        self._ngas(self.lg_files_data[2])


class AccumulatePGTUnrollRepeatData(unittest.TestCase):
    """
    Tests the repeat standard at the physical graph template level.
    Can currently only test apps.graph and files.graph, the translator cannot deal with groups
    or comments easily.
    """

    rmode = ReproducibilityFlags.REPEAT
    temp_out = tempfile.TemporaryDirectory()
    pgs_node_data = None
    pgs_file_data = None

    def _setup(self):
        _run_full_workflow(
            self.rmode, "apps", "test/reproducibility/reproGraphs/", self.temp_out.name
        )
        _run_full_workflow(
            self.rmode, "files", "test/reproducibility/reproGraphs/", self.temp_out.name
        )

        file = open(self.temp_out.name + "/" + "apps" + "PGS.graph")
        self.pgs_node_data = json.load(file)[0:-1]
        file.close()
        file = open(self.temp_out.name + "/" + "files" + "PGS.graph")
        self.pgs_file_data = json.load(file)[0:-1]
        file.close()

    def test_app_accumulate(self):
        """
        Only type matters
        """
        expected = {"type", "dt"}
        self._setup()
        for drop in enumerate(self.pgs_node_data):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            self.assertEqual(expected, hash_data.keys())

    def test_data_accumulate(self):
        """
        Only type matters
        """
        expected = {"type", "storage"}
        self._setup()
        for drop in enumerate(self.pgs_file_data):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            self.assertEqual(expected, hash_data.keys())

    @unittest.skip("pg_generator does not like sample graphs")
    def test_group_accumulate(self):
        """
        Only type matters
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Only type matters
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_other_accumulate(self):
        """
        Only type matters
        """
        self.assertEqual(True, False)


class AccumulatePGTPartitionRepeatData(unittest.TestCase):
    """
    Tests the repeat standard at the physical graph template level.
    """

    rmode = ReproducibilityFlags.REPEAT
    temp_out = tempfile.TemporaryDirectory()
    pgt_node_data = None
    pgt_file_data = None

    def _setup(self):
        _run_full_workflow(
            self.rmode, "apps", "test/reproducibility/reproGraphs/", self.temp_out.name
        )
        _run_full_workflow(
            self.rmode, "files", "test/reproducibility/reproGraphs/", self.temp_out.name
        )

        file = open(self.temp_out.name + "/" + "apps" + "PGT.graph")
        self.pgt_node_data = json.load(file)[0:-1]
        file.close()
        file = open(self.temp_out.name + "/" + "files" + "PGT.graph")
        self.pgt_file_data = json.load(file)[0:-1]
        file.close()

    def test_app_accumulate(self):
        """
        Only type matters when repeating.
        """
        expected = {"type", "dt"}
        self._setup()
        for drop in enumerate(self.pgt_node_data):
            hash_data = accumulate_pgt_partition_drop_data(drop[1])
            self.assertEqual(expected, hash_data.keys())

    def test_data_accumulate(self):
        """
        Only type matters when repeating.
        """
        expected = {"type", "storage"}
        self._setup()
        for drop in enumerate(self.pgt_file_data):
            hash_data = accumulate_pgt_partition_drop_data(drop[1])
            self.assertEqual(expected, hash_data.keys())

    @unittest.skip("pg_generator does not like sample graphs")
    def test_group_accumulate(self):
        """
        Only type matters when repeating.
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Only type matters when repeating.
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_other_accumulate(self):
        """
        Should not matter when repeating.
        """
        self.assertEqual(True, False)


class AccumulatePGRepeatData(unittest.TestCase):
    """
    Tests the repeat standard  at the physical graph level
    """

    rmode = ReproducibilityFlags.REPEAT
    temp_out = tempfile.TemporaryDirectory()
    pg_node_data = None
    pg_file_data = None

    def _setup(self):
        _run_full_workflow(
            self.rmode, "apps", "test/reproducibility/reproGraphs/", self.temp_out.name
        )
        _run_full_workflow(
            self.rmode, "files", "test/reproducibility/reproGraphs/", self.temp_out.name
        )

        file = open(self.temp_out.name + "/" + "apps" + "PGT.graph")
        self.pg_node_data = json.load(file)[0:-1]
        file.close()
        file = open(self.temp_out.name + "/" + "files" + "PGT.graph")
        self.pg_file_data = json.load(file)[0:-1]
        file.close()

    def test_app_accumulate(self):
        """
        Nothing matters for repeating.
        """
        expected = {}
        self._setup()
        for drop in enumerate(self.pg_node_data):
            hash_data = accumulate_pg_drop_data(drop[1])
            self.assertEqual(expected, dict(hash_data.keys()))

    def test_data_accumulate(self):
        """
        Nothing matters for repeating.
        """
        expected = {}
        self._setup()
        for drop in enumerate(self.pg_file_data):
            hash_data = accumulate_pg_drop_data(drop[1])
            self.assertEqual(expected, dict(hash_data.keys()))

    @unittest.skip("pg_generator does not like sample graphs")
    def test_group_accumulate(self):
        """
        Nothing matters for repeating.
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Nothing matters for repeating.
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_other_accumulate(self):
        """
        Does not matter
        """
        self.assertEqual(True, False)


class AccumulateLGTRecomputeData(unittest.TestCase):
    """
    Tests the recompute standard at the logical graph template level.
    """

    rmode = ReproducibilityFlags.RECOMPUTE
    expected = {
        "categoryType",
        "category",
        "inputPorts",
        "inputLocalPorts",
        "outputPorts",
        "outputLocalPorts",
        "streaming",
    }

    file = open("test/reproducibility/reproGraphs/apps.graph")
    lgt_node_data = json.load(file)["nodeDataArray"]
    file.close()
    file = open("test/reproducibility/reproGraphs/files.graph")
    lgt_files_data = json.load(file)["nodeDataArray"]
    file.close()
    file = open("test/reproducibility/reproGraphs/groups.graph")
    lgt_groups_data = json.load(file)["nodeDataArray"]
    file.close()
    file = open("test/reproducibility/reproGraphs/misc.graph")
    lgt_misc_data = json.load(file)["nodeDataArray"]
    file.close()

    def test_app_accumulate(self):
        """
        Tests that lgt recompute data is collected for application types
        """
        for drop in enumerate(self.lgt_node_data):
            hash_data = accumulate_lgt_drop_data(drop[1], self.rmode)
            self.assertEqual(self.expected, hash_data.keys())

    def test_data_accumulate(self):
        """
        Tests that lgt recompute data is collected for file types
        """
        for drop in enumerate(self.lgt_files_data):
            hash_data = accumulate_lgt_drop_data(drop[1], self.rmode)
            self.assertEqual(self.expected, hash_data.keys())

    def test_group_accumulate(self):
        """
        Tests that lgt recompute data is collected for group types
        """
        for drop in enumerate(self.lgt_groups_data):
            hash_data = accumulate_lgt_drop_data(drop[1], self.rmode)
            self.assertEqual(self.expected, hash_data.keys())

    def test_other_accumulate(self):
        """
        Tests that lgt recompute data is collected for other types
        """
        for drop in enumerate(self.lgt_misc_data):
            hash_data = accumulate_lgt_drop_data(drop[1], self.rmode)
            self.assertEqual(self.expected, hash_data.keys())


class AccumulateLGRecomputeData(unittest.TestCase):
    """
    Tests the recompute standard at the logical graph level.
    """

    rmode = ReproducibilityFlags.RECOMPUTE
    expected = {}
    temp_out = tempfile.TemporaryDirectory()
    lg_node_data = None
    lg_files_data = None
    lg_group_data = None
    lg_misc_data = None

    def _setup(self):
        _fill_workflow(
            self.rmode, "apps", "test/reproducibility/reproGraphs/", self.temp_out.name
        )
        _fill_workflow(
            self.rmode, "files", "test/reproducibility/reproGraphs/", self.temp_out.name
        )
        _fill_workflow(
            self.rmode,
            "groups",
            "test/reproducibility/reproGraphs/",
            self.temp_out.name,
        )
        _fill_workflow(
            self.rmode, "misc", "test/reproducibility/reproGraphs/", self.temp_out.name
        )

        file = open(self.temp_out.name + "/" + "apps" + "LG.graph")
        self.lg_node_data = json.load(file)["nodeDataArray"]
        file.close()
        file = open(self.temp_out.name + "/" + "files" + "LG.graph")
        self.lg_files_data = json.load(file)["nodeDataArray"]
        file.close()
        file = open(self.temp_out.name + "/" + "groups" + "LG.graph")
        self.lg_group_data = json.load(file)["nodeDataArray"]
        file.close()
        file = open(self.temp_out.name + "/" + "misc" + "LG.graph")
        self.lg_misc_data = json.load(file)["nodeDataArray"]
        file.close()

    def _bash(self, drop):
        expected = {"execution_time", "num_cpus", "Arg01"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _dynlib(self, drop):
        expected = {"execution_time", "num_cpus", "libpath"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _mpi(self, drop):
        expected = {"execution_time", "num_cpus", "num_of_procs"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _docker(self, drop):
        expected = {
            "execution_time",
            "num_cpus",
            "image",
            "command",
            "user",
            "ensureUserAndSwitch",
            "removeContainer",
            "additionalBindings",
        }
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _component(self, drop):
        expected = {"execution_time", "num_cpus", "appclass"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _memory(self, drop):
        expected = {"data_volume"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _file(self, drop):
        expected = {"data_volume", "check_filepath_exists", "filepath", "dirname"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _ngas(self, drop):
        expected = {"data_volume"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _groupby(self, drop):
        expected = {
            "inputApplicationName",
            "inputApplicationType",
            "group_key",
            "group_axis",
        }
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data)

    def _scatter(self, drop):
        expected = {
            "inputApplicationName",
            "inputApplicationType",
            "num_of_copies",
            "scatter_axis",
        }
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data)

    def _loop(self, drop):
        expected = {"inputApplicationName", "inputApplicationType", "num_of_iter"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data)

    def test_all_accumulate(self):
        """
        We make the data expected for each accounted for drop type explicit.
        """
        self._setup()
        self._bash(self.lg_node_data[0])
        self._dynlib(self.lg_node_data[1])
        self._mpi(self.lg_node_data[2])
        self._docker(self.lg_node_data[3])
        self._component(self.lg_node_data[4])
        self._memory(self.lg_files_data[0])
        self._file(self.lg_files_data[1])
        self._ngas(self.lg_files_data[2])


class AccumulatePGTUnrollRecomputeData(unittest.TestCase):
    """
    Tests the recompute standard at the physical graph template level.
    Can currently only test apps.graph and files.graph, the translator cannot deal with groups
    or comments easily.
    """

    rmode = ReproducibilityFlags.RECOMPUTE
    temp_out = tempfile.TemporaryDirectory()
    pgs_node_data = None
    pgs_file_data = None

    def _setup(self):
        _run_full_workflow(
            self.rmode, "apps", "test/reproducibility/reproGraphs/", self.temp_out.name
        )
        _run_full_workflow(
            self.rmode, "files", "test/reproducibility/reproGraphs/", self.temp_out.name
        )

        file = open(self.temp_out.name + "/" + "apps" + "PGS.graph")
        self.pgs_node_data = json.load(file)[0:-1]
        file.close()
        file = open(self.temp_out.name + "/" + "files" + "PGS.graph")
        self.pgs_file_data = json.load(file)[0:-1]
        file.close()

    def test_app_accumulate(self):
        """
        Type and rank matters.
        """
        expected = {"type", "dt", "rank"}
        self._setup()
        for drop in enumerate(self.pgs_node_data):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            self.assertEqual(expected, hash_data.keys())

    def test_data_accumulate(self):
        """
        Type and rank matters.
        """
        expected = {"type", "storage", "rank"}
        self._setup()
        for drop in enumerate(self.pgs_file_data):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            self.assertEqual(expected, hash_data.keys())

    @unittest.skip("pg_generator does not like sample graphs")
    def test_group_accumulate(self):
        """
        Type and rank matters.
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Type and rank matters.
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_other_accumulate(self):
        """
        Should not matter
        """
        self.assertEqual(True, False)


class AccumulatePGTPartitionRecomputeData(unittest.TestCase):
    """
    Tests the recompute standard at the physical graph template level.
    """

    rmode = ReproducibilityFlags.RECOMPUTE
    temp_out = tempfile.TemporaryDirectory()
    pgt_node_data = None
    pgt_file_data = None

    def _setup(self):
        _run_full_workflow(
            self.rmode, "apps", "test/reproducibility/reproGraphs/", self.temp_out.name
        )
        _run_full_workflow(
            self.rmode, "files", "test/reproducibility/reproGraphs/", self.temp_out.name
        )

        file = open(self.temp_out.name + "/" + "apps" + "PGT.graph")
        self.pgt_node_data = json.load(file)[0:-1]
        file.close()
        file = open(self.temp_out.name + "/" + "files" + "PGT.graph")
        self.pgt_file_data = json.load(file)[0:-1]
        file.close()

    def test_app_accumulate(self):
        """
        Type, rank and machine information matters
        """
        expected = ["type", "dt", "rank", "node", "island"]
        self._setup()
        for drop in enumerate(self.pgt_node_data):
            hash_data = accumulate_pgt_partition_drop_data(drop[1])
            self.assertEqual(sorted(expected), sorted(list(hash_data.keys())))

    def test_data_accumulate(self):
        """
        Type, rank and machine information matters
        """
        expected = ["type", "storage", "rank", "node", "island"]
        self._setup()
        for drop in enumerate(self.pgt_file_data):
            hash_data = accumulate_pgt_partition_drop_data(drop[1])
            self.assertEqual(sorted(expected), sorted(list(hash_data.keys())))

    @unittest.skip("pg_generator does not like sample graphs")
    def test_group_accumulate(self):
        """
        Type, rank and machine information matters
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Type, rank and machine information matters
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_other_accumulate(self):
        """
        Should not matter
        """
        self.assertEqual(True, False)


class AccumulatePGRecomputeData(unittest.TestCase):
    """
    Tests the recompute standard at the physical graph level
    """

    rmode = ReproducibilityFlags.RECOMPUTE
    temp_out = tempfile.TemporaryDirectory()
    pg_node_data = None
    pg_file_data = None

    def _setup(self):
        _run_full_workflow(
            self.rmode, "apps", "test/reproducibility/reproGraphs/", self.temp_out.name
        )
        _run_full_workflow(
            self.rmode, "files", "test/reproducibility/reproGraphs/", self.temp_out.name
        )

        file = open(self.temp_out.name + "/" + "apps" + "PGT.graph")
        self.pg_node_data = json.load(file)[0:-1]
        file.close()
        file = open(self.temp_out.name + "/" + "files" + "PGT.graph")
        self.pg_file_data = json.load(file)[0:-1]
        file.close()

    def test_app_accumulate(self):
        """
        Machine information matters when recomputing
        """
        expected = {"node", "island"}
        self._setup()
        for drop in enumerate(self.pg_node_data):
            hash_data = accumulate_pg_drop_data(drop[1])
            self.assertEqual(expected, hash_data.keys())

    def test_data_accumulate(self):
        """
        Machine information matters when recomputing
        """
        expected = {"node", "island"}
        self._setup()
        for drop in enumerate(self.pg_file_data):
            hash_data = accumulate_pg_drop_data(drop[1])
            self.assertEqual(expected, hash_data.keys())

    @unittest.skip("pg_generator does not like sample graphs")
    def test_group_accumulate(self):
        """
        Machine information matters when recomputing
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Machine information matters when recomputing
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_other_accumulate(self):
        """
        Should not matter.
        """
        self.assertEqual(True, False)


class AccumulateLGTReproduceData(unittest.TestCase):
    """
    Tests the reproduce standard at the logical graph template level.
    """

    rmode = ReproducibilityFlags.REPRODUCE
    expected = {"categoryType", "category"}

    file = open("test/reproducibility/reproGraphs/apps.graph")
    lgt_node_data = json.load(file)["nodeDataArray"]
    file.close()
    file = open("test/reproducibility/reproGraphs/files.graph")
    lgt_files_data = json.load(file)["nodeDataArray"]
    file.close()
    file = open("test/reproducibility/reproGraphs/groups.graph")
    lgt_groups_data = json.load(file)["nodeDataArray"]
    file.close()
    file = open("test/reproducibility/reproGraphs/misc.graph")
    lgt_misc_data = json.load(file)["nodeDataArray"]
    file.close()

    def test_app_accumulate(self):
        """
        Tests that lgt reproduce data is collected for application types
        """
        for drop in enumerate(self.lgt_node_data):
            hash_data = accumulate_lgt_drop_data(drop[1], self.rmode)
            self.assertEqual(self.expected, hash_data.keys())

    def test_data_accumulate(self):
        """
        Tests that lgt reproduce data is collected for file types
        """
        for drop in enumerate(self.lgt_files_data):
            hash_data = accumulate_lgt_drop_data(drop[1], self.rmode)
            self.assertEqual(self.expected, hash_data.keys())

    def test_group_accumulate(self):
        """
        Tests that lgt reproduce data is collected for group types
        """
        for drop in enumerate(self.lgt_groups_data):
            hash_data = accumulate_lgt_drop_data(drop[1], self.rmode)
            self.assertEqual(self.expected, hash_data.keys())

    def test_other_accumulate(self):
        """
        Tests that lgt reproduce data is collected for other types
        """
        for drop in enumerate(self.lgt_misc_data):
            hash_data = accumulate_lgt_drop_data(drop[1], self.rmode)
            self.assertEqual(self.expected, hash_data.keys())


class AccumulateLGReproduceData(unittest.TestCase):
    """
    Tests the reproduce standard at the logical graph level.
    """

    rmode = ReproducibilityFlags.REPRODUCE
    expected = {}
    temp_out = tempfile.TemporaryDirectory()
    lg_node_data = None
    lg_files_data = None
    lg_group_data = None
    lg_misc_data = None

    def _setup(self):
        _fill_workflow(
            self.rmode, "apps", "test/reproducibility/reproGraphs/", self.temp_out.name
        )
        _fill_workflow(
            self.rmode, "files", "test/reproducibility/reproGraphs/", self.temp_out.name
        )
        _fill_workflow(
            self.rmode,
            "groups",
            "test/reproducibility/reproGraphs/",
            self.temp_out.name,
        )
        _fill_workflow(
            self.rmode, "misc", "test/reproducibility/reproGraphs/", self.temp_out.name
        )

        file = open(self.temp_out.name + "/" + "apps" + "LG.graph")
        self.lg_node_data = json.load(file)["nodeDataArray"]
        file.close()
        file = open(self.temp_out.name + "/" + "files" + "LG.graph")
        self.lg_files_data = json.load(file)["nodeDataArray"]
        file.close()
        file = open(self.temp_out.name + "/" + "groups" + "LG.graph")
        self.lg_group_data = json.load(file)["nodeDataArray"]
        file.close()
        file = open(self.temp_out.name + "/" + "misc" + "LG.graph")
        self.lg_misc_data = json.load(file)["nodeDataArray"]
        file.close()

    def _bash(self, drop):
        expected = {}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, dict(hash_data.keys()))

    def _dynlib(self, drop):
        expected = {}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, dict(hash_data.keys()))

    def _mpi(self, drop):
        expected = {}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, dict(hash_data.keys()))

    def _docker(self, drop):
        expected = {}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, dict(hash_data.keys()))

    def _component(self, drop):
        expected = {}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, dict(hash_data.keys()))

    def _memory(self, drop):
        expected = {}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, dict(hash_data.keys()))

    def _file(self, drop):
        expected = {}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, dict(hash_data.keys()))

    def _ngas(self, drop):
        expected = {}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, dict(hash_data.keys()))

    def _groupby(self, drop):
        expected = {}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, dict(hash_data.keys()))

    def _scatter(self, drop):
        expected = {}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, dict(hash_data.keys()))

    def _loop(self, drop):
        expected = {}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, dict(hash_data.keys()))

    def test_all_accumulate(self):
        """
        We make the data expected for each accounted for drop type explicit.
        """
        self._setup()
        self._bash(self.lg_node_data[0])
        self._dynlib(self.lg_node_data[1])
        self._mpi(self.lg_node_data[2])
        self._docker(self.lg_node_data[3])
        self._component(self.lg_node_data[4])
        self._memory(self.lg_files_data[0])
        self._file(self.lg_files_data[1])
        self._ngas(self.lg_files_data[2])


class AccumulatePGTUnrollReproduceData(unittest.TestCase):
    """
    Tests the reproduce standard at the physical graph template level.
    Can currently only test apps.graph and files.graph, the translator cannot deal with groups
    or comments easily.
    """

    rmode = ReproducibilityFlags.REPRODUCE
    temp_out = tempfile.TemporaryDirectory()
    pgs_node_data = None
    pgs_file_data = None

    def _setup(self):
        _run_full_workflow(
            self.rmode, "apps", "test/reproducibility/reproGraphs/", self.temp_out.name
        )
        _run_full_workflow(
            self.rmode, "files", "test/reproducibility/reproGraphs/", self.temp_out.name
        )

        file = open(self.temp_out.name + "/" + "apps" + "PGS.graph")
        self.pgs_node_data = json.load(file)[0:-1]
        file.close()
        file = open(self.temp_out.name + "/" + "files" + "PGS.graph")
        self.pgs_file_data = json.load(file)[0:-1]
        file.close()

    def test_app_accumulate(self):
        """
        Only type matters
        """
        expected = {"type"}
        self._setup()
        for drop in enumerate(self.pgs_node_data):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            self.assertEqual(expected, hash_data.keys())

    def test_data_accumulate(self):
        """
        Only type matters
        """
        expected = {"type", "storage"}
        self._setup()
        for drop in enumerate(self.pgs_file_data):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            self.assertEqual(expected, hash_data.keys())

    @unittest.skip("pg_generator does not like sample graphs")
    def test_group_accumulate(self):
        """
        Only type matters
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Only type matters
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_other_accumulate(self):
        """
        Does not matter
        """
        self.assertEqual(True, False)


class AccumulatePGTPartitionReproduceData(unittest.TestCase):
    """
    Tests the reproduce standard at the physical graph template level.
    """

    rmode = ReproducibilityFlags.REPRODUCE
    temp_out = tempfile.TemporaryDirectory()
    pgt_node_data = None
    pgt_file_data = None

    def _setup(self):
        _run_full_workflow(
            self.rmode, "apps", "test/reproducibility/reproGraphs/", self.temp_out.name
        )
        _run_full_workflow(
            self.rmode, "files", "test/reproducibility/reproGraphs/", self.temp_out.name
        )

        file = open(self.temp_out.name + "/" + "apps" + "PGT.graph")
        self.pgt_node_data = json.load(file)[0:-1]
        file.close()
        file = open(self.temp_out.name + "/" + "files" + "PGT.graph")
        self.pgt_file_data = json.load(file)[0:-1]
        file.close()

    def test_app_accumulate(self):
        """
        Only type matters
        """
        expected = ["type"]
        self._setup()
        for drop in enumerate(self.pgt_node_data):
            hash_data = accumulate_pgt_partition_drop_data(drop[1])
            self.assertEqual(expected, list(hash_data.keys()))

    def test_data_accumulate(self):
        """
        Only type matters
        """
        expected = ["type", "storage"]
        self._setup()
        for drop in enumerate(self.pgt_file_data):
            hash_data = accumulate_pgt_partition_drop_data(drop[1])
            self.assertEqual(expected, list(hash_data.keys()))

    @unittest.skip("pg_generator does not like sample graphs")
    def test_group_accumulate(self):
        """
        Only type matters
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Only type matters
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_other_accumulate(self):
        """
        Does not matter
        """
        self.assertEqual(True, False)


class AccumulatePGReproduceData(unittest.TestCase):
    """
    Tests the reproduce standard at the physical graph level
    """

    rmode = ReproducibilityFlags.REPRODUCE
    temp_out = tempfile.TemporaryDirectory()
    pg_node_data = None
    pg_file_data = None

    def _setup(self):
        _run_full_workflow(
            self.rmode, "apps", "test/reproducibility/reproGraphs/", self.temp_out.name
        )
        _run_full_workflow(
            self.rmode, "files", "test/reproducibility/reproGraphs/", self.temp_out.name
        )

        file = open(self.temp_out.name + "/" + "apps" + "PGT.graph")
        self.pg_node_data = json.load(file)[0:-1]
        file.close()
        file = open(self.temp_out.name + "/" + "files" + "PGT.graph")
        self.pg_file_data = json.load(file)[0:-1]
        file.close()

    def test_app_accumulate(self):
        """
        App information does not matter when reproducing
        """
        expected = {}
        self._setup()
        for drop in enumerate(self.pg_node_data):
            hash_data = accumulate_pg_drop_data(drop[1])
            self.assertEqual(expected, dict(hash_data.keys()))

    def test_data_accumulate(self):
        """
        Only data information matters, not the type of drop
        """
        expected = {}
        self._setup()
        for drop in enumerate(self.pg_file_data):
            hash_data = accumulate_pg_drop_data(drop[1])
            self.assertEqual(expected, dict(hash_data.keys()))

    @unittest.skip("pg_generator does not like sample graphs")
    def test_group_accumulate(self):
        """
        Group information does not matter when reproducing
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Control information does not matter when reproducing
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_other_accumulate(self):
        """
        Does not matter
        """
        self.assertEqual(True, False)


class AccumulateLGTReplicateSciData(unittest.TestCase):
    """
    Tests the replicate-sci standard at the logical graph template level.
    """

    rmode = ReproducibilityFlags.REPLICATE_SCI
    expected = {
        "categoryType",
        "category",
        "inputPorts",
        "inputLocalPorts",
        "outputPorts",
        "outputLocalPorts",
        "streaming",
    }

    file = open("test/reproducibility/reproGraphs/apps.graph")
    lgt_node_data = json.load(file)["nodeDataArray"]
    file.close()
    file = open("test/reproducibility/reproGraphs/files.graph")
    lgt_files_data = json.load(file)["nodeDataArray"]
    file.close()
    file = open("test/reproducibility/reproGraphs/groups.graph")
    lgt_groups_data = json.load(file)["nodeDataArray"]
    file.close()
    file = open("test/reproducibility/reproGraphs/misc.graph")
    lgt_misc_data = json.load(file)["nodeDataArray"]
    file.close()

    def test_app_accumulate(self):
        """
        Tests that lgt replicate-sci data is collected for application types
        """
        for drop in enumerate(self.lgt_node_data):
            hash_data = accumulate_lgt_drop_data(drop[1], self.rmode)
            self.assertEqual(self.expected, hash_data.keys())

    def test_data_accumulate(self):
        """
        Tests that lgt replicate-sci data is collected for file types
        """
        for drop in enumerate(self.lgt_files_data):
            hash_data = accumulate_lgt_drop_data(drop[1], self.rmode)
            self.assertEqual(self.expected, hash_data.keys())

    def test_group_accumulate(self):
        """
        Tests that lgt replicate-sci data is collected for group types
        """
        for drop in enumerate(self.lgt_groups_data):
            hash_data = accumulate_lgt_drop_data(drop[1], self.rmode)
            self.assertEqual(self.expected, hash_data.keys())

    def test_other_accumulate(self):
        """
        Tests that lgt replicate-sci data is collected for other types
        """
        for drop in enumerate(self.lgt_misc_data):
            hash_data = accumulate_lgt_drop_data(drop[1], self.rmode)
            self.assertEqual(self.expected, hash_data.keys())


class AccumulateLGReplicateSciData(unittest.TestCase):
    """
    Tests the replicate-sci standard at the logical graph level.
    """

    rmode = ReproducibilityFlags.REPLICATE_SCI
    expected = {}
    temp_out = tempfile.TemporaryDirectory()
    lg_node_data = None
    lg_files_data = None
    lg_group_data = None
    lg_misc_data = None

    def _setup(self):
        _fill_workflow(
            self.rmode, "apps", "test/reproducibility/reproGraphs/", self.temp_out.name
        )
        _fill_workflow(
            self.rmode, "files", "test/reproducibility/reproGraphs/", self.temp_out.name
        )
        _fill_workflow(
            self.rmode,
            "groups",
            "test/reproducibility/reproGraphs/",
            self.temp_out.name,
        )
        _fill_workflow(
            self.rmode, "misc", "test/reproducibility/reproGraphs/", self.temp_out.name
        )

        file = open(self.temp_out.name + "/" + "apps" + "LG.graph")
        self.lg_node_data = json.load(file)["nodeDataArray"]
        file.close()
        file = open(self.temp_out.name + "/" + "files" + "LG.graph")
        self.lg_files_data = json.load(file)["nodeDataArray"]
        file.close()
        file = open(self.temp_out.name + "/" + "groups" + "LG.graph")
        self.lg_group_data = json.load(file)["nodeDataArray"]
        file.close()
        file = open(self.temp_out.name + "/" + "misc" + "LG.graph")
        self.lg_misc_data = json.load(file)["nodeDataArray"]
        file.close()

    def test_all_accumulate(self):
        """
        Similar to rerunning. Nothing matters
        """
        self._setup()
        for drop in enumerate(
            self.lg_node_data
            + self.lg_files_data
            + self.lg_group_data
            + self.lg_misc_data
        ):
            hash_data = accumulate_lg_drop_data(drop[1], self.rmode)
            self.assertEqual(self.expected, dict(hash_data.keys()))


class AccumulatePGTUnrollReplicateSciData(unittest.TestCase):
    """
    Tests the replicate-sci standard at the physical graph template level.
    Can currently only test apps.graph and files.graph, the translator cannot deal with groups
    or comments easily.
    """

    rmode = ReproducibilityFlags.REPLICATE_SCI
    temp_out = tempfile.TemporaryDirectory()
    pgs_node_data = None
    pgs_file_data = None

    def _setup(self):
        _run_full_workflow(
            self.rmode, "apps", "test/reproducibility/reproGraphs/", self.temp_out.name
        )
        _run_full_workflow(
            self.rmode, "files", "test/reproducibility/reproGraphs/", self.temp_out.name
        )

        file = open(self.temp_out.name + "/" + "apps" + "PGS.graph")
        self.pgs_node_data = json.load(file)[0:-1]
        file.close()
        file = open(self.temp_out.name + "/" + "files" + "PGS.graph")
        self.pgs_file_data = json.load(file)[0:-1]
        file.close()

    def test_app_accumulate(self):
        """
        Only type matters
        """
        expected = {"type", "dt"}
        self._setup()
        for drop in enumerate(self.pgs_node_data):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            self.assertEqual(expected, hash_data.keys())

    def test_data_accumulate(self):
        """
        Only type matters
        """
        expected = {"type", "storage"}
        self._setup()
        for drop in enumerate(self.pgs_file_data):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            self.assertEqual(expected, hash_data.keys())

    @unittest.skip("pg_generator does not like sample graphs")
    def test_group_accumulate(self):
        """
        Only type matters
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Only type matters
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_other_accumulate(self):
        """
        Does not matter
        """
        self.assertEqual(True, False)


class AccumulatePGTPartitionReplicateSciData(unittest.TestCase):
    """
    Tests the replicate-sci standard at the physical graph template level.
    """

    rmode = ReproducibilityFlags.REPLICATE_SCI
    temp_out = tempfile.TemporaryDirectory()
    pgt_node_data = None
    pgt_file_data = None

    def _setup(self):
        _run_full_workflow(
            self.rmode, "apps", "test/reproducibility/reproGraphs/", self.temp_out.name
        )
        _run_full_workflow(
            self.rmode, "files", "test/reproducibility/reproGraphs/", self.temp_out.name
        )

        file = open(self.temp_out.name + "/" + "apps" + "PGT.graph")
        self.pgt_node_data = json.load(file)[0:-1]
        file.close()
        file = open(self.temp_out.name + "/" + "files" + "PGT.graph")
        self.pgt_file_data = json.load(file)[0:-1]
        file.close()

    def test_app_accumulate(self):
        """
        Only type matters
        """
        expected = {"type", "dt"}
        self._setup()
        for drop in enumerate(self.pgt_node_data):
            hash_data = accumulate_pgt_partition_drop_data(drop[1])
            self.assertEqual(expected, hash_data.keys())

    def test_data_accumulate(self):
        """
        Only type matters
        """
        expected = {"type", "storage"}
        self._setup()
        for drop in enumerate(self.pgt_file_data):
            hash_data = accumulate_pgt_partition_drop_data(drop[1])
            self.assertEqual(expected, hash_data.keys())

    @unittest.skip("pg_generator does not like sample graphs")
    def test_group_accumulate(self):
        """
        Only type matters
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Only type matters
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_other_accumulate(self):
        """
        Does not matter
        """
        self.assertEqual(True, False)


class AccumulatePGReplicateSciData(unittest.TestCase):
    """
    Tests the replicate-sci standard at the physical graph level
    """

    rmode = ReproducibilityFlags.REPLICATE_SCI
    temp_out = tempfile.TemporaryDirectory()
    pg_node_data = None
    pg_file_data = None

    def _setup(self):
        _run_full_workflow(
            self.rmode, "apps", "test/reproducibility/reproGraphs/", self.temp_out.name
        )
        _run_full_workflow(
            self.rmode, "files", "test/reproducibility/reproGraphs/", self.temp_out.name
        )

        file = open(self.temp_out.name + "/" + "apps" + "PGT.graph")
        self.pg_node_data = json.load(file)[0:-1]
        file.close()
        file = open(self.temp_out.name + "/" + "files" + "PGT.graph")
        self.pg_file_data = json.load(file)[0:-1]
        file.close()

    def test_app_accumulate(self):
        """
        Nothing matters.
        """
        expected = {}
        self._setup()
        for drop in enumerate(self.pg_node_data):
            hash_data = accumulate_pg_drop_data(drop[1])
            self.assertEqual(expected, dict(hash_data.keys()))

    def test_data_accumulate(self):
        """
        Nothing matters.
        """
        expected = {}
        self._setup()
        for drop in enumerate(self.pg_node_data):
            hash_data = accumulate_pg_drop_data(drop[1])
            self.assertEqual(expected, dict(hash_data.keys()))

    @unittest.skip("pg_generator does not like sample graphs")
    def test_group_accumulate(self):
        """
        Nothing matters.
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Nothing matters.
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_other_accumulate(self):
        """
        Does not matter.
        """
        self.assertEqual(True, False)


class AccumulateLGTReplicateCompData(unittest.TestCase):
    """
    Tests the replicate-comp standard at the logical graph template level.
    """

    rmode = ReproducibilityFlags.REPLICATE_COMP
    expected = {
        "categoryType",
        "category",
        "inputPorts",
        "inputLocalPorts",
        "outputPorts",
        "outputLocalPorts",
        "streaming",
    }

    file = open("test/reproducibility/reproGraphs/apps.graph")
    lgt_node_data = json.load(file)["nodeDataArray"]
    file.close()
    file = open("test/reproducibility/reproGraphs/files.graph")
    lgt_files_data = json.load(file)["nodeDataArray"]
    file.close()
    file = open("test/reproducibility/reproGraphs/groups.graph")
    lgt_groups_data = json.load(file)["nodeDataArray"]
    file.close()
    file = open("test/reproducibility/reproGraphs/misc.graph")
    lgt_misc_data = json.load(file)["nodeDataArray"]
    file.close()

    def test_app_accumulate(self):
        """
        Tests that lgt replicate-comp data is collected for application types
        """
        for drop in enumerate(self.lgt_node_data):
            hash_data = accumulate_lgt_drop_data(drop[1], self.rmode)
            self.assertEqual(self.expected, hash_data.keys())

    def test_data_accumulate(self):
        """
        Tests that lgt replicate-comp data is collected for file types
        """
        for drop in enumerate(self.lgt_files_data):
            hash_data = accumulate_lgt_drop_data(drop[1], self.rmode)
            self.assertEqual(self.expected, hash_data.keys())

    def test_group_accumulate(self):
        """
        Tests that lgt replicate-comp data is collected for group types
        """
        for drop in enumerate(self.lgt_groups_data):
            hash_data = accumulate_lgt_drop_data(drop[1], self.rmode)
            self.assertEqual(self.expected, hash_data.keys())

    def test_other_accumulate(self):
        """
        Tests that lgt replicate-comp data is collected for other types
        """
        for drop in enumerate(self.lgt_misc_data):
            hash_data = accumulate_lgt_drop_data(drop[1], self.rmode)
            self.assertEqual(self.expected, hash_data.keys())


class AccumulateLGReplicateCompData(unittest.TestCase):
    """
    Tests the replicate-comp standard at the logical graph level.
    """

    rmode = ReproducibilityFlags.REPLICATE_COMP
    expected = {}
    temp_out = tempfile.TemporaryDirectory()
    lg_node_data = None
    lg_files_data = None
    lg_group_data = None
    lg_misc_data = None

    def _setup(self):
        _fill_workflow(
            self.rmode, "apps", "test/reproducibility/reproGraphs/", self.temp_out.name
        )
        _fill_workflow(
            self.rmode, "files", "test/reproducibility/reproGraphs/", self.temp_out.name
        )
        _fill_workflow(
            self.rmode,
            "groups",
            "test/reproducibility/reproGraphs/",
            self.temp_out.name,
        )
        _fill_workflow(
            self.rmode, "misc", "test/reproducibility/reproGraphs/", self.temp_out.name
        )

        file = open(self.temp_out.name + "/" + "apps" + "LG.graph")
        self.lg_node_data = json.load(file)["nodeDataArray"]
        file.close()
        file = open(self.temp_out.name + "/" + "files" + "LG.graph")
        self.lg_files_data = json.load(file)["nodeDataArray"]
        file.close()
        file = open(self.temp_out.name + "/" + "groups" + "LG.graph")
        self.lg_group_data = json.load(file)["nodeDataArray"]
        file.close()
        file = open(self.temp_out.name + "/" + "misc" + "LG.graph")
        self.lg_misc_data = json.load(file)["nodeDataArray"]
        file.close()

    def _bash(self, drop):
        expected = {"execution_time", "num_cpus", "Arg01"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _dynlib(self, drop):
        expected = {"execution_time", "num_cpus", "libpath"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _mpi(self, drop):
        expected = {"execution_time", "num_cpus", "num_of_procs"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _docker(self, drop):
        expected = {
            "execution_time",
            "num_cpus",
            "image",
            "command",
            "user",
            "ensureUserAndSwitch",
            "removeContainer",
            "additionalBindings",
        }
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _component(self, drop):
        expected = {"execution_time", "num_cpus", "appclass"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _memory(self, drop):
        expected = {"data_volume"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _file(self, drop):
        expected = {"data_volume", "check_filepath_exists", "filepath", "dirname"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _ngas(self, drop):
        expected = {"data_volume"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _groupby(self, drop):
        expected = {
            "inputApplicationName",
            "inputApplicationType",
            "group_key",
            "group_axis",
        }
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data)

    def _scatter(self, drop):
        expected = {
            "inputApplicationName",
            "inputApplicationType",
            "num_of_copies",
            "scatter_axis",
        }
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data)

    def _loop(self, drop):
        expected = {"inputApplicationName", "inputApplicationType", "num_of_iter"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data)

    def test_all_accumulate(self):
        """
        We make the data expected for each accounted for drop type explicit.
        """
        self._setup()
        self._bash(self.lg_node_data[0])
        self._dynlib(self.lg_node_data[1])
        self._mpi(self.lg_node_data[2])
        self._docker(self.lg_node_data[3])
        self._component(self.lg_node_data[4])
        self._memory(self.lg_files_data[0])
        self._file(self.lg_files_data[1])
        self._ngas(self.lg_files_data[2])


class AccumulatePGTUnrollReplicateCompData(unittest.TestCase):
    """
    Tests the replicate-comp standard at the physical graph template level.
    Can currently only test apps.graph and files.graph, the translator cannot deal with groups
    or comments easily.
    """

    rmode = ReproducibilityFlags.REPLICATE_COMP
    temp_out = tempfile.TemporaryDirectory()
    pgs_node_data = None
    pgs_file_data = None

    def _setup(self):
        _run_full_workflow(
            self.rmode, "apps", "test/reproducibility/reproGraphs/", self.temp_out.name
        )
        _run_full_workflow(
            self.rmode, "files", "test/reproducibility/reproGraphs/", self.temp_out.name
        )

        file = open(self.temp_out.name + "/" + "apps" + "PGS.graph")
        self.pgs_node_data = json.load(file)[0:-1]
        file.close()
        file = open(self.temp_out.name + "/" + "files" + "PGS.graph")
        self.pgs_file_data = json.load(file)[0:-1]
        file.close()

    def test_app_accumulate(self):
        """
        Type and rank matter
        """
        expected = {"type", "dt", "rank"}
        self._setup()
        for drop in enumerate(self.pgs_node_data):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            self.assertEqual(expected, hash_data.keys())

    def test_data_accumulate(self):
        """
        Type and rank matter
        """
        expected = {"type", "storage", "rank"}
        self._setup()
        for drop in enumerate(self.pgs_file_data):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            self.assertEqual(expected, hash_data.keys())

    @unittest.skip("pg_generator does not like sample graphs")
    def test_group_accumulate(self):
        """
        Type and rank matter
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Type and rank matter
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_other_accumulate(self):
        """
        Does not matter
        """
        self.assertEqual(True, False)


class AccumulatePGTPartitionReplicateCompData(unittest.TestCase):
    """
    Tests the replicate-comp standard at the physical graph template level.
    """

    rmode = ReproducibilityFlags.REPLICATE_COMP
    temp_out = tempfile.TemporaryDirectory()
    pgt_node_data = None
    pgt_file_data = None

    def _setup(self):
        _run_full_workflow(
            self.rmode, "apps", "test/reproducibility/reproGraphs/", self.temp_out.name
        )
        _run_full_workflow(
            self.rmode, "files", "test/reproducibility/reproGraphs/", self.temp_out.name
        )

        file = open(self.temp_out.name + "/" + "apps" + "PGT.graph")
        self.pgt_node_data = json.load(file)[0:-1]
        file.close()
        file = open(self.temp_out.name + "/" + "files" + "PGT.graph")
        self.pgt_file_data = json.load(file)[0:-1]
        file.close()

    def test_app_accumulate(self):
        """
        Type, rank and machine information matters.
        """
        expected = {"type", "dt", "rank", "node", "island"}
        self._setup()
        for drop in enumerate(self.pgt_node_data):
            hash_data = accumulate_pgt_partition_drop_data(drop[1])
            self.assertEqual(expected, hash_data.keys())

    def test_data_accumulate(self):
        """
        Type, rank and machine information matters.
        """
        expected = {"type", "storage", "rank", "node", "island"}
        self._setup()
        for drop in enumerate(self.pgt_file_data):
            hash_data = accumulate_pgt_partition_drop_data(drop[1])
            self.assertEqual(expected, hash_data.keys())

    @unittest.skip("pg_generator does not like sample graphs")
    def test_group_accumulate(self):
        """
        Type, rank and machine information matters.
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Type, rank and machine information matters.
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_other_accumulate(self):
        """
        Does not matter
        """
        self.assertEqual(True, False)


class AccumulatePGReplicateCompData(unittest.TestCase):
    """
    Tests the replicate-comp standard at the physical graph level
    """

    rmode = ReproducibilityFlags.REPLICATE_COMP
    temp_out = tempfile.TemporaryDirectory()
    pg_node_data = None
    pg_file_data = None

    def _setup(self):
        _run_full_workflow(
            self.rmode, "apps", "test/reproducibility/reproGraphs/", self.temp_out.name
        )
        _run_full_workflow(
            self.rmode, "files", "test/reproducibility/reproGraphs/", self.temp_out.name
        )

        file = open(self.temp_out.name + "/" + "apps" + "PG.graph")
        self.pg_node_data = json.load(file)[0:-1]
        file.close()
        file = open(self.temp_out.name + "/" + "files" + "PG.graph")
        self.pg_file_data = json.load(file)[0:-1]
        file.close()

    def test_app_accumulate(self):
        """
        Machine information matters.
        """
        expected = {"node", "island"}
        self._setup()
        for drop in enumerate(self.pg_node_data):
            hash_data = accumulate_pg_drop_data(drop[1])
            self.assertEqual(expected, hash_data.keys())

    def test_data_accumulate(self):
        """
        Machine information matters.
        """
        expected = {"node", "island"}
        self._setup()
        for drop in enumerate(self.pg_node_data):
            hash_data = accumulate_pg_drop_data(drop[1])
            self.assertEqual(expected, hash_data.keys())

    @unittest.skip("pg_generator does not like sample graphs")
    def test_group_accumulate(self):
        """
        Machine information matters.
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Machine information matters.
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_other_accumulate(self):
        """
        Does not matter.
        """
        self.assertEqual(True, False)


class AccumulateLGTReplicateTotalData(unittest.TestCase):
    """
    Tests the replicate-total standard at the logical graph template level.
    """

    rmode = ReproducibilityFlags.REPLICATE_TOTAL
    expected = {
        "categoryType",
        "category",
        "inputPorts",
        "inputLocalPorts",
        "outputPorts",
        "outputLocalPorts",
        "streaming",
    }

    file = open("test/reproducibility/reproGraphs/apps.graph")
    lgt_node_data = json.load(file)["nodeDataArray"]
    file.close()
    file = open("test/reproducibility/reproGraphs/files.graph")
    lgt_files_data = json.load(file)["nodeDataArray"]
    file.close()
    file = open("test/reproducibility/reproGraphs/groups.graph")
    lgt_groups_data = json.load(file)["nodeDataArray"]
    file.close()
    file = open("test/reproducibility/reproGraphs/misc.graph")
    lgt_misc_data = json.load(file)["nodeDataArray"]
    file.close()

    def test_app_accumulate(self):
        """
        Tests that lgt replicate-total data is collected for application types
        """
        for drop in enumerate(self.lgt_node_data):
            hash_data = accumulate_lgt_drop_data(drop[1], self.rmode)
            self.assertEqual(self.expected, hash_data.keys())

    def test_data_accumulate(self):
        """
        Tests that lgt replicate-total data is collected for file types
        """
        for drop in enumerate(self.lgt_files_data):
            hash_data = accumulate_lgt_drop_data(drop[1], self.rmode)
            self.assertEqual(self.expected, hash_data.keys())

    def test_group_accumulate(self):
        """
        Tests that lgt replicate-total data is collected for group types
        """
        for drop in enumerate(self.lgt_groups_data):
            hash_data = accumulate_lgt_drop_data(drop[1], self.rmode)
            self.assertEqual(self.expected, hash_data.keys())

    def test_other_accumulate(self):
        """
        Tests that lgt replicate-total data is collected for other types
        """
        for drop in enumerate(self.lgt_misc_data):
            hash_data = accumulate_lgt_drop_data(drop[1], self.rmode)
            self.assertEqual(self.expected, hash_data.keys())


class AccumulateLGReplicateTotalData(unittest.TestCase):
    """
    Tests the replicate-total standard at the logical graph level.
    """

    rmode = ReproducibilityFlags.REPLICATE_TOTAL
    expected = []
    temp_out = tempfile.TemporaryDirectory()
    lg_node_data = None
    lg_files_data = None
    lg_group_data = None
    lg_misc_data = None

    def _setup(self):
        _fill_workflow(
            self.rmode, "apps", "test/reproducibility/reproGraphs/", self.temp_out.name
        )
        _fill_workflow(
            self.rmode, "files", "test/reproducibility/reproGraphs/", self.temp_out.name
        )
        _fill_workflow(
            self.rmode,
            "groups",
            "test/reproducibility/reproGraphs/",
            self.temp_out.name,
        )
        _fill_workflow(
            self.rmode, "misc", "test/reproducibility/reproGraphs/", self.temp_out.name
        )

        file = open(self.temp_out.name + "/" + "apps" + "LG.graph")
        self.lg_node_data = json.load(file)["nodeDataArray"]
        file.close()
        file = open(self.temp_out.name + "/" + "files" + "LG.graph")
        self.lg_files_data = json.load(file)["nodeDataArray"]
        file.close()
        file = open(self.temp_out.name + "/" + "groups" + "LG.graph")
        self.lg_group_data = json.load(file)["nodeDataArray"]
        file.close()
        file = open(self.temp_out.name + "/" + "misc" + "LG.graph")
        self.lg_misc_data = json.load(file)["nodeDataArray"]
        file.close()

    def _bash(self, drop):
        expected = {"execution_time", "num_cpus", "Arg01"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _dynlib(self, drop):
        expected = {"execution_time", "num_cpus", "libpath"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _mpi(self, drop):
        expected = {"execution_time", "num_cpus", "num_of_procs"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _docker(self, drop):
        expected = {
            "execution_time",
            "num_cpus",
            "image",
            "command",
            "user",
            "ensureUserAndSwitch",
            "removeContainer",
            "additionalBindings",
        }
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _component(self, drop):
        expected = {"execution_time", "num_cpus", "appclass"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _memory(self, drop):
        expected = {"data_volume"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _file(self, drop):
        expected = {"data_volume", "check_filepath_exists"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _ngas(self, drop):
        expected = {"data_volume"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _groupby(self, drop):
        expected = {
            "inputApplicationName",
            "inputApplicationType",
            "group_key",
            "group_axis",
        }
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data)

    def _scatter(self, drop):
        expected = {
            "inputApplicationName",
            "inputApplicationType",
            "num_of_copies",
            "scatter_axis",
        }
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data)

    def _loop(self, drop):
        expected = {"inputApplicationName", "inputApplicationType", "num_of_iter"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data)

    def test_all_accumulate(self):
        """
        We make the data expected for each accounted for drop type explicit.
        """
        self._setup()
        self._bash(self.lg_node_data[0])
        self._dynlib(self.lg_node_data[1])
        self._mpi(self.lg_node_data[2])
        self._docker(self.lg_node_data[3])
        self._component(self.lg_node_data[4])
        self._memory(self.lg_files_data[0])
        self._file(self.lg_files_data[1])
        self._ngas(self.lg_files_data[2])


class AccumulatePGTUnrollReplicateTotalData(unittest.TestCase):
    """
    Tests the replicate-total standard at the physical graph template level.
    Can currently only test apps.graph and files.graph, the translator cannot deal with groups
    or comments easily.
    """

    rmode = ReproducibilityFlags.REPLICATE_TOTAL
    temp_out = tempfile.TemporaryDirectory()
    pgs_node_data = None
    pgs_file_data = None

    def _setup(self):
        _run_full_workflow(
            self.rmode, "apps", "test/reproducibility/reproGraphs/", self.temp_out.name
        )
        _run_full_workflow(
            self.rmode, "files", "test/reproducibility/reproGraphs/", self.temp_out.name
        )

        file = open(self.temp_out.name + "/" + "apps" + "PGS.graph")
        self.pgs_node_data = json.load(file)[0:-1]
        file.close()
        file = open(self.temp_out.name + "/" + "files" + "PGS.graph")
        self.pgs_file_data = json.load(file)[0:-1]
        file.close()

    def test_app_accumulate(self):
        """
        Type matters
        """
        expected = {"type", "dt"}
        self._setup()
        for drop in enumerate(self.pgs_node_data):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            self.assertEqual(expected, hash_data.keys())

    def test_data_accumulate(self):
        """
        Type matters
        """
        expected = {"type", "storage"}
        self._setup()
        for drop in enumerate(self.pgs_file_data):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            self.assertEqual(expected, hash_data.keys())

    @unittest.skip("pg_generator does not like sample graphs")
    def test_group_accumulate(self):
        """
        Type matters
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Type matters
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_other_accumulate(self):
        """
        Does not matter
        """
        self.assertEqual(True, False)


class AccumulatePGTPartitionReplicateTotalData(unittest.TestCase):
    """
    Tests the replicate-total standard at the physical graph template level.
    """

    rmode = ReproducibilityFlags.REPLICATE_TOTAL
    temp_out = tempfile.TemporaryDirectory()
    pgt_node_data = None
    pgt_file_data = None

    def _setup(self):
        _run_full_workflow(
            self.rmode, "apps", "test/reproducibility/reproGraphs/", self.temp_out.name
        )
        _run_full_workflow(
            self.rmode, "files", "test/reproducibility/reproGraphs/", self.temp_out.name
        )

        file = open(self.temp_out.name + "/" + "apps" + "PGT.graph")
        self.pgt_node_data = json.load(file)[0:-1]
        file.close()
        file = open(self.temp_out.name + "/" + "files" + "PGT.graph")
        self.pgt_file_data = json.load(file)[0:-1]
        file.close()

    def test_app_accumulate(self):
        """
        Only type matters
        """
        expected = {"type", "dt"}
        self._setup()
        for drop in enumerate(self.pgt_node_data):
            hash_data = accumulate_pgt_partition_drop_data(drop[1])
            self.assertEqual(expected, hash_data.keys())

    def test_data_accumulate(self):
        """
        Only type matters
        """
        expected = {"type", "storage"}
        self._setup()
        for drop in enumerate(self.pgt_file_data):
            hash_data = accumulate_pgt_partition_drop_data(drop[1])
            self.assertEqual(expected, hash_data.keys())

    @unittest.skip("pg_generator does not like sample graphs")
    def test_group_accumulate(self):
        """
        Only type matters
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Only type matters
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_other_accumulate(self):
        """
        Does not matter
        """
        self.assertEqual(True, False)


class AccumulatePGReplicateTotalData(unittest.TestCase):
    """
    Tests the replicate-total standard at the physical graph level
    """

    rmode = ReproducibilityFlags.REPLICATE_TOTAL
    temp_out = tempfile.TemporaryDirectory()
    pg_node_data = None
    pg_file_data = None

    def _setup(self):
        _run_full_workflow(
            self.rmode, "apps", "test/reproducibility/reproGraphs/", self.temp_out.name
        )
        _run_full_workflow(
            self.rmode, "files", "test/reproducibility/reproGraphs/", self.temp_out.name
        )

        file = open(self.temp_out.name + "/" + "apps" + "PGT.graph")
        self.pg_node_data = json.load(file)[0:-1]
        file.close()
        file = open(self.temp_out.name + "/" + "files" + "PGT.graph")
        self.pg_file_data = json.load(file)[0:-1]
        file.close()

    def test_app_accumulate(self):
        """
        Nothing matters
        """
        expected = {}
        self._setup()
        for drop in enumerate(self.pg_node_data):
            hash_data = accumulate_pg_drop_data(drop[1])
            self.assertEqual(expected, dict(hash_data.keys()))

    def test_data_accumulate(self):
        """
        Nothing matters
        """
        expected = {}
        self._setup()
        for drop in enumerate(self.pg_file_data):
            hash_data = accumulate_pg_drop_data(drop[1])
            self.assertEqual(expected, dict(hash_data.keys()))

    @unittest.skip("pg_generator does not like sample graphs")
    def test_group_accumulate(self):
        """
        Nothing matters
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Nothing matters
        """
        self.assertEqual(True, False)

    @unittest.skip("pg_generator does not like sample graphs")
    def test_other_accumulate(self):
        """
        Does not matter
        """
        self.assertEqual(True, False)
