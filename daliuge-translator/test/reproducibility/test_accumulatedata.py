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

import os
import json
import optparse
import tempfile
import unittest
import pytest

# from asyncio.log import logger
import logging
from importlib.resources import files
import daliuge_tests.reproGraphs as test_graphs

from dlg.common.reproducibility.constants import ReproducibilityFlags
from dlg.common.reproducibility.reproducibility import (
    accumulate_lgt_drop_data,
    accumulate_lg_drop_data,
    accumulate_pgt_unroll_drop_data,
    accumulate_pgt_partition_drop_data,
    accumulate_pg_drop_data,
)
from dlg.translator.tool_commands import (
    dlg_fill,
    dlg_unroll,
    dlg_partition,
    dlg_map,
)

SUPPORTED_WORKFLOWS = ["apps", "files", "misc", "groups"]

logger = logging.getLogger("__name__")


@pytest.fixture(scope="session")
def log_level(pytestconfig):
    return pytestconfig.getoption("--log-cli-level")


def _fill_workflow(
    rmode: ReproducibilityFlags,
    workflow: str,
    scratch_loc="./",
    log_level=log_level,
):
    # workflow = str(files(test_graphs) /  workflow)
    # # logger.debug(workflow_loc)
    lgt = str(files(test_graphs) / workflow) + ".graph"
    lgr = scratch_loc + "/" + workflow + "LG.graph"

    rmodes = str(rmode.value)

    parser = optparse.OptionParser()
    ll = "-v" if log_level else "-vv"
    dlg_fill(parser, ["-L", lgt, "-R", rmodes, "-o", lgr, "-f", "newline", ll])


def _run_full_workflow(
    rmode: ReproducibilityFlags,
    workflow: str,
    scratch_loc="./",
    log_level=log_level,
):
    lgr = scratch_loc + "/" + workflow + "LG.graph"
    pgs = scratch_loc + "/" + workflow + "PGS.graph"
    pgt = scratch_loc + "/" + workflow + "PGT.graph"
    pgr = scratch_loc + "/" + workflow + "PG.graph"

    ll = "-v" if log_level.__repr__ else "-vv"
    _fill_workflow(rmode, workflow, scratch_loc)
    parser = optparse.OptionParser()
    dlg_unroll(parser, ["-L", lgr, "-o", pgs, "-f", "newline", ll])
    parser = optparse.OptionParser()
    dlg_partition(parser, ["-P", pgs, "-o", pgt, "-f", "newline", ll])
    parser = optparse.OptionParser()
    dlg_map(
        parser,
        [
            "-P",
            pgt,
            "-N",
            "localhost, localhost",
            "-o",
            pgr,
            "-f",
            "newline",
            ll,
        ],
    )


def _run_workflows(
    rmode: ReproducibilityFlags,
    names: list,
    temp_out: tempfile.TemporaryDirectory,
):
    for wflow_name in names:
        _run_full_workflow(rmode, wflow_name, temp_out.name)


def _extract_reprodata(temp_out: tempfile.TemporaryDirectory, names: list, suffix: str):
    output = {}
    for wflow_name in names:
        file = open(f"{temp_out.name}{os.sep}{wflow_name}{suffix}")
        output[wflow_name] = json.load(file)[0:-1]
        file.close()
    return output


class AccumulateLGTRerunData(unittest.TestCase):
    """
    Tests the rerun standard at the logical graph template level.
    """

    rmode = ReproducibilityFlags.RERUN
    expected = {
        "category",
        "categoryType",
    }
    ddGraph = "graphs/ddTest.graph"

    with (files(test_graphs) / "apps.graph").open()  as f:  # @UndefinedVariable
        # # logger.debug(f"Loading graph: {f}")
        lgt_node_data = json.load(f)["nodeDataArray"]
    with (files(test_graphs) / "files.graph").open()  as f:  # @UndefinedVariable
        # # logger.debug(f"Loading graph: {f}")
        lgt_files_data = json.load(f)["nodeDataArray"]
    with (files(test_graphs) / "groups.graph").open()  as f:  # @UndefinedVariable
        # # logger.debug(f"Loading graph: {f}")
        lgt_groups_data = json.load(f)["nodeDataArray"]
    with (files(test_graphs) / "misc.graph").open()  as f:  # @UndefinedVariable
        # # logger.debug(f"Loading graph: {f}")
        lgt_misc_data = json.load(f)["nodeDataArray"]
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
            print(hash_data)
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
            self.rmode,
            "apps",
            self.temp_out.name,
        )
        _fill_workflow(
            self.rmode,
            "files",
            self.temp_out.name,
        )
        _fill_workflow(
            self.rmode,
            "groups",
            self.temp_out.name,
        )
        _fill_workflow(
            self.rmode,
            "misc",
            self.temp_out.name,
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
    setup = False
    graph_data = None

    def _setup(self):
        if not self.setup:
            _run_workflows(
                self.rmode,
                SUPPORTED_WORKFLOWS,
                self.temp_out,
            )
            self.graph_data = _extract_reprodata(
                self.temp_out, SUPPORTED_WORKFLOWS, "PGT.graph"
            )
            self.setup = True

    def test_app_accumulate(self):
        """
        The the application type matters for rerunning
        """
        expected = {"categoryType"}
        self._setup()
        for drop in enumerate(self.graph_data["apps"]):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            self.assertEqual(expected, hash_data[self.rmode.name].keys())

    def test_data_accumulate(self):
        """
        Only the storage type matters for rerunning
        """
        expected = {"categoryType", "storage"}
        self._setup()
        for drop in enumerate(self.graph_data["files"]):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            self.assertEqual(expected, hash_data[self.rmode.name].keys())

    def test_group_accumulate(self):
        """
        Only the drop type and input app type matters for rerunning
        """
        expected_app = {"categoryType", "dt"}
        expected_file = {"categoryType", "storage"}
        self._setup()
        for drop in enumerate(self.graph_data["groups"]):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            if "dt" in hash_data.keys():
                self.assertEqual(expected_app, hash_data.keys())
            if "storage" in hash_data.keys():
                self.assertEqual(expected_file, hash_data.keys())

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Only the drop type matters for rerunning
        """
        self.assertEqual(True, False)

    def test_other_accumulate(self):
        """
        Will only return the length of the dummy memory drop
        """
        self._setup()
        self.assertEqual(2, len(self.graph_data["misc"]))


class AccumulatePGTPartitionRerunData(unittest.TestCase):
    """
    Tests the rerun standard at the physical graph template level.
    """

    rmode = ReproducibilityFlags.RERUN
    temp_out = tempfile.TemporaryDirectory()
    setup = False
    graph_data = None

    def _setup(self):
        if not self.setup:
            _run_workflows(
                self.rmode,
                SUPPORTED_WORKFLOWS,
                self.temp_out,
            )
            self.graph_data = _extract_reprodata(
                self.temp_out, SUPPORTED_WORKFLOWS, "PGT.graph"
            )
            self.setup = True

    def test_app_accumulate(self):
        """
        Only application type matters for rerunning.
        """
        expected = {"categoryType"}
        self._setup()
        for drop in enumerate(self.graph_data["apps"]):
            hash_data = accumulate_pgt_partition_drop_data(drop[1])
            self.assertEqual(expected, hash_data[self.rmode.name].keys())

    def test_data_accumulate(self):
        """
        Only storage type matters for rerunning.
        """
        expected = {"categoryType", "storage"}
        self._setup()
        for drop in enumerate(self.graph_data["files"]):
            hash_data = accumulate_pgt_partition_drop_data(drop[1])
            self.assertEqual(expected, hash_data[self.rmode.name].keys())

    def test_group_accumulate(self):
        """
        Only drop type matters for rerunning.
        """
        expected_app = {"categoryType", "dt"}
        expected_file = {"categoryType", "storage"}
        self._setup()
        for drop in enumerate(self.graph_data["groups"]):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            if "dt" in hash_data.keys():
                self.assertEqual(expected_app, hash_data.keys())
            if "storage" in hash_data.keys():
                self.assertEqual(expected_file, hash_data.keys())

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Only drop type matters for rerunning.
        """
        self.assertEqual(True, False)

    def test_other_accumulate(self):
        """
        Should not matter for rerunning.
        Will only return the length of the dummy memory drop
        """
        self._setup()
        self.assertEqual(2, len(self.graph_data["misc"]))


class AccumulatePGRerunData(unittest.TestCase):
    """
    Tests the rerun standard at the physical graph level
    """

    rmode = ReproducibilityFlags.RERUN
    temp_out = tempfile.TemporaryDirectory()
    setup = False
    graph_data = None

    def _setup(self):
        if not self.setup:
            _run_workflows(
                self.rmode,
                SUPPORTED_WORKFLOWS,
                self.temp_out,
            )
            self.graph_data = _extract_reprodata(
                self.temp_out, SUPPORTED_WORKFLOWS, "PG.graph"
            )
            self.setup = True

    def test_app_accumulate(self):
        """
        Nothing matters for rerunning.
        """
        expected = {}
        self._setup()
        for drop in enumerate(self.graph_data["apps"]):
            hash_data = accumulate_pg_drop_data(drop[1])
            self.assertEqual(expected, dict(hash_data[self.rmode.name].keys()))

    def test_data_accumulate(self):
        """
        Nothing matters for rerunning.
        """
        expected = {}
        self._setup()
        for drop in enumerate(self.graph_data["files"]):
            hash_data = accumulate_pg_drop_data(drop[1])
            self.assertEqual(expected, dict(hash_data[self.rmode.name].keys()))

    def test_group_accumulate(self):
        """
        Nothing matters for rerunning.
        """
        expected_app = {"categoryType", "dt"}
        expected_file = {"categoryType", "storage"}
        self._setup()
        for drop in enumerate(self.graph_data["groups"]):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            if "dt" in hash_data.keys():
                self.assertEqual(expected_app, hash_data.keys())
            if "storage" in hash_data.keys():
                self.assertEqual(expected_file, hash_data.keys())

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Nothing matters for rerunning.
        """
        self.assertEqual(True, False)

    def test_other_accumulate(self):
        """
        Should not matter for rerunning.
        Will only return the length of the dummy memory drop
        """
        self._setup()
        self.assertEqual(2, len(self.graph_data["misc"]))


class AccumulateLGTRepeatData(unittest.TestCase):
    """
    Tests the repeat standard at the logical graph template level.
    """

    rmode = ReproducibilityFlags.REPEAT
    expected = {
        "category",
        "categoryType",
    }

    with (files(test_graphs) / "apps.graph").open()  as f:  # @UndefinedVariable
        # # logger.debug(f"Loading graph: {f}")
        lgt_node_data = json.load(f)["nodeDataArray"]
    with (files(test_graphs) / "files.graph").open()  as f:  # @UndefinedVariable
        # # logger.debug(f"Loading graph: {f}")
        lgt_files_data = json.load(f)["nodeDataArray"]
    with (files(test_graphs) / "groups.graph").open()  as f:  # @UndefinedVariable
        # # logger.debug(f"Loading graph: {f}")
        lgt_groups_data = json.load(f)["nodeDataArray"]
    with (files(test_graphs) / "misc.graph").open()  as f:  # @UndefinedVariable
        # # logger.debug(f"Loading graph: {f}")
        lgt_misc_data = json.load(f)["nodeDataArray"]

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
            self.rmode,
            "apps",
            self.temp_out.name,
        )
        _fill_workflow(
            self.rmode,
            "files",
            self.temp_out.name,
        )
        _fill_workflow(
            self.rmode,
            "groups",
            self.temp_out.name,
        )
        _fill_workflow(
            self.rmode,
            "misc",
            self.temp_out.name,
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
        expected = {"execution_time", "num_cpus", "dropclass"}
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
        expected = {
            "inputApplicationName",
            "inputApplicationType",
            "num_of_iter",
        }
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
    setup = False
    graph_data = None

    def _setup(self):
        if not self.setup:
            _run_workflows(
                self.rmode,
                SUPPORTED_WORKFLOWS,
                self.temp_out,
            )
            self.graph_data = _extract_reprodata(
                self.temp_out, SUPPORTED_WORKFLOWS, "PGS.graph"
            )
            self.setup = True

    def test_app_accumulate(self):
        """
        Only type matters
        """
        expected = {"categoryType"}
        self._setup()
        for drop in enumerate(self.graph_data["apps"]):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            self.assertEqual(expected, hash_data[self.rmode.name].keys())

    def test_data_accumulate(self):
        """
        Only type matters
        """
        expected = {"categoryType", "storage"}
        self._setup()
        for drop in enumerate(self.graph_data["files"]):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            self.assertEqual(expected, hash_data[self.rmode.name].keys())

    def test_group_accumulate(self):
        """
        Only type matters
        """
        expected_app = {"categoryType", "dt"}
        expected_file = {"categoryType", "storage"}
        self._setup()
        for drop in enumerate(self.graph_data["groups"]):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            if "dt" in hash_data.keys():
                self.assertEqual(expected_app, hash_data.keys())
            if "storage" in hash_data.keys():
                self.assertEqual(expected_file, hash_data.keys())

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Only type matters
        """
        self.assertEqual(True, False)

    def test_other_accumulate(self):
        """
        Should not matter for rerunning.
        Will only return the length of the dummy memory drop
        """
        self._setup()
        self.assertEqual(2, len(self.graph_data["misc"]))


class AccumulatePGTPartitionRepeatData(unittest.TestCase):
    """
    Tests the repeat standard at the physical graph template level.
    """

    rmode = ReproducibilityFlags.REPEAT
    temp_out = tempfile.TemporaryDirectory()
    setup = False
    graph_data = None

    def _setup(self):
        if not self.setup:
            _run_workflows(
                self.rmode,
                SUPPORTED_WORKFLOWS,
                self.temp_out,
            )
            self.graph_data = _extract_reprodata(
                self.temp_out, SUPPORTED_WORKFLOWS, "PGT.graph"
            )
            self.setup = True

    def test_app_accumulate(self):
        """
        Only type matters when repeating.
        """
        expected = {"categoryType"}
        self._setup()
        for drop in enumerate(self.graph_data["apps"]):
            hash_data = accumulate_pgt_partition_drop_data(drop[1])
            self.assertEqual(expected, hash_data[self.rmode.name].keys())

    def test_data_accumulate(self):
        """
        Only type matters when repeating.
        """
        expected = {"categoryType", "storage"}
        self._setup()
        for drop in enumerate(self.graph_data["files"]):
            hash_data = accumulate_pgt_partition_drop_data(drop[1])
            self.assertEqual(expected, hash_data[self.rmode.name].keys())

    def test_group_accumulate(self):
        """
        Only type matters when repeating.
        """
        expected_app = {"categoryType", "dt"}
        expected_file = {"categoryType", "storage"}
        self._setup()
        for drop in enumerate(self.graph_data["groups"]):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            if "dt" in hash_data.keys():
                self.assertEqual(expected_app, hash_data.keys())
            if "storage" in hash_data.keys():
                self.assertEqual(expected_file, hash_data.keys())

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Only type matters when repeating.
        """
        self.assertEqual(True, False)

    def test_other_accumulate(self):
        """
        Should not matter for rerunning.
        Will only return the length of the dummy memory drop
        """
        self._setup()
        self.assertEqual(2, len(self.graph_data["misc"]))


class AccumulatePGRepeatData(unittest.TestCase):
    """
    Tests the repeat standard  at the physical graph level
    """

    rmode = ReproducibilityFlags.REPEAT
    temp_out = tempfile.TemporaryDirectory()
    setup = False
    graph_data = None

    def _setup(self):
        if not self.setup:
            _run_workflows(
                self.rmode,
                SUPPORTED_WORKFLOWS,
                self.temp_out,
            )
            self.graph_data = _extract_reprodata(
                self.temp_out, SUPPORTED_WORKFLOWS, "PGT.graph"
            )
            self.setup = True

    def test_app_accumulate(self):
        """
        Nothing matters for repeating.
        """
        expected = {}
        self._setup()
        for drop in enumerate(self.graph_data["apps"]):
            hash_data = accumulate_pg_drop_data(drop[1])
            self.assertEqual(expected, dict(hash_data[self.rmode.name].keys()))

    def test_data_accumulate(self):
        """
        Nothing matters for repeating.
        """
        expected = {}
        self._setup()
        for drop in enumerate(self.graph_data["files"]):
            hash_data = accumulate_pg_drop_data(drop[1])
            self.assertEqual(expected, dict(hash_data[self.rmode.name].keys()))

    def test_group_accumulate(self):
        """
        Nothing matters for repeating.
        """
        expected_app = {"categoryType", "dt"}
        expected_file = {"categoryType", "storage"}
        self._setup()
        for drop in enumerate(self.graph_data["groups"]):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            if "dt" in hash_data.keys():
                self.assertEqual(expected_app, hash_data.keys())
            if "storage" in hash_data.keys():
                self.assertEqual(expected_file, hash_data.keys())

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Nothing matters for repeating.
        """
        self.assertEqual(True, False)

    def test_other_accumulate(self):
        """
        Should not matter for rerunning.
        Will only return the length of the dummy memory drop
        """
        self._setup()
        self.assertEqual(2, len(self.graph_data["misc"]))


class AccumulateLGTRecomputeData(unittest.TestCase):
    """
    Tests the recompute standard at the logical graph template level.
    """

    rmode = ReproducibilityFlags.RECOMPUTE
    expected = {
        "category",
        "categoryType",
    }

    with (files(test_graphs) / "apps.graph").open()  as f:  # @UndefinedVariable
        # # logger.debug(f"Loading graph: {f}")
        lgt_node_data = json.load(f)["nodeDataArray"]
    with (files(test_graphs) / "files.graph").open()  as f:  # @UndefinedVariable
        # # logger.debug(f"Loading graph: {f}")
        lgt_files_data = json.load(f)["nodeDataArray"]
    with (files(test_graphs) / "groups.graph").open()  as f:  # @UndefinedVariable
        # # logger.debug(f"Loading graph: {f}")
        lgt_groups_data = json.load(f)["nodeDataArray"]
    with (files(test_graphs) / "misc.graph").open()  as f:  # @UndefinedVariable
        # # logger.debug(f"Loading graph: {f}")
        lgt_misc_data = json.load(f)["nodeDataArray"]

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
            self.rmode,
            "apps",
            self.temp_out.name,
        )
        _fill_workflow(
            self.rmode,
            "files",
            self.temp_out.name,
        )
        _fill_workflow(
            self.rmode,
            "groups",
            self.temp_out.name,
        )
        _fill_workflow(
            self.rmode,
            "misc",
            self.temp_out.name,
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
        expected = {"execution_time", "num_cpus", "dropclass"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _memory(self, drop):
        expected = {"data_volume"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _file(self, drop):
        expected = {
            "data_volume",
            "check_filepath_exists",
            "filepath",
            "dirname",
        }
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
        expected = {
            "inputApplicationName",
            "inputApplicationType",
            "num_of_iter",
        }
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
    setup = False
    graph_data = None

    def _setup(self):
        if not self.setup:
            _run_workflows(
                self.rmode,
                SUPPORTED_WORKFLOWS,
                self.temp_out,
            )
            self.graph_data = _extract_reprodata(
                self.temp_out, SUPPORTED_WORKFLOWS, "PGS.graph"
            )
            self.setup = True

    def test_app_accumulate(self):
        """
        Type and rank matters.
        """
        expected = {"categoryType", "rank"}
        self._setup()
        for drop in enumerate(self.graph_data["apps"]):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            self.assertEqual(expected, hash_data[self.rmode.name].keys())

    def test_data_accumulate(self):
        """
        Type and rank matters.
        """
        expected = {"categoryType", "storage", "rank"}
        self._setup()
        for drop in enumerate(self.graph_data["files"]):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            self.assertEqual(expected, hash_data[self.rmode.name].keys())

    def test_group_accumulate(self):
        """
        Type and rank matters.
        """
        expected_app = {"categoryType", "dt", "rank"}
        expected_file = {"categoryType", "storage", "rank"}
        self._setup()
        for drop in enumerate(self.graph_data["groups"]):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            if "dt" in hash_data.keys():
                self.assertEqual(expected_app, hash_data.keys())
            if "storage" in hash_data.keys():
                self.assertEqual(expected_file, hash_data.keys())

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Type and rank matters.
        """
        self.assertEqual(True, False)

    def test_other_accumulate(self):
        """
        Should not matter for rerunning.
        Will only return the length of the dummy memory drop
        """
        self._setup()
        self.assertEqual(2, len(self.graph_data["misc"]))


class AccumulatePGTPartitionRecomputeData(unittest.TestCase):
    """
    Tests the recompute standard at the physical graph template level.
    """

    rmode = ReproducibilityFlags.RECOMPUTE
    temp_out = tempfile.TemporaryDirectory()
    setup = False
    graph_data = None

    def _setup(self):
        if not self.setup:
            _run_workflows(
                self.rmode,
                SUPPORTED_WORKFLOWS,
                self.temp_out,
            )
            self.graph_data = _extract_reprodata(
                self.temp_out, SUPPORTED_WORKFLOWS, "PGT.graph"
            )
            self.setup = True

    def test_app_accumulate(self):
        """
        Type, rank and machine information matters
        """
        expected = ["categoryType", "rank", "node", "island"]
        self._setup()
        for drop in enumerate(self.graph_data["apps"]):
            hash_data = accumulate_pgt_partition_drop_data(drop[1])
            self.assertEqual(
                sorted(expected),
                sorted(list(hash_data[self.rmode.name].keys())),
            )

    def test_data_accumulate(self):
        """
        Type, rank and machine information matters
        """
        expected = ["categoryType", "storage", "rank", "node", "island"]
        self._setup()
        for drop in enumerate(self.graph_data["files"]):
            hash_data = accumulate_pgt_partition_drop_data(drop[1])
            self.assertEqual(
                sorted(expected),
                sorted(list(hash_data[self.rmode.name].keys())),
            )

    def test_group_accumulate(self):
        """
        Type, rank and machine information matters
        """
        expected_app = {"categoryType", "rank"}
        expected_file = {"categoryType", "storage", "rank"}
        self._setup()
        for drop in enumerate(self.graph_data["groups"]):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            if "dt" in hash_data.keys():
                self.assertEqual(expected_app, hash_data.keys())
            if "storage" in hash_data.keys():
                self.assertEqual(expected_file, hash_data.keys())

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Type, rank and machine information matters
        """
        self.assertEqual(True, False)

    def test_other_accumulate(self):
        """
        Should not matter for rerunning.
        Will only return the length of the dummy memory drop
        """
        self._setup()
        self.assertEqual(2, len(self.graph_data["misc"]))


class AccumulatePGRecomputeData(unittest.TestCase):
    """
    Tests the recompute standard at the physical graph level
    """

    rmode = ReproducibilityFlags.RECOMPUTE
    temp_out = tempfile.TemporaryDirectory()
    setup = False
    graph_data = None

    def _setup(self):
        if not self.setup:
            _run_workflows(
                self.rmode,
                SUPPORTED_WORKFLOWS,
                self.temp_out,
            )
            self.graph_data = _extract_reprodata(
                self.temp_out, SUPPORTED_WORKFLOWS, "PG.graph"
            )
            self.setup = True

    def test_app_accumulate(self):
        """
        Machine information matters when recomputing
        """
        expected = {"node", "island"}
        self._setup()
        for drop in enumerate(self.graph_data["apps"]):
            hash_data = accumulate_pg_drop_data(drop[1])
            self.assertEqual(expected, hash_data[self.rmode.name].keys())

    def test_data_accumulate(self):
        """
        Machine information matters when recomputing
        """
        expected = {"node", "island"}
        self._setup()
        for drop in enumerate(self.graph_data["files"]):
            hash_data = accumulate_pg_drop_data(drop[1])
            self.assertEqual(expected, hash_data[self.rmode.name].keys())

    def test_group_accumulate(self):
        """
        Machine information matters when recomputing
        """
        expected_app = {"categoryType", "rank"}
        expected_file = {"categoryType", "storage", "rank"}
        self._setup()
        for drop in enumerate(self.graph_data["groups"]):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            if "dt" in hash_data.keys():
                self.assertEqual(expected_app, hash_data.keys())
            if "storage" in hash_data.keys():
                self.assertEqual(expected_file, hash_data.keys())

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Machine information matters when recomputing
        """
        self.assertEqual(True, False)

    def test_other_accumulate(self):
        """
        Should not matter for rerunning.
        Will only return the length of the dummy memory drop
        """
        self._setup()
        self.assertEqual(2, len(self.graph_data["misc"]))


class AccumulateLGTReproduceData(unittest.TestCase):
    """
    Tests the reproduce standard at the logical graph template level.
    """

    rmode = ReproducibilityFlags.REPRODUCE
    expected = {"category", "categoryType"}

    with (files(test_graphs) / "apps.graph").open()  as f:  # @UndefinedVariable
        # # logger.debug(f"Loading graph: {f}")
        lgt_node_data = json.load(f)["nodeDataArray"]
    with (files(test_graphs) / "files.graph").open()  as f:  # @UndefinedVariable
        # # logger.debug(f"Loading graph: {f}")
        lgt_files_data = json.load(f)["nodeDataArray"]
    with (files(test_graphs) / "groups.graph").open()  as f:  # @UndefinedVariable
        # # logger.debug(f"Loading graph: {f}")
        lgt_groups_data = json.load(f)["nodeDataArray"]
    with (files(test_graphs) / "misc.graph").open()  as f:  # @UndefinedVariable
        # # logger.debug(f"Loading graph: {f}")
        lgt_misc_data = json.load(f)["nodeDataArray"]

    def test_app_accumulate(self):
        """
        Tests that lgt reproduce data is collected for application types
        """
        for drop in enumerate(self.lgt_node_data):
            hash_data = accumulate_lgt_drop_data(drop[1], self.rmode)
            print(hash_data)
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
            self.rmode,
            "apps",
            self.temp_out.name,
        )
        _fill_workflow(
            self.rmode,
            "files",
            self.temp_out.name,
        )
        _fill_workflow(
            self.rmode,
            "groups",
            self.temp_out.name,
        )
        _fill_workflow(
            self.rmode,
            "misc",
            self.temp_out.name,
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
    setup = False
    graph_data = None

    def _setup(self):
        if not self.setup:
            _run_workflows(
                self.rmode,
                SUPPORTED_WORKFLOWS,
                self.temp_out,
            )
            self.graph_data = _extract_reprodata(
                self.temp_out, SUPPORTED_WORKFLOWS, "PGS.graph"
            )
            self.setup = True

    def test_app_accumulate(self):
        """
        Only type matters
        """
        expected = {"categoryType"}
        self._setup()
        for drop in enumerate(self.graph_data["apps"]):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            self.assertEqual(expected, hash_data[self.rmode.name].keys())

    def test_data_accumulate(self):
        """
        Only type matters
        """
        expected = {"categoryType", "storage"}
        self._setup()
        for drop in enumerate(self.graph_data["files"]):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            self.assertEqual(expected, hash_data[self.rmode.name].keys())

    def test_group_accumulate(self):
        """
        Only type matters
        """
        expected_app = {"categoryType", "dt"}
        expected_file = {"categoryType", "storage"}
        self._setup()
        for drop in enumerate(self.graph_data["groups"]):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            if "dt" in hash_data.keys():
                self.assertEqual(expected_app, hash_data.keys())
            if "storage" in hash_data.keys():
                self.assertEqual(expected_file, hash_data.keys())

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Only type matters
        """
        self.assertEqual(True, False)

    def test_other_accumulate(self):
        """
        Should not matter for rerunning.
        Will only return the length of the dummy memory drop
        """
        self._setup()
        self.assertEqual(2, len(self.graph_data["misc"]))


class AccumulatePGTPartitionReproduceData(unittest.TestCase):
    """
    Tests the reproduce standard at the physical graph template level.
    """

    rmode = ReproducibilityFlags.REPRODUCE
    temp_out = tempfile.TemporaryDirectory()
    setup = False
    graph_data = None

    def _setup(self):
        if not self.setup:
            _run_workflows(
                self.rmode,
                SUPPORTED_WORKFLOWS,
                self.temp_out,
            )
            self.graph_data = _extract_reprodata(
                self.temp_out, SUPPORTED_WORKFLOWS, "PGT.graph"
            )
            self.setup = True

    def test_app_accumulate(self):
        """
        Only type matters
        """
        expected = ["categoryType"]
        self._setup()
        for drop in enumerate(self.graph_data["apps"]):
            hash_data = accumulate_pgt_partition_drop_data(drop[1])
            self.assertEqual(expected, list(hash_data[self.rmode.name].keys()))

    def test_data_accumulate(self):
        """
        Only type matters
        """
        expected = ["categoryType", "storage"]
        self._setup()
        for drop in enumerate(self.graph_data["files"]):
            hash_data = accumulate_pgt_partition_drop_data(drop[1])
            self.assertEqual(expected, list(hash_data[self.rmode.name].keys()))

    def test_group_accumulate(self):
        """
        Only type matters
        """
        expected_app = {"categoryType", "dt"}
        expected_file = {"categoryType", "storage"}
        self._setup()
        for drop in enumerate(self.graph_data["groups"]):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            if "dt" in hash_data.keys():
                self.assertEqual(expected_app, hash_data.keys())
            if "storage" in hash_data.keys():
                self.assertEqual(expected_file, hash_data.keys())

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Only type matters
        """
        self.assertEqual(True, False)

    def test_other_accumulate(self):
        """
        Should not matter for rerunning.
        Will only return the length of the dummy memory drop
        """
        self._setup()
        self.assertEqual(2, len(self.graph_data["misc"]))


class AccumulatePGReproduceData(unittest.TestCase):
    """
    Tests the reproduce standard at the physical graph level
    """

    rmode = ReproducibilityFlags.REPRODUCE
    temp_out = tempfile.TemporaryDirectory()
    setup = False
    graph_data = None

    def _setup(self):
        if not self.setup:
            _run_workflows(
                self.rmode,
                SUPPORTED_WORKFLOWS,
                self.temp_out,
            )
            self.graph_data = _extract_reprodata(
                self.temp_out, SUPPORTED_WORKFLOWS, "PG.graph"
            )
            self.setup = True

    def test_app_accumulate(self):
        """
        App information does not matter when reproducing
        """
        expected = {}
        self._setup()
        for drop in enumerate(self.graph_data["apps"]):
            hash_data = accumulate_pg_drop_data(drop[1])
            self.assertEqual(expected, dict(hash_data[self.rmode.name].keys()))

    def test_data_accumulate(self):
        """
        Only data information matters, not the type of drop
        """
        expected = {}
        self._setup()
        for drop in enumerate(self.graph_data["files"]):
            hash_data = accumulate_pg_drop_data(drop[1])
            self.assertEqual(expected, dict(hash_data[self.rmode.name].keys()))

    def test_group_accumulate(self):
        """
        Group information does not matter when reproducing
        """
        expected_app = {"categoryType", "dt"}
        expected_file = {"categoryType", "storage"}
        self._setup()
        for drop in enumerate(self.graph_data["groups"]):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            if "dt" in hash_data.keys():
                self.assertEqual(expected_app, hash_data.keys())
            if "storage" in hash_data.keys():
                self.assertEqual(expected_file, hash_data.keys())

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Control information does not matter when reproducing
        """
        self.assertEqual(True, False)

    def test_other_accumulate(self):
        """
        Should not matter for rerunning.
        Will only return the length of the dummy memory drop
        """
        self._setup()
        self.assertEqual(2, len(self.graph_data["misc"]))


class AccumulateLGTReplicateSciData(unittest.TestCase):
    """
    Tests the replicate-sci standard at the logical graph template level.
    """

    rmode = ReproducibilityFlags.REPLICATE_SCI
    expected = {
        "category",
        "categoryType",
    }

    with (files(test_graphs) / "apps.graph").open()  as f:  # @UndefinedVariable
        # # logger.debug(f"Loading graph: {f}")
        lgt_node_data = json.load(f)["nodeDataArray"]
    with (files(test_graphs) / "files.graph").open()  as f:  # @UndefinedVariable
        # # logger.debug(f"Loading graph: {f}")
        lgt_files_data = json.load(f)["nodeDataArray"]
    with (files(test_graphs) / "groups.graph").open()  as f:  # @UndefinedVariable
        # # logger.debug(f"Loading graph: {f}")
        lgt_groups_data = json.load(f)["nodeDataArray"]
    with (files(test_graphs) / "misc.graph").open()  as f:  # @UndefinedVariable
        # # logger.debug(f"Loading graph: {f}")
        lgt_misc_data = json.load(f)["nodeDataArray"]

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
            self.rmode,
            "apps",
            self.temp_out.name,
        )
        _fill_workflow(
            self.rmode,
            "files",
            self.temp_out.name,
        )
        _fill_workflow(
            self.rmode,
            "groups",
            self.temp_out.name,
        )
        _fill_workflow(
            self.rmode,
            "misc",
            self.temp_out.name,
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
    setup = False
    graph_data = None

    def _setup(self):
        if not self.setup:
            _run_workflows(
                self.rmode,
                SUPPORTED_WORKFLOWS,
                self.temp_out,
            )
            self.graph_data = _extract_reprodata(
                self.temp_out, SUPPORTED_WORKFLOWS, "PGS.graph"
            )
            self.setup = True

    def test_app_accumulate(self):
        """
        Only type matters
        """
        expected = {"categoryType"}
        self._setup()
        for drop in enumerate(self.graph_data["apps"]):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            self.assertEqual(expected, hash_data[self.rmode.name].keys())

    def test_data_accumulate(self):
        """
        Only type matters
        """
        expected = {"categoryType", "storage"}
        self._setup()
        for drop in enumerate(self.graph_data["files"]):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            self.assertEqual(expected, hash_data[self.rmode.name].keys())

    def test_group_accumulate(self):
        """
        Only type matters
        """
        expected_app = {"categoryType", "dt"}
        expected_file = {"categoryType", "storage"}
        self._setup()
        for drop in enumerate(self.graph_data["groups"]):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            if "dt" in hash_data.keys():
                self.assertEqual(expected_app, hash_data.keys())
            if "storage" in hash_data.keys():
                self.assertEqual(expected_file, hash_data.keys())

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Only type matters
        """
        self.assertEqual(True, False)

    def test_other_accumulate(self):
        """
        Should not matter for rerunning.
        Will only return the length of the dummy memory drop
        """
        self._setup()
        self.assertEqual(2, len(self.graph_data["misc"]))


class AccumulatePGTPartitionReplicateSciData(unittest.TestCase):
    """
    Tests the replicate-sci standard at the physical graph template level.
    """

    rmode = ReproducibilityFlags.REPLICATE_SCI
    temp_out = tempfile.TemporaryDirectory()
    setup = False
    graph_data = None

    def _setup(self):
        if not self.setup:
            _run_workflows(
                self.rmode,
                SUPPORTED_WORKFLOWS,
                self.temp_out,
            )
            self.graph_data = _extract_reprodata(
                self.temp_out, SUPPORTED_WORKFLOWS, "PGT.graph"
            )
            self.setup = True

    def test_app_accumulate(self):
        """
        Only type matters
        """
        expected = {"categoryType"}
        self._setup()
        for drop in enumerate(self.graph_data["apps"]):
            hash_data = accumulate_pgt_partition_drop_data(drop[1])
            self.assertEqual(expected, hash_data[self.rmode.name].keys())

    def test_data_accumulate(self):
        """
        Only type matters
        """
        expected = {"categoryType", "storage"}
        self._setup()
        for drop in enumerate(self.graph_data["files"]):
            hash_data = accumulate_pgt_partition_drop_data(drop[1])
            self.assertEqual(expected, hash_data[self.rmode.name].keys())

    def test_group_accumulate(self):
        """
        Only type matters
        """
        expected_app = {"categoryType", "dt"}
        expected_file = {"categoryType", "storage"}
        self._setup()
        for drop in enumerate(self.graph_data["groups"]):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            if "dt" in hash_data.keys():
                self.assertEqual(expected_app, hash_data.keys())
            if "storage" in hash_data.keys():
                self.assertEqual(expected_file, hash_data.keys())

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Only type matters
        """
        self.assertEqual(True, False)

    def test_other_accumulate(self):
        """
        Should not matter for rerunning.
        Will only return the length of the dummy memory drop
        """
        self._setup()
        self.assertEqual(2, len(self.graph_data["misc"]))


class AccumulatePGReplicateSciData(unittest.TestCase):
    """
    Tests the replicate-sci standard at the physical graph level
    """

    rmode = ReproducibilityFlags.REPLICATE_SCI
    temp_out = tempfile.TemporaryDirectory()
    setup = False
    graph_data = None

    def _setup(self):
        if not self.setup:
            _run_workflows(
                self.rmode,
                SUPPORTED_WORKFLOWS,
                self.temp_out,
            )
            self.graph_data = _extract_reprodata(
                self.temp_out, SUPPORTED_WORKFLOWS, "PG.graph"
            )
            self.setup = True

    def test_app_accumulate(self):
        """
        Nothing matters.
        """
        expected = {}
        self._setup()
        for drop in enumerate(self.graph_data["apps"]):
            hash_data = accumulate_pg_drop_data(drop[1])
            self.assertEqual(expected, dict(hash_data[self.rmode.name].keys()))

    def test_data_accumulate(self):
        """
        Nothing matters.
        """
        expected = {}
        self._setup()
        for drop in enumerate(self.graph_data["apps"]):
            hash_data = accumulate_pg_drop_data(drop[1])
            self.assertEqual(expected, dict(hash_data[self.rmode.name].keys()))

    def test_group_accumulate(self):
        """
        Nothing matters.
        """
        expected_app = {"categoryType", "dt"}
        expected_file = {"categoryType", "storage"}
        self._setup()
        for drop in enumerate(self.graph_data["groups"]):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            if "dt" in hash_data.keys():
                self.assertEqual(expected_app, hash_data.keys())
            if "storage" in hash_data.keys():
                self.assertEqual(expected_file, hash_data.keys())

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Nothing matters.
        """
        self.assertEqual(True, False)

    def test_other_accumulate(self):
        """
        Should not matter for rerunning.
        Will only return the length of the dummy memory drop
        """
        self._setup()
        self.assertEqual(2, len(self.graph_data["misc"]))


class AccumulateLGTReplicateCompData(unittest.TestCase):
    """
    Tests the replicate-comp standard at the logical graph template level.
    """

    rmode = ReproducibilityFlags.REPLICATE_COMP
    expected = {
        "category",
        "categoryType",
    }

    with (files(test_graphs) / "apps.graph").open()  as f:  # @UndefinedVariable
        # # logger.debug(f"Loading graph: {f}")
        lgt_node_data = json.load(f)["nodeDataArray"]
    with (files(test_graphs) / "files.graph").open()  as f:  # @UndefinedVariable
        # # logger.debug(f"Loading graph: {f}")
        lgt_files_data = json.load(f)["nodeDataArray"]
    with (files(test_graphs) / "groups.graph").open()  as f:  # @UndefinedVariable
        # # logger.debug(f"Loading graph: {f}")
        lgt_groups_data = json.load(f)["nodeDataArray"]
    with (files(test_graphs) / "misc.graph").open()  as f:  # @UndefinedVariable
        # # logger.debug(f"Loading graph: {f}")
        lgt_misc_data = json.load(f)["nodeDataArray"]

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
            self.rmode,
            "apps",
            self.temp_out.name,
        )
        _fill_workflow(
            self.rmode,
            "files",
            self.temp_out.name,
        )
        _fill_workflow(
            self.rmode,
            "groups",
            self.temp_out.name,
        )
        _fill_workflow(
            self.rmode,
            "misc",
            self.temp_out.name,
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
        expected = {"execution_time", "num_cpus", "dropclass"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _memory(self, drop):
        expected = {"data_volume"}
        hash_data = accumulate_lg_drop_data(drop, self.rmode)
        self.assertEqual(expected, hash_data.keys())

    def _file(self, drop):
        expected = {
            "data_volume",
            "check_filepath_exists",
            "filepath",
            "dirname",
        }
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
        expected = {
            "inputApplicationName",
            "inputApplicationType",
            "num_of_iter",
        }
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
    setup = False
    graph_data = None

    def _setup(self):
        if not self.setup:
            _run_workflows(
                self.rmode,
                SUPPORTED_WORKFLOWS,
                self.temp_out,
            )
            self.graph_data = _extract_reprodata(
                self.temp_out, SUPPORTED_WORKFLOWS, "PGS.graph"
            )
            self.setup = True

    def test_app_accumulate(self):
        """
        Type and rank matter
        """
        expected = {"categoryType", "rank"}
        self._setup()
        for drop in enumerate(self.graph_data["apps"]):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            self.assertEqual(expected, hash_data[self.rmode.name].keys())

    def test_data_accumulate(self):
        """
        Type and rank matter
        """
        expected = {"categoryType", "storage", "rank"}
        self._setup()
        for drop in enumerate(self.graph_data["files"]):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            self.assertEqual(expected, hash_data[self.rmode.name].keys())

    def test_group_accumulate(self):
        """
        Type and rank matter
        """
        expected_app = {"categoryType", "dt", "rank"}
        expected_file = {"categoryType", "storage", "rank"}
        self._setup()
        for drop in enumerate(self.graph_data["groups"]):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            if "dt" in hash_data.keys():
                self.assertEqual(expected_app, hash_data.keys())
            if "storage" in hash_data.keys():
                self.assertEqual(expected_file, hash_data.keys())

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Type and rank matter
        """
        self.assertEqual(True, False)

    def test_other_accumulate(self):
        """
        Will only return the length of the dummy memory drop
        """
        self._setup()
        self.assertEqual(2, len(self.graph_data["misc"]))


class AccumulatePGTPartitionReplicateCompData(unittest.TestCase):
    """
    Tests the replicate-comp standard at the physical graph template level.
    """

    rmode = ReproducibilityFlags.REPLICATE_COMP
    temp_out = tempfile.TemporaryDirectory()
    setup = False
    graph_data = None

    def _setup(self):
        if not self.setup:
            _run_workflows(
                self.rmode,
                SUPPORTED_WORKFLOWS,
                self.temp_out,
            )
            self.graph_data = _extract_reprodata(
                self.temp_out, SUPPORTED_WORKFLOWS, "PGT.graph"
            )
            self.setup = True

    def test_app_accumulate(self):
        """
        Type, rank and machine information matters.
        """
        expected = {"categoryType", "rank", "node", "island"}
        self._setup()
        for drop in enumerate(self.graph_data["apps"]):
            hash_data = accumulate_pgt_partition_drop_data(drop[1])
            self.assertEqual(expected, hash_data[self.rmode.name].keys())

    def test_data_accumulate(self):
        """
        Type, rank and machine information matters.
        """
        expected = {"categoryType", "storage", "rank", "node", "island"}
        self._setup()
        for drop in enumerate(self.graph_data["files"]):
            hash_data = accumulate_pgt_partition_drop_data(drop[1])
            self.assertEqual(expected, hash_data[self.rmode.name].keys())

    def test_group_accumulate(self):
        """
        Type, rank and machine information matters.
        """
        expected_app = {"categoryType", "dt", "rank"}
        expected_file = {"categoryType", "storage", "rank"}
        self._setup()
        for drop in enumerate(self.graph_data["groups"]):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            if "dt" in hash_data.keys():
                self.assertEqual(expected_app, hash_data.keys())
            if "storage" in hash_data.keys():
                self.assertEqual(expected_file, hash_data.keys())

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Type, rank and machine information matters.
        """
        self.assertEqual(True, False)

    def test_other_accumulate(self):
        """
        Should not matter for rerunning.
        Will only return the length of the dummy memory drop
        """
        self._setup()
        self.assertEqual(2, len(self.graph_data["misc"]))


class AccumulatePGReplicateCompData(unittest.TestCase):
    """
    Tests the replicate-comp standard at the physical graph level
    """

    rmode = ReproducibilityFlags.REPLICATE_COMP
    temp_out = tempfile.TemporaryDirectory()
    setup = False
    graph_data = None

    def _setup(self):
        if not self.setup:
            _run_workflows(
                self.rmode,
                SUPPORTED_WORKFLOWS,
                self.temp_out,
            )
            self.graph_data = _extract_reprodata(
                self.temp_out, SUPPORTED_WORKFLOWS, "PG.graph"
            )
            self.setup = True

    def test_app_accumulate(self):
        """
        Machine information matters.
        """
        expected = {"node", "island"}
        self._setup()
        for drop in enumerate(self.graph_data["apps"]):
            hash_data = accumulate_pg_drop_data(drop[1])
            self.assertEqual(expected, hash_data[self.rmode.name].keys())

    def test_data_accumulate(self):
        """
        Machine information matters.
        """
        expected = {"node", "island"}
        self._setup()
        for drop in enumerate(self.graph_data["apps"]):
            hash_data = accumulate_pg_drop_data(drop[1])
            self.assertEqual(expected, hash_data[self.rmode.name].keys())

    def test_group_accumulate(self):
        """
        Machine information matters.
        """
        expected_app = {"categoryType", "dt", "rank"}
        expected_file = {"categoryType", "storage", "rank"}
        self._setup()
        for drop in enumerate(self.graph_data["groups"]):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            if "dt" in hash_data.keys():
                self.assertEqual(expected_app, hash_data.keys())
            if "storage" in hash_data.keys():
                self.assertEqual(expected_file, hash_data.keys())

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Machine information matters.
        """
        self.assertEqual(True, False)

    def test_other_accumulate(self):
        """
        Should not matter for rerunning.
        Will only return the length of the dummy memory drop
        """
        self._setup()
        self.assertEqual(2, len(self.graph_data["misc"]))


class AccumulateLGTReplicateTotalData(unittest.TestCase):
    """
    Tests the replicate-total standard at the logical graph template level.
    """

    rmode = ReproducibilityFlags.REPLICATE_TOTAL
    expected = {
        "category",
        "categoryType",
    }
    with (files(test_graphs) / "apps.graph").open()  as f:  # @UndefinedVariable
        # # logger.debug(f"Loading graph: {f}")
        lgt_node_data = json.load(f)["nodeDataArray"]
    with (files(test_graphs) / "files.graph").open()  as f:  # @UndefinedVariable
        # # logger.debug(f"Loading graph: {f}")
        lgt_files_data = json.load(f)["nodeDataArray"]
    with (files(test_graphs) / "groups.graph").open()  as f:  # @UndefinedVariable
        # # logger.debug(f"Loading graph: {f}")
        lgt_groups_data = json.load(f)["nodeDataArray"]
    with (files(test_graphs) / "misc.graph").open()  as f:  # @UndefinedVariable
        # # logger.debug(f"Loading graph: {f}")
        lgt_misc_data = json.load(f)["nodeDataArray"]

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
            self.rmode,
            "apps",
            self.temp_out.name,
        )
        _fill_workflow(
            self.rmode,
            "files",
            self.temp_out.name,
        )
        _fill_workflow(
            self.rmode,
            "groups",
            self.temp_out.name,
        )
        _fill_workflow(
            self.rmode,
            "misc",
            self.temp_out.name,
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
        expected = {"execution_time", "num_cpus", "dropclass"}
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
        expected = {
            "inputApplicationName",
            "inputApplicationType",
            "num_of_iter",
        }
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
    setup = False
    graph_data = None

    def _setup(self):
        if not self.setup:
            _run_workflows(
                self.rmode,
                SUPPORTED_WORKFLOWS,
                self.temp_out,
            )
            self.graph_data = _extract_reprodata(
                self.temp_out, SUPPORTED_WORKFLOWS, "PGS.graph"
            )
            self.setup = True

    def test_app_accumulate(self):
        """
        Type matters
        """
        expected = {"categoryType"}
        self._setup()
        for drop in enumerate(self.graph_data["apps"]):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            self.assertEqual(expected, hash_data[self.rmode.name].keys())

    def test_data_accumulate(self):
        """
        Type matters
        """
        expected = {"categoryType", "storage"}
        self._setup()
        for drop in enumerate(self.graph_data["files"]):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            self.assertEqual(expected, hash_data[self.rmode.name].keys())

    def test_group_accumulate(self):
        """
        Type matters
        """
        expected_app = {"categoryType", "dt"}
        expected_file = {"categoryType", "storage"}
        self._setup()
        for drop in enumerate(self.graph_data["groups"]):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            if "dt" in hash_data.keys():
                self.assertEqual(expected_app, hash_data.keys())
            if "storage" in hash_data.keys():
                self.assertEqual(expected_file, hash_data.keys())

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Type matters
        """
        self.assertEqual(True, False)

    def test_other_accumulate(self):
        """
        Should not matter for rerunning.
        Will only return the length of the dummy memory drop
        """
        self._setup()
        self.assertEqual(2, len(self.graph_data["misc"]))


class AccumulatePGTPartitionReplicateTotalData(unittest.TestCase):
    """
    Tests the replicate-total standard at the physical graph template level.
    """

    rmode = ReproducibilityFlags.REPLICATE_TOTAL
    temp_out = tempfile.TemporaryDirectory()
    setup = False
    graph_data = None

    def _setup(self):
        if not self.setup:
            _run_workflows(
                self.rmode,
                SUPPORTED_WORKFLOWS,
                self.temp_out,
            )
            self.graph_data = _extract_reprodata(
                self.temp_out, SUPPORTED_WORKFLOWS, "PGT.graph"
            )
            self.setup = True

    def test_app_accumulate(self):
        """
        Only type matters
        """
        expected = {"categoryType"}
        self._setup()
        for drop in enumerate(self.graph_data["apps"]):
            hash_data = accumulate_pgt_partition_drop_data(drop[1])
            self.assertEqual(expected, hash_data[self.rmode.name].keys())

    def test_data_accumulate(self):
        """
        Only type matters
        """
        expected = {"categoryType", "storage"}
        self._setup()
        for drop in enumerate(self.graph_data["files"]):
            hash_data = accumulate_pgt_partition_drop_data(drop[1])
            self.assertEqual(expected, hash_data[self.rmode.name].keys())

    def test_group_accumulate(self):
        """
        Only type matters
        """
        expected_app = {"categoryType", "dt"}
        expected_file = {"categoryType", "storage"}
        self._setup()
        for drop in enumerate(self.graph_data["groups"]):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            if "dt" in hash_data.keys():
                self.assertEqual(expected_app, hash_data.keys())
            if "storage" in hash_data.keys():
                self.assertEqual(expected_file, hash_data.keys())

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Only type matters
        """
        self.assertEqual(True, False)

    def test_other_accumulate(self):
        """
        Should not matter for rerunning.
        Will only return the length of the dummy memory drop
        """
        self._setup()
        self.assertEqual(2, len(self.graph_data["misc"]))


class AccumulatePGReplicateTotalData(unittest.TestCase):
    """
    Tests the replicate-total standard at the physical graph level
    """

    rmode = ReproducibilityFlags.REPLICATE_TOTAL
    temp_out = tempfile.TemporaryDirectory()
    setup = False
    graph_data = None

    def _setup(self):
        if not self.setup:
            _run_workflows(
                self.rmode,
                SUPPORTED_WORKFLOWS,
                self.temp_out,
            )
            self.graph_data = _extract_reprodata(
                self.temp_out, SUPPORTED_WORKFLOWS, "PG.graph"
            )
            self.setup = True

    def test_app_accumulate(self):
        """
        Nothing matters
        """
        expected = {}
        self._setup()
        for drop in enumerate(self.graph_data["apps"]):
            hash_data = accumulate_pg_drop_data(drop[1])
            self.assertEqual(expected, dict(hash_data[self.rmode.name].keys()))

    def test_data_accumulate(self):
        """
        Nothing matters
        """
        expected = {}
        self._setup()
        for drop in enumerate(self.graph_data["files"]):
            hash_data = accumulate_pg_drop_data(drop[1])
            self.assertEqual(expected, dict(hash_data[self.rmode.name].keys()))

    def test_group_accumulate(self):
        """
        Nothing matters
        """
        expected_app = {"categoryType", "dt"}
        expected_file = {"categoryType", "storage"}
        self._setup()
        for drop in enumerate(self.graph_data["groups"]):
            hash_data = accumulate_pgt_unroll_drop_data(drop[1])
            if "dt" in hash_data.keys():
                self.assertEqual(expected_app, hash_data.keys())
            if "storage" in hash_data.keys():
                self.assertEqual(expected_file, hash_data.keys())

    @unittest.skip("pg_generator does not like sample graphs")
    def test_control_accumulate(self):
        """
        Nothing matters
        """
        self.assertEqual(True, False)

    def test_other_accumulate(self):
        """
        Should not matter for rerunning.
        Will only return the length of the dummy memory drop
        """
        self._setup()
        self.assertEqual(2, len(self.graph_data["misc"]))
