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
This module ensures that one can submit a test-graph with the NOTHING flag and have nothing
happen.
"""
import json
import optparse
import tempfile
import unittest
import logging
import daliuge_tests.reproGraphs as test_graphs

from importlib.resources import files

from dlg.common.reproducibility.constants import (
    ReproducibilityFlags,
    ALL_RMODES,
)
from dlg.translator.tool_commands import (
    dlg_fill,
    dlg_unroll,
    dlg_partition,
    dlg_map,
)

logger = logging.getLogger("__name__")


def _run_full_workflow(
    rmode: ReproducibilityFlags,
    workflow: str,
    scratch_loc="./",
):
    workflow_loc = str(files(test_graphs))
    lgt = workflow_loc + "/" + workflow + ".graph"
    lgr = scratch_loc + "/" + workflow + "_" + str(rmode.value) + "LG.graph"
    pgs = scratch_loc + "/" + workflow + "_" + str(rmode.value) + "PGS.graph"
    pgt = scratch_loc + "/" + workflow + "_" + str(rmode.value) + "PGT.graph"
    pgr = scratch_loc + "/" + workflow + "_" + str(rmode.value) + "PG.graph"

    rmodes = str(rmode.value)
    parser = optparse.OptionParser()
    ll = ""
    dlg_fill(parser, ["-L", lgt, "-R", rmodes, "-o", lgr, "-f", "newline", ll])
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


def _read_graph(filename):
    file = open(filename)
    graph = json.load(file)
    file.close()
    return graph


class IntegrationNothingTest(unittest.TestCase):
    """
    Tests a simple graph up until submission for execution with the goal of asserting
    that running a graph with the NOTHING standard produces no reproducibility information.
    """

    temp_out = tempfile.TemporaryDirectory("out")

    def test_computation_sandwich(self):
        """
        Opens a simple computationSandwich graph in a temporary directory
        No data should be present at any level of abstraction, reflected by a null merkleroot.
        """
        graph_name = "HelloSPython"
        graph_loc = "reproducibility/reproGraphs/"
        rmode = ReproducibilityFlags.NOTHING
        _run_full_workflow(
            rmode=rmode,
            workflow=graph_name,
            scratch_loc=self.temp_out.name,
        )

        for filename in ["PGS", "PGT", "PG"]:
            pgr = (
                self.temp_out.name
                + "/"
                + graph_name
                + "_"
                + str(rmode.value)
                + f"{filename}.graph"
            )
            graph = _read_graph(pgr)
            for drop in graph:
                if "reprodata" in drop:
                    self.assertIn("0", drop["reprodata"]["rmode"])
                else:
                    self.assertIn("rmode", drop)
        lgr = (
            self.temp_out.name
            + "/"
            + graph_name
            + "_"
            + str(rmode.value)
            + "LG.graph"
        )
        graph = _read_graph(lgr)
        for drop in graph["nodeDataArray"]:
            self.assertIn("reprodata", drop)


class IntegrationHelloWorldTest(unittest.TestCase):
    """
    An example running multiple 'hello world' style workflows, comparing their hash-values.
    This test is the first actual reproducibility test.

    For now, until I can find a way to launch a daliuge from the test and shut it down when finished
    we will just go to the physical graph level.
    Until that point arrives, it will be difficult to test Reproduce and beyond fairly.
    TODO: Actually run full graphs as a test.

    We initalize the test by processing every graph for every level tested.

    HelloSPython and HelloSPython2 differ only in their input file arguments.

    """

    temp_out = tempfile.TemporaryDirectory("out")
    graph_loc = "reproducibility/reproGraphs/"
    graphs = {
        "HelloWorldBash": {},
        "HelloSBash": {},
        "HelloWorldFile": {},
        "HelloSPython": {},
        "HelloSPython2": {},
    }

    def _process_graphs(self, rmode: ReproducibilityFlags):
        for graph in list(self.graphs.keys()):
            _run_full_workflow(
                rmode=rmode,
                workflow=graph,
                scratch_loc=self.temp_out.name,
            )
            if rmode == ReproducibilityFlags.ALL:
                self.graphs[graph][rmode.value] = _read_graph(
                    self.temp_out.name
                    + "/"
                    + graph
                    + "_"
                    + str(rmode.value)
                    + "PG.graph"
                )[-1]
            else:
                self.graphs[graph][rmode.value] = _read_graph(
                    self.temp_out.name
                    + "/"
                    + graph
                    + "_"
                    + str(rmode.value)
                    + "PG.graph"
                )[-1][rmode.name]["signature"]

    def test_integration_rerun(self):
        """
        Compares the four hello world graphs by their signatures when rerunning
        HelloWorldBash RR-> HelloSBash
        HelloWorldBash !RR-> HelloWorldFile
        HelloWorldBash !RR-> HelloSPython
        HelloSBash !RR-> HelloWorldFile
        HelloSBash !RR-> HelloSPython
        HelloWorldFile !RR-> HelloSPython
        HelloSPython RR-> HelloSPython2
        """
        rmode = ReproducibilityFlags.RERUN
        self._process_graphs(rmode)
        self.assertEqual(
            self.graphs["HelloWorldBash"][rmode.value],
            self.graphs["HelloSBash"][rmode.value],
        )
        self.assertNotEqual(
            self.graphs["HelloWorldBash"][rmode.value],
            self.graphs["HelloWorldFile"][rmode.value],
        )
        self.assertNotEqual(
            self.graphs["HelloWorldBash"][rmode.value],
            self.graphs["HelloSPython"][rmode.value],
        )

        self.assertNotEqual(
            self.graphs["HelloSBash"][rmode.value],
            self.graphs["HelloWorldFile"][rmode.value],
        )
        self.assertNotEqual(
            self.graphs["HelloSBash"][rmode.value],
            self.graphs["HelloSPython"][rmode.value],
        )

        self.assertNotEqual(
            self.graphs["HelloWorldFile"][rmode.value],
            self.graphs["HelloSPython"][rmode.value],
        )

        self.assertEqual(
            self.graphs["HelloSPython"][rmode.value],
            self.graphs["HelloSPython2"][rmode.value],
        )

    def test_integration_repeat(self):
        """
        Compares the four hello world graphs by their signatures when repeating
        HelloWorldBash !RT-> HelloSBash
        HelloWorldBash !RT-> HelloWorldFile
        HelloWorldBash !RT-> HelloSPython
        HelloSBash !RT-> HelloWorldFile
        HelloSBash !RT-> HelloSPython
        HelloWorldFile !RT-> HelloSPython
        HelloSPython RT-> HelloSPython2
        """
        rmode = ReproducibilityFlags.REPEAT
        self._process_graphs(rmode)
        self.assertNotEqual(
            self.graphs["HelloWorldBash"][rmode.value],
            self.graphs["HelloSBash"][rmode.value],
        )
        self.assertNotEqual(
            self.graphs["HelloWorldBash"][rmode.value],
            self.graphs["HelloWorldFile"][rmode.value],
        )
        self.assertNotEqual(
            self.graphs["HelloWorldBash"][rmode.value],
            self.graphs["HelloSPython"][rmode.value],
        )

        self.assertNotEqual(
            self.graphs["HelloSBash"][rmode.value],
            self.graphs["HelloWorldFile"][rmode.value],
        )
        self.assertNotEqual(
            self.graphs["HelloSBash"][rmode.value],
            self.graphs["HelloSPython"][rmode.value],
        )

        self.assertNotEqual(
            self.graphs["HelloWorldFile"][rmode.value],
            self.graphs["HelloSPython"][rmode.value],
        )

        self.assertEqual(
            self.graphs["HelloSPython"][rmode.value],
            self.graphs["HelloSPython2"][rmode.value],
        )

    def test_integration_recompute(self):
        """
        Compares the four hello world graphs by their signatures when recomputing
        HelloWorldBash !RC-> HelloSBash
        HelloWorldBash !RC-> HelloWorldFile
        HelloWorldBash !RC-> HelloSPython
        HelloSBash !RC-> HelloWorldFile
        HelloSBash !RC-> HelloSPython
        HelloWorldFile !RC-> HelloSPython
        HelloSPython !RC-> HelloSPython2
        """
        rmode = ReproducibilityFlags.RECOMPUTE
        self._process_graphs(rmode)
        self.assertNotEqual(
            self.graphs["HelloWorldBash"][rmode.value],
            self.graphs["HelloSBash"][rmode.value],
        )
        self.assertNotEqual(
            self.graphs["HelloWorldBash"][rmode.value],
            self.graphs["HelloWorldFile"][rmode.value],
        )
        self.assertNotEqual(
            self.graphs["HelloWorldBash"][rmode.value],
            self.graphs["HelloSPython"][rmode.value],
        )

        self.assertNotEqual(
            self.graphs["HelloSBash"][rmode.value],
            self.graphs["HelloWorldFile"][rmode.value],
        )
        self.assertNotEqual(
            self.graphs["HelloSBash"][rmode.value],
            self.graphs["HelloSPython"][rmode.value],
        )

        self.assertNotEqual(
            self.graphs["HelloWorldFile"][rmode.value],
            self.graphs["HelloSPython"][rmode.value],
        )

        self.assertNotEqual(
            self.graphs["HelloSPython"][rmode.value],
            self.graphs["HelloSPython2"][rmode.value],
        )

    def test_integration_reproduce(self):
        """
        Compares the four hello world graphs by their signatures when reproducing
        HelloWorldBash RP-> HelloSBash
        HelloWorldBash !RP-> HelloWorldFile
        HelloWorldBash RP-> HelloSPython
        HelloSBash !RP-> HelloWorldFile
        HelloSBash RP-> HelloSPython
        HelloWorldFile !RP-> HelloSPython
        HelloSPython RP-> HelloSPython2

        HelloWorldFile is different to all others since it is a single file, the others have an
        input and output file.
        HelloSPython and HelloSPython2 differ in input filename only
        """
        rmode = ReproducibilityFlags.REPRODUCE
        self._process_graphs(rmode)
        self.assertEqual(
            self.graphs["HelloWorldBash"][rmode.value],
            self.graphs["HelloSBash"][rmode.value],
        )
        self.assertEqual(
            self.graphs["HelloWorldBash"][rmode.value],
            self.graphs["HelloWorldFile"][rmode.value],
        )

        self.assertEqual(
            self.graphs["HelloWorldBash"][rmode.value],
            self.graphs["HelloSPython"][rmode.value],
        )

        self.assertEqual(
            self.graphs["HelloSBash"][rmode.value],
            self.graphs["HelloWorldFile"][rmode.value],
        )
        self.assertEqual(
            self.graphs["HelloSBash"][rmode.value],
            self.graphs["HelloSPython"][rmode.value],
        )

        self.assertEqual(
            self.graphs["HelloWorldFile"][rmode.value],
            self.graphs["HelloSPython"][rmode.value],
        )

        self.assertEqual(
            self.graphs["HelloSPython"][rmode.value],
            self.graphs["HelloSPython2"][rmode.value],
        )

    def test_integration_replicate_sci(self):
        """
        Compares the four hello world graphs by their signatures when replicating scientifically
        HelloWorldBash RPLS-> HelloSBash
        HelloWorldBash !RPLS-> HelloWorldFile
        HelloWorldBash !RPLS-> HelloSPython
        HelloSBash !RPLS-> HelloWorldFile
        HelloSBash !RPLS-> HelloSPython
        HelloWorldFile !RPLS-> HelloSPython
        HelloSPython RPLS-> HelloSPython2
        """
        rmode = ReproducibilityFlags.REPLICATE_SCI
        self._process_graphs(rmode)
        self.assertEqual(
            self.graphs["HelloWorldBash"][rmode.value],
            self.graphs["HelloSBash"][rmode.value],
        )
        self.assertNotEqual(
            self.graphs["HelloWorldBash"][rmode.value],
            self.graphs["HelloWorldFile"][rmode.value],
        )

        self.assertNotEqual(
            self.graphs["HelloWorldBash"][rmode.value],
            self.graphs["HelloSPython"][rmode.value],
        )

        self.assertNotEqual(
            self.graphs["HelloSBash"][rmode.value],
            self.graphs["HelloWorldFile"][rmode.value],
        )
        self.assertNotEqual(
            self.graphs["HelloSBash"][rmode.value],
            self.graphs["HelloSPython"][rmode.value],
        )

        self.assertNotEqual(
            self.graphs["HelloWorldFile"][rmode.value],
            self.graphs["HelloSPython"][rmode.value],
        )

        self.assertEqual(
            self.graphs["HelloSPython"][rmode.value],
            self.graphs["HelloSPython2"][rmode.value],
        )

    def test_integration_replicate_comp(self):
        """
        Compares the four hello world graphs by their signatures when replicating computationally
        HelloWorldBash !RPLC-> HelloSBash
        HelloWorldBash !RPLC-> HelloWorldFile
        HelloWorldBash !RPLC-> HelloSPython
        HelloSBash !RPLC-> HelloWorldFile
        HelloSBash !RPLC-> HelloSPython
        HelloWorldFile !RPLC-> HelloSPython
        HelloSPython !RPLC-> HelloSPython2
        """
        rmode = ReproducibilityFlags.REPLICATE_COMP
        self._process_graphs(rmode)
        self.assertNotEqual(
            self.graphs["HelloWorldBash"][rmode.value],
            self.graphs["HelloSBash"][rmode.value],
        )
        self.assertNotEqual(
            self.graphs["HelloWorldBash"][rmode.value],
            self.graphs["HelloWorldFile"][rmode.value],
        )
        self.assertNotEqual(
            self.graphs["HelloWorldBash"][rmode.value],
            self.graphs["HelloSPython"][rmode.value],
        )

        self.assertNotEqual(
            self.graphs["HelloSBash"][rmode.value],
            self.graphs["HelloWorldFile"][rmode.value],
        )
        self.assertNotEqual(
            self.graphs["HelloSBash"][rmode.value],
            self.graphs["HelloSPython"][rmode.value],
        )

        self.assertNotEqual(
            self.graphs["HelloWorldFile"][rmode.value],
            self.graphs["HelloSPython"][rmode.value],
        )

        self.assertNotEqual(
            self.graphs["HelloSPython"][rmode.value],
            self.graphs["HelloSPython2"][rmode.value],
        )

    def test_integration_replicate_total(self):
        """
        Compares the four hello world graphs by their signatures when replicating totally
        HelloWorldBash !RPLT-> HelloSBash
        HelloWorldBash !RPLT-> HelloWorldFile
        HelloWorldBash !RPLT-> HelloSPython
        HelloSBash !RPLT-> HelloWorldFile
        HelloSBash !RPLT-> HelloSPython
        HelloWorldFile !RPLT-> HelloSPython
        HelloSPython RPLT-> HelloSPython2
        """
        rmode = ReproducibilityFlags.REPLICATE_TOTAL
        self._process_graphs(rmode)
        self.assertNotEqual(
            self.graphs["HelloWorldBash"][rmode.value],
            self.graphs["HelloSBash"][rmode.value],
        )
        self.assertNotEqual(
            self.graphs["HelloWorldBash"][rmode.value],
            self.graphs["HelloWorldFile"][rmode.value],
        )
        self.assertNotEqual(
            self.graphs["HelloWorldBash"][rmode.value],
            self.graphs["HelloSPython"][rmode.value],
        )

        self.assertNotEqual(
            self.graphs["HelloSBash"][rmode.value],
            self.graphs["HelloWorldFile"][rmode.value],
        )
        self.assertNotEqual(
            self.graphs["HelloSBash"][rmode.value],
            self.graphs["HelloSPython"][rmode.value],
        )

        self.assertNotEqual(
            self.graphs["HelloWorldFile"][rmode.value],
            self.graphs["HelloSPython"][rmode.value],
        )

        self.assertEqual(
            self.graphs["HelloSPython"][rmode.value],
            self.graphs["HelloSPython2"][rmode.value],
        )

    def test_integration_all(self):
        rmode = ReproducibilityFlags.ALL
        self._process_graphs(rmode)
        for rmode in ALL_RMODES:
            self._process_graphs(rmode)
        for graph in self.graphs:
            for rmode in ALL_RMODES:
                self.assertEqual(
                    self.graphs[graph][rmode.value],
                    self.graphs[graph][ReproducibilityFlags.ALL.value][
                        rmode.name
                    ]["signature"],
                )


class IntegrationSplitRmode(unittest.TestCase):
    """
    It is not unreasonable for different reproducibility standards enforced on different drops in
    a single workflow.
    This test class tests this functionality.
    """

    temp_out = tempfile.TemporaryDirectory("out")

    @unittest.skip(
        "Individual rmodes not yet supported again (needs re-working)"
    )
    def test_split_lgt(self):
        """
        Tests a simple 'hello world' graph (HelloWorldBash) where a single component has
        a pre-existing rmode in the graph file set (Replicate Computationally).
        The rest are run with a NOTHING standard.
        We should be able to tell:
          - This graph's signature should be different to the standard graph run completely in
            RERUN and NOTHING standards
          - The reprodata of the set component should contain values while the rest do not
        """
        graph_name = "HelloWorldBashSplit"
        control_graph_name = "HelloWorldBash"
        graph_loc = "reproducibility/reproGraphs/"
        rmode = ReproducibilityFlags.RERUN
        _run_full_workflow(
            rmode=rmode,
            workflow=graph_name,
            scratch_loc=self.temp_out.name,
        )
        pgr = (
            self.temp_out.name
            + "/"
            + graph_name
            + "_"
            + str(rmode.value)
            + "PG.graph"
        )
        _run_full_workflow(
            rmode=rmode,
            workflow=control_graph_name,
            scratch_loc=self.temp_out.name,
        )
        pgr_2 = (
            self.temp_out.name
            + "/"
            + control_graph_name
            + "_"
            + str(rmode.value)
            + "PG.graph"
        )

        graph = _read_graph(pgr)
        graph_reprodata = graph[-1]
        graph = graph[0:-1]
        control_graph = _read_graph(pgr_2)
        control_signature = control_graph[-1]["signature"]
        self.assertEqual(
            ReproducibilityFlags.RERUN.value, int(graph_reprodata["rmode"])
        )

        for drop in graph:
            if drop["reprodata"]["rmode"] == str(
                ReproducibilityFlags.RERUN.value
            ):
                self.assertIsNotNone(
                    drop["reprodata"]["lgt_data"]["merkleroot"]
                )
                self.assertIsNone(drop["reprodata"]["lg_data"]["merkleroot"])
                self.assertIsNotNone(
                    drop["reprodata"]["pgt_data"]["merkleroot"]
                )
                self.assertIsNone(drop["reprodata"]["pg_data"]["merkleroot"])
            else:
                self.assertIsNotNone(
                    drop["reprodata"]["lgt_data"]["merkleroot"]
                )
                self.assertIsNotNone(
                    drop["reprodata"]["lg_data"]["merkleroot"]
                )
                self.assertIsNotNone(
                    drop["reprodata"]["pgt_data"]["merkleroot"]
                )
                self.assertIsNotNone(
                    drop["reprodata"]["pg_data"]["merkleroot"]
                )
