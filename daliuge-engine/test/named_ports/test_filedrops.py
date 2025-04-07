#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2014
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

"""
This module is used to test FileDROP input/output naming through named ports.
"""
import datetime
import dill
import pytest
import unittest

from importlib.resources import files

from dlg.ddap_protocol import DROPStates
from test.dlg_end_to_end_utils import create_and_run_graph_spec_from_graph_file
from daliuge_tests.engine import test_filedrops as test_graphs

# Note this test will only run with a full installation of DALiuGE.
pexpect = pytest.importorskip("dlg.dropmake")

class TestBasicApp(unittest.TestCase):
    """
    Given a simple graph with:

    <return_path("test_{datetime}.txt")> --> <FileDrop>

    Where:
        >>> def return_path(fn):
        >>>     return fn

    Make sure that it runs and produces a graph with name test_<current_date>.txt, and
    that the content is also test_<current_date>.txt

    """

    graph = f"{files(test_graphs)}/Filepath_testPG.graph"

    def test_pyfunc_to_fileapp(self):
        """
        Confirm filename and file output is correct
        """
        leafs = create_and_run_graph_spec_from_graph_file(
            self, self.graph, resolve_path=False)
        for l in leafs:
            self.assertEqual(DROPStates.COMPLETED, l.status)

        dt = datetime.date.today().strftime("%Y-%m-%d")
        # Leaf Node 1 has been encoded first as numpy, second as UTF-8.
        leaf = leafs.pop()
        self.assertEqual(f"test_{dt}.txt", leaf.filename)
        desc = leaf.open()
        self.assertEqual(f"test_{dt}.txt", leaf.read(desc).decode())


class TestBasicAppWithMemoryDropFileInput(unittest.TestCase):
    """
    This is the same as the TestBasicApp graph, except we use a memory drop with Pydata
    that defines the path name instead.
    
    <Pydata("test_{datetime}.txt")> --> <return_path> --> <FileDROP>
    """

    graph = f"{files(test_graphs)}/Filepath_test_InputOutputPG.graph"

    def test_pyfuncapp_to_file(self):
        """
        Confirm filename and file output is correct
        """
        leafs = create_and_run_graph_spec_from_graph_file(
            self, self.graph, resolve_path=False, app_root=False)
        for l in leafs:
            self.assertEqual(DROPStates.COMPLETED, l.status)

        dt = datetime.date.today().strftime("%Y-%m-%d")
        # Leaf Node 1 has been encoded first as numpy, second as UTF-8.
        leaf = leafs.pop()
        self.assertEqual(f"test_{dt}.txt", leaf.filename)
        desc = leaf.open()
        self.assertEqual(f"test_{dt}.txt", leaf.read(desc).decode())


class TestAppFileChain(unittest.TestCase):
    """
    This is an extension of TestBasicApp graph, where we now read in the output and use
    the path of the first FileDROP, and do the same thing again whilst modifying the
    filename and data in the file.

    This shows that we can re-use data using the user-supplied filename approach.

    """
    graph = f"{files(test_graphs)}/Filepath_test_multiple_appsPG.graph"

    def test_pyfunc_to_file_with_multiple_apps(self):
        """
        Confirm filename and file output is correct
        """
        leafs = create_and_run_graph_spec_from_graph_file(
            self, self.graph, resolve_path=False)
        for l in leafs:
            self.assertEqual(DROPStates.COMPLETED, l.status)

        dt = datetime.date.today().strftime("%Y-%m-%d")

        leaf = leafs.pop()
        self.assertEqual(f"test_{dt}_redux.txt", leaf.filename)
        desc = leaf.open()

        self.assertTrue(f"test_{dt}.txt redux" in leaf.read(desc).decode())
