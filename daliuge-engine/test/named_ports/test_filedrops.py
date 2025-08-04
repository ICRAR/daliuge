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
import os
import shutil

import dill
import pytest
import unittest

from importlib.resources import files
from pathlib import Path

pexpect = pytest.importorskip("dlg.dropmake")

from dlg.ddap_protocol import DROPStates
from test.dlg_end_to_end_utils import create_and_run_graph_spec_from_graph_file
from daliuge_tests.engine import test_filedrops as test_graphs

# Note this test will only run with a full installation of DALiuGE.

class TestBasicApp(unittest.TestCase):
    """
    Test that the Graph produces:

        - A file as a side effect of the behaviour of the function ((hello_{dt}.txt)).
        - A file that is a result of copying the side effect filename, using the FileDROP
          connected to the AppDrop.
        - A file that is a result of the output of the function (the return)
    """

    graph = f"{files(test_graphs)}/Filepath_test_side_effectPG.graph"
    rundir = "/tmp/daliuge_tfiles"

    def setUp(self):
        if os.path.exists(self.rundir):
            shutil.rmtree(self.rundir)
        # if os.path.exists(self.rundir):
        #     shutil.rmtree(self.rundir)
        # os.makedirs(self.rundir)

    # def tearDown(self):
    #     if os.path.exists(self.rundir):
    #         shutil.rmtree(self.rundir)

    def test_pyfunc_to_fileapp(self):
        """
        Confirm filename and file output is correct
        """
        leafs = create_and_run_graph_spec_from_graph_file(
            self, self.graph, resolve_path=False, app_root=False)
        for l in leafs:
            self.assertEqual(DROPStates.COMPLETED, l.status)

        dt = datetime.date.today().strftime("%Y-%m-%d")
        leaf = leafs.pop()

        hello_filename = f"hello_{dt}.txt"
        hello_text = "Hello Ryan"
        hello_duplicate = "hello2.txt"

        output_filename = "output.txt"
        output_text = "Hello from write_file"

        expected_files = [hello_filename, hello_duplicate, output_filename]
        created_files = [p.name for p in Path(leaf.dirname).iterdir()]
        for e in expected_files:
            self.assertTrue(e in created_files)

        for p in Path(leaf.dirname).iterdir():
            if p.name == hello_filename or p.name == hello_duplicate:
                text = hello_text
            else:
                text = output_text
                with p.open() as fp:
                    self.assertEqual(text, fp.read())
