#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2025
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
Regression test to ensure we do not have errors with Object PyFuncApps.
"""
import pytest
import unittest

try:
    from importlib.resources import files
except ModuleNotFoundError:
    from importlib_resources import files

from dlg.apps.pyfunc import PyFuncApp
from dlg.ddap_protocol import DROPStates
from dlg.droputils import depthFirstTraverse

# Note this test will only run with a full installation of DALiuGE
pexpect = pytest.importorskip("dlg.dropmake")
from test.dlg_end_to_end_utils import (translate_graph,
                                       create_and_run_graph_spec)

from daliuge_tests.engine import graphs

class TestInit(unittest.TestCase):

    def test_basic_init(self):
        f = files(graphs)/'ObjectTest.graph'
        g = translate_graph(str(f), 'ObjectTest')

        roots, _ = create_and_run_graph_spec(self,g, app_root=False)
        nodes = [drop for drop, _ in depthFirstTraverse(roots[0])]
        for n in nodes:
            self.assertEqual(DROPStates.COMPLETED, n.status)