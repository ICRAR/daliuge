"""
Test Branching behaviour
"""
import pytest
import unittest

try:
    from importlib.resources import files
except ModuleNotFoundError:
    from importlib_resources import files

from dlg.apps.pyfunc import PyFuncApp
from dlg.apps.simple import Branch
from dlg.ddap_protocol import DROPStates
from dlg.droputils import depthFirstTraverse

# Note this test will only run with a full installation of DALiuGE
pexpect = pytest.importorskip("dlg.dropmake")
from test.dlg_end_to_end_utils import (translate_graph,
                                       create_and_run_graph_spec)

from daliuge_tests.engine import graphs


class TestBranchSkipping(unittest.TestCase):
    """
    Tests that confirm skipping behaviour makes sense for larger graphs.
    """

    def test_branch_skip(self):
        """
        This a basic branch that we know will skip the path of DROPs that in a PyFuncApp.
        We expect the PyFuncApp to be skipped, and this is what we are testing.
        """
        f = files(graphs)/'branch_skip.graph'
        g = translate_graph(str(f), 'test_branch_skip')

        roots, _ = create_and_run_graph_spec(self,g, app_root=False)
        nodes = [drop for drop, _ in depthFirstTraverse(roots[0])]
        for n in nodes:
            if isinstance(n, PyFuncApp) and not isinstance(n, Branch):
                self.assertEqual(DROPStates.SKIPPED, n.status)


    def test_branch_skip_multiple_input(self):
        """
        Here we do the same as test_branch_skip, except the PyFunc app is also relying
        on input from a secondary memoryDROP.

        This previously failed because when we received 1 skip, we needed all other
        inputs to be skipped. What happend was the app continued to execution and was
        then expecting input from the skipped drop!
        Now, this test confirms that if I am expecting multiple inputs but one of those
        inputs has skipped, I just skip everything regardless of the 'success' state
        of the other input drops.
        """

        f = files(graphs)/'branch_skip_multiple_input.graph'
        g = translate_graph(str(f), 'test_branch_skip_multiple_input')

        roots, leafs = create_and_run_graph_spec(self,g, app_root=False)
        nodes = [drop for drop, _ in depthFirstTraverse(roots[0])]
        for n in nodes:
            if isinstance(n, PyFuncApp) and not isinstance(n, Branch):
                self.assertEqual(DROPStates.SKIPPED, n.status)

    def test_loop_branch_exit_multiple_input(self):
        """
        Similar to the test_branch_skip_multiple_input but with a more complex graph
        where the branch is inside a loop.

        This also exposes  event ports as well as multiple cloned inputs to the
        exit application.
        """

        f = files(graphs)/'LoopWithBranchExit_multiple_input.graph'
        g = translate_graph(str(f), 'LoopWithBranchExit_multiple_input.graph')

        roots, leafs = create_and_run_graph_spec(self,g, app_root=True)
        nodes = [drop for drop, _ in depthFirstTraverse(roots[0])]
        for n in nodes:
            if isinstance(n, PyFuncApp) and not isinstance(n, Branch):
                self.assertEqual(DROPStates.SKIPPED, n.status)
