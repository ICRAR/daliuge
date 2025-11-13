"""
Test that the logical graph node is initialised correctly.
"""
import copy

from dlg.common.path_utils import get_lg_fpath


from dlg.dropmake import dm_utils
from dlg.dropmake.lg_node import LGNode


def convert_lg(lg_path: str):
    lg = dm_utils.load_lg(lg_path)
    lg = dm_utils.extract_globals(lg)
    lg = dm_utils.convert_fields(lg)
    lg = dm_utils.convert_construct(lg)
    lg = dm_utils.convert_subgraphs(lg)
    return lg

class TestConvertFields():

    def test_helloworld(self):
        """
        Convert fields and establish what has been updated.
        """
        lg_path = get_lg_fpath("logical_graph", "HelloWorld_simple.graph")
        lg = dm_utils.load_lg(lg_path)
        lg_converted = dm_utils.convert_fields(copy.deepcopy(lg))
        nodes = lg['nodeDataArray']
        converted_nodes = lg_converted['nodeDataArray']

    def test_branch(self):
        """
        Convert fields
        :return:
        """

class TestLGNodeImport():
    """
    Test we build and construct LGNodes correctly
    """

    def test_helloworld(self):
        """
        Given a hello world graph, do we accurately produce LGNode information.
        """

        lg_path = get_lg_fpath("logical_graph", "HelloWorld_simple.graph")
        lg = convert_lg(lg_path)
        app = lg["nodeDataArray"][0]
        group_q = {}
        done_dict = {}
        app_node = LGNode(app, group_q, done_dict, 'app')

    def test_branch(self):
        """
        Test what a graph node looks like after branch translation
        """

class TestLGNodeFailureCases():
    """
    Test we detect errors in LGNodes and raise where appropriately
    """