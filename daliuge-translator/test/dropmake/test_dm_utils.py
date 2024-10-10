#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2015
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
Test the dropmake.dm_utils functionality
"""

import json
import copy
import unittest
from dlg.dropmake.dm_utils import convert_construct, convert_subgraphs
<<<<<<< HEAD
=======
import daliuge_tests.dropmake as test_graphs
>>>>>>> master

try:
    from importlib.resources import files, as_file
except (ImportError, ModuleNotFoundError):
    from importlib_resources import files

<<<<<<< HEAD
NODES = 'nodeDataArray'
LINKS = 'linkDataArray'


def get_lg_fname(lg_name):
    return str(files(__package__) / f"logical_graphs/{lg_name}")
=======
NODES = "nodeDataArray"
LINKS = "linkDataArray"


def get_lg_fname(lg_name):
    return str(files(test_graphs) / f"logical_graphs/{lg_name}")
>>>>>>> master


def getNodeFromKey(lgo, key):
    for node in lgo[NODES]:
<<<<<<< HEAD
        if node['key'] == key:
=======
        if node["id"] == key:
>>>>>>> master
            return node
    return None


def getConstructNodeFromCategory(lgo, category):
    """
    Find the first node with the given construct

    E.g. n = getNodeFromConstruct(lgo, "Scatter")
    n['category'] == 'Scatter'
    n['categoryType'] == 'Construct'

    :param lgo: dict,  Logical Graph
    :param category: str, name of the category of node we want to inspect
    :return: dict, node of the Logical Graph
    """
    for node in lgo[NODES]:
<<<<<<< HEAD
        if node['category'] == category and node['categoryType'] == 'Construct':
            return node
    return None

class TestConvertSubGraphConstruct(unittest.TestCase):

=======
        if node["category"] == category and node["categoryType"] == "Construct":
            return node
    return None


class TestConvertSubGraphConstruct(unittest.TestCase):
>>>>>>> master
    def setUp(self):
        """
        Open a logical graph that we want to test using an approach similar to that
        used in dropmake.lg.LG
        """

    def getSubgraphNode(self, lgo):
        """
        For a given logical graph dictionary object, find the (first) Subgraph construct
        :param lgo:
        :return:
        """
        for node in lgo[NODES]:
<<<<<<< HEAD
            if node['category'] == 'SubGraph' and node['categoryType'] == 'Construct':
=======
            if node["category"] == "SubGraph" and node["categoryType"] == "Construct":
>>>>>>> master
                return node
        return None

    def test_convert_subgraphs_noinputapp(self):
<<<<<<< HEAD

        fname = get_lg_fname("ExampleSubgraphNoInput.graph")
        with open(fname, 'r') as fp:
=======
        fname = get_lg_fname("ExampleSubgraphNoInput.graph")
        with open(fname, "r") as fp:
>>>>>>> master
            lg = json.load(fp)
        previous_num_nodes = len(lg[NODES])
        previous_num_links = len(lg[LINKS])
        sg_node = self.getSubgraphNode(lg)
<<<<<<< HEAD
        self.assertFalse('hasInputApp' in sg_node)
        convert_subgraphs(lg)
        self.assertTrue('hasInputApp' in sg_node)
        self.assertFalse(sg_node['hasInputApp'])
=======
        self.assertFalse("hasInputApp" in sg_node)
        convert_subgraphs(lg)
        self.assertTrue("hasInputApp" in sg_node)
        self.assertFalse(sg_node["hasInputApp"])
>>>>>>> master
        self.assertEqual(previous_num_nodes, len(lg[NODES]))
        self.assertEqual(previous_num_links, len(lg[LINKS]))

    def test_convert_subgraphs_withinputapp(self):
        fname = get_lg_fname("ExampleSubgraphSimple.graph")
<<<<<<< HEAD
        with open(fname, 'r') as fp:
            lg = json.load(fp)
        sg_node = self.getSubgraphNode(lg)
        self.assertFalse('hasInputApp' in sg_node)
        self.assertEqual(6, len(lg[LINKS]))
        nSubGraphConstruct = getConstructNodeFromCategory(lg, 'SubGraph')
        nSubGraphKey = nSubGraphConstruct['key']
        convert_subgraphs(lg)
        self.assertTrue('hasInputApp' in sg_node)
        self.assertTrue(sg_node['hasInputApp'])
        nSubGraphApp = getNodeFromKey(lg, nSubGraphKey)
        self.assertEqual(nSubGraphKey, nSubGraphApp['key'])
        self.assertEqual("PythonApp", nSubGraphApp['category'])
        # We remove links from the Subgraph children
        self.assertEqual(4, len(lg[LINKS]))
        subgraphDataNode = getNodeFromKey(lg, -1)
        self.assertIsNotNone(subgraphDataNode['subgraph'])
=======
        with open(fname, "r") as fp:
            lg = json.load(fp)
        sg_node = self.getSubgraphNode(lg)
        self.assertFalse("hasInputApp" in sg_node)
        self.assertEqual(6, len(lg[LINKS]))
        nSubGraphConstruct = getConstructNodeFromCategory(lg, "SubGraph")
        nSubGraphKey = nSubGraphConstruct["id"]
        convert_subgraphs(lg)
        self.assertTrue("hasInputApp" in sg_node)
        self.assertTrue(sg_node["hasInputApp"])
        nSubGraphApp = getNodeFromKey(lg, nSubGraphKey)
        self.assertEqual(nSubGraphKey, nSubGraphApp["id"])
        self.assertEqual("PythonApp", nSubGraphApp["category"])
        # We remove links from the Subgraph children
        self.assertEqual(4, len(lg[LINKS]))
        subgraphDataNode = getNodeFromKey(
            lg, "bb9b78bc-b725-4b61-a12a-413bdcef7690")
        self.assertIsNotNone(subgraphDataNode["subgraph"])
>>>>>>> master


class TestConvertScatterGatherConstruct(unittest.TestCase):
    def test_convert_construct(self):
        """
        Confirm constructs converted correctly.
        This test uses an extremely simple Scatter/Gather graph:

            Scatter(InputApp) --> Data Drop(inside Scatter) --> Gather(InputApp)

        This gives us 3 nodes, 2 links. The Scatter/Gather num_of_copies/num_of_inputs
        is 4.

        For convert_construct, we expect the following outcome:
            - 2 new nodes will be added; PythonApp Scatter and Gathers
            - There will be the same number of links added, but the links will have
            changed from the constructs to the applications.
            - The groups will have changed numbers

        We expect the keys to transition as well.
        """
        fname = get_lg_fname("SuperBasicScatterGather.graph")
<<<<<<< HEAD
        with open(fname, 'r') as fp:
=======
        with open(fname, "r") as fp:
>>>>>>> master
            lg = json.load(fp)
        self.assertEqual(3, len(lg[NODES]))
        self.assertEqual(2, len(lg[LINKS]))

<<<<<<< HEAD
        nScatterConstruct = getConstructNodeFromCategory(lg, 'Scatter')
        nScatterKey = nScatterConstruct['key']
        nGatherConstruct = getConstructNodeFromCategory(lg, 'Gather')
        nGatherKey = nGatherConstruct['key']
=======
        nScatterConstruct = getConstructNodeFromCategory(lg, "Scatter")
        nScatterKey = nScatterConstruct["id"]
        nGatherConstruct = getConstructNodeFromCategory(lg, "Gather")
        nGatherKey = nGatherConstruct["id"]
>>>>>>> master

        convert_construct(lg)
        self.assertEqual(5, len(lg[NODES]))
        self.assertEqual(2, len(lg[LINKS]))

        # Confirm that the transition from construct-to-app has occured.
        nScatterApp = getNodeFromKey(lg, nScatterKey)
<<<<<<< HEAD
        self.assertEqual(nScatterKey, nScatterApp['key'])
        self.assertEqual("PythonApp", nScatterApp['category'])
        nGatherApp = getNodeFromKey(lg, nGatherKey)
        self.assertEqual(nGatherKey, nGatherApp['key'])
        self.assertEqual("PythonApp", nGatherApp['category'])
=======
        self.assertEqual(nScatterKey, nScatterApp["id"])
        self.assertEqual("PythonApp", nScatterApp["category"])
        nGatherApp = getNodeFromKey(lg, nGatherKey)
        self.assertEqual(nGatherKey, nGatherApp["id"])
        self.assertEqual("PythonApp", nGatherApp["category"])
>>>>>>> master
