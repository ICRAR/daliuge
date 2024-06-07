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
from dlg.dropmake.dm_utils import convert_subgraphs

NODES = 'nodeDataArray'
LINKS = 'linkDataArray'

class TestConvertSubGraphConstruct(unittest.TestCase):

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
            if node['category'] == 'SubGraph' and node['categoryType'] == 'Construct':
                return node
        return None

    def test_convert_subgraphs_noinputapp(self):
        with open("daliuge-translator/test/dropmake/logical_graphs"
                  "/ExampleSubgraphNoInput.graph") as fp:
            lg = json.load(fp)
        previous_num_nodes = len(lg[NODES])
        previous_num_links = len(lg[LINKS])
        sg_node = self.getSubgraphNode(lg)
        self.assertFalse('hasInputApp' in sg_node)
        convert_subgraphs(lg)
        self.assertTrue('hasInputApp' in sg_node)
        self.assertFalse(sg_node['hasInputApp'])
        self.assertEqual(previous_num_nodes, len(lg[NODES]))
        self.assertEqual(previous_num_links, len(lg[LINKS]))

    def test_convert_subgraphs_withinputapp(self):
        with open("daliuge-translator/test/dropmake/logical_graphs"
                  "/ExampleSubgraphSimple.graph") as fp:
            lg = json.load(fp)
        previous_num_nodes = len(lg[NODES])
        previous_num_links = len(lg[LINKS])
        sg_node = self.getSubgraphNode(lg)
        self.assertFalse('hasInputApp' in sg_node)
        convert_subgraphs(lg)
        convert_subgraphs(lg)
        self.assertTrue('hasInputApp' in sg_node)
        self.assertTrue(sg_node['hasInputApp'])
        self.assertNotEqual(previous_num_nodes, len(lg[NODES]))
        self.assertNotEqual(previous_num_links, len(lg[LINKS]))
        # TODO LIU-385: Add more specific test cases here so we are future proofing the
        #  test cases properly.

class TestConvertScatterGatherConstruct(unittest.TestCase):
    def test_convert_construct(self):
        pass
