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
import unittest
import json
import pkg_resources

from dlg import graph_loader
from dlg.common import Categories
from dlg.ddap_protocol import DROPLinkType, DROPRel
from dlg.drop import (
    InMemoryDROP,
    SharedMemoryDROP,
    ContainerDROP,
    AppDROP,
    DirectoryContainer,
)
from dlg.common import Categories
from dlg.apps.simple import RandomArrayApp


# Used in the textual representation of the graphs in these tests
class DummyApp(AppDROP):
    pass


class TestGraphLoader(unittest.TestCase):
    def test_singleMemoryDrop(self):
        dropSpecList = [{"oid": "A", "type": "plain", "storage": Categories.MEMORY}]
        a = graph_loader.createGraphFromDropSpecList(dropSpecList)[0]
        self.assertIsInstance(a, InMemoryDROP)
        self.assertEqual("A", a.oid)
        self.assertEqual("A", a.uid)

    def test_sharedMemoryDrop(self):
        dropSpecList = [{"oid": "A", "type": "plain", "storage": Categories.SHMEM}]
        a = graph_loader.createGraphFromDropSpecList(dropSpecList)[0]
        self.assertIsInstance(a, SharedMemoryDROP)
        self.assertEqual("A", a.oid)
        self.assertEqual("A", a.uid)

    def test_containerDrop(self):
        dropSpecList = [
            {"oid": "A", "type": "plain", "storage": Categories.MEMORY},
            {"oid": "B", "type": "container", "children": ["A"]},
        ]
        a = graph_loader.createGraphFromDropSpecList(dropSpecList)[0]
        self.assertIsInstance(a, InMemoryDROP)
        self.assertEqual("A", a.oid)
        self.assertEqual("A", a.uid)
        self.assertIsNotNone(a.parent)
        b = a.parent
        self.assertIsInstance(b, ContainerDROP)
        self.assertEqual("B", b.oid)
        self.assertEqual("B", b.uid)

        # A directory container
        dropSpecList = [
            {"oid": "A", "type": "plain", "storage": Categories.FILE, "dirname": "."},
            {
                "oid": "B",
                "type": "container",
                "container": "dlg.drop.DirectoryContainer",
                "children": ["A"],
                "dirname": ".",
            },
        ]
        a = graph_loader.createGraphFromDropSpecList(dropSpecList)[0]
        b = a.parent
        self.assertIsInstance(b, DirectoryContainer)

    def test_consumer(self):
        dropSpecList = [
            {
                "oid": "A",
                "type": "plain",
                "storage": Categories.MEMORY,
                "consumers": ["B"],
            },
            {"oid": "B", "type": "app", "app": "test.test_graph_loader.DummyApp"},
        ]
        a = graph_loader.createGraphFromDropSpecList(dropSpecList)[0]
        self.assertIsInstance(a, InMemoryDROP)
        self.assertEqual("A", a.oid)
        self.assertEqual("A", a.uid)
        self.assertEqual(1, len(a.consumers))
        b = a.consumers[0]
        self.assertIsInstance(b, DummyApp)
        self.assertEqual("B", b.oid)
        self.assertEqual("B", b.uid)
        self.assertEqual(a, b.inputs[0])

    def test_removeUnmetRelationships(self):
        # Unmet relationsips are
        # DROPRel(D, CONSUMER, A)
        # DROPRel(D, STREAMING_CONSUMER, C)
        # DROPRel(Z, PRODUCER, A)
        # DROPRel(X, PRODUCER, A)
        graphDesc = [
            {"oid": "A", "consumers": ["B", "D"], "producers": ["Z", "X"]},
            {"oid": "B", "outputs": ["C"]},
            {"oid": "C", "streamingConsumers": ["D"]},
        ]

        unmetRelationships = graph_loader.removeUnmetRelationships(graphDesc)
        self.assertEqual(4, len(unmetRelationships))
        self.assertIn(DROPRel("D", DROPLinkType.CONSUMER, "A"), unmetRelationships)
        self.assertIn(
            DROPRel("D", DROPLinkType.STREAMING_CONSUMER, "C"), unmetRelationships
        )
        self.assertIn(DROPRel("Z", DROPLinkType.PRODUCER, "A"), unmetRelationships)
        self.assertIn(DROPRel("X", DROPLinkType.PRODUCER, "A"), unmetRelationships)

        # The original dropSpecs have changed as well
        a = graphDesc[0]
        c = graphDesc[2]

        self.assertEqual(1, len(a["consumers"]))
        self.assertEqual("B", a["consumers"][0])
        self.assertFalse("producers" in a and len(a["producers"]) > 0)
        self.assertFalse("streamingConsumers" in c and len(c["streamingConsumers"]) > 0)

    def test_removeUnmetRelationships_named(self):

        with pkg_resources.resource_stream(
            "test", "graphs/HelloWorld_simplePG.graph"
        ) as f:  # @UndefinedVariable
            graphDesc = json.load(f)

        unmetRelationships = graph_loader.removeUnmetRelationships(graphDesc)
        self.assertEqual(0, len(unmetRelationships))

    def test_unNamedPorts(self):
        """
        Use a graph with un-named ports and check whether it is loading
        """
        with pkg_resources.resource_stream(
            "test", "graphs/funcTestPG.graph"
        ) as f:  # @UndefinedVariable
            graphSpec = json.load(f)
        # dropSpecs = graph_loader.loadDropSpecs(graphSpec)
        a = graph_loader.createGraphFromDropSpecList(graphSpec)
        dummy = a

    def test_namedPorts(self):
        """
        Use a graph with named ports and check whether it is loading
        """
        with pkg_resources.resource_stream(
            "test", "graphs/funcTestPG_namedPorts.graph"
        ) as f:  # @UndefinedVariable
            graphSpec = json.load(f)
        # dropSpecs = graph_loader.loadDropSpecs(graphSpec)
        a = graph_loader.createGraphFromDropSpecList(graphSpec)
        dummy = a

    def test_applicationArgs(self):
        """
        Use a graph with applicationArgs and make sure applications see their
        arguments
        """
        with pkg_resources.resource_stream(
            "test", "graphs/application_args.graph"
        ) as f:  # @UndefinedVariable
            graphSpec = json.load(f)
        graph = graph_loader.createGraphFromDropSpecList(graphSpec)
        app = graph[0]
        self.assertEqual(app.__class__, RandomArrayApp)
        self.assertEqual(app.size, 50)
        self.assertEqual(app.integer, True)
        self.assertEqual(app.low, 34)
        self.assertEqual(app.high, 3456)
