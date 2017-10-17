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

from dlg import graph_loader
from dlg.ddap_protocol import DROPLinkType, DROPRel
from dlg.drop import InMemoryDROP, ContainerDROP, \
    AppDROP, DirectoryContainer


# Used in the textual representation of the graphs in these tests
class DummyApp(AppDROP): pass

class TestGraphLoader(unittest.TestCase):

    def test_singleMemoryDrop(self):
        dropSpecList = [{"oid":"A", "type":"plain", "storage":"memory"}]
        a = graph_loader.createGraphFromDropSpecList(dropSpecList)[0]
        self.assertIsInstance(a, InMemoryDROP)
        self.assertEqual("A", a.oid)
        self.assertEqual("A", a.uid)

    def test_containerDrop(self):
        dropSpecList = [{"oid":"A", "type":"plain", "storage":"memory"},
                        {"oid":"B", "type":"container", "children":["A"]}]
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
        dropSpecList = [{"oid":"A", "type":"plain", "storage":"file", "dirname":"."},
                        {"oid":"B", "type":"container", "container":"dlg.drop.DirectoryContainer", "children":["A"], "dirname":"."}]
        a = graph_loader.createGraphFromDropSpecList(dropSpecList)[0]
        b = a.parent
        self.assertIsInstance(b, DirectoryContainer)

    def test_consumer(self):
        dropSpecList = [{"oid":"A", "type":"plain", "storage":"memory", "consumers":["B"]},
                        {"oid":"B", "type":"app", "app":"test.test_graph_loader.DummyApp"}]
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
        graphDesc = [{'oid':'A', 'consumers':['B', 'D'], 'producers':['Z','X']},
                     {'oid':'B', 'outputs':['C']},
                     {'oid':'C', 'streamingConsumers':['D']}]

        unmetRelationships = graph_loader.removeUnmetRelationships(graphDesc)
        self.assertEqual(4, len(unmetRelationships))
        self.assertIn(DROPRel('D', DROPLinkType.CONSUMER, 'A'), unmetRelationships)
        self.assertIn(DROPRel('D', DROPLinkType.STREAMING_CONSUMER, 'C'), unmetRelationships)
        self.assertIn(DROPRel('Z', DROPLinkType.PRODUCER, 'A'), unmetRelationships)
        self.assertIn(DROPRel('X', DROPLinkType.PRODUCER, 'A'), unmetRelationships)

        # The original dropSpecs have changed as well
        a = graphDesc[0]
        c = graphDesc[2]

        self.assertEqual(1, len(a['consumers']))
        self.assertEqual('B', a['consumers'][0])
        self.assertFalse('producers' in a)
        self.assertFalse('streamingConsumers' in c)