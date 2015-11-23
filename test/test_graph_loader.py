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
from cStringIO import StringIO
import unittest

from dfms import graph_loader
from dfms.drop import InMemoryDROP, ContainerDROP,\
    AppDROP, DirectoryContainer
from dfms.ddap_protocol import DROPLinkType, DROPRel

# Used in the textual representation of the graphs in these tests
class DummyApp(AppDROP): pass

class TestGraphLoader(unittest.TestCase):

    def test_singleMemoryDO(self):
        f = StringIO('[{"oid":"A", "type":"plain", "storage":"memory"}]')
        a = graph_loader.readObjectGraph(f)[0]
        self.assertIsInstance(a, InMemoryDROP)
        self.assertEquals("A", a.oid)
        self.assertEquals("A", a.uid)

    def test_containerDO(self):
        f = StringIO('[{"oid":"A", "type":"plain", "storage":"memory"}, \
                       {"oid":"B", "type":"container", "children":["A"]}]')
        a = graph_loader.readObjectGraph(f)[0]
        self.assertIsInstance(a, InMemoryDROP)
        self.assertEquals("A", a.oid)
        self.assertEquals("A", a.uid)
        self.assertIsNotNone(a.parent)
        b = a.parent
        self.assertIsInstance(b, ContainerDROP)
        self.assertEquals("B", b.oid)
        self.assertEquals("B", b.uid)

        # A directory container
        f = StringIO('[{"oid":"A", "type":"plain", "storage":"file", "dirname":"."}, {"oid":"B", "type":"container", "container":"dfms.drop.DirectoryContainer", "children":["A"], "dirname":"."}]')
        a = graph_loader.readObjectGraph(f)[0]
        b = a.parent
        self.assertIsInstance(b, DirectoryContainer)

    def test_consumer(self):
        f = StringIO('[{"oid":"A", "type":"plain", "storage":"memory", "consumers":["B"]}, \
                       {"oid":"B", "type":"app", "app":"test.test_graph_loader.DummyApp"}]')
        a = graph_loader.readObjectGraph(f)[0]
        self.assertIsInstance(a, InMemoryDROP)
        self.assertEquals("A", a.oid)
        self.assertEquals("A", a.uid)
        self.assertEquals(1, len(a.consumers))
        b = a.consumers[0]
        self.assertIsInstance(b, DummyApp)
        self.assertEquals("B", b.oid)
        self.assertEquals("B", b.uid)
        self.assertEquals(a, b.inputs[0])

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
        self.assertEquals(4, len(unmetRelationships))
        self.assertIn(DROPRel('D', DROPLinkType.CONSUMER, 'A'), unmetRelationships)
        self.assertIn(DROPRel('D', DROPLinkType.STREAMING_CONSUMER, 'C'), unmetRelationships)
        self.assertIn(DROPRel('Z', DROPLinkType.PRODUCER, 'A'), unmetRelationships)
        self.assertIn(DROPRel('X', DROPLinkType.PRODUCER, 'A'), unmetRelationships)

        # The original doSpecs have changed as well
        a = graphDesc[0]
        c = graphDesc[2]

        self.assertEquals(1, len(a['consumers']))
        self.assertEquals('B', a['consumers'][0])
        self.assertFalse('producers' in a)
        self.assertFalse('streamingConsumers' in c)