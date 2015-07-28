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
from dfms.data_object import InMemoryDataObject, ContainerDataObject,\
    FileDataObject, AppConsumer

# Used in the textual representation of the graphs in these tests
class DummyApp(AppConsumer):
    def run(self, dataObject): pass

class TestGraphLoader(unittest.TestCase):

    def test_singleMemoryDO(self):
        f = StringIO('[{"oid":"A", "type":"plain", "storage":"memory"}]')
        a = graph_loader.readObjectGraph(f)[0]
        self.assertIsInstance(a, InMemoryDataObject)
        self.assertEquals("A", a.oid)
        self.assertEquals("A", a.uid)

    def test_containerDO(self):
        f = StringIO('[{"oid":"A", "type":"plain", "storage":"memory"}, \
                       {"oid":"B", "type":"container", "children":["A"]}]')
        a = graph_loader.readObjectGraph(f)[0]
        self.assertIsInstance(a, InMemoryDataObject)
        self.assertEquals("A", a.oid)
        self.assertEquals("A", a.uid)
        self.assertIsNotNone(a.parent)
        b = a.parent
        self.assertIsInstance(b, ContainerDataObject)
        self.assertEquals("B", b.oid)
        self.assertEquals("B", b.uid)

    def test_consumer(self):
        f = StringIO('[{"oid":"A", "type":"plain", "storage":"memory", "consumers":["B"]}, \
                       {"oid":"B", "type":"app", "app":"test.test_graph_loader.DummyApp", "storage":"file"}]')
        a = graph_loader.readObjectGraph(f)[0]
        self.assertIsInstance(a, InMemoryDataObject)
        self.assertEquals("A", a.oid)
        self.assertEquals("A", a.uid)
        self.assertEquals(1, len(a.consumers))
        b = a.consumers[0]
        self.assertIsInstance(b, FileDataObject)
        self.assertIsInstance(b, DummyApp)
        self.assertEquals("B", b.oid)
        self.assertEquals("B", b.uid)
        self.assertEquals(a, b.producer)