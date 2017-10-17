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
'''
Created on 20 Jul 2015

@author: rtobar
'''

import unittest

import six

from dlg import droputils
from dlg.drop import InMemoryDROP, FileDROP, \
    BarrierAppDROP, dropdict
from dlg.droputils import DROPFile


class DropUtilsTest(unittest.TestCase):

    def _createGraph(self):
        """
        Creates the following graph of DROPs:

        A |--> B ----> D --> G --> I --|
          |--> C -|--> E --------------|-> H --> J
                  |--> F

        B, C, G and H are AppDOs. The names have been given in breadth-first
        order (although H has a dependency on I)
        """
        a =          InMemoryDROP('a', 'a')
        b =        BarrierAppDROP('b', 'b')
        c =        BarrierAppDROP('c', 'c')
        d =          InMemoryDROP('d', 'd')
        e =          InMemoryDROP('e', 'e')
        f =          InMemoryDROP('f', 'f')
        g =        BarrierAppDROP('g', 'g')
        h =        BarrierAppDROP('h', 'h')
        i =          InMemoryDROP('i', 'i')
        j =          InMemoryDROP('j', 'j')

        a.addConsumer(b)
        a.addConsumer(c)
        b.addOutput(d)
        c.addOutput(e)
        c.addOutput(f)
        d.addConsumer(g)
        e.addConsumer(h)
        g.addOutput(i)
        i.addConsumer(h)
        h.addOutput(j)

        return a, b, c, d, e, f, g, h, i, j

    def testDownstreamObjects(self):
        a, b, c, d, e, f, g, h, i, j = self._createGraph()
        self.assertDownstream(a, [b, c])
        self.assertDownstream(b, d)
        self.assertDownstream(c, [e, f])
        self.assertDownstream(d, g)
        self.assertDownstream(e, h)
        self.assertDownstream(f, [])
        self.assertDownstream(g, i)
        self.assertDownstream(h, j)
        self.assertDownstream(i, h)
        self.assertDownstream(j, [])

    def testUpstreamObjects(self):
        a, b, c, d, e, f, g, h, i, j = self._createGraph()
        self.assertUpstream(a, [])
        self.assertUpstream(b, a)
        self.assertUpstream(c, a)
        self.assertUpstream(e, c)
        self.assertUpstream(d, b)
        self.assertUpstream(f, c)
        self.assertUpstream(g, d)
        self.assertUpstream(h, [e, i])
        self.assertUpstream(i, g)
        self.assertUpstream(j, h)

    def assertDownstream(self, node, downstreamNodes):
        if not isinstance(downstreamNodes, list):
            downstreamNodes = [downstreamNodes]

        # Normal check
        self.assertSetEqual(set(downstreamNodes), set(droputils.getDownstreamObjects(node)))
        # Check the other way too
        for downNode in downstreamNodes:
            self.assertTrue(node in droputils.getUpstreamObjects(downNode))

    def assertUpstream(self, node, upstreamNodes):
        if not isinstance(upstreamNodes, list):
            upstreamNodes = [upstreamNodes]

        # Normal check
        self.assertSetEqual(set(upstreamNodes), set(droputils.getUpstreamObjects(node)))
        # Check the other way too
        for upNode in upstreamNodes:
            self.assertTrue(node in droputils.getDownstreamObjects(upNode))

    def testDepthFirstSearch(self):
        """
        Checks that our DFS method is correct
        """
        a, b, c, d, e, f, g, h, i, j = self._createGraph()
        nodesList = [drop for drop,_ in droputils.depthFirstTraverse(a)]
        self.assertListEqual([a, b, d, g, i, h, j, c, e, f], nodesList)

    def testBreadthFirstSearch(self):
        """
        Checks that our BFS method is correct
        """
        a, b, c, d, e, f, g, h, i, j = self._createGraph()
        nodesList = [drop for drop,_ in droputils.breadFirstTraverse(a)]
        self.assertListEqual([a, b, c, d, e, f, g, h, i, j], nodesList)

    def testGetEndNodes(self):
        """
        Checks that the getLeafNodes works correctly
        """
        a, _, _, _, _, f, _, _, _, j = self._createGraph()
        endNodes = droputils.getLeafNodes(a)
        self.assertSetEqual(set([j, f]), set(endNodes))

    def test_DROPFile(self):
        """
        This test exercises the DROPFile mechanism to read the data represented by
        a given DROP. The DROPFile class will decide whether the data should be read
        directly or through the DROP
        """
        drop = FileDROP('a', 'a', expectedSize=5)
        drop.write(b'abcde')
        with DROPFile(drop) as f:
            self.assertEqual(b'abcde', f.read())
            self.assertTrue(drop.isBeingRead())
            self.assertIsNotNone(f._io)
        self.assertFalse(drop.isBeingRead())

    def test_BFSWithFiltering(self):
        """
        Checks that the BFS works if the given function does filtering on the
        downstream DROPs.
        """
        a, _, c, _, e, _, _, h, _, j = self._createGraph()

        visitedNodes = []
        for drop, downStreamDrops in droputils.breadFirstTraverse(a):
            downStreamDrops[:] = [x for x in downStreamDrops if x.uid not in ('b','f')]
            visitedNodes.append(drop)

        self.assertEqual(5, len(visitedNodes))
        self.assertListEqual(visitedNodes, [a,c,e,h,j])

    def test_get_roots(self):
        """
        Check that the get_roots method from the droputils module works as intended
        """

        """
        A --> B
        """
        pg_spec = [{"oid":"A", "type":"plain", "storage":"memory", "consumers":["B"]},
                   {"oid":"B", "type":"app", "app":"test.test_graph_loader.DummyApp"}]
        roots = droputils.get_roots(pg_spec)
        self.assertEqual(1, len(roots))
        self.assertEqual('A', next(iter(roots)))

        """
        A --> B
        The same, but now B references A
        """
        pg_spec = [{"oid":"A", "type":"plain", "storage":"memory"},
                   {"oid":"B", "type":"app", "app":"test.test_graph_loader.DummyApp", "inputs": ["A"]}]
        roots = droputils.get_roots(pg_spec)
        self.assertEqual(1, len(roots))
        self.assertEqual('A', next(iter(roots)))

        """
        A --> C --> D --|
                        |--> E --> F
        B --------------|
        """
        pg_spec = [{"oid":"A", "type":"plain", "storage": "memory"},
                   {"oid":"B", "type":"plain", "storage": "memory"},
                   {"oid":"C", "type":"app", "app":"dlg.apps.crc.CRCApp", "inputs": ['A']},
                   {"oid":"D", "type":"plain", "storage": "memory", "producers": ["C"]},
                   {"oid":"E", "type":"app", "app":"test.test_drop.SumupContainerChecksum", "inputs": ["D"]},
                   {"oid":"F", "type":"plain", "storage": "memory", "producers":["E"]}]
        roots = droputils.get_roots(pg_spec)
        self.assertEqual(2, len(roots))
        self.assertListEqual(['A', 'B'], sorted(roots))

        # The same as before but using dropdicts
        pg_spec_dropdicts = [dropdict(dropspec) for dropspec in pg_spec]
        roots = droputils.get_roots(pg_spec_dropdicts)
        self.assertEqual(2, len(roots))
        self.assertListEqual(['A', 'B'], sorted(roots))