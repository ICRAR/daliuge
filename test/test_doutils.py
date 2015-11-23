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

from dfms import doutils
from dfms.data_object import InMemoryDataObject, FileDataObject, \
    BarrierAppDataObject
from dfms.doutils import DOFile


class DOUtilsTest(unittest.TestCase):

    def _createGraph(self):
        """
        Creates the following graph of DataObjects:

        A |--> B ----> D --> G --> I --|
          |--> C -|--> E --------------|-> H --> J
                  |--> F

        B, C, G and H are AppDOs. The names have been given in breadth-first
        order (although H has a dependency on I)
        """
        a =          InMemoryDataObject('a', 'a')
        b =        BarrierAppDataObject('b', 'b')
        c =        BarrierAppDataObject('c', 'c')
        d =          InMemoryDataObject('d', 'd')
        e =          InMemoryDataObject('e', 'e')
        f =          InMemoryDataObject('f', 'f')
        g =        BarrierAppDataObject('g', 'g')
        h =        BarrierAppDataObject('h', 'h')
        i =          InMemoryDataObject('i', 'i')
        j =          InMemoryDataObject('j', 'j')

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
        self.assertSetEqual(set(downstreamNodes), set(doutils.getDownstreamObjects(node)))
        # Check the other way too
        for downNode in downstreamNodes:
            self.assertTrue(node in doutils.getUpstreamObjects(downNode))

    def assertUpstream(self, node, upstreamNodes):
        if not isinstance(upstreamNodes, list):
            upstreamNodes = [upstreamNodes]

        # Normal check
        self.assertSetEqual(set(upstreamNodes), set(doutils.getUpstreamObjects(node)))
        # Check the other way too
        for upNode in upstreamNodes:
            self.assertTrue(node in doutils.getDownstreamObjects(upNode))

    def testDepthFirstSearch(self):
        """
        Checks that our DFS method is correct
        """
        a, b, c, d, e, f, g, h, i, j = self._createGraph()
        nodesList = []
        doutils.depthFirstTraverse(a, lambda n: nodesList.append(n))

        self.assertListEqual(nodesList, [a, b, d, g, i, h, j, c, e, f])
        pass

    def testBreadthFirstSearch(self):
        """
        Checks that our BFS method is correct
        """
        a, b, c, d, e, f, g, h, i, j = self._createGraph()
        nodesList = []
        doutils.breadFirstTraverse(a, lambda n: nodesList.append(n))

        self.assertListEqual(nodesList, [a, b, c, d, e, f, g, h, i, j])
        pass

    def testGetEndNodes(self):
        """
        Checks that the getLeafNodes works correctly
        """
        a, _, _, _, _, f, _, _, _, j = self._createGraph()
        endNodes = doutils.getLeafNodes(a)
        self.assertSetEqual(set([j, f]), set(endNodes))
        pass

    def test_DOFile(self):
        """
        This test exercises the DOFile mechanism to read the data represented by
        a given DROP. The DOFile class will decide whether the data should be read
        directly or through the DROP
        """
        do = FileDataObject('a', 'a', expectedSize=5)
        do.write('abcde')
        with DOFile(do) as f:
            self.assertEquals('abcde', f.read())
            self.assertTrue(do.isBeingRead())
            self.assertIsNotNone(f._io)
        self.assertFalse(do.isBeingRead())

    def test_BFSWithFiltering(self):
        """
        Checks that the BFS works if the given function does filtering on the
        downstream DROPs.
        """
        a, _, c, _, e, _, _, h, _, j = self._createGraph()

        visitedNodes = []
        def filtering(do, downStreamDOs):
            downStreamDOs[:] = [x for x in downStreamDOs if x.uid not in ('b','f')]
            visitedNodes.append(do)
        doutils.breadFirstTraverse(a, filtering)

        self.assertEquals(5, len(visitedNodes))
        self.assertListEqual(visitedNodes, [a,c,e,h,j])