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
from dfms import doutils
from dfms.data_object import InMemoryDataObject, ContainerDataObject,\
    InMemoryCRCResultDataObject
from dfms.events.event_broadcaster import LocalEventBroadcaster

'''
Created on 20 Jul 2015

@author: rtobar
'''

class DOUtilsTest(unittest.TestCase):

    def _createGraph(self):
        """
        Creates the following graph of DataObjects:

        A |--> B ----> D --|
          |--> C -|--> E --|-> G
                  |--> F
        """
        eb = LocalEventBroadcaster()
        a =          InMemoryDataObject('a', 'a', eb)
        b = InMemoryCRCResultDataObject('b', 'b', eb)
        c = InMemoryCRCResultDataObject('c', 'c', eb)
        d = InMemoryCRCResultDataObject('d', 'd', eb)
        e = InMemoryCRCResultDataObject('e', 'e', eb)
        f = InMemoryCRCResultDataObject('f', 'f', eb)
        g =         ContainerDataObject('g', 'g', eb)

        a.addConsumer(b)
        a.addConsumer(c)
        b.addConsumer(d)
        c.addConsumer(e)
        c.addConsumer(f)
        g.addChild(d)
        g.addChild(e)
        return a, b, c, d, e, f, g

    def testDownstreamObjects(self):
        a, b, c, d, e, f, g = self._createGraph()
        self.assertDownstream(a, [b, c])
        self.assertDownstream(b, d)
        self.assertDownstream(c, [e, f])
        self.assertDownstream(d, g)
        self.assertDownstream(e, g)
        self.assertDownstream(f, [])
        self.assertDownstream(g, [])

    def testUpstreamObjects(self):
        a, b, c, d, e, f, g = self._createGraph()
        self.assertUpstream(a, [])
        self.assertUpstream(b, a)
        self.assertUpstream(c, a)
        self.assertUpstream(e, c)
        self.assertUpstream(d, b)
        self.assertUpstream(f, c)
        self.assertUpstream(g, [e, d])

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
        a, b, c, d, e, f, g = self._createGraph()
        nodesList = []
        doutils.depthFirstTraverse(a, lambda n: nodesList.append(n))

        self.assertListEqual(nodesList, [a, b, d, g, c, e, f])
        pass

    def testBreadthFirstSearch(self):
        """
        Checks that our BFS method is correct
        """
        a, b, c, d, e, f, g = self._createGraph()
        nodesList = []
        doutils.breadFirstTraverse(a, lambda n: nodesList.append(n))

        self.assertListEqual(nodesList, [a, b, c, d, e, f, g])
        pass

    def testGetEndNodes(self):
        """
        Checks that the getEndNodes works correctly
        """
        a, _, _, _, _, f, g = self._createGraph()
        endNodes = doutils.getEndNodes(a)
        self.assertSetEqual(set([f, g]), set(endNodes))
        pass

if __name__ == "__main__":
    unittest.main()