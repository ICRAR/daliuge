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
from cmath import log
import json
import os
import unittest

from asyncio.log import logger
import pkg_resources

from dlg import runtime
from dlg import droputils
from dlg import utils
from dlg.ddap_protocol import DROPStates
from dlg.manager.composite_manager import DataIslandManager
from dlg.testutils import ManagerStarter

hostname = "localhost"


class LocalDimStarter(ManagerStarter):
    def setUp(self):
        super(LocalDimStarter, self).setUp()
        self.nm_info = self.start_nm_in_thread()
        self.dm = self.nm_info.manager
        self.dim = DataIslandManager([hostname])

    def tearDown(self):
        self.nm_info.stop()
        self.dim.shutdown()
        super(LocalDimStarter, self).tearDown()


class TestGraphs(LocalDimStarter, unittest.TestCase):
    """
    Class to test the execution of actual physical graphs, 
    rather than python constructions. Add additional graphs
    and associated tests as required.
    """

    def createSessionAndAddGraph(self, sessionId, graphSpec="", sleepTime=0):
        self.dim.createSession(sessionId)
        self.assertEqual(0, self.dim.getGraphSize(sessionId))
        self.dim.addGraphSpec(sessionId, graphSpec)
        self.assertEqual(len(graphSpec), self.dim.getGraphSize(sessionId))

    def test_ddGraph(self):
        """
        Graph is using dd to read a file and write to another. This is mainly
        to test that the separatorString parameter is working correctly.
        """
        sessionId = "lalo"
        ddGraph = "graphs/ddTest.graph"
        with pkg_resources.resource_stream(
            "test", ddGraph) as f:  # @UndefinedVariable
            logger.debug(f'Loading graph: {f}')
            graphSpec = json.load(f)
        self.createSessionAndAddGraph(sessionId, graphSpec=graphSpec)

        # Deploy now and get OIDs
        bs = graphSpec[0]["applicationArgs"]["bs"]["value"]
        count = graphSpec[0]["applicationArgs"]["count"]["value"]
        self.dim.deploySession(sessionId)
        a, c = [self.dm._sessions[sessionId].drops[x] for x in ("2022-02-11T08:05:47_-5_0", "2022-02-11T08:05:47_-3_0")]

        data = os.urandom(bs*count)
        logger.debug(f"Length of data produced: {len(data)}")
        with droputils.DROPWaiterCtx(self, c, 3):
            a.write(data)
            a.setCompleted()

        self.assertEqual(data, droputils.allDropContents(c))
