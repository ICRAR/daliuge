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

from dlg.drop import InMemoryDROP
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
        with pkg_resources.resource_stream("test", ddGraph) as f:  # @UndefinedVariable
            logger.debug(f"Loading graph: {f}")
            graphSpec = json.load(f)
        self.createSessionAndAddGraph(sessionId, graphSpec=graphSpec)

        # Deploy now and get OIDs
        bs = graphSpec[0]["applicationArgs"]["bs"]["value"]
        count = graphSpec[0]["applicationArgs"]["count"]["value"]
        self.dim.deploySession(sessionId)
        a, c = [
            self.dm._sessions[sessionId].drops[x]
            for x in ("2022-02-11T08:05:47_-5_0", "2022-02-11T08:05:47_-3_0")
        ]

        data = os.urandom(bs * count)
        logger.debug(f"Length of data produced: {len(data)}")
        with droputils.DROPWaiterCtx(self, c, 3):
            a.write(data)
            a.setCompleted()

        self.assertEqual(data, droputils.allDropContents(c))

    def test_namedPorts(self):
        """
        Use a graph with named ports and check whether it is runnning
        """
        init_oid = "2022-03-20T04:33:27_-2_0"  # first drop in graph
        sessionId = "lalo"
        with pkg_resources.resource_stream(
            "test", "graphs/funcTestPG_namedPorts.graph"
        ) as f:  # @UndefinedVariable
            graphSpec = json.load(f)
        # dropSpecs = graph_loader.loadDropSpecs(graphSpec)
        self.createSessionAndAddGraph(sessionId, graphSpec=graphSpec)

        # Deploy now and get OIDs
        self.dim.deploySession(sessionId)
        fd = self.dm._sessions[sessionId].drops["2022-03-20T04:33:27_-1_0"]
        init_drop = self.dm._sessions[sessionId].drops[init_oid]
        a = InMemoryDROP("a", "a")
        init_drop.addInput(a)
        logger.debug(f"PyfuncAPPDrop: {dir(fd)}")
        for i in fd.parameters["inputs"]:
            logger.debug(f"PyfuncAPPDrop input names:{i}")

        with droputils.DROPWaiterCtx(self, init_drop, 3):
            a.setCompleted()

    def test_namedPorts_with_kwonlyargs(self):
        """
        Use a graph with named ports and check whether it is runnning
        """
        init_oids = [
            "2022-03-30T03:46:01_-2_0",
            "2022-03-30T03:46:01_-6_0",
        ]  # first drops in graph
        sessionId = "lalo"
        with pkg_resources.resource_stream(
            "test", "graphs/pyfunc_glob_testPG.graph"
        ) as f:  # @UndefinedVariable
            graphSpec = json.load(f)
        # dropSpecs = graph_loader.loadDropSpecs(graphSpec)
        self.createSessionAndAddGraph(sessionId, graphSpec=graphSpec)

        # Deploy now and get OIDs
        self.dim.deploySession(sessionId)
        fd = self.dm._sessions[sessionId].drops["2022-03-30T03:46:01_-1_0"]
        i = 0
        start_drops = [InMemoryDROP(x, x) for x in ("a", "b")]
        for oid in init_oids:
            init_drop = self.dm._sessions[sessionId].drops[oid]
            init_drop.addInput(start_drops[i])
            i += 1
        logger.debug(f"PyfuncAPPDrop: {dir(fd)}")
        for i in fd.parameters["inputs"]:
            logger.debug(f"PyfuncAPPDrop input names:{i}")

        with droputils.DROPWaiterCtx(self, init_drop, 3):
            [a.setCompleted() for a in start_drops]

    def test_pos_only_args(self):
        """
        Use a graph with compile function to test positional only arguments
        """
        sessionId = "lalo"
        with pkg_resources.resource_stream(
                "test", "graphs/compilePG.graph"
            ) as f:  # @UndefinedVariable
            graphSpec = json.load(f)
        # dropSpecs = graph_loader.loadDropSpecs(graphSpec)
        self.createSessionAndAddGraph(sessionId, graphSpec=graphSpec)

        # Deploy now and get OIDs
        self.dim.deploySession(sessionId)
        sd = self.dm._sessions[sessionId].drops["2022-05-06T08:43:26_-2_0"]
        fd = self.dm._sessions[sessionId].drops["2022-05-06T08:43:26_-1_0"]
        with droputils.DROPWaiterCtx(self, fd, 3):
            sd.setCompleted()

        #logger.debug(f'PyfuncAPPDrop signature: {dir(fd)}')
        logger.debug(f'PyfuncAPPDrop status: {fd.status}')
        self.assertEqual(2, fd.status)

    def test_ArrayLoop(self):
        """
        Use a graph with compile function to test positional only arguments
        """
        sessionId = "lalo"
        start_drop = InMemoryDROP('a', 'a')
        with pkg_resources.resource_stream(
                "test", "graphs/ArrayLoopPG.graph"
            ) as f:  # @UndefinedVariable
            graphSpec = json.load(f)
        # dropSpecs = graph_loader.loadDropSpecs(graphSpec)
        self.createSessionAndAddGraph(sessionId, graphSpec=graphSpec)

        # Deploy now and get OIDs
        self.dim.deploySession(sessionId)
        sd = self.dm._sessions[sessionId].drops["2022-06-22T09:13:53_-1_0"]
        sd.addInput(start_drop)
        fd = self.dm._sessions[sessionId].drops["2022-06-22T09:13:53_-4_0/0/0"]
        with droputils.DROPWaiterCtx(self, fd, 3):
            start_drop.setCompleted()

        #logger.debug(f'PyfuncAPPDrop signature: {dir(fd)}')
        logger.debug(f'PyfuncAPPDrop status: {fd.status}')
        self.assertEqual(2, fd.status)
