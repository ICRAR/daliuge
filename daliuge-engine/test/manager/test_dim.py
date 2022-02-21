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
from asyncio.log import logger
import codecs
import json
import os
import time
import unittest

import pkg_resources

from dlg import runtime
from dlg import droputils
from dlg import utils
from dlg.common import tool, Categories
from dlg.ddap_protocol import DROPStates
from dlg.manager.composite_manager import DataIslandManager
from dlg.manager.session import SessionStates
from dlg.testutils import ManagerStarter
from test.manager import testutils


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


class TestDIM(LocalDimStarter, unittest.TestCase):
    def createSessionAndAddTypicalGraph(self, sessionId, sleepTime=0):
        graphSpec = [
            {
                "oid": "A",
                "type": "plain",
                "storage": Categories.MEMORY,
                "node": hostname,
                "consumers": ["B"],
            },
            {
                "oid": "B",
                "type": "app",
                "app": "dlg.apps.simple.SleepAndCopyApp",
                "sleepTime": sleepTime,
                "outputs": ["C"],
                "node": hostname,
            },
            {
                "oid": "C",
                "type": "plain",
                "storage": Categories.MEMORY,
                "node": hostname,
            },
        ]
        self.dim.createSession(sessionId)
        self.assertEqual(0, self.dim.getGraphSize(sessionId))
        self.dim.addGraphSpec(sessionId, graphSpec)
        self.assertEqual(len(graphSpec), self.dim.getGraphSize(sessionId))

    def test_createSession(self):
        sessionId = "lalo"
        self.dim.createSession(sessionId)
        self.assertEqual(1, len(self.dm.getSessionIds()))
        self.assertEqual(sessionId, self.dm.getSessionIds()[0])

    def test_addGraphSpec(self):

        sessionId = "lalo"

        # No node specified
        graphSpec = [{"oid": "A", "type": "plain", "storage": Categories.MEMORY}]
        self.assertRaises(Exception, self.dim.addGraphSpec, sessionId, graphSpec)

        # Wrong node specified
        graphSpec = [
            {
                "oid": "A",
                "type": "plain",
                "storage": Categories.MEMORY,
                "node": "unknown_host",
            }
        ]
        self.assertRaises(Exception, self.dim.addGraphSpec, sessionId, graphSpec)

        # OK
        graphSpec = [
            {
                "oid": "A",
                "type": "plain",
                "storage": Categories.MEMORY,
                "node": hostname,
            }
        ]
        self.dim.createSession(sessionId)
        self.assertEqual(0, self.dim.getGraphSize(sessionId))
        self.dim.addGraphSpec(sessionId, graphSpec)
        self.assertEqual(1, self.dim.getGraphSize(sessionId))
        graphFromDM = self.dm.getGraph(sessionId)
        self.assertEqual(1, len(graphFromDM))
        dropSpec = list(graphFromDM.values())[0]
        self.assertEqual("A", dropSpec["oid"])
        self.assertEqual("plain", dropSpec["type"])
        self.assertEqual("Memory", dropSpec["storage"])

    def test_deployGraph(self):

        sessionId = "lalo"
        self.createSessionAndAddTypicalGraph(sessionId)

        # Deploy now and get A and C
        self.dim.deploySession(sessionId)
        a, c = [self.dm._sessions[sessionId].drops[x] for x in ("A", "C")]

        data = os.urandom(10)
        with droputils.DROPWaiterCtx(self, c, 3):
            a.write(data)
            a.setCompleted()

        self.assertEqual(data, droputils.allDropContents(c))

    def test_deployGraphWithCompletedDOs(self):
        self._test_deployGraphWithCompletedDOs("lalo")

    def test_deployGraphWithCompletedDOs_sessionIdWithSpaces(self):
        self._test_deployGraphWithCompletedDOs("lala with spaces")

    def _test_deployGraphWithCompletedDOs(self, sessionId):

        self.createSessionAndAddTypicalGraph(sessionId, sleepTime=1)

        # Deploy now and get C
        self.dim.deploySession(sessionId, completedDrops=["A"])
        c = self.dm._sessions[sessionId].drops["C"]

        # This should be happening before the sleepTime expires
        with droputils.DROPWaiterCtx(self, c, 2):
            pass

        self.assertEqual(DROPStates.COMPLETED, c.status)

    def test_sessionStatus(self):
        def assertSessionStatus(sessionId, status):
            sessionStatus = self.dim.getSessionStatus(sessionId)
            self.assertEqual(1, len(sessionStatus))
            self.assertIn(hostname, sessionStatus)
            self.assertEqual(status, sessionStatus[hostname])
            self.assertEqual(status, self.dm.getSessionStatus(sessionId))

        sessionId = "lala"
        self.dim.createSession(sessionId)
        assertSessionStatus(sessionId, SessionStates.PRISTINE)

        sessionId = "lalo"
        self.createSessionAndAddTypicalGraph(sessionId)
        assertSessionStatus(sessionId, SessionStates.BUILDING)

        self.dm.deploySession(sessionId)
        assertSessionStatus(sessionId, SessionStates.RUNNING)

        a, c = [self.dm._sessions[sessionId].drops[x] for x in ("A", "C")]
        data = os.urandom(10)
        with droputils.DROPWaiterCtx(self, c, 3):
            a.write(data)
            a.setCompleted()

        assertSessionStatus(sessionId, SessionStates.FINISHED)

    def test_getGraph(self):

        sessionId = "lalo"
        self.createSessionAndAddTypicalGraph(sessionId)

        graphSpecFromDim = self.dim.getGraph(sessionId)
        self.assertEqual(3, len(graphSpecFromDim))
        for oid in ("A", "B", "C"):
            self.assertIn(oid, graphSpecFromDim)
        graphSepcFromDM = self.dm.getGraph(sessionId)
        self.assertDictEqual(graphSepcFromDM, graphSpecFromDim)

    def test_getGraphStatus(self):
        def assertGraphStatus(sessionId, expectedStatus):
            graphStatusByDim = self.dim.getGraphStatus(sessionId)
            graphStatusByDM = self.dm.getGraphStatus(sessionId)
            self.assertDictEqual(graphStatusByDim, graphStatusByDM)
            for dropStatus in graphStatusByDim.values():
                self.assertEqual(expectedStatus, dropStatus["status"])

        sessionId = "lala"
        self.createSessionAndAddTypicalGraph(sessionId)
        self.dim.deploySession(sessionId)
        assertGraphStatus(sessionId, DROPStates.INITIALIZED)

        a, c = [self.dm._sessions[sessionId].drops[x] for x in ("A", "C")]
        data = os.urandom(10)
        with droputils.DROPWaiterCtx(self, c, 3):
            a.write(data)
            a.setCompleted()
        assertGraphStatus(sessionId, DROPStates.COMPLETED)

    def test_doCancel(self):
        def assertGraphStatus(sessionId, expectedStatus):
            graphStatusByDim = self.dim.getGraphStatus(sessionId)
            graphStatusByDM = self.dm.getGraphStatus(sessionId)
            self.assertDictEqual(graphStatusByDim, graphStatusByDM)
            for dropStatus in graphStatusByDim.values():
                self.assertEqual(expectedStatus, dropStatus["status"])

        sessionId = "lala"
        self.createSessionAndAddTypicalGraph(sessionId, 10)
        self.dim.deploySession(sessionId)
        assertGraphStatus(sessionId, DROPStates.INITIALIZED)

        self.dim.cancelSession(sessionId)

        # a, c = [self.dm._sessions[sessionId].drops[x] for x in ('A', 'C')]
        # data = os.urandom(10)
        # with droputils.DROPWaiterCtx(self, c, 3):
        #    a.write(data)
        #    a.setCompleted()
        assertGraphStatus(sessionId, DROPStates.CANCELLED)


class TestREST(LocalDimStarter, unittest.TestCase):
    def test_fullRound(self):
        """
        A test that exercises most of the REST interface exposed on top of the
        DataIslandManager
        """

        sessionId = "lala"
        restPort = 8989  # don't interfere with EAGLE default port
        args = ["--port", str(restPort), "-N", hostname, "-qqq"]
        dimProcess = tool.start_process("dim", args)

        with testutils.terminating(dimProcess, timeout=10):

            # Wait until the REST server becomes alive
            self.assertTrue(
                utils.portIsOpen("localhost", restPort, timeout=10),
                "REST server didn't come up in time",
            )

            # The DIM is still empty
            sessions = testutils.get(self, "/sessions", restPort)
            self.assertEqual(0, len(sessions))
            dimStatus = testutils.get(self, "", restPort)
            self.assertEqual(1, len(dimStatus["hosts"]))
            self.assertEqual(hostname, dimStatus["hosts"][0])
            self.assertEqual(0, len(dimStatus["sessionIds"]))

            # Create a session and check it exists
            testutils.post(
                self, "/sessions", restPort, '{"sessionId":"%s"}' % (sessionId)
            )
            sessions = testutils.get(self, "/sessions", restPort)
            self.assertEqual(1, len(sessions))
            self.assertEqual(sessionId, sessions[0]["sessionId"])
            self.assertDictEqual(
                {hostname: SessionStates.PRISTINE}, sessions[0]["status"]
            )

            # Add this complex graph spec to the session
            # The UID of the two leaf nodes of this complex.js graph are T and S
            # Since the original complexGraph doesn't have node information
            # we need to add it manually before submitting -- otherwise it will
            # get rejected by the DIM.
            with pkg_resources.resource_stream(
                "test", "graphs/complex.js"
            ) as f:  # @UndefinedVariable
                complexGraphSpec = json.load(codecs.getreader("utf-8")(f))
                logger.debug(f'Loaded graph: {f}')
            for dropSpec in complexGraphSpec:
                dropSpec["node"] = hostname
            testutils.post(
                self,
                "/sessions/%s/graph/append" % (sessionId),
                restPort,
                json.dumps(complexGraphSpec),
            )
            self.assertEqual(
                {hostname: SessionStates.BUILDING},
                testutils.get(self, "/sessions/%s/status" % (sessionId), restPort),
            )

            # Now we deploy the graph...
            testutils.post(
                self,
                "/sessions/%s/deploy" % (sessionId),
                restPort,
                "completed=SL_A,SL_B,SL_C,SL_D,SL_K",
                mimeType="application/x-www-form-urlencoded",
            )
            self.assertEqual(
                {hostname: SessionStates.RUNNING},
                testutils.get(self, "/sessions/%s/status" % (sessionId), restPort),
            )

            # ...and write to all 5 root nodes that are listening in ports
            # starting at 1111
            msg = os.urandom(10)
            for i in range(5):
                utils.write_to(
                    "localhost", 1111 + i, msg, 2
                ), "Couldn't write data to localhost:%d" % (1111 + i)

            # Wait until the graph has finished its execution. We'll know
            # it finished by polling the status of the session
            while (
                SessionStates.RUNNING
                in testutils.get(
                    self, "/sessions/%s/status" % (sessionId), restPort
                ).values()
            ):
                time.sleep(0.2)

            self.assertEqual(
                {hostname: SessionStates.FINISHED},
                testutils.get(self, "/sessions/%s/status" % (sessionId), restPort),
            )
            testutils.delete(self, "/sessions/%s" % (sessionId), restPort)
            sessions = testutils.get(self, "/sessions", restPort)
            self.assertEqual(0, len(sessions))
