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
import codecs
import json
import os
import pathlib
import time
import unittest
import shutil
from asyncio.log import logger

from dlg import utils, droputils
from dlg.testutils import ManagerStarter
from dlg.common import tool
from dlg.constants import ISLAND_DEFAULT_REST_PORT, NODE_DEFAULT_REST_PORT
from dlg.ddap_protocol import DROPStates
from dlg.manager.composite_manager import DataIslandManager
from dlg.manager.session import SessionStates
from dlg.manager.manager_data import Node

from dlg.testutils import ManagerStarter
from test.dlg_engine_testutils import (
    RESTTestUtils,
    DROPManagerUtils,
    TerminatingTestHelper,
)

hostname = "localhost"
dim_host = f"{hostname}:{ISLAND_DEFAULT_REST_PORT}"
nm_host = f"{hostname}:{NODE_DEFAULT_REST_PORT}"


class LocalDimStarter(ManagerStarter):
    def setUp(self):
        super(LocalDimStarter, self).setUp()
        self.nm_info = self.start_nm_in_thread()
        self.dm = self.nm_info.manager
        self.dim = DataIslandManager(
            [f"{self.nm_info.server._server.listen}:{self.nm_info.server._server.port}"]
        )
        # self.dim_info = self.start_dim_in_thread()
        # self.dim = self.dim_info.manager

    # def test_invalid_host_format(self):
    #     invalid_host = "invalid_host_format"
    #     with self.assertRaises(ValueError):
    #         DataIslandManager([f"{invalid_host}"])

    def tearDown(self):
        self.nm_info.stop()
        self.dim.shutdown()
        super(LocalDimStarter, self).tearDown()


class TestDIM(LocalDimStarter, unittest.TestCase):
    def createSessionAndAddTypicalGraph(self, sessionId, sleepTime=0):
        graphSpec = [
            {
                "oid": "A",
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
                "node": nm_host,
                "consumers": ["B"],
            },
            {
                "oid": "B",
                "categoryType": "Application",
                "dropclass": "dlg.apps.simple.SleepAndCopyApp",
                "sleep_time": sleepTime,
                "outputs": ["C"],
                "node": nm_host,
            },
            {
                "oid": "C",
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
                "node": nm_host,
            },
        ]
        graphSpec = DROPManagerUtils.add_test_reprodata(graphSpec)
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
        graphSpec = [
            {
                "oid": "A",
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
            }
        ]
        self.assertRaises(Exception, self.dim.addGraphSpec, sessionId, graphSpec)

        # Wrong node specified
        graphSpec = [
            {
                "oid": "A",
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
                "node": "unknown_host",
            }
        ]
        graphSpec = DROPManagerUtils.add_test_reprodata(graphSpec)
        self.assertRaises(Exception, self.dim.addGraphSpec, sessionId, graphSpec)

        # OK
        graphSpec = [
            {
                "oid": "A",
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
                "node": nm_host,
            }
        ]
        graphSpec = DROPManagerUtils.add_test_reprodata(graphSpec)
        self.dim.createSession(sessionId)
        self.assertEqual(0, self.dim.getGraphSize(sessionId))
        self.dim.addGraphSpec(sessionId, graphSpec)
        self.assertEqual(1, self.dim.getGraphSize(sessionId))
        graphFromDM = self.dm.getGraph(sessionId)
        self.assertEqual(1, len(graphFromDM))
        dropSpec = list(graphFromDM.values())[0]
        self.assertEqual("A", dropSpec["oid"])
        self.assertEqual("Data", dropSpec["categoryType"])
        self.assertEqual("dlg.data.drops.memory.InMemoryDROP", dropSpec["dropclass"])

    def test_deployGraph(self):
        sessionId = "lalo"
        self.createSessionAndAddTypicalGraph(sessionId)

        # Deploy now and get A and C
        self.dim.deploySession(sessionId)
        a, c = [self.dm.sessions[sessionId].drops[x] for x in ("A", "C")]

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
        c = self.dm.sessions[sessionId].drops["C"]

        # This should be happening before the sleepTime expires
        with droputils.DROPWaiterCtx(self, c, 2):
            pass

        self.assertEqual(DROPStates.COMPLETED, c.status)

    def test_sessionStatus(self):
        def assertSessionStatus(sessionId, status):
            sessionStatus = self.dim.getSessionStatus(sessionId)
            self.assertEqual(1, len(sessionStatus))
            self.assertIn(str(Node(nm_host)), sessionStatus)
            self.assertEqual(status, sessionStatus[Node(nm_host)])
            self.assertEqual(status, self.dm.getSessionStatus(sessionId))

        sessionId = "lala"
        self.dim.createSession(sessionId)
        assertSessionStatus(sessionId, SessionStates.PRISTINE)

        sessionId = "lalo"
        self.createSessionAndAddTypicalGraph(sessionId)
        assertSessionStatus(sessionId, SessionStates.BUILDING)

        self.dm.deploySession(sessionId)
        assertSessionStatus(sessionId, SessionStates.RUNNING)

        a, c = [self.dm.sessions[sessionId].drops[x] for x in ("A", "C")]
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

        a, c = [self.dm.sessions[sessionId].drops[x] for x in ("A", "C")]
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

    def test_submit_unreprodata(self):
        """
        Need to ensure that the DIM can handle a graph with empty reprodata
        (the default if nothing is provided at translation time)
        """
        graphSpec = [
            {
                "oid": "A",
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
                "node": nm_host,
                "consumers": ["B"],
            },
            {
                "oid": "B",
                "categoryType": "Application",
                "dropclass": "dlg.apps.simple.SleepAndCopyApp",
                "sleep_time": 1,
                "outputs": ["C"],
                "node": nm_host,
            },
            {
                "oid": "C",
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
                "node": nm_host,
            },
            {},  # A dummy empty reprodata (the default if absolutely nothing is specified)
        ]
        self.dim.createSession("a")
        self.assertEqual(0, self.dim.getGraphSize("a"))
        self.dim.addGraphSpec("a", graphSpec)
        self.assertEqual(len(graphSpec), self.dim.getGraphSize("a"))

    def test_submit_noreprodata(self):
        """
        Need to ensure that the DIM can handle a graph with no reprodata
        (the default if nothing is provided at translation time)
        """
        graphSpec = [
            {
                "oid": "A",
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
                "node": nm_host,
                "consumers": ["B"],
            },
            {
                "oid": "B",
                "categoryType": "Application",
                "dropclass": "dlg.apps.simple.SleepAndCopyApp",
                "sleep_time": 1,
                "outputs": ["C"],
                "node": nm_host,
            },
            {
                "oid": "C",
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
                "node": nm_host,
            },
        ]
        self.dim.createSession("a")
        self.assertEqual(0, self.dim.getGraphSize("a"))
        self.dim.addGraphSpec("a", graphSpec)
        self.assertEqual(len(graphSpec), self.dim.getGraphSize("a"))


class TestREST(LocalDimStarter, unittest.TestCase):

    def test_fullRound(self):
        """
        A test that exercises most of the REST interface exposed on top of the
        DataIslandManager
        """
        cwd = pathlib.Path.cwd()
        os.makedirs("/tmp/test_dim_rest/", exist_ok=True)
        os.environ["DLG_ROOT"] = "/tmp/test_dim_rest"

        sessionId = "lala"
        nmPort = 8000  # NOTE: can't use any other port yet.
        dimPort = 8989  # don't interfere with EAGLE default port
        args = [
            "--port",
            str(dimPort),
            "-N",
            f"{hostname}:{nmPort}",
            "-qqq",
            "--dump_graphs",
        ]
        dimProcess = tool.start_process("dim", args)

        with TerminatingTestHelper(dimProcess, timeout=10):
            # Wait until the REST server becomes alive
            self.assertTrue(
                utils.portIsOpen("localhost", dimPort, timeout=10),
                "REST server didn't come up in time",
            )

            # The DIM is still empty
            sessions = RESTTestUtils.get(self, "/sessions", nmPort)
            self.assertEqual(0, len(sessions))
            dimStatus = RESTTestUtils.get(self, "", dimPort)
            self.assertEqual(1, len(dimStatus["hosts"]))
            self.assertEqual(Node(f"{hostname}:{nmPort}"), dimStatus["hosts"][0])
            self.assertEqual(0, len(dimStatus["sessionIds"]))

            # Create a session and check it exists
            RESTTestUtils.post(
                self, "/sessions", dimPort, '{"sessionId":"%s"}' % (sessionId)
            )
            sessions = RESTTestUtils.get(self, "/sessions", dimPort)
            self.assertEqual(1, len(sessions))
            self.assertEqual(sessionId, sessions[0]["sessionId"])
            nm_name = str(Node(f"{hostname}:{nmPort}"))
            self.assertDictEqual(
                {nm_name: SessionStates.PRISTINE}, sessions[0]["status"]
            )

            # Add this complex graph spec to the session
            # The UID of the two leaf nodes of this complex.js graph are T and S
            # Since the original complexGraph doesn't have node information
            # we need to add it manually before submitting -- otherwise it will
            import importlib.resources
            ref = importlib.resources.files('test').joinpath('graphs/complex.js')
            with ref.open('rb') as fp:
                complexGraphSpec = json.load(codecs.getreader("utf-8")(fp))
            logger.debug(f"Loaded graph: {fp}")
            # get rejected by the DIM.
            for dropSpec in complexGraphSpec:
                dropSpec["node"] = nm_host
            RESTTestUtils.post(
                self,
                "/sessions/%s/graph/append" % (sessionId),
                dimPort,
                json.dumps(complexGraphSpec),
            )
            self.assertEqual(
                {nm_name: SessionStates.BUILDING},
                RESTTestUtils.get(self, "/sessions/%s/status" % (sessionId), dimPort),
            )

            # Now we deploy the graph...
            RESTTestUtils.post(
                self,
                "/sessions/%s/deploy" % (sessionId),
                nmPort,
                "completed=SL_A,SL_B,SL_C,SL_D,SL_K",
                mimeType="application/x-www-form-urlencoded",
            )
            self.assertEqual(
                {nm_name: SessionStates.RUNNING},
                RESTTestUtils.get(self, "/sessions/%s/status" % (sessionId), dimPort),
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
                in RESTTestUtils.get(
                    self, "/sessions/%s/status" % (sessionId), dimPort
                ).values()
            ):
                time.sleep(0.2)

            self.assertEqual(
                {nm_name: SessionStates.FINISHED},
                RESTTestUtils.get(self, "/sessions/%s/status" % (sessionId), dimPort),
            )
            RESTTestUtils.delete(self, "/sessions/%s" % (sessionId), dimPort)
            sessions = RESTTestUtils.get(self, "/sessions", dimPort)
            self.assertEqual(0, len(sessions))
            pastSessions = RESTTestUtils.get(self, "/past_sessions", dimPort)
            self.assertEqual(1, len(pastSessions))
        # Reset environment and test directories
        os.chdir(cwd)
        shutil.rmtree("/tmp/test_dim_rest/", ignore_errors=True)
        del os.environ["DLG_ROOT"]
