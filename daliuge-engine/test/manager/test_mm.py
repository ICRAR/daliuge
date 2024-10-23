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
import sys
import time
import unittest

import pkg_resources
from dlg import droputils
from dlg import utils
from dlg.common import tool
from dlg.ddap_protocol import DROPStates
from dlg import constants
from dlg.manager.composite_manager import MasterManager
from dlg.manager.session import SessionStates
from dlg.manager.manager_data import Node
from dlg.testutils import ManagerStarter
from dlg.exceptions import NoSessionException
from test.dlg_engine_testutils import DROPManagerUtils, RESTTestUtils, TerminatingTestHelper
hostname = "localhost"


class DimAndNMStarter(ManagerStarter):
    def setUp(self):
        super(DimAndNMStarter, self).setUp()
        nm_port = constants.NODE_DEFAULT_REST_PORT
        dim_port = constants.ISLAND_DEFAULT_REST_PORT
        self.nm_info = self.start_nm_in_thread(port=nm_port)
        self.dim_info = self.start_dim_in_thread(port=dim_port)
        self.nm = self.nm_info.manager
        self.dim = self.dim_info.manager
        self.mm = MasterManager([f"{hostname}:{dim_port}"])

    def tearDown(self):
        self.mm.shutdown()
        self.dim_info.stop()
        self.nm_info.stop()


class TestMM(DimAndNMStarter, unittest.TestCase):
    def createSessionAndAddTypicalGraph(self, sessionId, sleepTime=0):
        nm_node = Node(f"{hostname}:{constants.NODE_DEFAULT_REST_PORT}")
        dim_node = Node(f"{hostname}:{constants.ISLAND_DEFAULT_REST_PORT}")
        graphSpec = [
            {
                "oid": "A",
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
                "island": str(dim_node),
                "node": str(nm_node),
                "consumers": ["B"],
            },
            {
                "oid": "B",
                "categoryType": "Application",
                "dropclass": "dlg.apps.simple.SleepAndCopyApp",
                "sleep_time": sleepTime,
                "outputs": ["C"],
                "node": str(nm_node),
                "island": str(dim_node),
            },
            {
                "oid": "C",
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
                "island": str(dim_node),
                "node": str(nm_node),
            },
        ]
        DROPManagerUtils.add_test_reprodata(graphSpec)
        self.mm.createSession(sessionId)
        self.mm.addGraphSpec(sessionId, graphSpec)

    def test_createSession(self):
        sessionId = "lalo"
        self.mm.createSession(sessionId)
        self.assertEqual(1, len(self.nm.getSessionIds()))
        self.assertEqual(sessionId, self.nm.getSessionIds()[0])

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
        self.assertRaises(Exception, self.mm.addGraphSpec, sessionId, graphSpec)

        # Wrong node specified
        graphSpec = [
            {
                "oid": "A",
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
                "node": "unknown_host",
            }
        ]
        self.assertRaises(Exception, self.mm.addGraphSpec, sessionId, graphSpec)
        nm_node = Node(f"{hostname}:{constants.NODE_DEFAULT_REST_PORT}")
        dim_node = Node(f"{hostname}:{constants.ISLAND_DEFAULT_REST_PORT}")
        # No island specified
        graphSpec = [
            {
                "oid": "A",
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
                "node": str(nm_node),
            }
        ]
        self.assertRaises(Exception, self.mm.addGraphSpec, sessionId, graphSpec)

        # Wrong island specified
        graphSpec = [
            {
                "oid": "A",
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
                "node": str(nm_node),
                "island": "unknown_host",
            }
        ]
        self.assertRaises(Exception, self.mm.addGraphSpec, sessionId, graphSpec)

        # OK
        graphSpec = [
            {
                "oid": "A",
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
                "node": str(nm_node),
                "island": str(dim_node)
            }
        ]
        self.mm.createSession(sessionId)
        self.mm.addGraphSpec(sessionId, graphSpec)

        graphFromNM = self.nm.getGraph(sessionId)
        graphFromDIM = self.dim.getGraph(sessionId)
        graphFromMM = self.dim.getGraph(sessionId)
        self.assertDictEqual(graphFromNM, graphFromDIM)
        self.assertDictEqual(graphFromMM, graphFromDIM)

        self.assertEqual(1, len(graphFromMM))
        dropSpec = list(graphFromMM.values())[0]
        self.assertEqual("A", dropSpec["oid"])
        self.assertEqual("Data", dropSpec["categoryType"])
        self.assertEqual("dlg.data.drops.memory.InMemoryDROP", dropSpec["dropclass"])

    def test_deployGraph(self):
        sessionId = "lalo"
        self.createSessionAndAddTypicalGraph(sessionId)

        # Deploy now and get A and C
        self.mm.deploySession(sessionId)
        a, c = [self.nm._sessions[sessionId].drops[x] for x in ("A", "C")]

        data = os.urandom(10)
        with droputils.DROPWaiterCtx(self, c, 3):
            a.write(data)
            a.setCompleted()

        self.assertEqual(data, droputils.allDropContents(c))

    def test_deployGraphWithCompletedDOs(self):
        sessionId = "lalo"
        self.createSessionAndAddTypicalGraph(sessionId, sleepTime=2)

        # Deploy now and get A
        self.mm.deploySession(sessionId, completedDrops=["A"])
        c = self.nm._sessions[sessionId].drops["C"]

        # This should be happening before the sleepTime expires
        with droputils.DROPWaiterCtx(self, c, 5):
            pass

        self.assertEqual(DROPStates.COMPLETED, c.status)

    def test_sessionStatus(self):
        def assertSessionStatus(sessionId, status):
            """
            MasterManager -> DIM(s) -> NM(s)

            We expect the keys of the sessions status of MM to be the DIMs, and
            the keys of the DIMs session status to be the NMs.
            """
            sessionStatusMM = self.mm.getSessionStatus(sessionId)
            sessionStatusDIM = self.dim.getSessionStatus(sessionId)
            sessionStatusNM = self.nm.getSessionStatus(sessionId)
            self.assertEqual(1, len(sessionStatusMM))
            dimNode = str(Node(f"{hostname}:{constants.ISLAND_DEFAULT_REST_PORT}"))
            self.assertIn(
                dimNode,
                sessionStatusMM
            )
            self.assertDictEqual(
                sessionStatusDIM,
                sessionStatusMM[dimNode]
            )
            nmNode = str(Node(f"{hostname}:{constants.NODE_DEFAULT_REST_PORT}"))
            self.assertEqual(
                sessionStatusNM,
                sessionStatusMM[dimNode][nmNode],
            )
            self.assertEqual(sessionStatusNM, status)

        sessionId = "lala"
        self.mm.createSession(sessionId)
        assertSessionStatus(sessionId, SessionStates.PRISTINE)

        sessionId = "lalo"
        self.createSessionAndAddTypicalGraph(sessionId)
        assertSessionStatus(sessionId, SessionStates.BUILDING)

        self.nm.deploySession(sessionId)
        assertSessionStatus(sessionId, SessionStates.RUNNING)

        a, c = [self.nm._sessions[sessionId].drops[x] for x in ("A", "C")]
        data = os.urandom(10)
        with droputils.DROPWaiterCtx(self, c, 3):
            a.write(data)
            a.setCompleted()

        assertSessionStatus(sessionId, SessionStates.FINISHED)

    def test_getGraph(self):
        sessionId = "lalo"
        self.createSessionAndAddTypicalGraph(sessionId)

        graphSpecFromMM = self.mm.getGraph(sessionId)
        self.assertEqual(3, len(graphSpecFromMM))
        for oid in ("A", "B", "C"):
            self.assertIn(oid, graphSpecFromMM)
        graphSepcFromNM = self.nm.getGraph(sessionId)
        graphSepcFromDIM = self.dim.getGraph(sessionId)
        self.assertDictEqual(graphSepcFromNM, graphSpecFromMM)
        self.assertDictEqual(graphSepcFromDIM, graphSpecFromMM)

    def test_getGraphStatus(self):
        def assertGraphStatus(sessionId, expectedStatus):
            graphStatusByDIM = self.dim.getGraphStatus(sessionId)
            graphStatusByDM = self.nm.getGraphStatus(sessionId)
            graphStatusByMM = self.mm.getGraphStatus(sessionId)
            self.assertDictEqual(graphStatusByDIM, graphStatusByMM)
            self.assertDictEqual(graphStatusByDIM, graphStatusByDM)
            for dropStatus in graphStatusByMM.values():
                self.assertEqual(expectedStatus, dropStatus["status"])

        sessionId = "lala"
        self.createSessionAndAddTypicalGraph(sessionId)
        self.mm.deploySession(sessionId)
        assertGraphStatus(sessionId, DROPStates.INITIALIZED)

        a, c = [self.nm._sessions[sessionId].drops[x] for x in ("A", "C")]
        data = os.urandom(10)
        with droputils.DROPWaiterCtx(self, c, 3):
            a.write(data)
            a.setCompleted()
        assertGraphStatus(sessionId, DROPStates.COMPLETED)


class TestREST(DimAndNMStarter, unittest.TestCase):
    def test_fullRound(self):
        """
        A test that exercises most of the REST interface exposed on top of the
        DataIslandManager
        """

        sessionId = "lala"
        restPort = 8989

        args = [
            "--port",
            str(restPort),
            "-N",
            f"{hostname}:{constants.NODE_DEFAULT_REST_PORT}",
            "-qqq",
        ]
        mmProcess = tool.start_process("mm", args)

        with TerminatingTestHelper(mmProcess, 10):
            # Wait until the REST server becomes alive
            self.assertTrue(
                utils.portIsOpen("localhost", restPort, timeout=10),
                "REST server didn't come up in time",
            )

            # The DIM is still empty
            sessions = RESTTestUtils.get(self, "/sessions", restPort)
            self.assertEqual(0, len(sessions))
            # nm_host = "localhost:{restPort}"
            dimStatus = RESTTestUtils.get(self, "", restPort)
            host = f"{dimStatus['hosts'][0].split(':', 1)[0]}:{restPort}"
            self.assertEqual(1, len(dimStatus["hosts"]))
            self.assertEqual(
                Node(f"{hostname}:{constants.NODE_DEFAULT_REST_PORT}"), dimStatus["hosts"][0]
            )
            self.assertEqual(0, len(dimStatus["sessionIds"]))

            # Create a session and check it exists
            RESTTestUtils.post(
                self, "/sessions", restPort, '{"sessionId":"%s"}' % (sessionId)
            )
            sessions = RESTTestUtils.get(self, "/sessions", restPort)
            self.assertEqual(1, len(sessions))
            self.assertEqual(sessionId, sessions[0]["sessionId"])
            self.assertDictEqual(
                {
                    Node(f"{hostname}:{constants.NODE_DEFAULT_REST_PORT}"): SessionStates.PRISTINE
                },
                sessions[0]["status"],
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
            for dropSpec in complexGraphSpec:
                dropSpec["node"] = f"{hostname}:{constants.NODE_DEFAULT_REST_PORT}"
                dropSpec["island"] = f"{hostname}:{constants.NODE_DEFAULT_REST_PORT}"
            RESTTestUtils.post(
                self,
                "/sessions/%s/graph/append" % (sessionId),
                restPort,
                json.dumps(complexGraphSpec),
            )
            self.assertEqual(
                {Node(f"{hostname}:8000"): SessionStates.BUILDING},
                RESTTestUtils.get(self, "/sessions/%s/status" % (sessionId), restPort),
            )

            # Now we deploy the graph...
            RESTTestUtils.post(
                self,
                "/sessions/%s/deploy" % (sessionId),
                restPort,
                "completed=SL_A,SL_B,SL_C,SL_D,SL_K",
                mimeType="application/x-www-form-urlencoded",
            )
            self.assertEqual(
                {Node(f"{hostname}:8000"): SessionStates.RUNNING},
                RESTTestUtils.get(self, "/sessions/%s/status" % (sessionId), restPort),
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
            while SessionStates.RUNNING in [
                RESTTestUtils.get(self, "/sessions/%s/status" % (sessionId), restPort)[
                    str(Node(f"{hostname}:{constants.NODE_DEFAULT_REST_PORT}"))
                ]
            ]:
                time.sleep(0.2)

            self.assertEqual(
                {
                    str(Node(f"{hostname}:{constants.NODE_DEFAULT_REST_PORT}")): SessionStates.FINISHED
                },
                RESTTestUtils.get(self, "/sessions/%s/status" % (sessionId), restPort),
            )
            RESTTestUtils.delete(self, "/sessions/%s" % (sessionId), restPort)
            sessions = RESTTestUtils.get(self, "/sessions", restPort)
            self.assertEqual(0, len(sessions))

            # Check log should not exist
            resp, _ = RESTTestUtils._get("/sessions/not_exist/logs", 8000)
            self.assertEqual(resp.status, 404)

            # Check logs exist and there is content
            resp, _ = RESTTestUtils._get("/sessions/{sessionId}/logs", restPort)
            self.assertGreater(int(resp.getheader("Content-Length")), 0)
