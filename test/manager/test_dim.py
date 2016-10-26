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
import subprocess
import sys
import threading
import time
import unittest

import pkg_resources

from dfms import droputils
from dfms import utils
from dfms.ddap_protocol import DROPStates
from dfms.manager import constants
from dfms.manager.composite_manager import DataIslandManager
from dfms.manager.node_manager import NodeManager
from dfms.manager.rest import NMRestServer
from dfms.manager.session import SessionStates
from dfms.utils import portIsOpen
from test import graphsRepository
from test.manager import testutils


hostname = 'localhost'

def setUpDimTests(self):

    # SleepAndCopyApps don't take time to execute
    graphsRepository.defaultSleepTime = 0

    # Start a DM. This is the DM which the DIM connects to.
    #
    # We start it here to avoid the DIM connecting via SSH to the localhost
    # and spawning a dfmsDM process; both things need proper setup which we
    # cannot do here (ssh publick key installation, ssh service up, proper
    # environment available, etc)
    #
    # Anyway, this is also useful because we can check that things have
    # occurred at the DM level in the test cases
    self.dm = NodeManager(False)
    self._dm_server = NMRestServer(self.dm)
    self._dm_t = threading.Thread(target=self._dm_server.start, args=(hostname,constants.NODE_DEFAULT_REST_PORT))
    self._dm_t.start()

    # The DIM we're testing
    self.dim = DataIslandManager([hostname])

    self.assertTrue(portIsOpen(hostname, constants.NODE_DEFAULT_REST_PORT, 5))

def tearDownDimTests(self):

    # Stop the server and wait until it's closed
    self._dm_server.stop()
    self._dm_t.join()
    self.dm.shutdown()
    self.assertFalse(self._dm_t.isAlive())
    self.dim.shutdown()

class TestDIM(unittest.TestCase):

    setUp = setUpDimTests
    tearDown = tearDownDimTests

    def createSessionAndAddTypicalGraph(self, sessionId, sleepTime=0):
        graphSpec = [{'oid':'A', 'type':'plain', 'storage':'memory', 'node':hostname, 'consumers':['B']},
                     {'oid':'B', 'type':'app', 'app':'test.graphsRepository.SleepAndCopyApp', 'sleepTime':sleepTime, 'outputs':['C'], 'node':hostname},
                     {'oid':'C', 'type':'plain', 'storage':'memory', 'node':hostname}]
        self.dim.createSession(sessionId)
        self.assertEqual(0, self.dim.getGraphSize(sessionId))
        self.dim.addGraphSpec(sessionId, graphSpec)
        self.assertEqual(len(graphSpec), self.dim.getGraphSize(sessionId))

    def test_createSession(self):
        sessionId = 'lalo'
        self.dim.createSession(sessionId)
        self.assertEqual(1, len(self.dm.getSessionIds()))
        self.assertEqual(sessionId, self.dm.getSessionIds()[0])

    def test_addGraphSpec(self):

        sessionId = 'lalo'

        # No node specified
        graphSpec = [{'oid':'A', 'type':'plain', 'storage':'memory'}]
        self.assertRaises(Exception, self.dim.addGraphSpec, sessionId, graphSpec)

        # Wrong node specified
        graphSpec = [{'oid':'A', 'type':'plain', 'storage':'memory', 'node':'unknown_host'}]
        self.assertRaises(Exception, self.dim.addGraphSpec, sessionId, graphSpec)

        # OK
        graphSpec = [{'oid':'A', 'type':'plain', 'storage':'memory', 'node':hostname}]
        self.dim.createSession(sessionId)
        self.assertEqual(0, self.dim.getGraphSize(sessionId))
        self.dim.addGraphSpec(sessionId, graphSpec)
        self.assertEqual(1, self.dim.getGraphSize(sessionId))
        graphFromDM = self.dm.getGraph(sessionId)
        self.assertEqual(1, len(graphFromDM))
        dropSpec = list(graphFromDM.values())[0]
        self.assertEqual('A', dropSpec['oid'])
        self.assertEqual('plain', dropSpec['type'])
        self.assertEqual('memory', dropSpec['storage'])

    def test_deployGraph(self):

        sessionId = 'lalo'
        self.createSessionAndAddTypicalGraph(sessionId)

        # Deploy now and get A and C
        self.dim.deploySession(sessionId)
        a, c = [self.dm._sessions[sessionId].drops[x] for x in ('A', 'C')]

        data = os.urandom(10)
        with droputils.DROPWaiterCtx(self, c, 3):
            a.write(data)
            a.setCompleted()

        self.assertEqual(data, droputils.allDropContents(c))

    def test_deployGraphWithCompletedDOs(self):
        self._test_deployGraphWithCompletedDOs('lalo')

    def test_deployGraphWithCompletedDOs_sessionIdWithSpaces(self):
        self._test_deployGraphWithCompletedDOs('lala with spaces')

    def _test_deployGraphWithCompletedDOs(self, sessionId):

        self.createSessionAndAddTypicalGraph(sessionId, sleepTime=1)

        # Deploy now and get C
        self.dim.deploySession(sessionId, completedDrops=['A'])
        c = self.dm._sessions[sessionId].drops['C']

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

        sessionId = 'lala'
        self.dim.createSession(sessionId)
        assertSessionStatus(sessionId, SessionStates.PRISTINE)

        sessionId = 'lalo'
        self.createSessionAndAddTypicalGraph(sessionId)
        assertSessionStatus(sessionId, SessionStates.BUILDING)

        self.dm.deploySession(sessionId)
        assertSessionStatus(sessionId, SessionStates.RUNNING)

        a, c = [self.dm._sessions[sessionId].drops[x] for x in ('A', 'C')]
        data = os.urandom(10)
        with droputils.DROPWaiterCtx(self, c, 3):
            a.write(data)
            a.setCompleted()

        assertSessionStatus(sessionId, SessionStates.FINISHED)

    def test_getGraph(self):

        sessionId = 'lalo'
        self.createSessionAndAddTypicalGraph(sessionId)

        graphSpecFromDim = self.dim.getGraph(sessionId)
        self.assertEqual(3, len(graphSpecFromDim))
        for oid in ('A','B','C'):
            self.assertIn(oid, graphSpecFromDim)
        graphSepcFromDM = self.dm.getGraph(sessionId)
        self.assertDictEqual(graphSepcFromDM, graphSpecFromDim)

    def test_getGraphStatus(self):

        def assertGraphStatus(sessionId, expectedStatus):
            graphStatusByDim = self.dim.getGraphStatus(sessionId)
            graphStatusByDM = self.dm.getGraphStatus(sessionId)
            self.assertDictEqual(graphStatusByDim, graphStatusByDM)
            for dropStatus in graphStatusByDim.values():
                self.assertEqual(expectedStatus, dropStatus['status'])

        sessionId = 'lala'
        self.createSessionAndAddTypicalGraph(sessionId)
        self.dim.deploySession(sessionId)
        assertGraphStatus(sessionId, DROPStates.INITIALIZED)

        a, c = [self.dm._sessions[sessionId].drops[x] for x in ('A', 'C')]
        data = os.urandom(10)
        with droputils.DROPWaiterCtx(self, c, 3):
            a.write(data)
            a.setCompleted()
        assertGraphStatus(sessionId, DROPStates.COMPLETED)


class TestREST(unittest.TestCase):

    setUp = setUpDimTests
    tearDown = tearDownDimTests

    def test_fullRound(self):
        """
        A test that exercises most of the REST interface exposed on top of the
        DataIslandManager
        """

        sessionId = 'lala'
        restPort  = 8888
        args = [sys.executable, '-m', 'dfms.tool', 'dim', \
                '--port', str(restPort), '-N',hostname, '-qqq']
        dimProcess = subprocess.Popen(args)

        with testutils.terminating(dimProcess, 10):

            # Wait until the REST server becomes alive
            self.assertTrue(utils.portIsOpen('localhost', restPort, 10), "REST server didn't come up in time")

            # The DIM is still empty
            sessions = testutils.get(self, '/sessions', restPort)
            self.assertEqual(0, len(sessions))
            dimStatus = testutils.get(self, '', restPort)
            self.assertEqual(1, len(dimStatus['hosts']))
            self.assertEqual(hostname, dimStatus['hosts'][0])
            self.assertEqual(0, len(dimStatus['sessionIds']))

            # Create a session and check it exists
            testutils.post(self, '/sessions', restPort, '{"sessionId":"%s"}' % (sessionId))
            sessions = testutils.get(self, '/sessions', restPort)
            self.assertEqual(1, len(sessions))
            self.assertEqual(sessionId, sessions[0]['sessionId'])
            self.assertDictEqual({hostname: SessionStates.PRISTINE}, sessions[0]['status'])

            # Add this complex graph spec to the session
            # The UID of the two leaf nodes of this complex.js graph are T and S
            # Since the original complexGraph doesn't have node information
            # we need to add it manually before submitting -- otherwise it will
            # get rejected by the DIM.
            with pkg_resources.resource_stream('test', 'graphs/complex.js') as f: # @UndefinedVariable
                complexGraphSpec = json.load(codecs.getreader('utf-8')(f))
            for dropSpec in complexGraphSpec:
                dropSpec['node'] = hostname
            testutils.post(self, '/sessions/%s/graph/append' % (sessionId), restPort, json.dumps(complexGraphSpec))
            self.assertEqual({hostname: SessionStates.BUILDING}, testutils.get(self, '/sessions/%s/status' % (sessionId), restPort))

            # Now we deploy the graph...
            testutils.post(self, '/sessions/%s/deploy' % (sessionId), restPort, "completed=SL_A,SL_B,SL_C,SL_D,SL_K", mimeType='application/x-www-form-urlencoded')
            self.assertEqual({hostname: SessionStates.RUNNING}, testutils.get(self, '/sessions/%s/status' % (sessionId), restPort))

            # ...and write to all 5 root nodes that are listening in ports
            # starting at 1111
            msg = os.urandom(10)
            for i in range(5):
                utils.write_to('localhost', 1111+i, msg, 2), "Couldn't write data to localhost:%d" % (1111+i)

            # Wait until the graph has finished its execution. We'll know
            # it finished by polling the status of the session
            while SessionStates.RUNNING in testutils.get(self, '/sessions/%s/status' % (sessionId), restPort).values():
                time.sleep(0.2)

            self.assertEqual({hostname: SessionStates.FINISHED}, testutils.get(self, '/sessions/%s/status' % (sessionId), restPort))
            testutils.delete(self, '/sessions/%s' % (sessionId), restPort)
            sessions = testutils.get(self, '/sessions', restPort)
            self.assertEqual(0, len(sessions))