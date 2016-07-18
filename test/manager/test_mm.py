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

import Pyro4
import pkg_resources

from dfms import droputils
from dfms import utils
from dfms.ddap_protocol import DROPStates
from dfms.manager import constants
from dfms.manager.composite_manager import DataIslandManager, MasterManager
from dfms.manager.node_manager import NodeManager
from dfms.manager.rest import NMRestServer, CompositeManagerRestServer
from dfms.manager.session import SessionStates
from dfms.utils import portIsOpen
from test import graphsRepository
from test.manager import testutils


hostname = 'localhost'

def setUpMMTests(self):

    # SleepAndCopyApps don't take time to execute
    graphsRepository.defaultSleepTime = 0

    # Start a NM and a DIM. See test_dim for more details
    self.nm = NodeManager(False)
    self._nm_server = NMRestServer(self.nm)
    self._nm_t = threading.Thread(name="lala",target=self._nm_server.start, args=(hostname,constants.NODE_DEFAULT_REST_PORT))
    self._nm_t.start()

    # The DIM we're testing
    self.dim = DataIslandManager([hostname])
    self._dim_server = CompositeManagerRestServer(self.dim)
    self._dim_t = threading.Thread(name="lalo",target=self._dim_server.start, args=(hostname,constants.ISLAND_DEFAULT_REST_PORT))
    self._dim_t.start()

    self.mm = MasterManager([hostname])

    # Make sure the managers have started
    self.assertTrue(portIsOpen(hostname, constants.NODE_DEFAULT_REST_PORT, 5))
    self.assertTrue(portIsOpen(hostname, constants.ISLAND_DEFAULT_REST_PORT, 5))

def tearDownMMTests(self):
    self._nm_server.stop()
    self._nm_t.join()
    self._dim_server.stop()
    self._dim_t.join()
    self.dim.shutdown()
    self.mm.shutdown()

class TestMM(unittest.TestCase):

    setUp = setUpMMTests
    tearDown = tearDownMMTests

    def createSessionAndAddTypicalGraph(self, sessionId, sleepTime=0):
        graphSpec = [{'oid':'A', 'type':'plain', 'storage':'memory', 'island':hostname, 'node':hostname, 'consumers':['B']},
                     {'oid':'B', 'type':'app', 'app':'test.graphsRepository.SleepAndCopyApp', 'sleepTime':sleepTime, 'outputs':['C'], 'node':hostname, 'island':hostname},
                     {'oid':'C', 'type':'plain', 'storage':'memory', 'island':hostname, 'node':hostname}]
        self.mm.createSession(sessionId)
        self.mm.addGraphSpec(sessionId, graphSpec)

    def test_createSession(self):
        sessionId = 'lalo'
        self.mm.createSession(sessionId)
        self.assertEqual(1, len(self.nm.getSessionIds()))
        self.assertEqual(sessionId, self.nm.getSessionIds()[0])

    def test_addGraphSpec(self):

        sessionId = 'lalo'

        # No node specified
        graphSpec = [{'oid':'A', 'type':'plain', 'storage':'memory'}]
        self.assertRaises(Exception, self.mm.addGraphSpec, sessionId, graphSpec)

        # Wrong node specified
        graphSpec = [{'oid':'A', 'type':'plain', 'storage':'memory', 'node':'unknown_host'}]
        self.assertRaises(Exception, self.mm.addGraphSpec, sessionId, graphSpec)

        # No island specified
        graphSpec = [{'oid':'A', 'type':'plain', 'storage':'memory', 'node':hostname}]
        self.assertRaises(Exception, self.mm.addGraphSpec, sessionId, graphSpec)

        # Wrong island specified
        graphSpec = [{'oid':'A', 'type':'plain', 'storage':'memory', 'node':hostname, 'island':'unknown_host'}]
        self.assertRaises(Exception, self.mm.addGraphSpec, sessionId, graphSpec)

        # OK
        graphSpec = [{'oid':'A', 'type':'plain', 'storage':'memory', 'node':hostname, 'island':hostname}]
        self.mm.createSession(sessionId)
        self.mm.addGraphSpec(sessionId, graphSpec)

        graphFromNM = self.nm.getGraph(sessionId)
        graphFromDIM = self.dim.getGraph(sessionId)
        graphFromMM = self.dim.getGraph(sessionId)
        self.assertDictEqual(graphFromNM, graphFromDIM)
        self.assertDictEqual(graphFromMM, graphFromDIM)

        self.assertEqual(1, len(graphFromMM))
        dropSpec = graphFromMM.values()[0]
        self.assertEqual('A', dropSpec['oid'])
        self.assertEqual('plain', dropSpec['type'])
        self.assertEqual('memory', dropSpec['storage'])

    def test_deployGraph(self):

        sessionId = 'lalo'
        self.createSessionAndAddTypicalGraph(sessionId)

        # Deploy now and get the uris. With that we get then A's and C's proxies
        uris = self.mm.deploySession(sessionId)
        a = Pyro4.Proxy(uris['A'])
        c = Pyro4.Proxy(uris['C'])

        data = os.urandom(10)
        with droputils.EvtConsumerProxyCtx(self, c, 3):
            a.write(data)
            a.setCompleted()

        self.assertEqual(data, droputils.allDropContents(c))

    def test_deployGraphWithCompletedDOs(self):

        sessionId = 'lalo'
        self.createSessionAndAddTypicalGraph(sessionId, sleepTime=1)

        # Deploy now and get the uris. With that we get then A's and C's proxies
        uris = self.mm.deploySession(sessionId, completedDrops=['A'])
        c = Pyro4.Proxy(uris['C'])

        # This should be happening before the sleepTime expires
        with droputils.EvtConsumerProxyCtx(self, c, 2):
            pass

        self.assertEqual(DROPStates.COMPLETED, c.status)

    def test_sessionStatus(self):

        def assertSessionStatus(sessionId, status):
            sessionStatusMM  = self.mm.getSessionStatus(sessionId)
            sessionStatusDIM = self.dim.getSessionStatus(sessionId)
            sessionStatusNM  = self.nm.getSessionStatus(sessionId)
            self.assertEqual(1, len(sessionStatusMM))
            self.assertIn(hostname, sessionStatusMM)
            self.assertDictEqual(sessionStatusDIM, sessionStatusMM[hostname])
            self.assertEqual(sessionStatusNM, sessionStatusMM[hostname][hostname])
            self.assertEqual(sessionStatusNM, status)

        sessionId = 'lala'
        self.mm.createSession(sessionId)
        assertSessionStatus(sessionId, SessionStates.PRISTINE)

        sessionId = 'lalo'
        self.createSessionAndAddTypicalGraph(sessionId)
        assertSessionStatus(sessionId, SessionStates.BUILDING)

        uris = self.nm.deploySession(sessionId)
        assertSessionStatus(sessionId, SessionStates.RUNNING)

        a = Pyro4.Proxy(uris['A'])
        c = Pyro4.Proxy(uris['C'])
        data = os.urandom(10)
        with droputils.EvtConsumerProxyCtx(self, c, 3):
            a.write(data)
            a.setCompleted()

        assertSessionStatus(sessionId, SessionStates.FINISHED)

    def test_getGraph(self):

        sessionId = 'lalo'
        self.createSessionAndAddTypicalGraph(sessionId)

        graphSpecFromMM = self.mm.getGraph(sessionId)
        self.assertEqual(3, len(graphSpecFromMM))
        for oid in ('A','B','C'):
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
            for dropStatus in graphStatusByMM.viewvalues():
                self.assertEqual(expectedStatus, dropStatus['status'])

        sessionId = 'lala'
        self.createSessionAndAddTypicalGraph(sessionId)
        uris = self.mm.deploySession(sessionId)
        assertGraphStatus(sessionId, DROPStates.INITIALIZED)

        a = Pyro4.Proxy(uris['A'])
        c = Pyro4.Proxy(uris['C'])
        data = os.urandom(10)
        with droputils.EvtConsumerProxyCtx(self, c, 3):
            a.write(data)
            a.setCompleted()
        assertGraphStatus(sessionId, DROPStates.COMPLETED)


class TestREST(unittest.TestCase):

    setUp = setUpMMTests
    tearDown = tearDownMMTests

    def test_fullRound(self):
        """
        A test that exercises most of the REST interface exposed on top of the
        DataIslandManager
        """

        sessionId = 'lala'
        restPort  = 8888

        args = [sys.executable, '-m', 'dfms.manager.cmdline', 'dfmsMM', \
                '--port', str(restPort), '-N',hostname, '-qqq']
        mmProcess = subprocess.Popen(args)

        with testutils.terminating(mmProcess, 10):

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
            self.assertDictEqual({hostname: {hostname: SessionStates.PRISTINE}}, sessions[0]['status'])

            # Add this complex graph spec to the session
            # The UID of the two leaf nodes of this complex.js graph are T and S
            # Since the original complexGraph doesn't have node information
            # we need to add it manually before submitting -- otherwise it will
            # get rejected by the DIM.
            with pkg_resources.resource_stream('test', 'graphs/complex.js') as f: # @UndefinedVariable
                complexGraphSpec = json.load(codecs.getreader('utf-8')(f))
            for dropSpec in complexGraphSpec:
                dropSpec['node'] = hostname
                dropSpec['island'] = hostname
            testutils.post(self, '/sessions/%s/graph/append' % (sessionId), restPort, json.dumps(complexGraphSpec))
            self.assertEqual({hostname: {hostname: SessionStates.BUILDING}}, testutils.get(self, '/sessions/%s/status' % (sessionId), restPort))

            # Now we deploy the graph...
            testutils.post(self, '/sessions/%s/deploy' % (sessionId), restPort, "completed=SL_A,SL_B,SL_C,SL_D,SL_K", mimeType='application/x-www-form-urlencoded')
            self.assertEqual({hostname: {hostname: SessionStates.RUNNING}}, testutils.get(self, '/sessions/%s/status' % (sessionId), restPort))

            # ...and write to all 5 root nodes that are listening in ports
            # starting at 1111
            msg = os.urandom(10)
            for i in range(5):
                self.assertTrue(utils.writeToRemotePort('localhost', 1111+i, msg, 2), "Couldn't write data to localhost:%d" % (1111+i))

            # Wait until the graph has finished its execution. We'll know
            # it finished by polling the status of the session
            while SessionStates.RUNNING in testutils.get(self, '/sessions/%s/status' % (sessionId), restPort)[hostname].viewvalues():
                time.sleep(0.2)

            self.assertEqual({hostname: {hostname: SessionStates.FINISHED}}, testutils.get(self, '/sessions/%s/status' % (sessionId), restPort))
            testutils.delete(self, '/sessions/%s' % (sessionId), restPort)
            sessions = testutils.get(self, '/sessions', restPort)
            self.assertEqual(0, len(sessions))