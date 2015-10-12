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
import httplib
import json
import multiprocessing
import random
import socket
import string
from test import graphsRepository
import threading
import time
import unittest

import Pyro4
from Pyro4.naming import NameServerDaemon
import pkg_resources

from dfms import doutils
from dfms.ddap_protocol import DOStates
from dfms.manager import cmdline
from dfms.manager.data_island_manager import DataIslandManager
from dfms.manager.data_object_mgr import DataObjectMgr
from dfms.manager.session import SessionStates
from dfms.utils import portIsOpen


dimId = 'lala'
hostname = socket.gethostname()

def setUpDimTests(self):

    # SleepAndCopyApps don't take time to execute
    graphsRepository.defaultSleepTime = 0

    # Start the NS
    self._nsDaemon = NameServerDaemon()
    threading.Thread(target=lambda:self._nsDaemon.requestLoop()).start()

    # Start a DOM. This is the DOM which the DIM connects to.
    #
    # We start it here to avoid the DIM connecting via SSH to the localhost
    # and spawning a dfmsDOM process; both things need proper setup which we
    # cannot do here (ssh publick key installation, ssh service up, proper
    # environment available, etc)
    #
    # Anyway, this is also useful because we can check that things have
    # occurred at the DOM level in the test cases
    domId = 'dom_' + hostname
    self.dom = DataObjectMgr(domId, False)
    self._domDaemon = Pyro4.Daemon(host='0.0.0.0', port=4000)
    self._nsDaemon.nameserver.register(domId, self._domDaemon.register(self.dom, objectId='DOM_for_test'))
    threading.Thread(target=lambda: self._domDaemon.requestLoop()).start()

    # The DIM we're testing
    self.dim = DataIslandManager(dimId, [hostname])

def tearDownDimTests(self):
    self._domDaemon.shutdown()
    self._nsDaemon.shutdown()
    while portIsOpen('localhost', Pyro4.config.NS_PORT):
        time.sleep(0.01)

class TestDIM(unittest.TestCase):

    setUp = setUpDimTests
    tearDown = tearDownDimTests

    def createSessionAndAddTypicalGraph(self, sessionId):
        graphSpec = [{'oid':'A', 'type':'plain', 'storage':'memory', 'location':hostname, 'consumers':['B']},
                     {'oid':'B', 'type':'app', 'app':'test.graphsRepository.SleepAndCopyApp', 'sleepTime':0, 'outputs':['C'], 'location':hostname},
                     {'oid':'C', 'type':'plain', 'storage':'memory', 'location':hostname}]
        self.dim.createSession(sessionId)
        self.dim.addGraphSpec(sessionId, graphSpec)

    def test_createSession(self):
        sessionId = 'lalo'
        self.dim.createSession(sessionId)
        self.assertEquals(1, len(self.dom.getSessionIds()))
        self.assertEquals(sessionId, self.dom.getSessionIds()[0])

    def test_addGraphSpec(self):

        sessionId = 'lalo'

        # No location specified
        graphSpec = [{'oid':'A', 'type':'plain', 'storage':'memory'}]
        self.assertRaises(Exception, self.dim.addGraphSpec, sessionId, graphSpec)

        # Wrong location specified
        graphSpec = [{'oid':'A', 'type':'plain', 'storage':'memory', 'location':'unknown_host'}]
        self.assertRaises(Exception, self.dim.addGraphSpec, sessionId, graphSpec)

        # OK
        graphSpec = [{'oid':'A', 'type':'plain', 'storage':'memory', 'location':hostname}]
        self.dim.createSession(sessionId)
        self.dim.addGraphSpec(sessionId, graphSpec)
        graphFromDOM = self.dom.getGraph(sessionId)
        self.assertEquals(1, len(graphFromDOM))
        doSpec = graphFromDOM.values()[0]
        self.assertEquals('A', doSpec['oid'])
        self.assertEquals('plain', doSpec['type'])
        self.assertEquals('memory', doSpec['storage'])

    def test_deployGraph(self):

        sessionId = 'lalo'
        self.createSessionAndAddTypicalGraph(sessionId)

        # Deploy now and get the uris. With that we get then A's and C's proxies
        uris = self.dim.deploySession(sessionId)
        a = Pyro4.Proxy(uris['A'])
        c = Pyro4.Proxy(uris['C'])

        data = ''.join([random.choice(string.ascii_letters + string.digits) for _ in xrange(10)])
        with doutils.EvtConsumerProxyCtx(self, c, 3):
            a.write(data)
            a.setCompleted()

        self.assertEquals(data, doutils.allDataObjectContents(c))

    def test_sessionStatus(self):

        def assertSessionStatus(sessionId, status):
            sessionStatus = self.dim.getSessionStatus(sessionId)
            self.assertEquals(1, len(sessionStatus))
            self.assertIn(hostname, sessionStatus)
            self.assertEquals(status, sessionStatus[hostname])
            self.assertEquals(status, self.dom.getSessionStatus(sessionId))

        sessionId = 'lala'
        self.dim.createSession(sessionId)
        assertSessionStatus(sessionId, SessionStates.PRISTINE)

        sessionId = 'lalo'
        self.createSessionAndAddTypicalGraph(sessionId)
        assertSessionStatus(sessionId, SessionStates.BUILDING)

        uris = self.dim.deploySession(sessionId)
        assertSessionStatus(sessionId, SessionStates.RUNNING)

        a = Pyro4.Proxy(uris['A'])
        c = Pyro4.Proxy(uris['C'])
        data = ''.join([random.choice(string.ascii_letters + string.digits) for _ in xrange(10)])
        with doutils.EvtConsumerProxyCtx(self, c, 3):
            a.write(data)
            a.setCompleted()

        assertSessionStatus(sessionId, SessionStates.FINISHED)

    def test_getGraph(self):

        sessionId = 'lalo'
        self.createSessionAndAddTypicalGraph(sessionId)

        graphSpecFromDim = self.dim.getGraph(sessionId)
        self.assertEquals(3, len(graphSpecFromDim))
        for oid in ('A','B','C'):
            self.assertIn(oid, graphSpecFromDim)
        graphSepcFromDom = self.dom.getGraph(sessionId)
        self.assertDictEqual(graphSepcFromDom, graphSpecFromDim)

    def test_getGraphStatus(self):

        def assertGraphStatus(sessionId, expectedStatus):
            graphStatusByDim = self.dim.getGraphStatus(sessionId)
            graphStatusByDom = self.dom.getGraphStatus(sessionId)
            self.assertDictEqual(graphStatusByDim, graphStatusByDom)
            for doStatus in graphStatusByDim.viewvalues():
                self.assertEquals(expectedStatus, doStatus)

        sessionId = 'lala'
        self.createSessionAndAddTypicalGraph(sessionId)
        uris = self.dim.deploySession(sessionId)
        assertGraphStatus(sessionId, DOStates.INITIALIZED)

        a = Pyro4.Proxy(uris['A'])
        c = Pyro4.Proxy(uris['C'])
        data = ''.join([random.choice(string.ascii_letters + string.digits) for _ in xrange(10)])
        with doutils.EvtConsumerProxyCtx(self, c, 3):
            a.write(data)
            a.setCompleted()
        assertGraphStatus(sessionId, DOStates.COMPLETED)


def startDIM(restPort):
    # Make sure the graph executes quickly once triggered
    cmdline.dfmsDIM(['--no-pyro','--rest','--restPort', str(restPort),'-i','dimID','-N',hostname])

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

        dimProcess = multiprocessing.Process(target=lambda restPort: startDIM(restPort), args=[restPort])
        dimProcess.start()

        try:
            self.assertTrue(dimProcess.is_alive())

            # Wait until the REST server becomes alive
            tries = 0
            while True and tries < 20: # max 10s
                try:
                    s = socket.create_connection(('localhost', restPort), 1)
                    break
                except:
                    time.sleep(0.2)
                    tries += 1
                    pass

            self.assertLess(tries, 20, "REST server didn't come up in time")

            # The DIM is still empty
            sessions = self.get('/sessions', restPort)
            self.assertEquals(0, len(sessions))
            dimStatus = self.get('', restPort)
            self.assertEquals(1, len(dimStatus['nodes']))
            self.assertEquals(hostname, dimStatus['nodes'][0])
            self.assertEquals(0, len(dimStatus['sessionIds']))

            # Create a session and check it exists
            self.post('/sessions', restPort, '{"sessionId":"%s"}' % (sessionId))
            sessions = self.get('/sessions', restPort)
            self.assertEquals(1, len(sessions))
            self.assertEquals(sessionId, sessions[0]['sessionId'])
            self.assertDictEqual({hostname: SessionStates.PRISTINE}, sessions[0]['status'])

            # Add this complex graph spec to the session
            # The UID of the two leaf nodes of this complex.js graph are T and S
            # Since the original complexGraph doesn't have location information
            # we need to add it manually before submitting -- otherwise it will
            # get rejected by the DIM.
            complexGraphSpec = json.load(pkg_resources.resource_stream(__name__, 'graphs/complex.js')) # @UndefinedVariable
            for doSpec in complexGraphSpec:
                doSpec['location'] = hostname
            self.post('/sessions/%s/graph/append' % (sessionId), restPort, json.dumps(complexGraphSpec))
            self.assertEquals({hostname: SessionStates.BUILDING}, self.get('/sessions/%s/status' % (sessionId), restPort))

            # Now we deploy the graph...
            self.post('/sessions/%s/deploy' % (sessionId), restPort)
            self.assertEquals({hostname: SessionStates.RUNNING}, self.get('/sessions/%s/status' % (sessionId), restPort))

            # ...and write to all 5 root nodes that are listening in ports
            # starting at 1111
            msg = ''.join([random.SystemRandom().choice(string.ascii_letters + string.digits) for _ in xrange(10)])
            for i in xrange(5):
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(('localhost', 1111+i))
                s.send(msg)
                s.close()

            # Wait until the graph has finished its execution. We'll know
            # it finished by polling the status of the session
            while SessionStates.RUNNING in self.get('/sessions/%s/status' % (sessionId), restPort).viewvalues():
                time.sleep(0.2)

            self.assertEquals({hostname: SessionStates.FINISHED}, self.get('/sessions/%s/status' % (sessionId), restPort))
            self.delete('/sessions/%s' % (sessionId), restPort)
            sessions = self.get('/sessions', restPort)
            self.assertEquals(0, len(sessions))

        finally:
            dimProcess.terminate()

    def get(self, url, port):
        conn = httplib.HTTPConnection('localhost', port, timeout=3)
        conn.request('GET', '/api' + url)
        res = conn.getresponse()
        self.assertEquals(httplib.OK, res.status)
        jsonRes = json.load(res)
        res.close()
        conn.close()
        return jsonRes

    def post(self, url, port, content=None):
        conn = httplib.HTTPConnection('localhost', port, timeout=3)
        headers = {'Content-Type': 'application/json'} if content else {}
        conn.request('POST', '/api' + url, content, headers)
        res = conn.getresponse()
        self.assertEquals(httplib.OK, res.status)
        conn.close()

    def delete(self, url, port):
        conn = httplib.HTTPConnection('localhost', port, timeout=3)
        conn.request('DELETE', '/api' + url)
        res = conn.getresponse()
        self.assertEquals(httplib.OK, res.status)
        conn.close()