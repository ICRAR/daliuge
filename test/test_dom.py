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
import socket
from test import graphsRepository
import threading
import time
import unittest

import Pyro4
import pkg_resources

from dfms import doutils, ngaslite
from dfms.ddap_protocol import DOStates
from dfms.dom import cmdline
from dfms.dom.data_object_mgr import DataObjectMgr
from dfms.dom.session import SessionStates
from dfms.doutils import EvtConsumer
import string
import random


class EvtConsumerProxyCtx(object):
    def __init__(self, test, dos, timeout=1):
        self._dos = doutils.listify(dos)
        self._test = test
        self._timeout = timeout
        self._evts = []
    def __enter__(self):
        daemon = Pyro4.Daemon()
        t = threading.Thread(None, lambda: daemon.requestLoop())
        t.daemon = 1
        t.start()

        for do in self._dos:
            evt = threading.Event()
            consumer = EvtConsumer(evt)
            uri = daemon.register(consumer)
            consumerProxy = Pyro4.Proxy(uri)
            do.addConsumer(consumerProxy)
            self._evts.append(evt)

        self.daemon = daemon
        self.t = t
        return self
    def __exit__(self, typ, value, traceback):
        to = self._timeout
        allFine = True
        try:
            for evt in self._evts:
                self._test.assertTrue(evt.wait(to), "Waiting for DO failed with timeout %d" % to)
        except:
            allFine = False
        finally:
            self.daemon.shutdown()
            self.t.join(to)
            if allFine:
                self._test.assertFalse(self.t.isAlive())

class TestDOM(unittest.TestCase):

    def test_runGraphOneDOPerDOM(self):
        """
        A test that creates three DataObjects in two different DOMs, wire two of
        them together externally (i.e., using their proxies), and runs the graph.
        For this the graphs that are fed into the DOMs must *not* express the
        inter-DOM relationships. The graph looks like:

        DOM #1     DOM #2
        =======    =============
        | A --|----|-> B --> C |
        =======    =============
        """
        dom1 = DataObjectMgr(1, useDLM=False)
        dom2 = DataObjectMgr(2, useDLM=False)

        sessionId = 's1'
        g1 = '[{"oid":"A", "type":"plain", "storage": "memory"}]'
        g2 = '[{"oid":"B", "type":"app", "app":"dfms.data_object.CRCAppDataObject"},\
               {"oid":"C", "type":"plain", "storage": "memory", "producers":["B"]}]'

        uris1 = dom1.quickDeploy(sessionId, g1)
        uris2 = dom2.quickDeploy(sessionId, g2)
        self.assertEquals(1, len(uris1))
        self.assertEquals(2, len(uris2))

        # We externally wire the Proxy objects now
        a = Pyro4.Proxy(uris1[0])
        b = Pyro4.Proxy(uris2[0])
        c = Pyro4.Proxy(uris2[1])
        a.addConsumer(b)

        # Run! We wait until c is completed
        with EvtConsumerProxyCtx(self, c, 1):
            a.write('a')
            a.setCompleted()

        for do in a, b, c:
            self.assertEquals(DOStates.COMPLETED, do.status)
        self.assertEquals(a.checksum, int(doutils.allDataObjectContents(c)))

        for doProxy in a,b,c:
            doProxy._pyroRelease()

        dom1.destroySession(sessionId)
        dom2.destroySession(sessionId)

    def test_runGraphSeveralDOsPerDOM(self):
        """
        A test that creates several DataObjects in two different DOMs and  runs
        the graph. The graph looks like this

        DOM #1                 DOM #2
        ===================    ================
        | A --> C --> D --|----|-|            |
        |                 |    | |--> E --> F |
        | B --------------|----|-|            |
        ===================    ================

        :see: `self.test_runGraphSingleDOPerDOM`
        """
        dom1 = DataObjectMgr(1, useDLM=False)
        dom2 = DataObjectMgr(2, useDLM=False)

        sessionId = 's1'
        g1 = '[{"oid":"A", "type":"plain", "storage": "memory", "consumers":["C"]},\
               {"oid":"B", "type":"plain", "storage": "memory"},\
               {"oid":"C", "type":"app", "app":"dfms.data_object.CRCAppDataObject"},\
               {"oid":"D", "type":"plain", "storage": "memory", "producers": ["C"]}]'
        g2 = '[{"oid":"E", "type":"app", "app":"test.test_data_object.SumupContainerChecksum"},\
               {"oid":"F", "type":"plain", "storage": "memory", "producers":["E"]}]'

        uris1 = dom1.quickDeploy(sessionId, g1)
        uris2 = dom2.quickDeploy(sessionId, g2)
        self.assertEquals(4, len(uris1))
        self.assertEquals(2, len(uris2))

        # We externally wire the Proxy objects to establish the inter-DOM
        # relationships
        a = Pyro4.Proxy(uris1[0])
        b = Pyro4.Proxy(uris1[1])
        c = Pyro4.Proxy(uris1[2])
        d = Pyro4.Proxy(uris1[3])
        e = Pyro4.Proxy(uris2[0])
        f = Pyro4.Proxy(uris2[1])
        for do,uid in [(a,'A'),(b,'B'),(c,'C'),(d,'D'),(e,'E'),(f,'F')]:
            self.assertEquals(uid, do.uid, "Proxy is not the DO we think should be (assumed: %s/ actual: %s)" % (uid, do.uid))
        e.addInput(d)
        e.addInput(b)

        # Run! The sole fact that this doesn't throw exceptions is already
        # a good proof that everything is working as expected
        with EvtConsumerProxyCtx(self, f, 5):
            a.write('a')
            a.setCompleted()
            b.write('a')
            b.setCompleted()

        for do in a,b,c,d,e,f:
            self.assertEquals(DOStates.COMPLETED, do.status, "DO %s is not COMPLETED" % (do.uid))

        self.assertEquals(a.checksum, int(doutils.allDataObjectContents(d)))
        self.assertEquals(b.checksum + d.checksum, int(doutils.allDataObjectContents(f)))

        for doProxy in a,b,c,d,e,f:
            doProxy._pyroRelease()

        dom1.destroySession(sessionId)
        dom2.destroySession(sessionId)

    def test_runWithFourDOMs(self):
        """
        A test that creates several DataObjects in two different DOMs and  runs
        the graph. The graph looks like this

                     DOM #2
                     +--------------------------+
                     |        |--> C --|        |
                 +---|--> B --|--> D --|--> F --|--|
                 |   |        |--> E --|        |  |
        DOM #1   |   +--------------------------+  |  DOM #4
        +-----+  |                                 |  +---------------------+
        |     |  |                                 |--|--> L --|            |
        | A --|--+                                    |        |--> N --> O |
        |     |  |                                 |--|--> M --|            |
        +-----+  |   DOM #3                        |  +---------------------+
                 |   +--------------------------+  |
                 |   |        |--> H --|        |  |
                 +---|--> G --|--> I --|--> K --|--|
                     |        |--> J --|        |
                     +--------------------------+

        B, F, G, K and N are AppDOs; the rest are plain in-memory DOs
        """

        dom1 = DataObjectMgr(1, useDLM=False)
        dom2 = DataObjectMgr(2, useDLM=False)
        dom3 = DataObjectMgr(3, useDLM=False)
        dom4 = DataObjectMgr(4, useDLM=False)

        # The SumUpContainerChecksum is a BarrierAppDO
        sumCRCCAppSpec = '"type":"app", "app":"test.graphsRepository.SleepAndCopyApp", "sleepTime": 0'
        memoryDOSpec   = '"type":"plain", "storage":"memory"'

        sessionId = 's1'
        g1 = '[{{"oid":"A", {0}, "expectedSize":1}}]'.format(memoryDOSpec)
        g2 = '[{{"oid":"B", {0}, "outputs":["C","D","E"]}},\
               {{"oid":"C", {1}}},\
               {{"oid":"D", {1}}},\
               {{"oid":"E", {1}}},\
               {{"oid":"F", {0}, "inputs":["C","D","E"]}}]'.format(sumCRCCAppSpec, memoryDOSpec)
        g3 = '[{{"oid":"G", {0}, "outputs":["H","I","J"]}},\
               {{"oid":"H", {1}}},\
               {{"oid":"I", {1}}},\
               {{"oid":"J", {1}}},\
               {{"oid":"K", {0}, "inputs":["H","I","J"]}}]'.format(sumCRCCAppSpec, memoryDOSpec)
        g4 = '[{{"oid":"L", {1}}},\
               {{"oid":"M", {1}}},\
               {{"oid":"N", {0}, "inputs":["L","M"], "outputs":["O"]}},\
               {{"oid":"O", {1}}}]'.format(sumCRCCAppSpec, memoryDOSpec)

        uris1 = dom1.quickDeploy(sessionId, g1)
        uris2 = dom2.quickDeploy(sessionId, g2)
        uris3 = dom3.quickDeploy(sessionId, g3)
        uris4 = dom4.quickDeploy(sessionId, g4)
        self.assertEquals(1, len(uris1))
        self.assertEquals(5, len(uris2))
        self.assertEquals(5, len(uris3))
        self.assertEquals(4, len(uris4))

        # We externally wire the Proxy objects to establish the inter-DOM
        # relationships. Intra-DOM relationships are already established
        proxies = []
        for uri in uris1 + uris2 + uris3 + uris4:
            proxies.append(Pyro4.Proxy(uri))

        a = proxies[0]
        b = proxies[1]
        f = proxies[5]
        g = proxies[6]
        k = proxies[10]
        l = proxies[11]
        m = proxies[12]
        o = proxies[14]

        a.addConsumer(b)
        a.addConsumer(g)
        f.addOutput(l)
        k.addOutput(m)

        # Run! This should trigger the full execution of the graph
        with EvtConsumerProxyCtx(self, o, 1):
            a.write('a')

        for doProxy in proxies:
            self.assertEquals(DOStates.COMPLETED, doProxy.status, "Status of '%s' is not COMPLETED" % doProxy.uid)
            doProxy._pyroRelease()

        for dom in [dom1, dom2, dom3, dom4]:
            dom.destroySession(sessionId)

def startDOM(restPort):
    # Make sure the graph executes quickly once triggered
    graphsRepository.defaultSleepTime = 0
    cmdline.main(['--no-pyro','--rest','--restPort', str(restPort),'-i','domID'])

class TestREST(unittest.TestCase):

    def test_fullRound(self):
        """
        A test that exercises most of the REST interface exposed on top of the
        DataObjectManager
        """

        sessionId = 'lala'
        restPort  = 8888

        domProcess = multiprocessing.Process(target=lambda restPort: startDOM(restPort), args=[restPort])
        domProcess.start()

        try:
            self.assertTrue(domProcess.is_alive())

            # Wait until the REST server becomes alive
            tries = 0
            while True and tries < 20: # max 10s
                try:
                    s = socket.create_connection(('localhost', restPort), 1)
                    break
                except:
                    time.sleep(1)
                    tries += 1
                    pass

            self.assertLess(tries, 20, "REST server didn't come up in time")

            # The DOM is still empty
            domInfo = self.get('', restPort)
            self.assertEquals(0, len(domInfo['sessions']))

            # Create a session and check it exists
            self.post('/sessions', restPort, '{"sessionId":"%s"}' % (sessionId))
            domInfo = self.get('', restPort)
            self.assertEquals(1, len(domInfo['sessions']))
            self.assertEquals(sessionId, domInfo['sessions'][0]['sessionId'])
            self.assertEquals(SessionStates.PRISTINE, domInfo['sessions'][0]['status'])

            # Add this complex graph spec to the session
            # The UID of the two leaf nodes of this complex.js graph are T and S
            self.post('/sessions/%s/graph/parts' % (sessionId), restPort, pkg_resources.resource_string(__name__, 'graphs/complex.js'))  # @UndefinedVariable

            # We create two final archiving nodes, but this time from a template
            # available on the server-side
            self.post('/templates/dfms.dom.repository.archiving_app/materialize?uid=archiving1&host=ngas.ddns.net&port=7777&sessionId=%s' % (sessionId), restPort)
            self.post('/templates/dfms.dom.repository.archiving_app/materialize?uid=archiving2&host=ngas.ddns.net&port=7777&sessionId=%s' % (sessionId), restPort)

            # And link them to the leaf nodes of the complex graph
            self.post('/sessions/%s/graph/link?rhOID=archiving1&lhOID=S&linkType=0' % (sessionId), restPort)
            self.post('/sessions/%s/graph/link?rhOID=archiving2&lhOID=T&linkType=0' % (sessionId), restPort)

            # Now we deploy the graph...
            self.post('/sessions/%s/deploy' % (sessionId), restPort)

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
            while self.get('/sessions/%s/status' % (sessionId), restPort) == SessionStates.RUNNING:
                time.sleep(1)

            self.assertEquals(SessionStates.FINISHED, self.get('/sessions/%s/status' % (sessionId), restPort))
            self.delete('/sessions/%s' % (sessionId), restPort)

            # We put an NGAS archiving at the end of the chain, let's check that the DOs were copied over there
            # Since the graph consists on several SleepAndCopy apps, T should contain the message repeated
            # 9 times, and S should have it 4 times
            def checkReplica(doId, copies):
                response = ngaslite.retrieve('ngas.ddns.net', doId)
                buff = response.read()
                self.assertEquals(msg*copies, buff, "%s's replica doesn't look correct" % (doId))
            checkReplica('T', 9)
            checkReplica('S', 4)

        finally:
            domProcess.terminate()

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