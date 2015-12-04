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
import string
import time
import unittest

import Pyro4
import pkg_resources

from dfms import ngaslite, utils
from dfms import droputils
from dfms.ddap_protocol import DROPStates
from dfms.manager import cmdline
from dfms.manager.node_manager import NodeManager
from dfms.manager.repository import memory, sleepAndCopy
from dfms.manager.session import SessionStates


class TestDM(unittest.TestCase):

    def test_runGraphOneDOPerDOM(self):
        """
        A test that creates three DROPs in two different DMs, wire two of
        them together externally (i.e., using their proxies), and runs the graph.
        For this the graphs that are fed into the DMs must *not* express the
        inter-DM relationships. The graph looks like:

        DM #1      DM #2
        =======    =============
        | A --|----|-> B --> C |
        =======    =============
        """
        dm1 = NodeManager(1, useDLM=False)
        dm2 = NodeManager(2, useDLM=False)

        sessionId = 's1'
        g1 = [{"oid":"A", "type":"plain", "storage": "memory"}]
        g2 = [{"oid":"B", "type":"app", "app":"dfms.apps.crc.CRCApp"},
              {"oid":"C", "type":"plain", "storage": "memory", "producers":["B"]}]

        uris1 = dm1.quickDeploy(sessionId, g1)
        uris2 = dm2.quickDeploy(sessionId, g2)
        self.assertEquals(1, len(uris1))
        self.assertEquals(2, len(uris2))

        # We externally wire the Proxy objects now
        a = Pyro4.Proxy(uris1['A'])
        b = Pyro4.Proxy(uris2['B'])
        c = Pyro4.Proxy(uris2['C'])
        a.addConsumer(b)

        # Run! We wait until c is completed
        with droputils.EvtConsumerProxyCtx(self, c, 1):
            a.write('a')
            a.setCompleted()

        for drop in a, b, c:
            self.assertEquals(DROPStates.COMPLETED, drop.status)
        self.assertEquals(a.checksum, int(droputils.allDropContents(c)))

        for dropProxy in a,b,c:
            dropProxy._pyroRelease()

        dm1.destroySession(sessionId)
        dm2.destroySession(sessionId)

    def test_runGraphSeveralDropsPerDM(self):
        """
        A test that creates several DROPs in two different DMs and  runs
        the graph. The graph looks like this

        DM #1                  DM #2
        ===================    ================
        | A --> C --> D --|----|-|            |
        |                 |    | |--> E --> F |
        | B --------------|----|-|            |
        ===================    ================

        :see: `self.test_runGraphSingleDOPerDOM`
        """
        dm1 = NodeManager(1, useDLM=False)
        dm2 = NodeManager(2, useDLM=False)

        sessionId = 's1'
        g1 = [{"oid":"A", "type":"plain", "storage": "memory", "consumers":["C"]},
               {"oid":"B", "type":"plain", "storage": "memory"},
               {"oid":"C", "type":"app", "app":"dfms.apps.crc.CRCApp"},
               {"oid":"D", "type":"plain", "storage": "memory", "producers": ["C"]}]
        g2 = [{"oid":"E", "type":"app", "app":"test.test_drop.SumupContainerChecksum"},
               {"oid":"F", "type":"plain", "storage": "memory", "producers":["E"]}]

        uris1 = dm1.quickDeploy(sessionId, g1)
        uris2 = dm2.quickDeploy(sessionId, g2)
        self.assertEquals(4, len(uris1))
        self.assertEquals(2, len(uris2))

        # We externally wire the Proxy objects to establish the inter-DM
        # relationships
        a = Pyro4.Proxy(uris1['A'])
        b = Pyro4.Proxy(uris1['B'])
        c = Pyro4.Proxy(uris1['C'])
        d = Pyro4.Proxy(uris1['D'])
        e = Pyro4.Proxy(uris2['E'])
        f = Pyro4.Proxy(uris2['F'])
        for drop,uid in [(a,'A'),(b,'B'),(c,'C'),(d,'D'),(e,'E'),(f,'F')]:
            self.assertEquals(uid, drop.uid, "Proxy is not the DROP we think should be (assumed: %s/ actual: %s)" % (uid, drop.uid))
        e.addInput(d)
        e.addInput(b)

        # Run! The sole fact that this doesn't throw exceptions is already
        # a good proof that everything is working as expected
        with droputils.EvtConsumerProxyCtx(self, f, 5):
            a.write('a')
            a.setCompleted()
            b.write('a')
            b.setCompleted()

        for drop in a,b,c,d,e,f:
            self.assertEquals(DROPStates.COMPLETED, drop.status, "DROP %s is not COMPLETED" % (drop.uid))

        self.assertEquals(a.checksum, int(droputils.allDropContents(d)))
        self.assertEquals(b.checksum + d.checksum, int(droputils.allDropContents(f)))

        for dropProxy in a,b,c,d,e,f:
            dropProxy._pyroRelease()

        dm1.destroySession(sessionId)
        dm2.destroySession(sessionId)

    def test_runWithFourDMs(self):
        """
        A test that creates several DROPs in two different DMs and  runs
        the graph. The graph looks like this

                      DM #2
                     +--------------------------+
                     |        |--> C --|        |
                 +---|--> B --|--> D --|--> F --|--|
                 |   |        |--> E --|        |  |
        DM #1    |   +--------------------------+  |   DM #4
        +-----+  |                                 |  +---------------------+
        |     |  |                                 |--|--> L --|            |
        | A --|--+                                    |        |--> N --> O |
        |     |  |                                 |--|--> M --|            |
        +-----+  |    DM #3                        |  +---------------------+
                 |   +--------------------------+  |
                 |   |        |--> H --|        |  |
                 +---|--> G --|--> I --|--> K --|--|
                     |        |--> J --|        |
                     +--------------------------+

        B, F, G, K and N are AppDOs; the rest are plain in-memory DROPs
        """

        dm1 = NodeManager(1, useDLM=False)
        dm2 = NodeManager(2, useDLM=False)
        dm3 = NodeManager(3, useDLM=False)
        dm4 = NodeManager(4, useDLM=False)

        sessionId = 's1'
        g1 = [memory('A', expectedSize=1)]
        g2 = [sleepAndCopy('B', outputs=['C','D','E'], sleepTime=0),
              memory('C'),
              memory('D'),
              memory('E'),
              sleepAndCopy('F', inputs=['C','D','E'], sleepTime=0)]
        g3 = [sleepAndCopy('G', outputs=['H','I','J'], sleepTime=0),
              memory('H'),
              memory('I'),
              memory('J'),
              sleepAndCopy('K', inputs=['H','I','J'], sleepTime=0)]
        g4 = [memory('L'),
              memory('M'),
              sleepAndCopy('N', inputs=['L','M'], outputs=['O'], sleepTime=0),
              memory('O')]

        uris1 = dm1.quickDeploy(sessionId, g1)
        uris2 = dm2.quickDeploy(sessionId, g2)
        uris3 = dm3.quickDeploy(sessionId, g3)
        uris4 = dm4.quickDeploy(sessionId, g4)
        self.assertEquals(1, len(uris1))
        self.assertEquals(5, len(uris2))
        self.assertEquals(5, len(uris3))
        self.assertEquals(4, len(uris4))
        allUris = {}
        allUris.update(uris1)
        allUris.update(uris2)
        allUris.update(uris3)
        allUris.update(uris4)

        # We externally wire the Proxy objects to establish the inter-DM
        # relationships. Intra-DM relationships are already established
        proxies = {}
        for uid,uri in allUris.viewitems():
            proxies[uid] = Pyro4.Proxy(uri)

        a = proxies['A']
        b = proxies['B']
        f = proxies['F']
        g = proxies['G']
        k = proxies['K']
        l = proxies['L']
        m = proxies['M']
        o = proxies['O']

        a.addConsumer(b)
        a.addConsumer(g)
        f.addOutput(l)
        k.addOutput(m)

        # Run! This should trigger the full execution of the graph
        with droputils.EvtConsumerProxyCtx(self, o, 1):
            a.write('a')

        for dropProxy in proxies.viewvalues():
            self.assertEquals(DROPStates.COMPLETED, dropProxy.status, "Status of '%s' is not COMPLETED: %d" % (dropProxy.uid, dropProxy.status))
            dropProxy._pyroRelease()

        for dm in [dm1, dm2, dm3, dm4]:
            dm.destroySession(sessionId)

def startDM(restPort):
    # Make sure the graph executes quickly once triggered
    from test import graphsRepository
    graphsRepository.defaultSleepTime = 0
    cmdline.dfmsNM(['--no-pyro','--rest','--restPort', str(restPort),'-i','dmID', '-q'])

class TestREST(unittest.TestCase):

    def test_fullRound(self):
        """
        A test that exercises most of the REST interface exposed on top of the
        NodeManager
        """

        sessionId = 'lala'
        restPort  = 8888

        dmProcess = multiprocessing.Process(target=lambda restPort: startDM(restPort), args=[restPort])
        dmProcess.start()

        try:
            self.assertTrue(dmProcess.is_alive())

            # Wait until the REST server becomes alive
            self.assertTrue(utils.portIsOpen('localhost', restPort, 10), "REST server didn't come up in time")

            # The DM is still empty
            dmInfo = self.get('', restPort)
            self.assertEquals(0, len(dmInfo['sessions']))

            # Create a session and check it exists
            self.post('/sessions', restPort, '{"sessionId":"%s"}' % (sessionId))
            dmInfo = self.get('', restPort)
            self.assertEquals(1, len(dmInfo['sessions']))
            self.assertEquals(sessionId, dmInfo['sessions'][0]['sessionId'])
            self.assertEquals(SessionStates.PRISTINE, dmInfo['sessions'][0]['status'])

            # Add this complex graph spec to the session
            # The UID of the two leaf nodes of this complex.js graph are T and S
            # PRO-242: use timestamps for final DROPs that get archived into the public NGAS
            graph = json.loads(pkg_resources.resource_string('test', 'graphs/complex.js')) # @UndefinedVariable
            suffix = '_' + str(int(time.time()))
            oidsToReplace = ('S','T')
            for dropSpec in graph:
                if dropSpec['oid'] in oidsToReplace:
                    dropSpec['oid'] += suffix
                for rel in ('inputs','outputs'):
                    if rel in dropSpec:
                        for oid in dropSpec[rel][:]:
                            if oid in oidsToReplace:
                                dropSpec[rel].remove(oid)
                                dropSpec[rel].append(oid + suffix)

            self.post('/sessions/%s/graph/append' % (sessionId), restPort, json.dumps(graph))

            # We create two final archiving nodes, but this time from a template
            # available on the server-side
            self.post('/templates/dfms.manager.repository.archiving_app/materialize?uid=archiving1&host=ngas.ddns.net&port=7777&sessionId=%s' % (sessionId), restPort)
            self.post('/templates/dfms.manager.repository.archiving_app/materialize?uid=archiving2&host=ngas.ddns.net&port=7777&sessionId=%s' % (sessionId), restPort)

            # And link them to the leaf nodes of the complex graph
            self.post('/sessions/%s/graph/link?rhOID=archiving1&lhOID=S%s&linkType=0' % (sessionId, suffix), restPort)
            self.post('/sessions/%s/graph/link?rhOID=archiving2&lhOID=T%s&linkType=0' % (sessionId, suffix), restPort)

            # Now we deploy the graph...
            self.post('/sessions/%s/deploy' % (sessionId), restPort, 'completed=SL_A,SL_B,SL_C,SL_D,SL_K', mimeType='application/x-www-form-urlencoded')

            # ...and write to all 5 root nodes that are listening in ports
            # starting at 1111
            msg = ''.join([random.SystemRandom().choice(string.ascii_letters + string.digits) for _ in xrange(10)])
            for i in xrange(5):
                self.assertTrue(utils.writeToRemotePort('localhost', 1111+i, msg, 2), "Couldn't write data to localhost:%d" % (1111+i))

            # Wait until the graph has finished its execution. We'll know
            # it finished by polling the status of the session
            while self.get('/sessions/%s/status' % (sessionId), restPort) == SessionStates.RUNNING:
                time.sleep(0.2)

            self.assertEquals(SessionStates.FINISHED, self.get('/sessions/%s/status' % (sessionId), restPort))
            self.delete('/sessions/%s' % (sessionId), restPort)

            # We put an NGAS archiving at the end of the chain, let's check that the DROPs were copied over there
            # Since the graph consists on several SleepAndCopy apps, T should contain the message repeated
            # 9 times, and S should have it 4 times
            def checkReplica(dropId, copies):
                response = ngaslite.retrieve('ngas.ddns.net', dropId)
                buff = response.read()
                self.assertEquals(msg*copies, buff, "%s's replica doesn't look correct" % (dropId))
            checkReplica('T%s' % (suffix), 9)
            checkReplica('S%s' % (suffix), 4)

        finally:
            dmProcess.terminate()

    def get(self, url, port):
        conn = httplib.HTTPConnection('localhost', port, timeout=3)
        conn.request('GET', '/api' + url)
        res = conn.getresponse()
        self.assertEquals(httplib.OK, res.status)
        jsonRes = json.load(res)
        res.close()
        conn.close()
        return jsonRes

    def post(self, url, port, content=None, mimeType=None):
        conn = httplib.HTTPConnection('localhost', port, timeout=3)
        headers = {mimeType or 'Content-Type': 'application/json'} if content else {}
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