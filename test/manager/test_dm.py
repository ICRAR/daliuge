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
import collections
import json
import os
import subprocess
import sys
import threading
import time
import unittest

import pkg_resources

from dfms import droputils
from dfms import ngaslite, utils
from dfms.ddap_protocol import DROPStates, DROPRel, DROPLinkType
from dfms.drop import BarrierAppDROP
from dfms.manager.node_manager import NodeManager
from dfms.manager.repository import memory, sleepAndCopy
from dfms.manager.session import SessionStates
from test.manager import testutils


hostname = 'localhost'

def quickDeploy(nm, sessionId, graphSpec, node_subscriptions={}):
    nm.createSession(sessionId)
    nm.addGraphSpec(sessionId, graphSpec)
    nm.add_node_subscriptions(sessionId, node_subscriptions)
    return nm.deploySession(sessionId)

class ErroneousApp(BarrierAppDROP):
    def run(self):
        raise Exception("Sorry, we always fail")

def nm_conninfo(n):
    return 'localhost', 5553 + n, 6666 + n

class TestDM(unittest.TestCase):

    def __init__(self, methodName):
        super(TestDM, self).__init__(methodName)
        self._dms = []

    def _start_dm(self, **kwargs):
        host, events_port, rpc_port = nm_conninfo(len(self._dms))
        nm = NodeManager(useDLM=False, host=host, events_port = events_port, rpc_port = rpc_port, **kwargs)
        self._dms.append(nm)
        return nm

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        for nm in self._dms:
            nm.shutdown()

    def test_error_listener(self):

        evt = threading.Event()
        erroneous_drops = []
        class listener(object):
            def on_error(self, drop):
                erroneous_drops.append(drop.uid)
                if len(erroneous_drops) == 2: # both 'C' and 'B' failed already
                    evt.set()

        sessionId = 'lala'
        dm = self._start_dm(error_listener=listener())
        g = [{"oid":"A", "type":"plain", "storage": "memory"},
             {"oid":"B", "type":"app", "app":"test.manager.test_dm.ErroneousApp", "inputs": ["A"]},
             {"oid":"C", "type":"plain", "storage": "memory", "producers":["B"]}]
        dm.createSession(sessionId)
        dm.addGraphSpec(sessionId, g)
        dm.deploySession(sessionId, ["A"])

        self.assertTrue(evt.wait(10), "Didn't receive errors on time")

    def test_runGraphOneDOPerDOM(self):
        """
        A test that creates three DROPs in two different DMs and runs the graph.
        For this the graphs that are fed into the DMs must *not* express the
        inter-DM relationships, although they are still passed down
        separately. The graph looks like:

        DM #1      DM #2
        =======    =============
        | A --|----|-> B --> C |
        =======    =============
        """
        dm1, dm2 = [self._start_dm() for _ in range(2)]

        sessionId = 's1'
        g1 = [{"oid":"A", "type":"plain", "storage": "memory"}]
        g2 = [{"oid":"B", "type":"app", "app":"dfms.apps.crc.CRCApp"},
              {"oid":"C", "type":"plain", "storage": "memory", "producers":["B"]}]

        rels = [DROPRel('B', DROPLinkType.CONSUMER, 'A')]
        uris1 = quickDeploy(dm1, sessionId, g1, {nm_conninfo(1): rels})
        uris2 = quickDeploy(dm2, sessionId, g2, {nm_conninfo(0): rels})
        self.assertEqual(1, len(uris1))
        self.assertEqual(2, len(uris2))

        # Run! We wait until c is completed
        a = dm1._sessions[sessionId].drops['A']
        b,c = [dm2._sessions[sessionId].drops[x] for x in ('B', 'C')]
        with droputils.DROPWaiterCtx(self, c, 1):
            a.write('a')
            a.setCompleted()

        for drop in a, b, c:
            self.assertEqual(DROPStates.COMPLETED, drop.status)
        self.assertEqual(a.checksum, int(droputils.allDropContents(c)))

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
        dm1, dm2 = [self._start_dm() for _ in range(2)]

        sessionId = 's1'
        g1 = [{"oid":"A", "type":"plain", "storage": "memory", "consumers":["C"]},
              {"oid":"B", "type":"plain", "storage": "memory"},
              {"oid":"C", "type":"app", "app":"dfms.apps.crc.CRCApp"},
              {"oid":"D", "type":"plain", "storage": "memory", "producers": ["C"]}]
        g2 = [{"oid":"E", "type":"app", "app":"test.test_drop.SumupContainerChecksum"},
              {"oid":"F", "type":"plain", "storage": "memory", "producers":["E"]}]

        rels = [DROPRel('D', DROPLinkType.INPUT, 'E'),
                DROPRel('B', DROPLinkType.INPUT, 'E')]
        uris1 = quickDeploy(dm1, sessionId, g1, {nm_conninfo(1): rels})
        uris2 = quickDeploy(dm2, sessionId, g2, {nm_conninfo(0): rels})

        self.assertEqual(4, len(uris1))
        self.assertEqual(2, len(uris2))

        # Run! The sole fact that this doesn't throw exceptions is already
        # a good proof that everything is working as expected
        a,b,c,d = [dm1._sessions[sessionId].drops[x] for x in ('A', 'B', 'C', 'D')]
        e,f = [dm2._sessions[sessionId].drops[x] for x in ('E', 'F')]
        with droputils.DROPWaiterCtx(self, f, 5):
            a.write('a')
            a.setCompleted()
            b.write('a')
            b.setCompleted()

        for drop in a,b,c,d,e,f:
            self.assertEqual(DROPStates.COMPLETED, drop.status, "DROP %s is not COMPLETED" % (drop.uid))

        self.assertEqual(a.checksum, int(droputils.allDropContents(d)))
        self.assertEqual(b.checksum + d.checksum, int(droputils.allDropContents(f)))

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

        dm1, dm2, dm3, dm4 = [self._start_dm() for _ in range(4)]

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

        rels_12 = [DROPRel('A', DROPLinkType.INPUT, 'B')]
        rels_13 = [DROPRel('A', DROPLinkType.INPUT, 'G')]
        rels_24 = [DROPRel('F', DROPLinkType.PRODUCER, 'L')]
        rels_34 = [DROPRel('K', DROPLinkType.PRODUCER, 'M')]
        uris1 = quickDeploy(dm1, sessionId, g1, {nm_conninfo(1): rels_12, nm_conninfo(2): rels_13})
        uris2 = quickDeploy(dm2, sessionId, g2, {nm_conninfo(0): rels_12, nm_conninfo(3): rels_24})
        uris3 = quickDeploy(dm3, sessionId, g3, {nm_conninfo(0): rels_13, nm_conninfo(3): rels_34})
        uris4 = quickDeploy(dm4, sessionId, g4, {nm_conninfo(1): rels_24, nm_conninfo(2): rels_34})

        self.assertEqual(1, len(uris1))
        self.assertEqual(5, len(uris2))
        self.assertEqual(5, len(uris3))
        self.assertEqual(4, len(uris4))

        a = dm1._sessions[sessionId].drops['A']
        o = dm4._sessions[sessionId].drops['O']
        drops = []
        for x in (dm1, dm2, dm3, dm4):
            drops += x._sessions[sessionId].drops.values()

        # Run! This should trigger the full execution of the graph
        with droputils.DROPWaiterCtx(self, o, 5):
            a.write('a')

        for drop in drops:
            self.assertEqual(DROPStates.COMPLETED, drop.status, "Status of '%s' is not COMPLETED: %d" % (drop.uid, drop.status))

        for dm in [dm1, dm2, dm3, dm4]:
            dm.destroySession(sessionId)

    def test_many_relationships(self):
        """
        A test in which a drop is related to many other drops that live in a
        separate DM.

        Drop A is accessed by many applications (B1, B2, .., BN), which should
        not exhaust resources on DM #1. We collapse all into C so we can monitor
        only its status to know that the execution is over.

        DM #1                     DM #2
        =======    ====================
        |     |    | |--> B1 --|      |
        |     |    | |--> B2 --|      |
        | A --|----|-|--> B3 --|--> C |
        |     |    | |.........|      |
        |     |    | |--> BN --|      |
        =======    ====================
        """

        dm1, dm2 = [self._start_dm() for _ in range(2)]

        sessionId = 's1'
        N = 100
        g1 = [{"oid":"A", "type":"plain", "storage": "memory"}]
        g2 = [{"oid":"C", "type":"plain", "storage": "memory"}]
        rels = []
        for i in range(N):
            b_oid = "B%d" % (i,)
            # SleepAndCopyApp effectively opens the input drop
            g2.append({"oid":b_oid, "type":"app", "app":"test.graphsRepository.SleepAndCopyApp", "outputs":["C"], "sleepTime": 0})
            rels.append(DROPRel('A', DROPLinkType.INPUT, b_oid))

        uris1 = quickDeploy(dm1, sessionId, g1, {nm_conninfo(1): rels})
        uris2 = quickDeploy(dm2, sessionId, g2, {nm_conninfo(0): rels})
        self.assertEqual(1,   len(uris1))
        self.assertEqual(1+N, len(uris2))

        # Run! The sole fact that this doesn't throw exceptions is already
        # a good proof that everything is working as expected
        a = dm1._sessions[sessionId].drops['A']
        c = dm2._sessions[sessionId].drops['C']
        with droputils.DROPWaiterCtx(self, c, 30):
            a.write('a')
            a.setCompleted()

        for i in range(N):
            drop = dm2._sessions[sessionId].drops["B%d" % (i,)]
            self.assertEqual(DROPStates.COMPLETED, drop.status)
        dm1.destroySession(sessionId)
        dm2.destroySession(sessionId)

class TestREST(unittest.TestCase):

    def test_fullRound(self):
        """
        A test that exercises most of the REST interface exposed on top of the
        NodeManager
        """

        sessionId = 'lala'
        restPort  = 8888

        args = [sys.executable, '-m', 'dfms.manager.cmdline', 'dfmsNM', \
                '--port', str(restPort), '-qqq']
        dmProcess = subprocess.Popen(args)

        with testutils.terminating(dmProcess, 10):

            # Wait until the REST server becomes alive
            self.assertTrue(utils.portIsOpen('localhost', restPort, 10), "REST server didn't come up in time")

            # The DM is still empty
            dmInfo = testutils.get(self, '', restPort)
            self.assertEqual(0, len(dmInfo['sessions']))

            # Create a session and check it exists
            testutils.post(self, '/sessions', restPort, '{"sessionId":"%s"}' % (sessionId))
            dmInfo = testutils.get(self, '', restPort)
            self.assertEqual(1, len(dmInfo['sessions']))
            self.assertEqual(sessionId, dmInfo['sessions'][0]['sessionId'])
            self.assertEqual(SessionStates.PRISTINE, dmInfo['sessions'][0]['status'])

            # Add this complex graph spec to the session
            # The UID of the two leaf nodes of this complex.js graph are T and S
            # PRO-242: use timestamps for final DROPs that get archived into the public NGAS
            with pkg_resources.resource_stream('test', 'graphs/complex.js') as f: # @UndefinedVariable
                graph = json.load(codecs.getreader('utf-8')(f))
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

            testutils.post(self, '/sessions/%s/graph/append' % (sessionId), restPort, json.dumps(graph))

            # We create two final archiving nodes, but this time from a template
            # available on the server-side
            timeout = 10
            testutils.post(self, '/templates/dfms.manager.repository.archiving_app/materialize?uid=archiving1&host=ngas.ddns.net&port=7777&sessionId=%s&connect_timeout=%f&timeout=%f' % (sessionId, timeout, timeout), restPort)
            testutils.post(self, '/templates/dfms.manager.repository.archiving_app/materialize?uid=archiving2&host=ngas.ddns.net&port=7777&sessionId=%s&connect_timeout=%f&timeout=%f' % (sessionId, timeout, timeout), restPort)

            # And link them to the leaf nodes of the complex graph
            testutils.post(self, '/sessions/%s/graph/link?rhOID=archiving1&lhOID=S%s&linkType=0' % (sessionId, suffix), restPort)
            testutils.post(self, '/sessions/%s/graph/link?rhOID=archiving2&lhOID=T%s&linkType=0' % (sessionId, suffix), restPort)

            # Now we deploy the graph...
            testutils.post(self, '/sessions/%s/deploy' % (sessionId), restPort, 'completed=SL_A,SL_B,SL_C,SL_D,SL_K', mimeType='application/x-www-form-urlencoded')

            # ...and write to all 5 root nodes that are listening in ports
            # starting at 1111
            msg = os.urandom(10)
            for i in range(5):
                self.assertTrue(utils.writeToRemotePort('localhost', 1111+i, msg, 2), "Couldn't write data to localhost:%d" % (1111+i))

            # Wait until the graph has finished its execution. We'll know
            # it finished by polling the status of the session
            while testutils.get(self, '/sessions/%s/status' % (sessionId), restPort) == SessionStates.RUNNING:
                time.sleep(0.2)

            self.assertEqual(SessionStates.FINISHED, testutils.get(self, '/sessions/%s/status' % (sessionId), restPort))
            testutils.delete(self, '/sessions/%s' % (sessionId), restPort)

            # We put an NGAS archiving at the end of the chain, let's check that the DROPs were copied over there
            # Since the graph consists on several SleepAndCopy apps, T should contain the message repeated
            # 9 times, and S should have it 4 times
            def checkReplica(dropId, copies):
                response = ngaslite.retrieve('ngas.ddns.net', dropId)
                buff = response.read()
                self.assertEqual(msg*copies, buff, "%s's replica doesn't look correct" % (dropId))
            checkReplica('T%s' % (suffix), 9)
            checkReplica('S%s' % (suffix), 4)