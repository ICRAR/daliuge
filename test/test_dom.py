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
import threading
import unittest

import Pyro4

from dfms import doutils
from dfms.ddap_protocol import DOStates
from dfms.dom.data_object_mgr import DataObjectMgr
from dfms.doutils import EvtConsumer


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
               {"oid":"C", "type":"plain", "storage": "memory", "producer":"B"}]'

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
               {"oid":"D", "type":"plain", "storage": "memory", "producer": "C"}]'
        g2 = '[{"oid":"E", "type":"app", "app":"test.test_data_object.SumupContainerChecksum"},\
               {"oid":"F", "type":"plain", "storage": "memory", "producer":"E"}]'

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
            self.assertEquals(DOStates.COMPLETED, do.status)

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