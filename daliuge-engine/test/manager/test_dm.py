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
import copy
import os
import sys
import threading
import unittest
from time import sleep
import multiprocessing
import random

from dlg import droputils
from dlg.common import dropdict
from dlg.ddap_protocol import DROPStates, DROPRel, DROPLinkType
from dlg.apps.app_base import BarrierAppDROP
from dlg.manager.node_manager import NodeManager
from dlg.manager.manager_data import Node

from test.dlg_engine_testconstants import DEFAULT_TEST_REPRO, DEFAULT_TEST_GRAPH_REPRO
from test.dlg_engine_testutils import DROPManagerUtils, NMTestsMixIn

try:
    from crc32c import crc32c  # @UnusedImport
except:
    from binascii import crc32  # @Reimport

random.seed(42)

hostname = "localhost"

def memory(uid, **kwargs):
    dropSpec = dropdict(
        {
            "oid": uid,
            "categoryType": "Data",
            "dropclass": "dlg.data.drops.memory.InMemoryDROP",
            "reprodata": DEFAULT_TEST_REPRO.copy(),
        }
    )
    dropSpec.update(kwargs)
    return dropSpec


def sleepAndCopy(uid, **kwargs):
    dropSpec = dropdict(
        {
            "oid": uid,
            "categoryType": "Application",
            "dropclass": "dlg.apps.simple.SleepAndCopyApp",
            "reprodata": DEFAULT_TEST_REPRO.copy(),
        }
    )
    dropSpec.update(kwargs)
    return dropSpec

class ErroneousApp(BarrierAppDROP):
    def run(self):
        raise Exception("Sorry, we always fail")


class NodeManagerTestsBase(NMTestsMixIn):
    def _deploy_error_graph(self, **kwargs):
        sessionId = f"s{random.randint(0, 1000)}"
        g = [
            memory("A"),
            {
                "oid": "B",
                "categoryType": "Application",
                "dropclass": "test.manager.test_dm.ErroneousApp",
                "inputs": ["A"],
            },
            memory("C", producers=["B"]),
        ]
        DROPManagerUtils.add_test_reprodata(g)
        dm = self._start_dm(**kwargs)
        dm.createSession(sessionId)
        dm.addGraphSpec(sessionId, g)
        dm.deploySession(sessionId, ["A"])

    def test_error_listener(self):
        evt = threading.Event()
        erroneous_drops = []

        class listener(object):
            def on_error(self, drop):
                erroneous_drops.append(drop.uid)
                if (
                        len(erroneous_drops) == 2
                ):  # both 'C' and 'B' failed already
                    evt.set()

        self._deploy_error_graph(error_listener=listener())
        self.assertTrue(evt.wait(10), "Didn't receive errors on time")

    def test_event_listener(self):
        """Tests that user-provided event listeners work"""

        evt = threading.Event()

        class listener(object):
            def __init__(self):
                self.recv = 0

            def handleEvent(self, _evt):
                self.recv += 1
                if self.recv == 3:
                    evt.set()

        self._deploy_error_graph(event_listeners=[listener()])
        self.assertTrue(evt.wait(10), "Didn't receive events on time")

    def _test_runGraphOneDOPerDOM(self, repeats=1):
        g1 = [memory("A")]
        g2 = [
            {
                "oid": "B",
                "categoryType": "Application",
                "dropclass": "dlg.apps.crc.CRCApp",
            },
            memory("C", producers=["B"]),
        ]
        rels = [DROPRel("B", DROPLinkType.CONSUMER, "A")]
        a_data = os.urandom(32)
        c_data = str(crc32c(a_data, 0)).encode("utf8")
        node_managers = [
            self._start_dm() for _ in range(2)
        ]
        ids = [0] * repeats
        for n in range(repeats):
            choice = 0
            while choice in ids:
                choice = random.randint(0, 1000)
            ids[n] = choice
            sessionId = f"s{choice}"
            self._test_runGraphInTwoNMs(
                copy.deepcopy(g1),
                copy.deepcopy(g2),
                rels,
                a_data,
                c_data,
                sessionId=sessionId,
                node_managers=node_managers,
            )

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
        self._test_runGraphOneDOPerDOM()

    def test_runGraphOneDOPerDOMTwice(self):
        """Like test_runGraphOneDOPerDOM but runs two sessions succesively"""
        self._test_runGraphOneDOPerDOM(2)

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

        sessionId = "s1"
        g1 = [
            memory("A", consumers=["C"]),
            memory("B"),
            {
                "oid": "C",
                "categoryType": "Application",
                "dropclass": "dlg.apps.crc.CRCApp",
            },
            memory("D", producers=["C"]),
        ]
        g2 = [
            {
                "oid": "E",
                "categoryType": "Application",
                "dropclass": "test.test_drop.SumupContainerChecksum",
            },
            memory("F", producers=["E"]),
        ]
        DROPManagerUtils.add_test_reprodata(g1)
        DROPManagerUtils.add_test_reprodata(g2)
        rels = [
            DROPRel("D", DROPLinkType.INPUT, "E"),
            DROPRel("B", DROPLinkType.INPUT, "E"),
        ]
        DROPManagerUtils.quickDeploy(dm1, sessionId, g1, {DROPManagerUtils.nm_conninfo(1): rels})
        DROPManagerUtils.quickDeploy(dm2, sessionId, g2, {DROPManagerUtils.nm_conninfo(0): rels})

        self.assertEqual(4, len(dm1.sessions[sessionId].drops))
        self.assertEqual(2, len(dm2.sessions[sessionId].drops))

        # Run! The sole fact that this doesn't throw exceptions is already
        # a good proof that everything is working as expected
        a, b, c, d = [
            dm1.sessions[sessionId].drops[x] for x in ("A", "B", "C", "D")
        ]
        e, f = [dm2.sessions[sessionId].drops[x] for x in ("E", "F")]
        with droputils.DROPWaiterCtx(self, f, 5):
            a.write(b"a")
            a.setCompleted()
            b.write(b"a")
            b.setCompleted()

        for drop in a, b, c, d, e, f:
            self.assertEqual(
                DROPStates.COMPLETED,
                drop.status,
                "DROP %s is not COMPLETED" % (drop.uid),
            )

        self.assertEqual(a.checksum, int(droputils.allDropContents(d)))
        self.assertEqual(
            b.checksum + d.checksum, int(droputils.allDropContents(f))
        )

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

        dm1, dm2, dm3, dm4 = [
            self._start_dm() for _ in range(4)
        ]

        sessionId = f"s{random.randint(0, 1000)}"
        g1 = [memory("A", expectedSize=1)]
        g2 = [
            sleepAndCopy("B", outputs=["C", "D", "E"], sleepTime=0),
            memory("C"),
            memory("D"),
            memory("E"),
            sleepAndCopy("F", inputs=["C", "D", "E"], sleepTime=0),
        ]
        g3 = [
            sleepAndCopy("G", outputs=["H", "I", "J"], sleepTime=0),
            memory("H"),
            memory("I"),
            memory("J"),
            sleepAndCopy("K", inputs=["H", "I", "J"], sleepTime=0),
        ]
        g4 = [
            memory("L"),
            memory("M"),
            sleepAndCopy("N", inputs=["L", "M"], outputs=["O"], sleepTime=0),
            memory("O"),
        ]
        for g in [g1, g2, g3, g4]:
            DROPManagerUtils.add_test_reprodata(g)
        rels_12 = [DROPRel("A", DROPLinkType.INPUT, "B")]
        rels_13 = [DROPRel("A", DROPLinkType.INPUT, "G")]
        rels_24 = [DROPRel("F", DROPLinkType.PRODUCER, "L")]
        rels_34 = [DROPRel("K", DROPLinkType.PRODUCER, "M")]
        DROPManagerUtils.quickDeploy(
            dm1,
            sessionId,
            g1,
            {
                DROPManagerUtils.nm_conninfo(1): rels_12,
                DROPManagerUtils.nm_conninfo(2): rels_13
            },
        )
        DROPManagerUtils.quickDeploy(
            dm2,
            sessionId,
            g2,
            {
                DROPManagerUtils.nm_conninfo(0): rels_12,
                DROPManagerUtils.nm_conninfo(3): rels_24
            },
        )
        DROPManagerUtils.quickDeploy(
            dm3,
            sessionId,
            g3,
            {
                DROPManagerUtils.nm_conninfo(0): rels_13,
                DROPManagerUtils.nm_conninfo(3): rels_34},
        )
        DROPManagerUtils.quickDeploy(
            dm4,
            sessionId,
            g4,
            {
                DROPManagerUtils.nm_conninfo(1): rels_24,
                DROPManagerUtils.nm_conninfo(2): rels_34},
        )

        self.assertEqual(1, len(dm1.sessions[sessionId].drops))
        self.assertEqual(5, len(dm2.sessions[sessionId].drops))
        self.assertEqual(5, len(dm3.sessions[sessionId].drops))
        self.assertEqual(4, len(dm4.sessions[sessionId].drops))

        a = dm1.sessions[sessionId].drops["A"]
        o = dm4.sessions[sessionId].drops["O"]
        drops = []
        for x in (dm1, dm2, dm3, dm4):
            drops += x.sessions[sessionId].drops.values()

        # Run! This should trigger the full execution of the graph
        with droputils.DROPWaiterCtx(self, o, 5):
            a.write(b"a")

        for drop in drops:
            self.assertEqual(
                DROPStates.COMPLETED,
                drop.status,
                "Status of '%s' is not COMPLETED: %d"
                % (drop.uid, drop.status),
            )

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

        sessionId = f"s{random.randint(0, 1000)}"
        N = 100
        g1 = [memory("A")]
        g2 = [memory("C")]
        rels = []
        for i in range(N):
            b_oid = "B%d" % (i,)
            # SleepAndCopyApp effectively opens the input drop
            g2.append(sleepAndCopy(b_oid, outputs=["C"], sleepTime=0))
            rels.append(DROPRel("A", DROPLinkType.INPUT, b_oid))
        DROPManagerUtils.add_test_reprodata(g1)
        DROPManagerUtils.add_test_reprodata(g2)
        DROPManagerUtils.quickDeploy(dm1, sessionId, g1, {DROPManagerUtils.nm_conninfo(1): rels})
        DROPManagerUtils.quickDeploy(dm2, sessionId, g2, {DROPManagerUtils.nm_conninfo(0): rels})
        self.assertEqual(1, len(dm1.sessions[sessionId].drops))
        self.assertEqual(1 + N, len(dm2.sessions[sessionId].drops))

        # Run! The sole fact that this doesn't throw exceptions is already
        # a good proof that everything is working as expected
        a = dm1.sessions[sessionId].drops["A"]
        c = dm2.sessions[sessionId].drops["C"]
        with droputils.DROPWaiterCtx(self, c, 10):
            a.write(b"a")
            a.setCompleted()

        for i in range(N):
            drop = dm2.sessions[sessionId].drops["B%d" % (i,)]
            self.assertEqual(DROPStates.COMPLETED, drop.status)
        dm1.destroySession(sessionId)
        dm2.destroySession(sessionId)

    def test_runGraphSeveralDropsPerDM_with_get_consumer_nodes(self):
        """
        A test that creates several DROPs in two different DMs and runs
        the graph. Checks the node address(s) of the consumers in the second DM.
        The graph looks like this

        DM #1                  DM #2
        ===================    ================
        | A --> C --> D --|----|-| --> E      |
        |                 |    | |
        |                 |    | | --> F      |
        ===================    ================

        :see: `self.test_runGraphSeveralDropsPerDM_with_get_consumer_nodes`
        """
        ip_addr_1 = "8.8.8.8"
        ip_addr_2 = "8.8.8.9"

        dm1, dm2 = [self._start_dm() for _ in range(2)]

        sessionId = f"s{random.randint(0, 1000)}"
        g1 = [
            memory("A", consumers=["C"]),
            {
                "oid": "C",
                "categoryType": "Application",
                "dropclass": "dlg.apps.crc.CRCApp",
                "consumers": ["D"],
            },
            memory("D", producers=["C"]),
        ]
        g2 = [
            {
                "oid": "E",
                "categoryType": "Application",
                "dropclass": "test.test_drop.SumupContainerChecksum",
                "node": ip_addr_1,
            },
            {
                "oid": "F",
                "categoryType": "Application",
                "dropclass": "test.test_drop.SumupContainerChecksum",
                "node": ip_addr_2,
            },
        ]
        DROPManagerUtils.add_test_reprodata(g1)
        DROPManagerUtils.add_test_reprodata(g2)
        rels = [
            DROPRel("D", DROPLinkType.INPUT, "E"),
            DROPRel("D", DROPLinkType.INPUT, "F"),
        ]
        DROPManagerUtils.quickDeploy(dm1, sessionId, g1, {DROPManagerUtils.nm_conninfo(1): rels})
        DROPManagerUtils.quickDeploy(dm2, sessionId, g2, {DROPManagerUtils.nm_conninfo(0): rels})

        self.assertEqual(3, len(dm1.sessions[sessionId].drops))
        self.assertEqual(2, len(dm2.sessions[sessionId].drops))

        cons_nodes = dm1.sessions[sessionId].drops["D"].get_consumers_nodes()

        self.assertTrue(ip_addr_1 in cons_nodes)
        self.assertTrue(ip_addr_2 in cons_nodes)

        dm1.destroySession(sessionId)
        dm2.destroySession(sessionId)

    def test_run_streaming_consumer_remotely(self):
        """
        A test that checks that a streaming consumer works correctly across
        node managers when its input is in a different node, like this:

        DM #1                 DM #2
        ==================    ==============
        | A --> B --> C -|----|--> D --> E |
        ==================    ==============

        Here B is anormal application and D is a streaming consumer of C.
        We use A and E to compare that all data flows correctly.
        """

        g1 = [
            memory("A"),
            {
                "oid": "B",
                "categoryType": "Application",
                "dropclass": "dlg.apps.simple.CopyApp",
                "inputs": ["A"],
                "outputs": ["C"],
            },
            memory("C"),
        ]
        g2 = [
            {
                "oid": "D",
                "categoryType": "Application",
                "dropclass": "dlg.apps.crc.CRCStreamApp",
                "outputs": ["E"],
            },
            memory("E"),
        ]
        rels = [DROPRel("C", DROPLinkType.STREAMING_INPUT, "D")]
        a_data = os.urandom(32)
        e_data = str(crc32c(a_data, 0)).encode("utf8")
        self._test_runGraphInTwoNMs(g1, g2, rels, a_data, e_data, leaf_oid="E")

    def test_run_streaming_consumer_remotely2(self):
        """
        Like above, but C is hostd by DM #2.
        """

        g1 = [
            memory("A"),
            {
                "oid": "B",
                "categoryType": "Application",
                "dropclass": "dlg.apps.simple.CopyApp",
                "inputs": ["A"],
            },
        ]
        g2 = [
            memory("C"),
            {
                "oid": "D",
                "categoryType": "Application",
                "dropclass": "dlg.apps.crc.CRCStreamApp",
                "streamingInputs": ["C"],
                "outputs": ["E"],
            },
            memory("E"),
        ]
        rels = [DROPRel("C", DROPLinkType.OUTPUT, "B")]
        a_data = os.urandom(32)
        e_data = str(crc32c(a_data, 0)).encode("utf8")
        self._test_runGraphInTwoNMs(g1, g2, rels, a_data, e_data, leaf_oid="E")


class TestDMMultiThreading(NodeManagerTestsBase, unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.use_processes = False

    def test_run_invalid_shmem_graph(self):
        """
        Our shared memory implementation does not support Python < 3.7
        This test asserts that a graph containing shared memory drops will not run if running
        in python < 3.8, and that it *does* run with python >= 3.8
        """

        graph = [
            {
                "oid": "A",
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.SharedMemoryDROP",
            }
        ]
        graph = DROPManagerUtils.add_test_reprodata(graph)
        dm = self._start_dm()
        sessionID = "s1"
        if sys.version_info < (3, 8):
            self.assertRaises(
                NotImplementedError, DROPManagerUtils.quickDeploy, dm, sessionID, graph
            )
        else:
            DROPManagerUtils.quickDeploy(dm, sessionID, graph)
            self.assertEqual(1, len(dm.sessions[sessionID].drops))
            dm.destroySession(sessionID)
