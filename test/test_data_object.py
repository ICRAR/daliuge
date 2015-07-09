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

from dfms.data_object import FileDataObject, AppConsumer, InMemoryDataObject, InMemoryCRCResultDataObject,\
    ContainerDataObject, ContainerAppConsumer
from dfms.events.event_broadcaster import LocalEventBroadcaster

import os, unittest, threading, sys
import logging
from cStringIO import StringIO
from dfms import doutils
from dfms.ddap_protocol import DOStates

try:
    from crc32c import crc32
except:
    from binascii import crc32

ONE_MB = 1024 ** 2
logging.basicConfig(format="%(asctime)-15s [%(levelname)-5s] [%(threadName)-15s] %(name)s#%(funcName)s:%(lineno)s %(msg)s", level=logging.DEBUG, stream=sys.stdout)

def _start_ns_thread(ns_daemon):
    ns_daemon.requestLoop()

def isContainer(do):
    return isinstance(do, ContainerDataObject)

class SumupContainerChecksum(AppConsumer, InMemoryDataObject):
    """
    A dummy AppConsumer/DataObject that recursivelly sums up the checksums of
    all children of the ContainerDataObject it consumes, and then stores the
    final result in memory
    """
    def run(self, dataObject):
        if not isContainer(dataObject):
            raise Exception("This consumer consumes only Container DataObjects")
        crcSum = self.sumUpCRC(dataObject, 0)
        self.write(str(crcSum))
        self.setCompleted()

    def sumUpCRC(self, container, crcSum):
        for c in container.children:
            if isContainer(c):
                crcSum += self.sumUpCRC(container, crcSum)
            else:
                crcSum += c.checksum
        return crcSum

class TestDataObject(unittest.TestCase):

    def _eventThread(self, eventservice, host):
        eventservice.start(host)

    def _nameThread(self, nameservice):
        nameservice.start()

    def setUp(self):
        """
        library-specific setup
        """
        self._test_do_sz = 16 # MB
        self._test_block_sz =  2 # MB
        self._test_num_blocks = self._test_do_sz / self._test_block_sz
        self._test_block = str(bytearray(os.urandom(self._test_block_sz * ONE_MB)))

    def tearDown(self):
        """
        library-specific shutdown
        """
        pass

    def test_write_FileDataObject(self):
        """
        Test a FileDataObject and a simple AppDataObject (for checksum calculation)
        """
        eventbc = LocalEventBroadcaster()

        dobA = FileDataObject('oid:A', 'uid:A', eventbc, expectedSize = self._test_do_sz * ONE_MB)
        dobB = InMemoryCRCResultDataObject('oid:B', 'uid:B', eventbc)
        dobA.addConsumer(dobB)

        # Write to A. After the last write it will switch to COMPLETE
        # and it will trigger B to consume A's content and build its own
        # data, which is the CRC of the incoming data
        test_crc = 0
        for _ in range(self._test_num_blocks):
            dobA.write(self._test_block)
            test_crc = crc32(self._test_block, test_crc)

        # Read the checksum from dobB
        dobBChecksum = int(doutils.allDataObjectContents(dobB))

        self.assertNotEquals(dobA.checksum, 0)
        self.assertEquals(dobA.checksum, test_crc)
        self.assertEquals(dobA.checksum, dobBChecksum)

    def test_write_InMemoryDataObject(self):
        """
        Test an AbstractDataObject and a simple AppDataObject (for checksum calculation)
        """
        eventbc=LocalEventBroadcaster()

        dobA = InMemoryDataObject('oid:A', 'uid:A', eventbc, expectedSize = self._test_do_sz * ONE_MB)
        dobB = InMemoryCRCResultDataObject('oid:B', 'uid:B', eventbc)
        dobA.addConsumer(dobB)

        test_crc = 0
        for _ in range(self._test_num_blocks):
            dobA.write(self._test_block)
            test_crc = crc32(self._test_block, test_crc)

        # Read the checksum from dobB
        dobBChecksum = int(doutils.allDataObjectContents(dobB))

        self.assertNotEquals(dobA.checksum, 0)
        self.assertEquals(dobA.checksum, test_crc)
        self.assertEquals(dobBChecksum, test_crc)

    def test_simple_chain(self):
        '''
        Simple test that creates a pipeline-like chain of commands.
        In this case we simulate a pipeline that does this, holding
        each intermediate result in memory:

        cat someFile | grep 'a' | sort | rev
        '''

        class GrepResult(AppConsumer):
            def appInitialize(self, **kwargs):
                self._substring = kwargs['substring']

            def run(self, do):
                allLines = StringIO(doutils.allDataObjectContents(do)).readlines()
                for line in allLines:
                    if self._substring in line:
                        self.write(line)
                self.setCompleted()

        class SortResult(AppConsumer):
            def run(self, do):
                sortedLines = StringIO(doutils.allDataObjectContents(do)).readlines()
                sortedLines.sort()
                for line in sortedLines:
                    self.write(line)
                self.setCompleted()

        class RevResult(AppConsumer):
            def run(self, do):
                allLines = StringIO(doutils.allDataObjectContents(do)).readlines()
                for line in allLines:
                    buf = ''
                    for c in line:
                        if c == ' ' or c == '\n':
                            self.write(buf[::-1])
                            self.write(c)
                            buf = ''
                        else:
                            buf += c
                self.setCompleted()

        class InMemoryGrepResult(GrepResult, InMemoryDataObject): pass
        class InMemorySortResult(SortResult, InMemoryDataObject): pass
        class InMemoryRevResult(RevResult, InMemoryDataObject): pass

        leb = LocalEventBroadcaster()
        a = InMemoryDataObject('oid:A', 'uid:A', leb)
        b = InMemoryGrepResult('oid:B', 'uid:B', leb, substring="a")
        c = InMemorySortResult('oid:C', 'uid:C', leb)
        d = InMemoryRevResult('oid:D', 'uid:D', leb)

        a.addConsumer(b)
        b.addConsumer(c)
        c.addConsumer(d)

        # Initial write
        contents = "first line\nwe have an a here\nand another one\nnoone knows me"
        bResExpected = "we have an a here\nand another one\n"
        cResExpected = "and another one\nwe have an a here\n"
        dResExpected = "dna rehtona eno\new evah na a ereh\n"
        a.write(contents)
        a.setCompleted()

        # Get intermediate and final results and compare
        actualRes   = []
        for i in [b, c, d]:
            desc = i.open()
            actualRes.append(i.read(desc))
            i.close(desc)
        map(lambda x, y: self.assertEquals(x, y), [bResExpected, cResExpected, dResExpected], actualRes)

    def test_join(self):
        """
        Using the container data object to implement a join/barrier dataflow.

        A1, A2 and A3 are FileDataObjects
        B1, B2 and B3 are CRCResultDataObjects
        C is a ContainerDataObject
        D is a SumupContainerChecksum

        --> A1 --> B1 --|
        --> A2 --> B2 --|--> C --> D
        --> A3 --> B3 --|

        Upon writing all A* DOs, the execution of B* DOs should be triggered,
        after which "C" will transition to COMPLETE. Finally, "D" will also be
        triggered, and will hold the sum of B1, B2 and B3's contents
        """

        eventbc = LocalEventBroadcaster()

        filelen = self._test_do_sz * ONE_MB
        #create file data objects
        doA1 = FileDataObject('oid:A1', 'uid:A1', eventbc, expectedSize=filelen)
        doA2 = FileDataObject('oid:A2', 'uid:A2', eventbc, expectedSize=filelen)
        doA3 = FileDataObject('oid:A3', 'uid:A3', eventbc, expectedSize=filelen)

        # CRC Result DOs, storing the result in memory
        doB1 = InMemoryCRCResultDataObject('oid:B1', 'uid:B1', eventbc)
        doB2 = InMemoryCRCResultDataObject('oid:B2', 'uid:B2', eventbc)
        doB3 = InMemoryCRCResultDataObject('oid:B3', 'uid:B3', eventbc)

        # The Container DO that groups together the CRC Result DOs
        doC = ContainerDataObject('oid:C', 'uid:C', eventbc)

        # The final DO that sums up the CRCs from the container DO
        doD = SumupContainerChecksum('oid:D', 'uid:D', eventbc)

        # Wire together
        doAList = [doA1,doA2,doA3]
        doBList = [doB1,doB2,doB3]
        for doA,doB in map(lambda a,b: (a,b), doAList, doBList):
            doA.addConsumer(doB)
        for doB in doBList:
            doC.addChild(doB)
        doC.addConsumer(doD)

        # Write data into the initial "A" DOs, which should trigger
        # the whole chain explained above
        for dobA in doAList: # this should be parallel for
            for _ in range(self._test_num_blocks):
                dobA.write(self._test_block)

        # All DOs are completed now that the chain executed correctly
        for do in doAList + doBList:
            self.assertTrue(do.status, DOStates.COMPLETED)

        # The results we want to compare
        sum_crc = doB1.checksum + doB2.checksum + doB3.checksum
        dobDData = int(doutils.allDataObjectContents(doD))

        self.assertNotEquals(sum_crc, 0)
        self.assertEquals(sum_crc, dobDData)

    def test_z_lmc(self):
        """
        A more complex test that simulates the LMC (or DataFlowManager)
        submitting a physical graph via the DataManager, and in turn via two
        different DOMs. The graph that gets submitted looks like this:

           -----------------Data-Island------------------
          |                     |                       |
          | A --------> B ------|------> C --------> D  |
          |   Data Object Mgr   |    Data Object Mgr    |
          |       001           |        002            |
          -----------------------------------------------

        Here only A is a FileDataObject; B, C and D are
        InMemoryCRCResultDataObject, meaning that D holds
        A's checksum's checksum's checksum.

        The most interesting part of this exercise though is that it
        crosses boundaries of DOMs, and show that DOs are correctly
        talking to each other remotely in the current prototype with
        Pyro and Pyro4 (or not)
        """

        import datetime
        import Pyro4
        import Pyro
        from dfms.data_manager import DataManager
        from dfms import data_object_mgr, dataflow_manager

        ns_host = 'localhost'
        my_host = 'localhost'
        my_port = 7778
        Pyro.config.PYRO_NS_HOSTNAME = ns_host
        Pyro.config.PYRO_HOST = my_host
        Pyro.config.PYRO_NS_PORT = 9090

        # 1.1. launch Pyro4 name service, DOMs register on it
        _, ns4Daemon, _ = Pyro4.naming.startNS(host=ns_host, port=my_port)
        ns4Thread = threading.Thread(None, lambda x: x.requestLoop(), 'NS4Thrd', [ns4Daemon])
        ns4Thread.setDaemon(1)
        ns4Thread.start()

        # 1.2 Start Pyro NameServer and EventServer, used by the
        #     PyroEventBroadcaster used in this exercise
        from Pyro.naming import NameServerStarter
        nsStarter = NameServerStarter()
        nsThread = threading.Thread(None, lambda x: x.start(), 'NSThrd', [nsStarter])
        nsThread.setDaemon(1)
        nsThread.start()
        self.assertTrue(nsStarter.waitUntilStarted(2))

        from Pyro.EventService.Server import EventServiceStarter
        esStarter = EventServiceStarter()
        esThread = threading.Thread(None, lambda x: x.start(), 'ESThrd', [esStarter])
        esThread.setDaemon(1)
        esThread.start()
        self.assertTrue(esStarter.waitUntilStarted(2))

        # Now comes the real work
        try:
            # 2. launch data_object_manager
            id1 = '001'
            id2 = '002'
            data_object_mgr.launchServer(id1, as_daemon=True,
                                         nsHost=ns_host, myHost=my_host, port=my_port)
            data_object_mgr.launchServer(id2, as_daemon=True,
                                         nsHost=ns_host, myHost=my_host, port=my_port)

            # 3. ask dataflow_manager to build the physical dataflow
            obsId = datetime.datetime.now().strftime('%Y-%m-%dT%H-%M-%S.%f') # a dummy observation id
            (pdgRoot, doms) = dataflow_manager.buildSimpleIngestPDG(obsId, ns_host, port=my_port)

            a = pdgRoot
            b = a.consumers[0]
            c = b.consumers[0]
            d = c.consumers[0]
            for do in [a,b,c,d]:
                self.assertTrue(do.status, DOStates.INITIALIZED)

            # 4. start a single data manager
            print "**** step 4"
            dmgr = DataManager()
            dmgr.start() # start the daemon

            print "**** step 5"
            # 5. submit the graph to data manager
            res_avail = dmgr.submitPDG(pdgRoot, doms)
            if (not res_avail):
                raise Exception("Resource is not available in the data manager!")

            print "**** step 6"
            # 6. start the pipeline (simulate CSP)
            # Since the events are asynchronously we wait
            # on an event set when D is COMPLETED
            evt = threading.Event()
            class EvtConsumer():
                def consume(self, do):
                    evt.set()
            consumer = EvtConsumer()
            daemon = Pyro4.Daemon()
            consumerUri = daemon.register(consumer)
            t = threading.Thread(None, lambda: daemon.requestLoop(), "tmp", [])
            t.start()
            d.addConsumer(Pyro4.Proxy(consumerUri))

            pdgRoot.write(' ')
            pdgRoot.setCompleted()
            self.assertTrue(evt.wait(5)) # Should take only a fraction of a second anyway
            daemon.shutdown()
            t.join()

            for do in [a,b,c,d]:
                self.assertEquals(do.status, DOStates.COMPLETED)

            # Check that B holds A's checksum and so forth
            for prod, cons in [(a,b), (b,c), (c,d)]:
                consContents = int(doutils.allDataObjectContents(cons))
                self.assertEquals(prod.checksum, consContents,
                                  "%s/%s's checksum did not match %s/%s's content: %d/%d" %
                                  (a.oid, a.uid, b.oid, b.uid, prod.checksum, consContents))

            print "**** step 7"
            # 7. tear down data objects of this observation on each data object manager
            for dom in doms:
                ret = dom.shutdownDOBDaemon(obsId)
                print '%s was shutdown, ret code = %d' % (dom.getURI(), ret)

            # 8. shutdown the data manager daemon
            dmgr.shutdown()

        except Exception:
            print("Pyro traceback:")
            print("".join(Pyro4.util.getPyroTraceback()))
            raise
        finally:
            # 9. shutdown name service
            try:
                esStarter.shutdown()
                nsStarter.shutdown()
                ns4Daemon.shutdown()
                esThread.join()
                nsThread.join()
                ns4Thread.join()
            except:
                pass

    def test_container_app_do(self):
        """
        A small method that tests that the ContainerAppConsumer concept works

        The graph constructed by this example looks as follow:

                        |--> D
        A --> B --> C --|
                        |--> E

        Here C is a ContainerAppConsumer, meaning that it consumes the data
        from B and fills the D and E DataObjects, which are its children.
        """

        class NumberWriterApp(InMemoryDataObject, AppConsumer):
            def run(self, dataObject):
                howMany = int(doutils.allDataObjectContents(dataObject))
                for i in xrange(howMany):
                    self.write(str(i) + " ")
                self.setCompleted()

        class OddAndEvenContainerApp(ContainerAppConsumer):
            def run(self, dataObject):
                numbers = doutils.allDataObjectContents(dataObject).strip().split()
                for n in numbers:
                    self._children[int(n) % 2].write(n + " ")
                self._children[0].setCompleted()
                self._children[1].setCompleted()

        # Create DOs
        eb = LocalEventBroadcaster()
        a =     InMemoryDataObject('oid:A', 'uid:A', eb)
        b =        NumberWriterApp('oid:B', 'uid:B', eb)
        c = OddAndEvenContainerApp('oid:C', 'uid:C', eb)
        d =     InMemoryDataObject('oid:D', 'uid:D', eb)
        e =     InMemoryDataObject('oid:E', 'uid:E', eb)

        # Wire them together
        a.addConsumer(b)
        b.addConsumer(c)
        c.addChild(d)
        c.addChild(e)

        # Start the execution
        a.write('20')
        a.setCompleted()


        # Check the final results are correct
        for do in [a,b,c,d,e]:
            self.assertEquals(do.status, DOStates.COMPLETED)
        self.assertEquals("0 2 4 6 8 10 12 14 16 18", doutils.allDataObjectContents(d).strip())
        self.assertEquals("1 3 5 7 9 11 13 15 17 19", doutils.allDataObjectContents(e).strip())


if __name__ == '__main__':
    unittest.main()