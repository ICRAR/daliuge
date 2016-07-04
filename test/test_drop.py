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

from cStringIO import StringIO
import contextlib
import os, unittest
import random
import shutil
import sqlite3
import tempfile

from dfms import droputils
from dfms.ddap_protocol import DROPStates, ExecutionMode, AppDROPStates
from dfms.drop import FileDROP, AppDROP, InMemoryDROP, \
    NullDROP, BarrierAppDROP, \
    DirectoryContainer, ContainerDROP, InputFiredAppDROP, RDBMSDrop
from dfms.droputils import DROPWaiterCtx
from dfms.exceptions import InvalidDropException


try:
    from crc32c import crc32
except:
    from binascii import crc32

ONE_MB = 1024 ** 2

def _start_ns_thread(ns_daemon):
    ns_daemon.requestLoop()

def isContainer(drop):
    # return isinstance(drop, ContainerDROP)
    # A Pyro-friendly way to check for a ContainerDROP is to see if
    # invoking its 'children' attribute fails or not
    try:
        drop.children
        return True
    except AttributeError:
        return False

class SumupContainerChecksum(BarrierAppDROP):
    """
    A dummy BarrierAppDROP that recursively sums up the checksums of
    all the individual DROPs it consumes, and then stores the final
    result in its output DROP
    """
    def run(self):
        crcSum = 0
        for inputDrop in self.inputs:
            if inputDrop.status == DROPStates.COMPLETED:
                crcSum += inputDrop.checksum
        outputDrop = self.outputs[0]
        outputDrop.write(str(crcSum))

class TestDROP(unittest.TestCase):

    def setUp(self):
        """
        library-specific setup
        """
        self._test_drop_sz = 16 # MB
        self._test_block_sz =  2 # MB
        self._test_num_blocks = self._test_drop_sz / self._test_block_sz
        self._test_block = str(bytearray(os.urandom(self._test_block_sz * ONE_MB)))

    def tearDown(self):
        shutil.rmtree("/tmp/sdp_dfms", True)

    def test_NullDROP(self):
        """
        Check that the NullDROP is usable for testing
        """
        a = NullDROP('A', 'A', expectedSize=5)
        a.write("1234")
        a.write("5")
        allContents = droputils.allDropContents(a)
        self.assertFalse(allContents)

    def test_write_FileDROP(self):
        """
        Test a FileDROP and a simple AppDROP (for checksum calculation)
        """
        self._test_write_withDropType(FileDROP)

    def test_write_InMemoryDROP(self):
        """
        Test an InMemoryDROP and a simple AppDROP (for checksum calculation)
        """
        self._test_write_withDropType(InMemoryDROP)

    def _test_write_withDropType(self, dropType):
        """
        Test an AbstractDROP and a simple AppDROP (for checksum calculation)
        """
        a = dropType('oid:A', 'uid:A', expectedSize = self._test_drop_sz * ONE_MB)
        b = SumupContainerChecksum('oid:B', 'uid:B')
        c = InMemoryDROP('oid:C', 'uid:C')
        b.addInput(a)
        b.addOutput(c)

        test_crc = 0
        with DROPWaiterCtx(self, c):
            for _ in range(self._test_num_blocks):
                a.write(self._test_block)
                test_crc = crc32(self._test_block, test_crc)

        # Read the checksum from c
        cChecksum = int(droputils.allDropContents(c))

        self.assertNotEquals(a.checksum, 0)
        self.assertEquals(a.checksum, test_crc)
        self.assertEquals(cChecksum, test_crc)

    def test_simple_chain(self):
        '''
        Simple test that creates a pipeline-like chain of commands.
        In this case we simulate a pipeline that does this, holding
        each intermediate result in memory:

        cat someFile | grep 'a' | sort | rev
        '''

        class GrepResult(BarrierAppDROP):
            def initialize(self, **kwargs):
                super(GrepResult, self).initialize(**kwargs)
                self._substring = kwargs['substring']

            def run(self):
                drop = self.inputs[0]
                output = self.outputs[0]
                allLines = StringIO(droputils.allDropContents(drop)).readlines()
                for line in allLines:
                    if self._substring in line:
                        output.write(line)

        class SortResult(BarrierAppDROP):
            def run(self):
                drop = self.inputs[0]
                output = self.outputs[0]
                sortedLines = StringIO(droputils.allDropContents(drop)).readlines()
                sortedLines.sort()
                for line in sortedLines:
                    output.write(line)

        class RevResult(BarrierAppDROP):
            def run(self):
                drop = self.inputs[0]
                output = self.outputs[0]
                allLines = StringIO(droputils.allDropContents(drop)).readlines()
                for line in allLines:
                    buf = ''
                    for c in line:
                        if c == ' ' or c == '\n':
                            output.write(buf[::-1])
                            output.write(c)
                            buf = ''
                        else:
                            buf += c

        a = InMemoryDROP('oid:A', 'uid:A')
        b = GrepResult('oid:B', 'uid:B', substring="a")
        c = InMemoryDROP('oid:C', 'uid:C')
        d = SortResult('oid:D', 'uid:D')
        e = InMemoryDROP('oid:E', 'uid:E')
        f = RevResult('oid:F', 'oid:F')
        g = InMemoryDROP('oid:G', 'uid:G')

        a.addConsumer(b)
        b.addOutput(c)
        c.addConsumer(d)
        d.addOutput(e)
        e.addConsumer(f)
        f.addOutput(g)

        # Initial write
        contents = "first line\nwe have an a here\nand another one\nnoone knows me"
        cResExpected = "we have an a here\nand another one\n"
        eResExpected = "and another one\nwe have an a here\n"
        gResExpected = "dna rehtona eno\new evah na a ereh\n"

        with DROPWaiterCtx(self, g):
            a.write(contents)
            a.setCompleted()

        # Get intermediate and final results and compare
        actualRes   = []
        for i in [c, e, g]:
            actualRes.append(droputils.allDropContents(i))
        map(lambda x, y: self.assertEquals(x, y), [cResExpected, eResExpected, gResExpected], actualRes)

    def test_errorState(self):
        a = InMemoryDROP('a', 'a')
        b = SumupContainerChecksum('b', 'b')
        c = InMemoryDROP('c', 'c')
        c.addProducer(b)
        b.addInput(a)
        a.setError()
        self.assertEquals(DROPStates.ERROR, a.status)
        self.assertEquals(DROPStates.ERROR, b.status)
        self.assertEquals(DROPStates.ERROR, c.status)

    def test_branch_failure(self):
        self.branch_failure(False)

    def test_branch_too_many_failures(self):
        self.branch_failure(True)

    def branch_failure(self, tooManyFailures):
        """
        Using the container data object to implement a join/barrier dataflow.

        A1, A2 and A3 are FileDROPs
        B1, B2 and B3 are SumupContainerChecksum
        C1, C2 and C3 are InMemoryDROPs
        D is a SumupContainerChecksum
        E is a InMemoryDROP

        --> A1 --> B1 --> C1 --|
        --> A2 --> B2 --> C2 --|--> D --> E
        --> A3 --> B3 --> C3 --|

        Upon writing all A* DROPs, the execution of B* DROPs should be triggered,
        after which "C" will transition to COMPLETE. Once all "C"s have moved to
        COMPLETED "D"'s execution will also be triggered, and finally E will
        hold the sum of B1, B2 and B3's checksums
        """

        #create file data objects
        a1 = InMemoryDROP('oid:A1', 'uid:A1')
        a2 = InMemoryDROP('oid:A2', 'uid:A2')
        a3 = InMemoryDROP('oid:A3', 'uid:A3')

        # CRC Result DROPs, storing the result in memory
        b1 = SumupContainerChecksum('oid:B1', 'uid:B1')
        b2 = SumupContainerChecksum('oid:B2', 'uid:B2')
        b3 = SumupContainerChecksum('oid:B3', 'uid:B3')
        c1 = InMemoryDROP('oid:C1', 'uid:C1')
        c2 = InMemoryDROP('oid:C2', 'uid:C2')
        c3 = InMemoryDROP('oid:C3', 'uid:C3')

        # The final DROP that sums up the CRCs from the container DROP
        d = SumupContainerChecksum('oid:D', 'uid:D', input_error_threshold = 33)
        e = InMemoryDROP('oid:E', 'uid:E')

        # Wire together
        dropAList = [a1,a2,a3]
        dropBList = [b1,b2,b3]
        dropCList = [c1,c2,c3]
        for dropA,dropB in map(lambda a,b: (a,b), dropAList, dropBList):
            dropA.addConsumer(dropB)
        for dropB,dropC in map(lambda b,c: (b,c), dropBList, dropCList):
            dropB.addOutput(dropC)
        for dropC in dropCList:
            dropC.addConsumer(d)
        d.addOutput(e)

        # Write data into the initial "A" DROPs, which should trigger
        # the whole chain explained above
        with DROPWaiterCtx(self, e):
            #for dropA in dropAList: # this should be parallel for
            a1.write(' '); a1.setCompleted()
            if tooManyFailures:
                a2.setError()
            else:
                a2.write(' '); a2.setCompleted()
            a3.setError()

        if tooManyFailures:
            completedDrops = dropAList[0:1] + dropBList[0:1] + dropCList[0:1]
            errorDrops = dropAList[1:] + dropBList[1:] + dropCList[1:] + [d, e]
        else:
            completedDrops = dropAList[0:2] + dropBList[0:2] + dropCList[0:2] + [d, e]
            errorDrops = dropAList[2:] + dropBList[2:] + dropCList[2:]

        for drop in completedDrops:
            self.assertEquals(drop.status, DROPStates.COMPLETED)
        for drop in errorDrops:
            self.assertEquals(drop.status, DROPStates.ERROR)

        # The results we want to compare
        # (only in case that at least two branches executed)
        if not tooManyFailures:
            sum_crc = c1.checksum + c2.checksum
            dropEData = int(droputils.allDropContents(e))

            self.assertNotEquals(sum_crc, 0)
            self.assertEquals(sum_crc, dropEData)

    def test_join(self):
        """
        Using the container data object to implement a join/barrier dataflow.

        A1, A2 and A3 are FileDROPs
        B1, B2 and B3 are SumupContainerChecksum
        C1, C2 and C3 are InMemoryDROPs
        D is a SumupContainerChecksum
        E is a InMemoryDROP

        --> A1 --> B1 --> C1 --|
        --> A2 --> B2 --> C2 --|--> D --> E
        --> A3 --> B3 --> C3 --|

        Upon writing all A* DROPs, the execution of B* DROPs should be triggered,
        after which "C" will transition to COMPLETE. Once all "C"s have moved to
        COMPLETED "D"'s execution will also be triggered, and finally E will
        hold the sum of B1, B2 and B3's checksums
        """

        filelen = self._test_drop_sz * ONE_MB
        #create file data objects
        a1 = FileDROP('oid:A1', 'uid:A1', expectedSize=filelen)
        a2 = FileDROP('oid:A2', 'uid:A2', expectedSize=filelen)
        a3 = FileDROP('oid:A3', 'uid:A3', expectedSize=filelen)

        # CRC Result DROPs, storing the result in memory
        b1 = SumupContainerChecksum('oid:B1', 'uid:B1')
        b2 = SumupContainerChecksum('oid:B2', 'uid:B2')
        b3 = SumupContainerChecksum('oid:B3', 'uid:B3')
        c1 = InMemoryDROP('oid:C1', 'uid:C1')
        c2 = InMemoryDROP('oid:C2', 'uid:C2')
        c3 = InMemoryDROP('oid:C3', 'uid:C3')

        # The final DROP that sums up the CRCs from the container DROP
        d = SumupContainerChecksum('oid:D', 'uid:D')
        e = InMemoryDROP('oid:E', 'uid:E')

        # Wire together
        dropAList = [a1,a2,a3]
        dropBList = [b1,b2,b3]
        dropCList = [c1,c2,c3]
        for dropA,dropB in map(lambda a,b: (a,b), dropAList, dropBList):
            dropA.addConsumer(dropB)
        for dropB,dropC in map(lambda b,c: (b,c), dropBList, dropCList):
            dropB.addOutput(dropC)
        for dropC in dropCList:
            dropC.addConsumer(d)
        d.addOutput(e)

        # Write data into the initial "A" DROPs, which should trigger
        # the whole chain explained above
        with DROPWaiterCtx(self, e):
            for dropA in dropAList: # this should be parallel for
                for _ in range(self._test_num_blocks):
                    dropA.write(self._test_block)

        # All DROPs are completed now that the chain executed correctly
        for drop in dropAList + dropBList + dropCList:
            self.assertEqual(drop.status, DROPStates.COMPLETED)

        # The results we want to compare
        sum_crc = c1.checksum + c2.checksum + c3.checksum
        dropEData = int(droputils.allDropContents(e))

        self.assertNotEquals(sum_crc, 0)
        self.assertEquals(sum_crc, dropEData)

    def test_app_multiple_outputs(self):
        """
        A small method that tests that the AppDROPs writing to two
        different DROPs outputs works

        The graph constructed by this example looks as follow:

                              |--> E
        A --> B --> C --> D --|
                              |--> F

        Here B and D are an AppDROPs, with D writing to two DROPs
        outputs (E and F) and reading from C. C, in turn, is written by B, which
        in turns reads the data from A
        """

        # This is used as "B"
        class NumberWriterApp(BarrierAppDROP):
            def run(self):
                inputDrop = self.inputs[0]
                output = self.outputs[0]
                howMany = int(droputils.allDropContents(inputDrop))
                for i in xrange(howMany):
                    output.write(str(i) + " ")

        # This is used as "D"
        class OddAndEvenContainerApp(BarrierAppDROP):
            def run(self):
                inputDrop = self.inputs[0]
                outputs = self.outputs

                numbers = droputils.allDropContents(inputDrop).strip().split()
                for n in numbers:
                    outputs[int(n) % 2].write(n + " ")

        # Create DROPs
        a =     InMemoryDROP('oid:A', 'uid:A')
        b =        NumberWriterApp('oid:B', 'uid:B')
        c =     InMemoryDROP('oid:A', 'uid:A')
        d = OddAndEvenContainerApp('oid:D', 'uid:D')
        e =     InMemoryDROP('oid:E', 'uid:E')
        f =     InMemoryDROP('oid:F', 'uid:F')

        # Wire them together
        a.addConsumer(b)
        b.addOutput(c)
        c.addConsumer(d)
        d.addOutput(e)
        d.addOutput(f)

        # Start the execution
        with DROPWaiterCtx(self, [e,f]):
            a.write('20')
            a.setCompleted()

        # Check the final results are correct
        for drop in [a,b,c,d,e]:
            self.assertEquals(drop.status, DROPStates.COMPLETED, "%r is not yet COMPLETED" % (drop))
        self.assertEquals("0 2 4 6 8 10 12 14 16 18", droputils.allDropContents(e).strip())
        self.assertEquals("1 3 5 7 9 11 13 15 17 19", droputils.allDropContents(f).strip())


    def test_dropWroteFromOutside(self):
        """
        A different scenario to those tested above, in which the data
        represented by the DROP isn't actually written *through* the
        DROP. Still, the DROP needs to be moved to COMPLETED once
        the data is written, and reading from it should still yield a correct
        result
        """

        # Write, but not through the DROP
        a = FileDROP('A', 'A')
        filename = a.path
        msg = 'a message'
        with open(filename, 'w') as f:
            f.write(msg)
        a.setCompleted()

        # Read from the DROP
        self.assertEquals(msg, droputils.allDropContents(a))
        self.assertIsNone(a.checksum)
        self.assertIsNone(a.size)

        # We can manually set the size because the DROP wasn't able to calculate
        # it itself; if we couldn't an exception would be thrown
        a.size = len(msg)

    def test_stateMachine(self):
        """
        A simple test to check that some transitions are invalid
        """

        # Nice and easy
        drop = InMemoryDROP('a', 'a')
        self.assertEquals(drop.status, DROPStates.INITIALIZED)
        drop.write('a')
        self.assertEquals(drop.status, DROPStates.WRITING)
        drop.setCompleted()
        self.assertEquals(drop.status, DROPStates.COMPLETED)

        # Try to overwrite the DROP's checksum and size
        self.assertRaises(Exception, lambda: setattr(drop, 'checksum', 0))
        self.assertRaises(Exception, lambda: setattr(drop, 'size', 0))

        # Try to write on a DROP that is already COMPLETED
        self.assertRaises(Exception, drop.write, '')

        # Invalid reading on a DROP that isn't COMPLETED yet
        drop = InMemoryDROP('a', 'a')
        self.assertRaises(Exception, drop.open)
        self.assertRaises(Exception, drop.read, 1)
        self.assertRaises(Exception, drop.close, 1)

        # Invalid file descriptors used to read/close
        drop.setCompleted()
        fd = drop.open()
        otherFd = random.SystemRandom().randint(0, 1000)
        self.assertNotEquals(fd, otherFd)
        self.assertRaises(Exception, drop.read, otherFd)
        self.assertRaises(Exception, drop.close, otherFd)
        # but using the correct one should be OK
        drop.read(fd)
        self.assertTrue(drop.isBeingRead())
        drop.close(fd)

        # Expire it, then try to set it as COMPLETED again
        drop.status = DROPStates.EXPIRED
        self.assertRaises(Exception, drop.setCompleted)

    def test_externalGraphExecutionDriver(self):
        self._test_graphExecutionDriver(ExecutionMode.EXTERNAL)

    def test_DropGraphExecutionDriver(self):
        self._test_graphExecutionDriver(ExecutionMode.DROP)

    def _test_graphExecutionDriver(self, mode):
        """
        A small test to check that DROPs executions can be driven externally if
        required, and not always internally by themselves
        """
        a = InMemoryDROP('a', 'a', executionMode=mode, expectedSize=1)
        b = SumupContainerChecksum('b', 'b')
        c = InMemoryDROP('c', 'c')
        a.addConsumer(b)
        c.addProducer(b)

        # Write and check
        dropsToWaitFor = [] if mode == ExecutionMode.EXTERNAL else [c]
        with DROPWaiterCtx(self, dropsToWaitFor):
            a.write('1')

        if mode == ExecutionMode.EXTERNAL:
            # b hasn't been triggered
            self.assertEquals(c.status, DROPStates.INITIALIZED)
            self.assertEquals(b.status, DROPStates.INITIALIZED)
            self.assertEquals(b.execStatus, AppDROPStates.NOT_RUN)
            # Now let b consume a
            with DROPWaiterCtx(self, [c]):
                b.dropCompleted('a', DROPStates.COMPLETED)
            self.assertEquals(c.status, DROPStates.COMPLETED)
        elif mode == ExecutionMode.DROP:
            # b is already done
            self.assertEquals(c.status, DROPStates.COMPLETED)

    def test_objectAsNormalAndStreamingInput(self):
        """
        A test that checks that a DROP can act as normal and streaming input of
        different AppDROPs at the same time. We use the following graph:

        A --|--> B --> D
            |--> C --> E

        Here B uses A as a streaming input, while C uses it as a normal
        input
        """

        class LastCharWriterApp(AppDROP):
            def initialize(self, **kwargs):
                super(LastCharWriterApp, self).initialize(**kwargs)
                self._lastChar = None
            def dataWritten(self, uid, data):
                self.execStatus = AppDROPStates.RUNNING
                outputDrop = self.outputs[0]
                self._lastChar = data[-1]
                outputDrop.write(self._lastChar)
            def dropCompleted(self, uid, status):
                self.execStatus = AppDROPStates.FINISHED
                self._notifyAppIsFinished()

        a = InMemoryDROP('a', 'a')
        b = LastCharWriterApp('b', 'b')
        c = SumupContainerChecksum('c', 'c')
        d = InMemoryDROP('d', 'd')
        e = InMemoryDROP('e', 'e')
        a.addStreamingConsumer(b)
        a.addConsumer(c)
        b.addOutput(d)
        c.addOutput(e)

        # Consumer cannot be normal and streaming at the same time
        self.assertRaises(Exception, a.addConsumer, b)
        self.assertRaises(Exception, a.addStreamingConsumer, c)

        # Write a little, then check the consumers
        def checkDropStates(aStatus, dStatus, eStatus, lastChar):
            self.assertEquals(aStatus, a.status)
            self.assertEquals(dStatus, d.status)
            self.assertEquals(eStatus, e.status)
            self.assertEquals(lastChar, b._lastChar)

        checkDropStates(DROPStates.INITIALIZED , DROPStates.INITIALIZED, DROPStates.INITIALIZED, None)
        a.write('abcde')
        checkDropStates(DROPStates.WRITING, DROPStates.WRITING, DROPStates.INITIALIZED, 'e')
        a.write('fghij')
        checkDropStates(DROPStates.WRITING, DROPStates.WRITING, DROPStates.INITIALIZED, 'j')
        a.write('k')
        with DROPWaiterCtx(self, [d,e]):
            a.setCompleted()
        checkDropStates(DROPStates.COMPLETED, DROPStates.COMPLETED, DROPStates.COMPLETED, 'k')

        self.assertEquals('ejk', droputils.allDropContents(d))

    def test_fileDROP_delete_parent_dir(self):
        """
        A test to check that FileDROPs delete their parent directory upon
        drop.delete() if they are instructed to do so.
        """

        def assertFiles(delete_parent_directory, parentDirExists, tempDir=None):
            tempDir = tempDir or tempfile.mkdtemp()
            a = FileDROP('a', 'a', dirname=tempDir, delete_parent_directory=delete_parent_directory)
            a.write(' ')
            a.setCompleted()
            self.assertTrue(a.exists())
            self.assertTrue(os.path.isdir(tempDir))
            a.delete()
            self.assertFalse(a.exists())
            self.assertEquals(parentDirExists, os.path.isdir(tempDir))
            if parentDirExists:
                shutil.rmtree(tempDir)

        # Test 1: no deletion commanded, directory exists after .delete()
        assertFiles(False, True)
        # Test 2: deletion commanded, directory doesn't exist after .delete()
        assertFiles(True, False)
        # Test 3: deletion commanded, directory not empty, delete still works
        tempDir = tempfile.mkdtemp()
        with open(os.path.join(tempDir, 'b'), 'w') as f:
            f.write(' ')
        assertFiles(True, True, tempDir=tempDir)

    def test_directoryContainer(self):
        """
        A small, simple test for the DirectoryContainer DROP that checks it allows
        only valid children to be added
        """

        # Prepare our playground
        cwd = os.getcwd()
        os.chdir('/tmp')
        dirname  = ".hidden"
        dirname2 = ".hidden/inside"
        if not os.path.exists(dirname2):
            os.makedirs(dirname2)

        # DROPs involved
        a = FileDROP('a', 'a', dirname=dirname)
        b = FileDROP('b', 'b', dirname=dirname)
        c = FileDROP('c', 'c', dirname=dirname2)
        d = FileDROP('d', 'd', dirname=dirname2)
        cont1 = DirectoryContainer('e', 'e', dirname=dirname)
        cont2 = DirectoryContainer('f', 'f', dirname=dirname2)

        # Paths are absolutely reported
        self.assertEquals(os.path.realpath('/tmp/.hidden'), os.path.realpath(cont1.path))
        self.assertEquals(os.path.realpath('/tmp/.hidden/inside'), os.path.realpath(cont2.path))

        # Certain children-to-be are rejected
        self.assertRaises(TypeError, cont1.addChild, NullDROP('g', 'g'))
        self.assertRaises(TypeError, cont1.addChild, InMemoryDROP('h', 'h'))
        self.assertRaises(TypeError, cont1.addChild, ContainerDROP('i', 'i'))
        self.assertRaises(Exception, cont1.addChild, c)
        self.assertRaises(Exception, cont1.addChild, d)
        self.assertRaises(Exception, cont2.addChild, a)
        self.assertRaises(Exception, cont2.addChild, b)

        # These children are correct
        cont1.addChild(a)
        cont1.addChild(b)
        cont2.addChild(c)
        cont2.addChild(d)

        # Revert to previous state
        shutil.rmtree(dirname, True)
        os.chdir(cwd)

    def test_multipleProducers(self):
        """
        A test that checks that multiple-producers correctly drive the state of
        their shared output
        """
        class App(BarrierAppDROP): pass

        a,b,c,d,e = [App(chr(ord('A') + i), chr(ord('A') + i)) for i in xrange(5)]
        f = InMemoryDROP('F', 'F')
        for drop in a,b,c,d,e:
            drop.addOutput(f)

        self.assertEquals(DROPStates.INITIALIZED, f.status)
        for drop in a,b,c,d,e:
            self.assertEquals(AppDROPStates.NOT_RUN, drop.execStatus)

        # Run the first 4 ones, F should still be in INITIALIZED
        for drop in a,b,c,d:
            drop.execute()
        self.assertEquals(DROPStates.INITIALIZED, f.status)
        self.assertEquals(AppDROPStates.NOT_RUN, e.execStatus)
        for drop in a,b,c,d:
            self.assertEquals(AppDROPStates.FINISHED, drop.execStatus)

        # Run the final one, now F should be COMPLETED
        e.execute()
        self.assertEquals(DROPStates.COMPLETED, f.status)
        for drop in a,b,c,d,e:
            self.assertEquals(AppDROPStates.FINISHED, drop.execStatus)

    def test_eager_inputFired_app(self):
        """
        Tests that InputFiredApps works as expected
        """

        # No n_effective_inputs given
        self.assertRaises(InvalidDropException, InputFiredAppDROP, 'a', 'a')
        # Invalid values
        self.assertRaises(InvalidDropException, InputFiredAppDROP, 'a', 'a', n_effective_inputs=-2)
        self.assertRaises(InvalidDropException, InputFiredAppDROP, 'a', 'a', n_effective_inputs=0)

        # More effective inputs than inputs
        a = InMemoryDROP('b', 'b')
        b = InputFiredAppDROP('a', 'a', n_effective_inputs=2)
        b.addInput(a)
        self.assertRaises(Exception, a.setCompleted)

        # 2 effective inputs, 4 outputs. Trigger 2 inputs and make sure the
        # app has run
        a,b,c,d = [InMemoryDROP(str(i), str(i)) for i in xrange(4)]
        e = InputFiredAppDROP('e', 'e', n_effective_inputs=2)
        map(lambda x: e.addInput(x), [a,b,c,d])

        with DROPWaiterCtx(self, e, 5):
            a.setCompleted()
            b.setCompleted()

        self.assertEquals(AppDROPStates.FINISHED, e.execStatus)
        self.assertEquals(DROPStates.COMPLETED, a.status)
        self.assertEquals(DROPStates.COMPLETED, b.status)
        self.assertEquals(DROPStates.INITIALIZED, c.status)
        self.assertEquals(DROPStates.INITIALIZED, d.status)

    def test_n_tries_app(self):

        class FailOnlyTheFirstTimeApp(BarrierAppDROP):
            def initialize(self, **kwargs):
                BarrierAppDROP.initialize(self, **kwargs)
                self.i = 0
            def run(self):
                if self.i == 0:
                    self.i = 1
                    raise Exception

        # Check that we have a normal failure with the default values
        a = FailOnlyTheFirstTimeApp('a', 'a')
        a.execute()
        self.assertEquals(DROPStates.ERROR, a.status)
        self.assertEquals(AppDROPStates.ERROR, a.execStatus)

        # But it should run if we specify a bigger amount of tries
        a = FailOnlyTheFirstTimeApp('a', 'a', n_tries=2)
        a.execute()
        self.assertEquals(DROPStates.COMPLETED, a.status)
        self.assertEquals(AppDROPStates.FINISHED, a.execStatus)

    def test_rdbms_drop(self):

        dbfile = 'test_rdbms_drop.db'

        with contextlib.closing(sqlite3.connect(dbfile)) as conn:  # @UndefinedVariable
            with contextlib.closing(conn.cursor()) as cur:
                cur.execute('CREATE TABLE super_mega_table(a_string varchar(64) PRIMARY KEY, an_integer integer)');

        try:
            a = RDBMSDrop('a', 'a', dbmodule='sqlite3', dbtable='super_mega_table', dbparams={'database': dbfile})
            a.insert({'a_string': 'hello', 'an_integer': 0})
            a.insert({'a_string': 'hello1', 'an_integer': 1})

            res = a.select(columns=("an_integer",))
            self.assertEquals(2, len(res))

            res = a.select(columns=("an_integer",), condition="an_integer < 1")
            self.assertEquals(1, len(res))
            self.assertEquals(0, res[0][0])
        finally:
            os.unlink(dbfile)

if __name__ == '__main__':
    unittest.main()
