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
import os, unittest
import random
import shutil

from dfms import droputils
from dfms.drop import FileDROP, AppDROP, InMemoryDROP, \
    NullDROP, BarrierAppDROP, \
    DirectoryContainer, ContainerDROP
from dfms.ddap_protocol import DROPStates, ExecutionMode, AppDROPStates
from dfms.droputils import DROPWaiterCtx


try:
    from crc32c import crc32
except:
    from binascii import crc32

ONE_MB = 1024 ** 2

def _start_ns_thread(ns_daemon):
    ns_daemon.requestLoop()

def isContainer(do):
    # return isinstance(do, ContainerDROP)
    # A Pyro-friendly way to check for a ContainerDROP is to see if
    # invoking its 'children' attribute fails or not
    try:
        do.children
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
        for inputDO in self.inputs:
            crcSum += inputDO.checksum
        outputDO = self.outputs[0]
        outputDO.write(str(crcSum))

class TestDataObject(unittest.TestCase):

    def setUp(self):
        """
        library-specific setup
        """
        self._test_do_sz = 16 # MB
        self._test_block_sz =  2 # MB
        self._test_num_blocks = self._test_do_sz / self._test_block_sz
        self._test_block = str(bytearray(os.urandom(self._test_block_sz * ONE_MB)))

    def tearDown(self):
        shutil.rmtree("/tmp/sdp_dfms", True)

    def test_NullDataObject(self):
        """
        Check that the NullDROP is usable for testing
        """
        a = NullDROP('A', 'A', expectedSize=5)
        a.write("1234")
        a.write("5")
        allContents = droputils.allDataObjectContents(a)
        self.assertFalse(allContents)

    def test_write_FileDataObject(self):
        """
        Test a FileDROP and a simple AppDROP (for checksum calculation)
        """
        self._test_write_withDataObjectType(FileDROP)

    def test_write_InMemoryDataObject(self):
        """
        Test an InMemoryDROP and a simple AppDROP (for checksum calculation)
        """
        self._test_write_withDataObjectType(InMemoryDROP)

    def _test_write_withDataObjectType(self, doType):
        """
        Test an AbstractDataObject and a simple AppDROP (for checksum calculation)
        """
        dobA = doType('oid:A', 'uid:A', expectedSize = self._test_do_sz * ONE_MB)
        dobB = SumupContainerChecksum('oid:B', 'uid:B')
        dobC = InMemoryDROP('oid:C', 'uid:C')
        dobB.addInput(dobA)
        dobB.addOutput(dobC)

        test_crc = 0
        with DROPWaiterCtx(self, dobC):
            for _ in range(self._test_num_blocks):
                dobA.write(self._test_block)
                test_crc = crc32(self._test_block, test_crc)

        # Read the checksum from dobC
        dobCChecksum = int(droputils.allDataObjectContents(dobC))

        self.assertNotEquals(dobA.checksum, 0)
        self.assertEquals(dobA.checksum, test_crc)
        self.assertEquals(dobCChecksum, test_crc)

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
                do = self._inputs.values()[0]
                output = self._outputs.values()[0]
                allLines = StringIO(droputils.allDataObjectContents(do)).readlines()
                for line in allLines:
                    if self._substring in line:
                        output.write(line)

        class SortResult(BarrierAppDROP):
            def run(self):
                do = self._inputs.values()[0]
                output = self._outputs.values()[0]
                sortedLines = StringIO(droputils.allDataObjectContents(do)).readlines()
                sortedLines.sort()
                for line in sortedLines:
                    output.write(line)

        class RevResult(BarrierAppDROP):
            def run(self):
                do = self._inputs.values()[0]
                output = self._outputs.values()[0]
                allLines = StringIO(droputils.allDataObjectContents(do)).readlines()
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
            actualRes.append(droputils.allDataObjectContents(i))
        map(lambda x, y: self.assertEquals(x, y), [cResExpected, eResExpected, gResExpected], actualRes)

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

        filelen = self._test_do_sz * ONE_MB
        #create file data objects
        doA1 = FileDROP('oid:A1', 'uid:A1', expectedSize=filelen)
        doA2 = FileDROP('oid:A2', 'uid:A2', expectedSize=filelen)
        doA3 = FileDROP('oid:A3', 'uid:A3', expectedSize=filelen)

        # CRC Result DROPs, storing the result in memory
        doB1 = SumupContainerChecksum('oid:B1', 'uid:B1')
        doB2 = SumupContainerChecksum('oid:B2', 'uid:B2')
        doB3 = SumupContainerChecksum('oid:B3', 'uid:B3')
        doC1 = InMemoryDROP('oid:C1', 'uid:C1')
        doC2 = InMemoryDROP('oid:C2', 'uid:C2')
        doC3 = InMemoryDROP('oid:C3', 'uid:C3')

        # The final DROP that sums up the CRCs from the container DROP
        doD = SumupContainerChecksum('oid:D', 'uid:D')
        doE = InMemoryDROP('oid:E', 'uid:E')

        # Wire together
        doAList = [doA1,doA2,doA3]
        doBList = [doB1,doB2,doB3]
        doCList = [doC1,doC2,doC3]
        for doA,doB in map(lambda a,b: (a,b), doAList, doBList):
            doA.addConsumer(doB)
        for doB,doC in map(lambda b,c: (b,c), doBList, doCList):
            doB.addOutput(doC)
        for doC in doCList:
            doC.addConsumer(doD)
        doD.addOutput(doE)

        # Write data into the initial "A" DROPs, which should trigger
        # the whole chain explained above
        with DROPWaiterCtx(self, doE):
            for dobA in doAList: # this should be parallel for
                for _ in range(self._test_num_blocks):
                    dobA.write(self._test_block)

        # All DROPs are completed now that the chain executed correctly
        for do in doAList + doBList + doCList:
            self.assertTrue(do.status, DROPStates.COMPLETED)

        # The results we want to compare
        sum_crc = doC1.checksum + doC2.checksum + doC3.checksum
        dobEData = int(droputils.allDataObjectContents(doE))

        self.assertNotEquals(sum_crc, 0)
        self.assertEquals(sum_crc, dobEData)

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
                inputDO = self._inputs.values()[0]
                output = self._outputs.values()[0]
                howMany = int(droputils.allDataObjectContents(inputDO))
                for i in xrange(howMany):
                    output.write(str(i) + " ")

        # This is used as "D"
        class OddAndEvenContainerApp(BarrierAppDROP):
            def run(self):
                inputDO = self._inputs.values()[0]
                outputs = self._outputs.values()

                numbers = droputils.allDataObjectContents(inputDO).strip().split()
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
        for do in [a,b,c,d,e]:
            self.assertEquals(do.status, DROPStates.COMPLETED, "%r is not yet COMPLETED" % (do))
        self.assertEquals("0 2 4 6 8 10 12 14 16 18", droputils.allDataObjectContents(e).strip())
        self.assertEquals("1 3 5 7 9 11 13 15 17 19", droputils.allDataObjectContents(f).strip())


    def test_dataObjectWroteFromOutside(self):
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
        self.assertEquals(msg, droputils.allDataObjectContents(a))
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
        do = InMemoryDROP('a', 'a')
        self.assertEquals(do.status, DROPStates.INITIALIZED)
        do.write('a')
        self.assertEquals(do.status, DROPStates.WRITING)
        do.setCompleted()
        self.assertEquals(do.status, DROPStates.COMPLETED)

        # Try to overwrite the DROP's checksum and size
        self.assertRaises(Exception, lambda: setattr(do, 'checksum', 0))
        self.assertRaises(Exception, lambda: setattr(do, 'size', 0))

        # Try to write on a DROP that is already COMPLETED
        self.assertRaises(Exception, do.write, '')

        # Invalid reading on a DROP that isn't COMPLETED yet
        do = InMemoryDROP('a', 'a')
        self.assertRaises(Exception, do.open)
        self.assertRaises(Exception, do.read, 1)
        self.assertRaises(Exception, do.close, 1)

        # Invalid file descriptors used to read/close
        do.setCompleted()
        fd = do.open()
        otherFd = random.SystemRandom().randint(0, 1000)
        self.assertNotEquals(fd, otherFd)
        self.assertRaises(Exception, do.read, otherFd)
        self.assertRaises(Exception, do.close, otherFd)
        # but using the correct one should be OK
        do.read(fd)
        self.assertTrue(do.isBeingRead())
        do.close(fd)

        # Expire it, then try to set it as COMPLETED again
        do.status = DROPStates.EXPIRED
        self.assertRaises(Exception, do.setCompleted)

    def test_externalGraphExecutionDriver(self):
        self._test_graphExecutionDriver(ExecutionMode.EXTERNAL)

    def test_DOGraphExecutionDriver(self):
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
        dosToWaitFor = [] if mode == ExecutionMode.EXTERNAL else [c]
        with DROPWaiterCtx(self, dosToWaitFor):
            a.write('1')

        if mode == ExecutionMode.EXTERNAL:
            # b hasn't been triggered
            self.assertEquals(c.status, DROPStates.INITIALIZED)
            # Now let b consume a
            with DROPWaiterCtx(self, [c]):
                b.dataObjectCompleted('a')
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

        Here B is uses A as a streaming input, while C uses it as a normal
        input
        """

        class LastCharWriterApp(AppDROP):
            def initialize(self, **kwargs):
                super(LastCharWriterApp, self).initialize(**kwargs)
                self._lastChar = None
            def dataWritten(self, uid, data):
                self.execStatus = AppDROPStates.RUNNING
                outputDO = self._outputs.values()[0]
                self._lastChar = data[-1]
                outputDO.write(self._lastChar)
            def dataObjectCompleted(self, uid):
                self.execStatus = AppDROPStates.FINISHED

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
        self.assertRaises(Exception, lambda: a.addConsumer(b))
        self.assertRaises(Exception, lambda: a.addStreamingConsumer(c))

        # Write a little, then check the consumers
        def checkDOStates(aStatus, dStatus, eStatus, lastChar):
            self.assertEquals(aStatus, a.status)
            self.assertEquals(dStatus, d.status)
            self.assertEquals(eStatus, e.status)
            self.assertEquals(lastChar, b._lastChar)

        checkDOStates(DROPStates.INITIALIZED , DROPStates.INITIALIZED, DROPStates.INITIALIZED, None)
        a.write('abcde')
        checkDOStates(DROPStates.WRITING, DROPStates.WRITING, DROPStates.INITIALIZED, 'e')
        a.write('fghij')
        checkDOStates(DROPStates.WRITING, DROPStates.WRITING, DROPStates.INITIALIZED, 'j')
        a.write('k')
        with DROPWaiterCtx(self, [d,e]):
            a.setCompleted()
        checkDOStates(DROPStates.COMPLETED, DROPStates.COMPLETED, DROPStates.COMPLETED, 'k')

        self.assertEquals('ejk', droputils.allDataObjectContents(d))

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
        for do in a,b,c,d,e:
            do.addOutput(f)

        self.assertEquals(DROPStates.INITIALIZED, f.status)
        for do in a,b,c,d,e:
            self.assertEquals(AppDROPStates.NOT_RUN, do.execStatus)

        # Run the first 4 ones, F should still be in INITIALIZED
        for do in a,b,c,d:
            do.execute()
        self.assertEquals(DROPStates.INITIALIZED, f.status)
        self.assertEquals(AppDROPStates.NOT_RUN, e.execStatus)
        for do in a,b,c,d:
            self.assertEquals(AppDROPStates.FINISHED, do.execStatus)

        # Run the final one, now F should be COMPLETED
        e.execute()
        self.assertEquals(DROPStates.COMPLETED, f.status)
        for do in a,b,c,d,e:
            self.assertEquals(AppDROPStates.FINISHED, do.execStatus)

if __name__ == '__main__':
    unittest.main()