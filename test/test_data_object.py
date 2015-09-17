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

from dfms import doutils
from dfms.data_object import FileDataObject, AppDataObject, InMemoryDataObject, \
    InMemorySocketListenerDataObject, \
    NullDataObject, CRCAppDataObject, BarrierAppDataObject, \
    DirectoryContainer, ContainerDataObject
from dfms.ddap_protocol import DOStates, ExecutionMode, AppDOStates
from dfms.doutils import DOWaiterCtx


try:
    from crc32c import crc32
except:
    from binascii import crc32

ONE_MB = 1024 ** 2

def _start_ns_thread(ns_daemon):
    ns_daemon.requestLoop()

def isContainer(do):
    # return isinstance(do, ContainerDataObject)
    # A Pyro-friendly way to check for a ContainerDataObject is to see if
    # invoking its 'children' attribute fails or not
    try:
        do.children
        return True
    except AttributeError:
        return False

class SumupContainerChecksum(BarrierAppDataObject):
    """
    A dummy BarrierAppDataObject that recursively sums up the checksums of
    all the individual DataObjects it consumes, and then stores the final
    result in its output DataObject
    """
    def run(self):
        crcSum = 0
        for inputDO in self._inputs.values():
            crcSum += inputDO.checksum
        outputDO = self._outputs.values()[0]
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
        Check that the NullDataObject is usable for testing
        """
        a = NullDataObject('A', 'A', expectedSize=5)
        a.write("1234")
        a.write("5")
        allContents = doutils.allDataObjectContents(a)
        self.assertFalse(allContents)

    def test_write_FileDataObject(self):
        """
        Test a FileDataObject and a simple AppDataObject (for checksum calculation)
        """
        self._test_write_withDataObjectType(FileDataObject)

    def test_write_InMemoryDataObject(self):
        """
        Test an InMemoryDataObject and a simple AppDataObject (for checksum calculation)
        """
        self._test_write_withDataObjectType(InMemoryDataObject)

    def _test_write_withDataObjectType(self, doType):
        """
        Test an AbstractDataObject and a simple AppDataObject (for checksum calculation)
        """
        dobA = doType('oid:A', 'uid:A', expectedSize = self._test_do_sz * ONE_MB)
        dobB = CRCAppDataObject('oid:B', 'uid:B')
        dobC = InMemoryDataObject('oid:C', 'uid:C')
        dobB.addInput(dobA)
        dobB.addOutput(dobC)

        test_crc = 0
        with DOWaiterCtx(self, dobC):
            for _ in range(self._test_num_blocks):
                dobA.write(self._test_block)
                test_crc = crc32(self._test_block, test_crc)

        # Read the checksum from dobC
        dobCChecksum = int(doutils.allDataObjectContents(dobC))

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

        class GrepResult(BarrierAppDataObject):
            def initialize(self, **kwargs):
                super(GrepResult, self).initialize(**kwargs)
                self._substring = kwargs['substring']

            def run(self):
                do = self._inputs.values()[0]
                output = self._outputs.values()[0]
                allLines = StringIO(doutils.allDataObjectContents(do)).readlines()
                for line in allLines:
                    if self._substring in line:
                        output.write(line)

        class SortResult(BarrierAppDataObject):
            def run(self):
                do = self._inputs.values()[0]
                output = self._outputs.values()[0]
                sortedLines = StringIO(doutils.allDataObjectContents(do)).readlines()
                sortedLines.sort()
                for line in sortedLines:
                    output.write(line)

        class RevResult(BarrierAppDataObject):
            def run(self):
                do = self._inputs.values()[0]
                output = self._outputs.values()[0]
                allLines = StringIO(doutils.allDataObjectContents(do)).readlines()
                for line in allLines:
                    buf = ''
                    for c in line:
                        if c == ' ' or c == '\n':
                            output.write(buf[::-1])
                            output.write(c)
                            buf = ''
                        else:
                            buf += c

        a = InMemoryDataObject('oid:A', 'uid:A')
        b = GrepResult('oid:B', 'uid:B', substring="a")
        c = InMemoryDataObject('oid:C', 'uid:C')
        d = SortResult('oid:D', 'uid:D')
        e = InMemoryDataObject('oid:E', 'uid:E')
        f = RevResult('oid:F', 'oid:F')
        g = InMemoryDataObject('oid:G', 'uid:G')

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

        with DOWaiterCtx(self, g):
            a.write(contents)
            a.setCompleted()

        # Get intermediate and final results and compare
        actualRes   = []
        for i in [c, e, g]:
            actualRes.append(doutils.allDataObjectContents(i))
        map(lambda x, y: self.assertEquals(x, y), [cResExpected, eResExpected, gResExpected], actualRes)

    def test_join(self):
        """
        Using the container data object to implement a join/barrier dataflow.

        A1, A2 and A3 are FileDataObjects
        B1, B2 and B3 are CRCResultDataObjects
        C1, C2 and C3 are InMemoryDataObjects
        D is a SumupContainerChecksum
        E is a InMemoryDataObject

        --> A1 --> B1 --> C1 --|
        --> A2 --> B2 --> C2 --|--> D --> E
        --> A3 --> B3 --> C3 --|

        Upon writing all A* DOs, the execution of B* DOs should be triggered,
        after which "C" will transition to COMPLETE. Once all "C"s have moved to
        COMPLETED "D"'s execution will also be triggered, and finally E will
        hold the sum of B1, B2 and B3's checksums
        """

        filelen = self._test_do_sz * ONE_MB
        #create file data objects
        doA1 = FileDataObject('oid:A1', 'uid:A1', expectedSize=filelen)
        doA2 = FileDataObject('oid:A2', 'uid:A2', expectedSize=filelen)
        doA3 = FileDataObject('oid:A3', 'uid:A3', expectedSize=filelen)

        # CRC Result DOs, storing the result in memory
        doB1 = CRCAppDataObject('oid:B1', 'uid:B1')
        doB2 = CRCAppDataObject('oid:B2', 'uid:B2')
        doB3 = CRCAppDataObject('oid:B3', 'uid:B3')
        doC1 = InMemoryDataObject('oid:C1', 'uid:C1')
        doC2 = InMemoryDataObject('oid:C2', 'uid:C2')
        doC3 = InMemoryDataObject('oid:C3', 'uid:C3')

        # The final DO that sums up the CRCs from the container DO
        doD = SumupContainerChecksum('oid:D', 'uid:D')
        doE = InMemoryDataObject('oid:E', 'uid:E')

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

        # Write data into the initial "A" DOs, which should trigger
        # the whole chain explained above
        with DOWaiterCtx(self, doE):
            for dobA in doAList: # this should be parallel for
                for _ in range(self._test_num_blocks):
                    dobA.write(self._test_block)

        # All DOs are completed now that the chain executed correctly
        for do in doAList + doBList + doCList:
            self.assertTrue(do.status, DOStates.COMPLETED)

        # The results we want to compare
        sum_crc = doC1.checksum + doC2.checksum + doC3.checksum
        dobEData = int(doutils.allDataObjectContents(doE))

        self.assertNotEquals(sum_crc, 0)
        self.assertEquals(sum_crc, dobEData)

    def test_app_multiple_outputs(self):
        """
        A small method that tests that the AppDataObjects writing to two
        different DataObjects outputs works

        The graph constructed by this example looks as follow:

                              |--> E
        A --> B --> C --> D --|
                              |--> F

        Here B and D are an AppDataObjects, with D writing to two DataObjects
        outputs (E and F) and reading from C. C, in turn, is written by B, which
        in turns reads the data from A
        """

        # This is used as "B"
        class NumberWriterApp(BarrierAppDataObject):
            def run(self):
                inputDO = self._inputs.values()[0]
                output = self._outputs.values()[0]
                howMany = int(doutils.allDataObjectContents(inputDO))
                for i in xrange(howMany):
                    output.write(str(i) + " ")

        # This is used as "D"
        class OddAndEvenContainerApp(BarrierAppDataObject):
            def run(self):
                inputDO = self._inputs.values()[0]
                outputs = self._outputs.values()

                numbers = doutils.allDataObjectContents(inputDO).strip().split()
                for n in numbers:
                    outputs[int(n) % 2].write(n + " ")

        # Create DOs
        a =     InMemoryDataObject('oid:A', 'uid:A')
        b =        NumberWriterApp('oid:B', 'uid:B')
        c =     InMemoryDataObject('oid:A', 'uid:A')
        d = OddAndEvenContainerApp('oid:D', 'uid:D')
        e =     InMemoryDataObject('oid:E', 'uid:E')
        f =     InMemoryDataObject('oid:F', 'uid:F')

        # Wire them together
        a.addConsumer(b)
        b.addOutput(c)
        c.addConsumer(d)
        d.addOutput(e)
        d.addOutput(f)

        # Start the execution
        with DOWaiterCtx(self, [e,f]):
            a.write('20')
            a.setCompleted()

        # Check the final results are correct
        for do in [a,b,c,d,e]:
            self.assertEquals(do.status, DOStates.COMPLETED, "%r is not yet COMPLETED" % (do))
        self.assertEquals("0 2 4 6 8 10 12 14 16 18", doutils.allDataObjectContents(e).strip())
        self.assertEquals("1 3 5 7 9 11 13 15 17 19", doutils.allDataObjectContents(f).strip())

    def test_socket_listener(self):
        '''
        A simple test to check that SocketListeners are indeed working as expected;
        that is, they write the data they receive into themselves, and set themselves
        as completed when the connection is closed from the client side

        The data flow diagram looks like this:

        clientSocket --> A --> B
        '''

        host = 'localhost'
        port = 9933
        data = 'shine on you crazy diamond'

        a = InMemorySocketListenerDataObject('oid:A', 'uid:A', host=host, port=port)
        b = CRCAppDataObject('oid:B', 'uid:B')
        c = InMemoryDataObject('oid:C', 'uid:C')
        a.addConsumer(b)
        b.addOutput(c)

        # Create the socket, write, and close the connection, allowing
        # A to move to COMPLETED
        with DOWaiterCtx(self, c, 3): # That's plenty of time
            import socket
            socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            socket.connect((host, port))
            socket.send(data)
            socket.close()

        for do in [a,b,c]:
            self.assertEquals(DOStates.COMPLETED, do.status)

        # Our expectations are fulfilled!
        aContents = doutils.allDataObjectContents(a)
        cContents = int(doutils.allDataObjectContents(c))
        self.assertEquals(data, aContents)
        self.assertEquals(crc32(data, 0), cContents)

    def test_dataObjectWroteFromOutside(self):
        """
        A different scenario to those tested above, in which the data
        represented by the DataObject isn't actually written *through* the
        DataObject. Still, the DataObject needs to be moved to COMPLETED once
        the data is written, and reading from it should still yield a correct
        result
        """

        # Write, but not through the DO
        a = FileDataObject('A', 'A')
        filename = a.path
        msg = 'a message'
        with open(filename, 'w') as f:
            f.write(msg)
        a.setCompleted()

        # Read from the DO
        self.assertEquals(msg, doutils.allDataObjectContents(a))
        self.assertIsNone(a.checksum)
        self.assertIsNone(a.size)

        # We can manually set the size because the DO wasn't able to calculate
        # it itself; if we couldn't an exception would be thrown
        a.size = len(msg)

    def test_stateMachine(self):
        """
        A simple test to check that some transitions are invalid
        """

        # Nice and easy
        do = InMemoryDataObject('a', 'a')
        self.assertEquals(do.status, DOStates.INITIALIZED)
        do.write('a')
        self.assertEquals(do.status, DOStates.WRITING)
        do.setCompleted()
        self.assertEquals(do.status, DOStates.COMPLETED)

        # Try to overwrite the DO's checksum and size
        self.assertRaises(Exception, lambda: setattr(do, 'checksum', 0))
        self.assertRaises(Exception, lambda: setattr(do, 'size', 0))

        # Try to write on a DO that is already COMPLETED
        self.assertRaises(Exception, do.write, '')

        # Failure to initialize (ports < 1024 cannot be opened by normal users)
        self.assertRaises(Exception, InMemorySocketListenerDataObject, 'a', 'a', host='localhost', port=1)

        # Invalid reading on a DO that isn't COMPLETED yet
        do = InMemoryDataObject('a', 'a')
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
        do.status = DOStates.EXPIRED
        self.assertRaises(Exception, do.setCompleted)

    def test_externalGraphExecutionDriver(self):
        self._test_graphExecutionDriver(ExecutionMode.EXTERNAL)

    def test_DOGraphExecutionDriver(self):
        self._test_graphExecutionDriver(ExecutionMode.DO)

    def _test_graphExecutionDriver(self, mode):
        """
        A small test to check that DOs executions can be driven externally if
        required, and not always internally by themselves
        """
        a = InMemoryDataObject('a', 'a', executionMode=mode, expectedSize=1)
        b = CRCAppDataObject('b', 'b')
        c = InMemoryDataObject('c', 'c')
        a.addConsumer(b)
        c.addProducer(b)

        # Write and check
        dosToWaitFor = [] if mode == ExecutionMode.EXTERNAL else [c]
        with DOWaiterCtx(self, dosToWaitFor):
            a.write('1')

        if mode == ExecutionMode.EXTERNAL:
            # b hasn't been triggered
            self.assertEquals(c.status, DOStates.INITIALIZED)
            # Now let b consume a
            b.dataObjectCompleted('a')
            self.assertEquals(c.status, DOStates.COMPLETED)
        elif mode == ExecutionMode.DO:
            # b is already done
            self.assertEquals(c.status, DOStates.COMPLETED)

    def test_objectAsNormalAndStreamingInput(self):
        """
        A test that checks that a DO can act as normal and streaming input of
        different AppDataObjects at the same time. We use the following graph:

        A --|--> B --> D
            |--> C --> E

        Here B is uses A as a streaming input, while C uses it as a normal
        input
        """

        class LastCharWriterApp(AppDataObject):
            def initialize(self, **kwargs):
                super(LastCharWriterApp, self).initialize(**kwargs)
                self._lastChar = None
            def dataWritten(self, uid, data):
                self.execStatus = AppDOStates.RUNNING
                outputDO = self._outputs.values()[0]
                self._lastChar = data[-1]
                outputDO.write(self._lastChar)
            def dataObjectCompleted(self, uid):
                self.execStatus = AppDOStates.FINISHED

        a = InMemoryDataObject('a', 'a')
        b = LastCharWriterApp('b', 'b')
        c = CRCAppDataObject('c', 'c')
        d = InMemoryDataObject('d', 'd')
        e = InMemoryDataObject('e', 'e')
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

        checkDOStates(DOStates.INITIALIZED , DOStates.INITIALIZED, DOStates.INITIALIZED, None)
        a.write('abcde')
        checkDOStates(DOStates.WRITING, DOStates.WRITING, DOStates.INITIALIZED, 'e')
        a.write('fghij')
        checkDOStates(DOStates.WRITING, DOStates.WRITING, DOStates.INITIALIZED, 'j')
        a.write('k')
        with DOWaiterCtx(self, [d,e]):
            a.setCompleted()
        checkDOStates(DOStates.COMPLETED, DOStates.COMPLETED, DOStates.COMPLETED, 'k')

        self.assertEquals('ejk', doutils.allDataObjectContents(d))

    def test_directoryContainer(self):
        """
        A small, simple test for the DirectoryContainer DO that checks it allows
        only valid children to be added
        """

        # Prepare our playground
        cwd = os.getcwd()
        os.chdir('/tmp')
        dirname  = ".hidden"
        dirname2 = ".hidden/inside"
        if not os.path.exists(dirname2):
            os.makedirs(dirname2)

        # DOs involved
        a = FileDataObject('a', 'a', dirname=dirname)
        b = FileDataObject('b', 'b', dirname=dirname)
        c = FileDataObject('c', 'c', dirname=dirname2)
        d = FileDataObject('d', 'd', dirname=dirname2)
        cont1 = DirectoryContainer('e', 'e', dirname=dirname)
        cont2 = DirectoryContainer('f', 'f', dirname=dirname2)

        # Paths are absolutely reported
        self.assertEquals(os.path.realpath('/tmp/.hidden'), os.path.realpath(cont1.path))
        self.assertEquals(os.path.realpath('/tmp/.hidden/inside'), os.path.realpath(cont2.path))

        # Certain children-to-be are rejected
        self.assertRaises(TypeError, cont1.addChild, NullDataObject('g', 'g'))
        self.assertRaises(TypeError, cont1.addChild, InMemoryDataObject('h', 'h'))
        self.assertRaises(TypeError, cont1.addChild, ContainerDataObject('i', 'i'))
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
        class App(BarrierAppDataObject): pass

        a,b,c,d,e = [App(chr(ord('A') + i), chr(ord('A') + i)) for i in xrange(5)]
        f = InMemoryDataObject('F', 'F')
        for do in a,b,c,d,e:
            do.addOutput(f)

        self.assertEquals(DOStates.INITIALIZED, f.status)
        for do in a,b,c,d,e:
            self.assertEquals(AppDOStates.NOT_RUN, do.execStatus)

        # Run the first 4 ones, F should still be in INITIALIZED
        for do in a,b,c,d:
            do.execute()
        self.assertEquals(DOStates.INITIALIZED, f.status)
        self.assertEquals(AppDOStates.NOT_RUN, e.execStatus)
        for do in a,b,c,d:
            self.assertEquals(AppDOStates.FINISHED, do.execStatus)

        # Run the final one, now F should be COMPLETED
        e.execute()
        self.assertEquals(DOStates.COMPLETED, f.status)
        for do in a,b,c,d,e:
            self.assertEquals(AppDOStates.FINISHED, do.execStatus)

if __name__ == '__main__':
    unittest.main()