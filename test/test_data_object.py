"""
"""

from dfms.data_object import AbstractDataObject, AppDataObject, StreamDataObject, FileDataObject, ComputeStreamChecksum, ComputeFileChecksum, ContainerDataObject
from dfms.event_broadcaster import LocalEventBroadcaster

import os, unittest

try:
    from crc32c import crc32
except:
    from binascii import crc32

ONE_MB = 1024 ** 2

class SumupContainerChecksum(AppDataObject):
    """
    A dummy component that sums up checksums of all children of the host
    ContainerDataObject (CDO), and then set the CDO's checksum as the sum
    """
    def appInitialize(self, **kwargs):
        self._bufsize = 4 * 1024 ** 2

    def get_file_checksum(self, filename):
        fo = open(filename, "r")
        buf = fo.read(self._bufsize)
        crc = 0
        while (buf != ""):
            crc = crc32(buf, crc)
            buf = fo.read(self._bufsize)
        fo.close()
        return crc

    def run(self, producer, **kwargs):
        if (isinstance(producer, ContainerDataObject)):
            c = 0
            for child in producer._children:
                c += self.get_file_checksum(child._fnm)
            producer.setChecksum(c)
        else:
            raise Exception("Not a container data object!")

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
        """
        library-specific shutdown
        """
        pass

    def TestEventHandler(self, event):
        #print
        print "Test event from {0}: {1} = {2}".format(event.oid, event.type, event.status)
        #print event.source

    def test_write_FileDataObject(self):
        """
        Test an AbstractDataObject and a simple AppDataObject (for checksum calculation)
        """

        dobA = FileDataObject('oid:A', 'uid:A', eventbc=LocalEventBroadcaster([self.TestEventHandler]), file_length = self._test_do_sz * ONE_MB)
        dobB = ComputeFileChecksum('oid:B', 'uid:B', eventbc=LocalEventBroadcaster([self.TestEventHandler]))
        dobA.addConsumer(dobB)

        dobA.open()

        test_crc = 0
        for i in range(self._test_num_blocks):
            dobA.write(None, chunk = self._test_block)
            test_crc = crc32(self._test_block, test_crc)

        dobA.close()
        self.assertTrue((test_crc == dobA.getChecksum() and 0 != test_crc),
                        msg = "test_crc = {0}, dob_crc = {1}".format(test_crc, dobA.getChecksum()))

    def test_write_StreamDataObject(self):
        """
        Test an AbstractDataObject and a simple AppDataObject (for checksum calculation)
        """

        dobA = StreamDataObject('oid:A', 'uid:A', eventbc=LocalEventBroadcaster())
        dobB = ComputeStreamChecksum('oid:B', 'uid:B', eventbc=LocalEventBroadcaster())
        dobA.addConsumer(dobB)

        dobA.open()

        test_crc = 0
        for i in range(self._test_num_blocks):
            dobA.write(None, chunk = self._test_block)
            test_crc = crc32(self._test_block, test_crc)

        dobA.close()
        self.assertTrue((test_crc == dobA.getChecksum() and 0 != test_crc),
                        msg = "test_crc = {0}, dob_crc = {1}".format(test_crc, dobA.getChecksum()))

    def test_join(self):
        """
        Using the container data object to implement a join/barrier dataflow

        -->A1(a1)--->|
        -->A2(a2)--->|-->B(b)
        -->A3(a3)--->|

        """
        filelen = self._test_do_sz * ONE_MB
        dobAList = []
        #create file data objects
        dobA1 = FileDataObject('oid:A1', 'uid:A1', eventbc=LocalEventBroadcaster([self.TestEventHandler]),
                               file_length=filelen)
        dobA2 = FileDataObject('oid:A2', 'uid:A2', eventbc=LocalEventBroadcaster([self.TestEventHandler]),
                               file_length=filelen)
        dobA3 = FileDataObject('oid:A3', 'uid:A3', eventbc=LocalEventBroadcaster([self.TestEventHandler]),
                               file_length=filelen)
        dobAList.append(dobA1)
        dobAList.append(dobA2)
        dobAList.append(dobA3)

        # create CRC component attached to the file data object
        dob_a1 = ComputeFileChecksum('oid:a1', 'uid:a1',
                                     eventbc=LocalEventBroadcaster([self.TestEventHandler]))
        dobA1.addConsumer(dob_a1)
        dob_a2 = ComputeFileChecksum('oid:a2', 'uid:a2',
                                     eventbc=LocalEventBroadcaster([self.TestEventHandler]))
        dobA2.addConsumer(dob_a2)
        dob_a3 = ComputeFileChecksum('oid:a3', 'uid:a3',
                                     eventbc=LocalEventBroadcaster([self.TestEventHandler]))
        dobA3.addConsumer(dob_a3)

        dobB = ContainerDataObject('oid:B', 'uid:B', eventbc=LocalEventBroadcaster())
        for dobA in dobAList:
            dobA.setParent(dobB)
            dobB.addChild(dobA)

        dob_b = SumupContainerChecksum('oid:b', 'uid:b',
                                       eventbc=LocalEventBroadcaster([self.TestEventHandler]))
        dobB.addConsumer(dob_b)

        for dobA in dobAList: # this should be parallel for
            dobA.open()
            #test_crc = 0
            for i in range(self._test_num_blocks):
                dobA.write(None, chunk = self._test_block)
                #test_crc = crc32(self._test_block, test_crc)
            dobA.close()

        sum_crc = 0
        for dobA in dobAList:
            sum_crc += dobA.getChecksum()

        self.assertTrue((sum_crc == dobB.getChecksum() and 0 != sum_crc),
                        msg = "sum_crc = {0}, dob_crc = {1}".format(sum_crc, dobB.getChecksum()))

if __name__ == '__main__':
    unittest.main()


