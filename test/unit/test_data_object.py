"""
"""

from dfms.data_object import AbstractDataObject, AppDataObject, StreamDataObject, FileDataObject, ComputeStreamChecksum, ComputeFileChecksum

import os, unittest

try:
    from crc32c import crc32
except:
    from binascii import crc32

ONE_MB = 1024 ** 2

class TestDataObject(unittest.TestCase):
    
    def setUp(self):
        """
        library-specific setup
        """
        self._test_do_sz = 64 # MB
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
        Test an AbstractDataObject and a simple AppDataObject (for checksum calculation)
        """
        
        dobA = FileDataObject('oid:A', 'uid:A', file_length = self._test_do_sz * ONE_MB)
        dobB = ComputeFileChecksum('oid:B', 'uid:B')
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
       
        dobA = StreamDataObject('oid:A', 'uid:A')
        dobB = ComputeStreamChecksum('oid:B', 'uid:B')
        dobA.addConsumer(dobB)
        
        dobA.open()
        
        test_crc = 0
        for i in range(self._test_num_blocks):
            dobA.write(None, chunk = self._test_block)
            test_crc = crc32(self._test_block, test_crc)
        
        dobA.close()
        self.assertTrue((test_crc == dobA.getChecksum() and 0 != test_crc), 
                        msg = "test_crc = {0}, dob_crc = {1}".format(test_crc, dobA.getChecksum()))


if __name__ == '__main__':
    unittest.main()


