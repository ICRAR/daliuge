"""
"""

from dfms.data_object import AbstractDataObject, AppDataObject, StreamDataObject, FileDataObject, ComputeStreamChecksum, ComputeFileChecksum, ContainerDataObject
from dfms.events.event_broadcaster import LocalEventBroadcaster
from dfms.events.pyro.pyro_event_broadcaster import PyroEventBroadcaster
from Pyro.EventService.Server import EventServiceStarter
from Pyro.naming import NameServerStarter
import Pyro.core

import os, unittest, threading, socket

try:
    from crc32c import crc32
except:
    from binascii import crc32

ONE_MB = 1024 ** 2

def _start_ns_thread(ns_daemon):
    ns_daemon.requestLoop()

class AccumulateChecksum(AppDataObject):
    """
    A dummy component that sums up checksums of all children of the host
    """
    def appInitialize(self, **kwargs):
        pass

    def run(self, **kwargs):
        self.checksum += kwargs['checksum']


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

    def TestEventHandler(self, event):
        #print
        print "Test event from {0}: {1} = {2}".format(event.oid, event.type, event.status)
        #print event.source

    def test_write_FileDataObject(self):
        """
        Test an AbstractDataObject and a simple AppDataObject (for checksum calculation)
        """
        Pyro.config.PYRO_HOST = 'localhost'
        nameservice = NameServerStarter()
        eventservice = EventServiceStarter()

        nameThread = threading.Thread(None, self._nameThread, 'namethread', (nameservice,))
        nameThread.setDaemon(True)
        nameThread.start()
        nameservice.waitUntilStarted()

        eventThread = threading.Thread(None, self._eventThread, 'eventthread', (eventservice, Pyro.config.PYRO_HOST ))
        eventThread.setDaemon(True)
        eventThread.start()
        eventservice.waitUntilStarted()

        eventbc = PyroEventBroadcaster()

        dobA = FileDataObject('oid:A', 'uid:A', eventbc=eventbc, subs=[self.TestEventHandler], file_length = self._test_do_sz * ONE_MB)
        dobB = ComputeFileChecksum('oid:B', 'uid:B', eventbc=eventbc, subs=[self.TestEventHandler])
        dobA.addConsumer(dobB)

        dobA.open()

        test_crc = 0
        for i in range(self._test_num_blocks):
            dobA.write(chunk = self._test_block)
            test_crc = crc32(self._test_block, test_crc)

        dobA.close()
        
        self.assertTrue((test_crc == dobB.checksum and 0 != test_crc),
                        msg = "test_crc = {0}, dob_crc = {1}".format(test_crc, dobB.checksum))
        
        nameservice.shutdown()

    def test_write_StreamDataObject(self):
        """
        Test an AbstractDataObject and a simple AppDataObject (for checksum calculation)
        """
        eventbc=LocalEventBroadcaster()

        dobA = StreamDataObject('oid:A', 'uid:A', eventbc=eventbc)
        dobB = ComputeStreamChecksum('oid:B', 'uid:B', eventbc=eventbc)
        dobA.addConsumer(dobB)

        dobA.open()

        test_crc = 0
        for i in range(self._test_num_blocks):
            dobA.write(chunk = self._test_block)
            test_crc = crc32(self._test_block, test_crc)

        dobA.close()
        self.assertTrue((test_crc == dobB.checksum and 0 != test_crc),
                        msg = "test_crc = {0}, dob_crc = {1}".format(test_crc, dobB.checksum))

    def test_join(self):
        """
        Using the container data object to implement a join/barrier dataflow

        -->A1(a1)--->|
        -->A2(a2)--->|-->B(b)
        -->A3(a3)--->|

        """
        eventbc = LocalEventBroadcaster()

        filelen = self._test_do_sz * ONE_MB
        dobAList = []
        #create file data objects
        dobA1 = FileDataObject('oid:A1', 'uid:A1', eventbc=eventbc, subs=[self.TestEventHandler],
                               file_length=filelen)
        dobA2 = FileDataObject('oid:A2', 'uid:A2', eventbc=eventbc, subs=[self.TestEventHandler],
                               file_length=filelen)
        dobA3 = FileDataObject('oid:A3', 'uid:A3', eventbc=eventbc, subs=[self.TestEventHandler],
                               file_length=filelen)
        dobAList.append(dobA1)
        dobAList.append(dobA2)
        dobAList.append(dobA3)

        # create CRC component attached to the file data object
        dob_a1 = ComputeFileChecksum('oid:a1', 'uid:a1',
                                     eventbc=eventbc, subs=[self.TestEventHandler])
        dobA1.addConsumer(dob_a1)
        
        dob_a2 = ComputeFileChecksum('oid:a2', 'uid:a2',
                                     eventbc=eventbc, subs=[self.TestEventHandler])
        dobA2.addConsumer(dob_a2)
        
        dob_a3 = ComputeFileChecksum('oid:a3', 'uid:a3',
                                     eventbc=eventbc, subs=[self.TestEventHandler])
        dobA3.addConsumer(dob_a3)

        dobB = ContainerDataObject('oid:B', 'uid:B', eventbc=eventbc)
        for dobA in dobAList:
            dobA.parent = dobB
            dobB.addChild(dobA)

        # bring all the checksums back
        accum = AccumulateChecksum('oid:accum', 'uid:accum',
                                       eventbc=eventbc, subs=[self.TestEventHandler])
        dob_a1.addConsumer(accum)
        dob_a2.addConsumer(accum)
        dob_a3.addConsumer(accum)

        # run it
        for dobA in dobAList: # this should be parallel for
            dobA.open()
            #test_crc = 0
            for i in range(self._test_num_blocks):
                dobA.write(chunk = self._test_block)
                #test_crc = crc32(self._test_block, test_crc)
            dobA.close()

        # get individual checksums then sum
        sum_crc = dob_a1.checksum + dob_a2.checksum + dob_a3.checksum

        # check individual sum equals accumulated sum
        self.assertTrue((sum_crc == accum.checksum and 0 != sum_crc),
                        msg = "sum_crc = {0}, dob_crc = {1}".format(sum_crc, accum.checksum))

    def test_lmc(self):
        """
        """
        import datetime
        import Pyro4
        from dfms.data_manager import DataManager
        from dfms import data_object_mgr, dataflow_manager
        from dfms.ddap_protocol import DOStates

        # 1. launch name service
        ns_host = 'localhost'
        my_host = 'localhost'
        my_port = 7778

        ns_uri, ns_daemon, bcsvr = Pyro4.naming.startNS(host=ns_host, port=my_port)
        args = (ns_daemon,)
        thref = threading.Thread(None, _start_ns_thread, 'NSThrd', args)
        thref.setDaemon(1)
        print 'Launching naming service daemon'
        thref.start()

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
            (pdg, doms) = dataflow_manager.buildSimpleIngestPDG(obsId, ns_host, port=my_port)

            # 4. start a single data manager
            print "**** step 4"
            dmgr = DataManager()
            dmgr.start() # start the daemon

            print "**** step 5"
            # 5. submit the graph to data manager
            res_avail = dmgr.submitPDG(pdg, doms)
            if (not res_avail):
                raise Exception("Resource is not available in the data manager!")

            print "**** step 6"
            # 6. start the pipeline (simulate CSP)
            pdg.write()

            do1 = pdg.consumers[0]
            do2 = do1.consumers[0].consumers[0]
            self.assertTrue(do1.status == DOStates.DIRTY and do1.status == DOStates.DIRTY)

            print "**** step 7"
            # 7. tear down data objects of this observation on each data object manager
            for dom in doms:
                ret = dom.shutdownDOBDaemon(obsId)
                print '%s was shutdown, ret code = %d' % (dom.getURI(), ret)

            # 8. shutdown the data manager daemon
            dmgr.shutdown()

        finally:
            # 9. shutdown name service
            try:
                ns_daemon.shutdown()
            except:
                pass


if __name__ == '__main__':
    unittest.main()


