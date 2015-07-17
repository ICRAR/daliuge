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

import logging
from dfms.lifecycle import dlm
from dfms import data_object
from dfms.events.event_broadcaster import LocalEventBroadcaster
from unittest.case import TestCase
import time
from dfms.ddap_protocol import DOStates, DOPhases
import os
import sys
import unittest

'''
Created on 22 Jun 2015

@author: rtobar
'''

logging.basicConfig(format="%(asctime)-15s [%(levelname)-5s] %(name)s#%(funcName)s:%(lineno)s %(msg)s", level=logging.DEBUG, stream=sys.stdout)

class TestDataLifecycleManager(TestCase):

    def _writeAndClose(self, dataObject):
        '''
        :param dfms.data_object.AbstractDataObject dataObject:
        '''
        dataObject.write(' ')
        # all DOs submitted to this method have expectedSize=1, so this
        # will trigger the change to COMPLETED

    def test_basicCreation(self):
        manager = dlm.DataLifecycleManager()
        manager.startup()
        manager.cleanup()

    def test_dataObjectAddition(self):
        with dlm.DataLifecycleManager() as manager:
            bcaster = LocalEventBroadcaster()
            dataObject = data_object.FileDataObject('oid:A', 'uid:A1', bcaster, expectedSize=10)
            manager.addDataObject(dataObject)

    def test_dataObjectCompleteTriggersReplication(self):
        with dlm.DataLifecycleManager() as manager:
            bcaster = LocalEventBroadcaster()
            dataObject = data_object.FileDataObject('oid:A', 'uid:A1', bcaster, expectedSize=1)
            manager.addDataObject(dataObject)
            self._writeAndClose(dataObject)

            # The call to close() should have turned it into a SOLID object
            # because the DLM replicated it
            self.assertEquals(DOPhases.SOLID, dataObject.phase)
            self.assertEquals(2, len(manager.getDataObjectUids(dataObject)))

            # Try the same with a non-precious data object, it shouldn't be replicated
            dataObject = data_object.FileDataObject('oid:B', 'uid:B1', bcaster, expectedSize=1, precious=False)
            manager.addDataObject(dataObject)
            self._writeAndClose(dataObject)
            self.assertEquals(DOPhases.GAS, dataObject.phase)
            self.assertEquals(1, len(manager.getDataObjectUids(dataObject)))

    def test_expiringNormalDataObject(self):

        with dlm.DataLifecycleManager(checkPeriod=0.5) as manager:
            bcaster = LocalEventBroadcaster()
            dataObject = data_object.FileDataObject('oid:A', 'uid:A1', bcaster, expectedSize=1, lifespan=0.5)
            manager.addDataObject(dataObject)

            # Writing moves the DO to COMPLETE
            self._writeAndClose(dataObject)

            # Wait now, the DO should be moved by the DLM to EXPIRED
            time.sleep(1)

            self.assertEquals(DOStates.EXPIRED, dataObject.status)


    def test_expiringContainerDataObject(self):

        with dlm.DataLifecycleManager(checkPeriod=0.5) as manager:

            bcaster = LocalEventBroadcaster()
            dataObject1 = data_object.FileDataObject('oid:A', 'uid:A1', bcaster, expectedSize=1, lifespan=0.5)
            dataObject2 = data_object.FileDataObject('oid:B', 'uid:B1', bcaster, expectedSize=1, lifespan=0.5)
            dataObject3 = data_object.FileDataObject('oid:C', 'uid:C1', bcaster, expectedSize=1, lifespan=1.5)
            containerDO = data_object.ContainerDataObject('oid:D', 'uid:D1', bcaster)
            containerDO.addChild(dataObject1)
            containerDO.addChild(dataObject2)
            containerDO.addChild(dataObject3)

            manager.addDataObject(dataObject1)
            manager.addDataObject(dataObject2)
            manager.addDataObject(dataObject3)
            manager.addDataObject(containerDO)

            # Writing moves DOs to COMPLETE
            for do in [dataObject1, dataObject2, dataObject3]:
                self._writeAndClose(do)

            # Wait a bit, DO #1 and #2 should be have been moved by the DLM to EXPIRED,
            time.sleep(1)
            expired    = [dataObject1, dataObject2]
            notExpired = [dataObject3, containerDO]
            for do in expired:
                self.assertEquals(DOStates.EXPIRED, do.status)
            for do in notExpired:
                self.assertNotEquals(DOStates.EXPIRED, do.status)

            # Wait a bit more, now all DOs should be expired
            time.sleep(1)
            for do in [dataObject1, dataObject2, dataObject3, containerDO]:
                self.assertEquals(DOStates.EXPIRED, do.status)

    def test_lostDataObject(self):
        with dlm.DataLifecycleManager(checkPeriod=0.5) as manager:
            bcaster = LocalEventBroadcaster()
            do = data_object.FileDataObject('oid:A', 'uid:A1', bcaster, expectedSize=1, lifespan=10, precious=False)
            manager.addDataObject(do)
            self._writeAndClose(do)

            # "externally" remove the file, its contents now don't exist
            os.unlink(do._fnm)

            # Let the DLM do its work
            time.sleep(1)

            # Check that the DO is marked as LOST
            self.assertEquals(DOPhases.LOST, do.phase)

    def test_cleanupExpiredDataObjects(self):
        with dlm.DataLifecycleManager(checkPeriod=0.5, cleanupPeriod=2) as manager:
            bcaster = LocalEventBroadcaster()
            do = data_object.FileDataObject('oid:A', 'uid:A1', bcaster, expectedSize=1, lifespan=1, precious=False)
            manager.addDataObject(do)
            self._writeAndClose(do)

            # Wait 2 seconds, the DO is still COMPLETED
            time.sleep(0.5)
            self.assertEquals(DOStates.COMPLETED, do.status)
            self.assertTrue(do.exists())

            # Wait 5 more second, now it should be expired but still there
            time.sleep(1)
            self.assertEquals(DOStates.EXPIRED, do.status)
            self.assertTrue(do.exists())

            # Wait 2 more seconds, now it should have been deleted
            time.sleep(1)
            self.assertEquals(DOStates.DELETED, do.status)
            self.assertFalse(do.exists())

if __name__ == '__main__':
    unittest.main()