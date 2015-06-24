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

'''
Created on 22 Jun 2015

@author: rtobar
'''

logging.basicConfig(format="%(asctime)-15s [%(levelname)s] %(name)s#%(funcName)s:%(lineno)s %(msg)s", level=logging.DEBUG)

class TestDataLifecycleManager(TestCase):

    def test_basicCreation(self):
        manager = dlm.DataLifecycleManager()
        manager.startup()
        manager.cleanup()

    def test_DOAddition(self):
        manager = dlm.DataLifecycleManager()
        manager.startup()

        try:
            bcaster = LocalEventBroadcaster()
            dataObject = data_object.FileDataObject('oid:A', 'uid:A1', bcaster, file_length=10)
            manager.addDO(dataObject)

            dataObject.open()
            dataObject.write(chunk=' ')

        finally:
            manager.cleanup()

    def test_DOToCompleteTriggersEventHandling(self):
        manager = dlm.DataLifecycleManager()
        manager.startup()

        try:
            bcaster = LocalEventBroadcaster()
            dataObject = data_object.FileDataObject('oid:A', 'uid:A1', bcaster, file_length=1)
            manager.addDO(dataObject)

            dataObject.open()
            dataObject.write(chunk=' ')

        finally:
            manager.cleanup()