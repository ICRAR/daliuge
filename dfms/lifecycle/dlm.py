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
'''
A simple implementation of the data lifecycle manager (DLM). The rest of this
documentation is an extract from the PDR document where the DLM is defined and
its requirements laid out.

The data layer needs a subsystem that automates and manages the migration of
DataObjects through the various life cycle states. This is a consequence of the
sheer number of parallel tasks when dealing with the very high data rate and
volume. The goal of data life-cycle management is the optimal placement of data
in terms of cost and performance and employs a multi-tiered storage system to do
so. It is important to note that the life-cycle of a DataObject can span from
minutes to many years and therefore seamlessly covers the processing as well as
the long term storage (science archive) domain.

Data life-cycle management has to support a number of requirements:
 1. Migration of data from one medium to another: SKA1-SYS_REQ-2728
 2. Aggregation of DataObjects into DataProducts and DataPackages: this is
    non-trivial because of concurrent workflows, SKA1-SYS_REQ-2128, SDP_REQ-252,
    and the need for data tracing, provenance and access control,
    SKA1-SYS_REQ-2821, SDP_REQ-255
 3. Migration between storage layers, includes SDP_REQ-263
 4. Replication/distribution including resilience (preciousness) support, incl.
    SKA1-SYS_REQ-2350, SDP_REQ-260 - 262
 5. Retirement of expired (temporary) data, incl. SDP_REQ-256.

Other parts of the documentation state that the DLM is configured by a Data
Lifecycle Configurator Manager, but for the prototyping we can use a self-
configured DLM

@author: rtobar
'''

import logging
import threading

import hsm.manager
import registry
from dfms.data_object import FileDataObject
from dfms.ddap_protocol import DOStates, DOPhases

_logger = logging.getLogger(__name__)

class ExpiredDataObjectChecker(threading.Thread):
    """
    Small class that calls the deleteExpiredDataObjects() method
    on the given DLM every 10 seconds until it is signaled to stop
    """

    def __init__(self, dlm, finishedEvent):
        threading.Thread.__init__(self)
        self._finishedEvent = finishedEvent
        self._dlm = dlm

    def run(self):
        ev = self._finishedEvent
        dlm = self._dlm
        while True:
            if ev.is_set():
                break
            dlm.deleteExpiredDataObjects()
            ev.wait(10)

class DataLifecycleManager(object):

    def __init__(self):
        self._hsm = hsm.manager.HierarchicalStorageManager()
        self._reg = registry.InMemoryRegistry()
        self._dos = {}

    def startup(self):
        # Spawn the background thread that will check for expired DOs
        finishedEvent = threading.Event()
        dataExpiredChecker = ExpiredDataObjectChecker(self, finishedEvent)
        dataExpiredChecker.start()

        self._dataExpiredChecker = dataExpiredChecker
        self._finishedEvent = finishedEvent

    def cleanup(self):
        _logger.info("Cleaning up DLM")

        # Unsubscribe to all events coming from the DOs
        for do in self._dos.itervalues():
            do.unsubscribe(self.doEventHandler)

        # Join the background thread
        self._finishedEvent.set()
        self._dataExpiredChecker.join()
        pass

    def deleteExpiredDataObjects(self):
        _logger.info("Deleting expired DOs")
        for do in self._dos.itervalues():
            if do.status == DOStates.EXPIRED:
                do.delete()

    def addDO(self, dataObject):
        self._dos[dataObject.uid] = dataObject
        dataObject.subscribe(self.doEventHandler)

    def doEventHandler(self, event):
        if event.type == 'status' and event.status == DOStates.COMPLETED:
            self.handleCompletedDO(self._dos[event.uid])
        else:
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug('Received event: ' + str(event.__dict__))

    def handleCompletedDO(self, dataObject):
        '''
        :param dfms.data_object.AbstractDataObject dataObject:
        '''
        # Check the kind of storage used by this DO. If it's already persisted
        # in a persistent storage media we don't need to save it again
        oid = dataObject.oid
        uid = dataObject.uid
        _logger.debug('Detected switch to COMPLETED state on DataObject %s/%s' % (oid, uid))
        if isinstance(dataObject, FileDataObject):
            pass

    def replicateDataObject(self, dataObject):
        '''
        :param dfms.data_object.AbstractDataObject dataObject:
        '''

        oid = dataObject.oid()
        uid = dataObject.uid()

        # TODO: Actually check if DOs are replicable actually. For instance,
        # AppDOs or container DOs are probably not replicable
        canBeReplicated = True
        if not canBeReplicated:
            raise Exception("DataObject %s/%s cannot be replicated" % (oid, uid))

        # Check that the DO is complete already
        if dataObject.status() != DOStates.COMPLETED:
            raise Exception("DataObject %s/%s not in COMPLETED state" % (oid, uid))

        # Get the size of the DO. This cannot currently be done in some of them,
        # like in the AbstractDataObject
        size = dataObject.size

        # Check which layer of the hsm should host the replicated copy
        store = self._hsm.getSlowestStore()
        availableSpace = store.getAvailableSpace()

        if size > availableSpace:
            raise Exception("Cannot replicate DO to store %s: not enough space left")

        newDO = store.generateDataObject()
        newDO.setPhase(DOPhases.SOLID)
        self._reg.addInstance(newDO)