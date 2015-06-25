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
import random
import string
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
import time

import hsm.manager
import registry
from dfms.data_object import FileDataObject
from dfms.ddap_protocol import DOStates, DOPhases

_logger = logging.getLogger(__name__)

class DataObjectChecker(threading.Thread):
    """
    Small class that performs several background checks while the DLM is running
    until the DLM signals it to stop
    """

    def __init__(self, dlm, checkPeriod, finishedEvent):
        threading.Thread.__init__(self)
        self._finishedEvent = finishedEvent
        self._dlm = dlm
        self._checkPeriod = checkPeriod

    def run(self):
        ev = self._finishedEvent
        dlm = self._dlm
        while True:
            ev.wait(self._checkPeriod)
            if ev.is_set():
                break

            # It's not clear WHEN expired DOs must be deleted.
            #dlm.deleteExpiredDataObjects()

            # Expire DOs that are already too old
            dlm.expireCompletedDataObjects()

            # Check that DOs still exist, and mark them as lost
            # if they are not found
            dlm.deleteLostDataObjects()

class DataLifecycleManager(object):

    def __init__(self, **kwargs):
        self._hsm = hsm.manager.HierarchicalStorageManager()
        self._reg = registry.InMemoryRegistry()
        self._dos = {}

        self._checkPeriod = 10
        if kwargs.has_key('checkPeriod'):
            self._checkPeriod = float(kwargs['checkPeriod'])

    def startup(self):
        # Spawn the background thread that will check for expired DOs
        finishedEvent = threading.Event()
        dataExpiredChecker = DataObjectChecker(self, self._checkPeriod, finishedEvent)
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

    #
    # Support for 'with' keyword
    #
    def __enter__(self):
        self.startup()
        return self

    def __exit__(self, typ, value, traceback):
        self.cleanup()

    def deleteExpiredDataObjects(self):
        for do in self._dos.itervalues():
            if do.status == DOStates.EXPIRED:
                if _logger.isEnabledFor(logging.DEBUG):
                    _logger.debug("Deleting DataObject %s/%s" % (do.oid, do.uid))
                do.delete()
                do.status = DOStates.DELETED

    def expireCompletedDataObjects(self):
        now = time.time()
        for do in self._dos.itervalues():
            if do.status == DOStates.COMPLETED and now > do.expirationDate:
                if (_logger.isEnabledFor(logging.DEBUG)):
                    _logger.debug('Marking DataObject %s/%s as EXPIRED' % (do.oid, do.uid))
                do.status = DOStates.EXPIRED

    def _disappeared(self, dataObject):
        return dataObject.status != DOStates.DELETED and not dataObject.exists()

    def deleteLostDataObjects(self):

        toRemove = []
        for do in self._dos.itervalues():
            if self._disappeared(do):

                toRemove.append(do)
                if _logger.isEnabledFor(logging.WARNING):
                    _logger.warning('DataObject %s/%s has disappeared' % (do.oid, do.uid))

                # Check if it's replicated
                uids = self._reg.getDataObjectUids(do)
                definitelyLost = False
                if len(uids) <= 1:
                    definitelyLost = True
                else:

                    # Replicas haven't disappeared as well, right?
                    replicas = []
                    for uid in uids:
                        if uid == do.uid: pass
                        siblingDO = self._dos[uid]
                        if not self._disappeared(siblingDO):
                            replicas.append(siblingDO)
                        else:
                            if _logger.isEnabledFor(logging.WARNING):
                                _logger.warning('DataObject %s/%s (replicated from %s/%s) has disappeared' % (siblingDO.oid, siblingDO.uid, do.oid, do.uid))
                            toRemove.append(siblingDO)

                    if len(replicas) > 1:
                        _logger.info("DataObject %s/%s has still more than one replica, no action needed")
                    elif len(replicas) == 1:
                        _logger.info("Only one replica left for DataObject %s/%s, will create a new one")
                        self.replicateDataObject(replicas[0])
                    else:
                        definitelyLost = True

                if definitelyLost:
                    _logger.error("No available replica found for DataObject %s/%s, the data is DEFINITELY LOST" % (do.oid, do.uid))
                    do.phase = DOPhases.LOST
                    do.status = DOStates.DELETED
                    self._reg.setDataObjectPhase(do, do.phase)

        # All those objects identified as lost have to go now
        for do in toRemove:
            del self._dos[do.uid]

    def addDataObject(self, dataObject):

        # Keep track of the DO and subscribe to the events it generates
        self._dos[dataObject.uid] = dataObject
        dataObject.phase = DOPhases.GAS
        dataObject.subscribe(self.doEventHandler)
        self._reg.addDataObject(dataObject)

        # Keep track of its expiration date
        # TODO: We currently use a background thread that scans
        #       all DOs for their status. Instead, we could use
        #       event subscription and handle the particular case
        #       of DOs going to EXPIRED

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
        if dataObject.precious:
            _logger.debug("Replicating DataObject %s/%s because it's precious" % (oid, uid))
            self.replicateDataObject(dataObject)

    def replicateDataObject(self, dataObject):
        '''
        :param dfms.data_object.AbstractDataObject dataObject:
        '''

        oid = dataObject.oid
        uid = dataObject.uid

        # TODO: Actually check if DOs are replicable actually. For instance,
        # AppDOs or container DOs are probably not replicable
        canBeReplicated = True
        if not canBeReplicated:
            raise Exception("DataObject %s/%s cannot be replicated" % (oid, uid))

        # Check that the DO is complete already
        if dataObject.status != DOStates.COMPLETED:
            raise Exception("DataObject %s/%s not in COMPLETED state" % (oid, uid))

        # Get the size of the DO. This cannot currently be done in some of them,
        # like in the AbstractDataObject
        size = dataObject.size

        # Check which layer of the hsm should host the replicated copy
        store = self._hsm.getSlowestStore()
        availableSpace = store.getAvailableSpace()

        if size > availableSpace:
            raise Exception("Cannot replicate DO to store %s: not enough space left")

        # Create new DO and write the contents of the original into it
        # TODO: In a real world application this will probably happen in a separate
        #       worker thread with a working queue, or in separate threads, one
        #       for each replication task (or a mix of the two)
        newUid = 'uid:' + ''.join([random.choice(string.ascii_letters + string.digits) for _ in xrange(10)])

        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug('Creating new DataObject %s/%s from %s/%s' % (dataObject.oid, newUid, dataObject.oid, dataObject.uid))

        newDO = FileDataObject(dataObject.oid, newUid, dataObject._bcaster, file_length=dataObject.size, precious=dataObject.precious)
        newDO.open()
        dataObject.open(mode='r')
        buf = dataObject.read()
        while buf:
            newDO.write(chunk=buf)
        dataObject.close()
        newDO.close()

        # The DOs (both) should now be tagged as SOLID
        newDO.phase = DOPhases.SOLID
        dataObject.phase = DOPhases.SOLID

        # Update our own registry
        self._dos[newUid] = newDO
        self._reg.addDataObjectInstance(newDO)
        self._reg.setDataObjectPhase(dataObject, DOPhases.SOLID)

    def getDataObjectUids(self, dataObject):
        return self._reg.getDataObjectUids(dataObject)