#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2014
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
# Who                   When          What
# ------------------------------------------------
# chen.wu@icrar.org   15/Feb/2015     Created
#
"""
Data object is the centre of the data-driven architecture
It should be based on the UML class diagram
"""

from abc import ABCMeta, abstractmethod
from cStringIO import StringIO
import collections
import heapq
import logging
from operator import __or__
import os, time
import random
import socket
import sys
import threading
import warnings

from ddap_protocol import DOStates
from dfms.ddap_protocol import ExecutionMode, ChecksumTypes
from dfms.events.event_broadcaster import LocalEventBroadcaster
from dfms.io import OpenMode, FileIO, MemoryIO, NgasIO, ErrorIO, NullIO


try:
    from crc32c import crc32
    _checksumType = ChecksumTypes.CRC_32C
except:
    from binascii import crc32
    _checksumType = ChecksumTypes.CRC_32


_logger = logging.getLogger(__name__)



#===============================================================================
# DataObject classes follow
#===============================================================================


class AbstractDataObject(object):
    """
    Base class for all DataObject implementations.

    A DataObject is a representation of a piece of data. DataObjects are created,
    written once, potentially read many times, and they finally potentially
    expire and get deleted. Subclasses implement different storage mechanisms
    to hold the data represented by the DataObject.

    If the data represented by this DataObject is written *through* this object
    (i.e., calling the ``write`` method), this DataObject will keep track of the
    data's size and checksum. If the data is written externally, the size and
    checksum can be fed into this object for future reference.

    DataObjects can have consumers attached to them. 'Normal' consumers will
    wait until the DataObject they 'consume' (their 'producer') moves to the
    COMPLETED state and then will 'consume' it, most typically by opening it
    and reading its contents, but any other operation could also be performed.
    How the consumption is triggered depends on the producer's ``executionMode``
    flag, which dictates whether it should trigger the consumption itself or
    if it should be manually triggered by an external entity. On the other hand,
    'immediate' consumers receive the data that is written into its publisher
    *as it gets written*. This mechanism is driven always by the DataObject that
    acts as 'immediate producer'. Apart from receiving the data as it gets
    written into the DataObject, immediate consumers are also notified when the
    DataObjects moves to the COMPLETED state, at which point no more data should
    be expected to arrive at the consumer side.
    """

    # This ensures that:
    #  - This class cannot be instantiated
    #  - Subclasses implement methods decorated with @abstractmethod
    __metaclass__ = ABCMeta

    def __init__(self, oid, uid,
                 executionMode=ExecutionMode.DO,
                 **kwargs):
        """
        Constructor
        oid:    object id (string)
        uid:    uuid    (string)
        """

        super(AbstractDataObject, self).__init__()

        # So far only these three are mandatory
        self._oid = oid
        self._uid = uid

        self._bcaster = LocalEventBroadcaster()

        # Maybe we want to have a different default value for this one?
        self._executionMode = executionMode

        # 1-to-N relationship: one DataObject may have many consumers, but
        # has only 1 producer. The potential consumers and producer are always
        # AppDataObjects instances
        self._consumers = []
        self._producer  = None

        # 'Immediate' consumers are objects that consume the data written in
        # this DataObject *as it gets written*, and therefore don't have to
        # wait until this DataObject has moved to COMPLETED.
        # An object cannot be an immediate consumers and a 'normal' consumer
        # at the same time, although this rule is imposed simply to enforce
        # efficiency (why would a consumer want to consume the data twice?) and
        # not because it's technically impossible.
        # TODO: A better name would be highly beneficial to avoid confusion in
        #       the future between 'normal' and 'immediate'/'quick'/'eager'
        #       consumers.
        self._immediateConsumers = []
        self._immediateProducer  = None

        self._refCount = 0
        self._refLock  = threading.Lock()
        self._location = None
        self._parent   = None
        self._status   = None
        self._statusLock = threading.RLock()
        self._phase    = None

        # Calculating the checksum and maintaining the data size internally
        # implies that the data represented by this DataObject is written
        # *through* this DataObject. This might not always be the case though,
        # since data could be written externally and the DataObject simply be
        # moved to COMPLETED at the end of the process. In this case we return a
        # None checksum and size (when requested), signaling that we don't have
        # this information.
        # Note also that the setters of these two properties also allow to set
        # a value on them, but only if they are None
        self._checksum     = None
        self._checksumType = None
        self._size         = None

        # "DataObject descriptors" used for reading.
        # Instead of passing file objects or more complex data types in our
        # open/read/close calls we use integers, mainly because Pyro doesn't
        # handle file types and other classes (like StringIO) well, but also
        # because it requires less transport.
        # TODO: Make these threadsafe, no lock around them yet
        self._readDescriptors = {}

        # Expected lifespan for this object, used by to expire them
        lifespan = -1
        if kwargs.has_key('lifespan'):
            lifespan = float(kwargs['lifespan'])
        self._expirationDate = -1
        if lifespan != -1:
            self._expirationDate = time.time() + lifespan

        # Expected data size, used to automatically move the DO to COMPLETED
        # after successive calls to write()
        self._expectedSize = -1
        if kwargs.has_key('expectedSize'):
            self._expectedSize = int(kwargs['expectedSize'])

        # All DOs are precious unless stated otherwise; used for replication
        self._precious = True
        if kwargs.has_key('precious'):
            self._precious = bool(kwargs['precious'])

        # The writing IO instance we use in our write method. It's initialized
        # to None because it's lazily initialized in the write method, since
        # data might be written externally and not through this DO
        self._wio = None

        try:
            self.initialize(**kwargs)
            self.status = DOStates.INITIALIZED
        except:
            # It doesn't make sense to set an internal status here because
            # the creation of the object is actually raising an exception,
            # and the object doesn't get created and assigned to any variable
            # Still, the FAILED state could be used for other purposes, like
            # failure during writing for example.
            raise

    def _getArg(self, kwargs, key, default):
        val = default
        if kwargs.has_key(key) and kwargs[key]:
            val = kwargs[key]
        elif _logger.isEnabledFor(logging.DEBUG):
            _logger.debug("Defaulting %s to %s" % (key, str(val)))
        return val

    def __hash__(self):
        return hash(self._uid)

    def __repr__(self):
        re = "%s %s/%s" % (self.__class__.__name__, self.oid, self.uid)
        if self.location:
            re += "@{0}".format(self.location)
        return re

    def initialize(self, **kwargs):
        """
        Hook for subclass initialization.
        """

    def incrRefCount(self):
        """
        Increments the reference count of this DataObject by one atomically.
        """
        with self._refLock:
            self._refCount += 1

    def decrRefCount(self):
        """
        Decrements the reference count of this DataObject by one atomically.
        """
        with self._refLock:
            self._refCount -= 1

    def open(self, **kwargs):
        """
        Opens the DataObject for reading, and returns a "DataObject descriptor"
        that must be used when invoking the read() and close() method.
        DataObjects maintain a internal reference count based on the number
        of times they are opened for reading; because of that after a successful
        call to this method the corresponding close() method must eventually be
        invoked. Failing to do so will result in DataObjects not expiring and
        getting deleted.
        """
        # TODO: We could also allow opening EXPIRED DOs, in which case
        # it could trigger its "undeletion", but this would require an automatic
        # recalculation of its new expiration date, which is maybe something we
        # don't have to have
        if self.status != DOStates.COMPLETED:
            raise Exception("DataObject %s/%s is in state %s (!=COMPLETED), cannot be opened for reading" % (self._oid, self._uid, self.status))

        descriptor = io = self.getIO()
        io.open(OpenMode.OPEN_READ, **kwargs)

        # Save the real descriptor in the dictionary and return its key instead
        while True:
            key = random.SystemRandom().randint(-sys.maxint - 1, sys.maxint)
            if key not in self._readDescriptors:
                break
        self._readDescriptors[key] = descriptor

        # This occurs only after a successful opening
        self.incrRefCount()
        self._fire('open')

        return key

    def close(self, descriptor, **kwargs):
        """
        Closes the given DataObject descriptor, decreasing the DataObject's
        internal reference count and releasing the underlying resources
        associated to the descriptor.
        """
        self._checkStateAndDescriptor(descriptor)

        # Decrement counter and then actually close
        self.decrRefCount()
        io = self._readDescriptors.pop(descriptor)
        io.close(**kwargs)

    def read(self, descriptor, count=4096, **kwargs):
        """
        Reads `count` bytes from the given DataObject `descriptor`.
        """
        self._checkStateAndDescriptor(descriptor)
        io = self._readDescriptors[descriptor]
        return io.read(count, **kwargs)

    def _checkStateAndDescriptor(self, descriptor):
        if self.status != DOStates.COMPLETED:
            raise Exception("DataObject %s/%s is in state %s (!=COMPLETED), cannot be read" % (self._oid, self._uid, self.status))
        if descriptor not in self._readDescriptors:
            raise Exception("Illegal descriptor %d given, remember to open() first" % (descriptor))

    def isBeingRead(self):
        """
        Returns `True` if the DataObject is currently being read; `False`
        otherwise
        """
        with self._refLock:
            return self._refCount > 0

    def write(self, data, **kwargs):
        '''
        Writes the given `data` into this DataObject. This method is only meant
        to be called while the DataObject is in INITIALIZED or WRITING state;
        once the DataObject is COMPLETE or beyond only reading is allowed.
        The underlying storage mechanism is responsible for implementing the
        final writing logic via the `self.writeMeta()` method.
        '''

        if self.status not in [DOStates.INITIALIZED, DOStates.WRITING]:
            raise Exception("No more writing expected")

        # We lazily initialize our writing IO instance because the data of this
        # DO might not be written through this DO
        if not self._wio:
            self._wio = self.getIO()
            self._wio.open(OpenMode.OPEN_WRITE)
        nbytes = self._wio.write(data)

        dataLen = len(data)
        if nbytes != dataLen:
            # TODO: Maybe this should be an actual error?
            warnings.warn('Not all data was correctly written by %s (%d/%d bytes written)' % (self, nbytes, dataLen))

        # see __init__ for the initialization to None
        if self._size is None:
            self._size = 0
        self._size += nbytes

        # Trigger our immediate consumers
        if self._immediateConsumers:
            writtenData = buffer(data, 0, nbytes)
            for immediateConsumer in self._immediateConsumers:
                immediateConsumer.dataWritten(self.uid, writtenData)

        # Update our internal checksum
        self._updateChecksum(data)

        # If we know how much data we'll receive, keep track of it and
        # automatically switch to COMPLETED
        if self._expectedSize > 0:
            remaining = self._expectedSize - self._size
            if remaining > 0:
                self.status = DOStates.WRITING
            else:
                if remaining < 0:
                    _logger.warning("Received and wrote more bytes than expected: " + str(-remaining))
                _logger.debug("Automatically moving DataObject %s/%s to COMPLETED, all expected data arrived" % (self.oid, self.uid))
                self.setCompleted()
        else:
            self.status = DOStates.WRITING

        return nbytes

    @abstractmethod
    def getIO(self):
        """
        Returns an instance of one of the `dfms.io.DataIO` instances that
        handles the data contents of this DataObject.
        """

    def delete(self):
        """
        Deletes the data represented by this DataObject.
        """
        self.getIO().delete()

    def exists(self):
        """
        Returns `True` if the data represented by this DataObject exists indeed
        in the underlying storage mechanism
        """
        return self.getIO().exists()

    @abstractmethod
    def dataURL(self):
        """
        A URL that points to the data referenced by this DataObject. Different
        DataObject implementations will use different URI schemes.
        """

    def _updateChecksum(self, chunk):
        # see __init__ for the initialization to None
        if self._checksum is None:
            self._checksum = 0
            self._checksumType = _checksumType
        self._checksum = crc32(chunk, self._checksum)

    @property
    def checksum(self):
        """
        The checksum value for the data represented by this DataObject. Its
        value is automatically calculated if the data was actually written
        through this DataObject (using the `self.write()` method directly or
        indirectly). In the case that the data has been externally written, the
        checksum can be set externally after the DataObject has been moved to
        COMPLETED or beyond.

        :see: `self.checksumType`
        """
        return self._checksum

    @checksum.setter
    def checksum(self, value):
        if self._checksum is not None:
            raise Exception("The checksum for DataObject %s is already calculated, cannot overwrite with new value" % (self))
        if self.status in [DOStates.INITIALIZED, DOStates.WRITING]:
            raise Exception("DataObject %s is still not fully written, cannot manually set a checksum yet" % (self))
        self._checksum = value

    @property
    def checksumType(self):
        """
        The algorithm used to compute this DataObject's data checksum. Its value
        if automatically set if the data was actually written through this
        DataObject (using the `self.write()` method directly or indirectly). In
        the case that the data has been externally written, the checksum type
        can be set externally after the DataObject has been moved to COMPLETED
        or beyond.

        :see: `self.checksum`
        """
        return self._checksumType

    @checksumType.setter
    def checksumType(self, value):
        if self._checksumType is not None:
            raise Exception("The checksum type for DataObject %s is already set, cannot overwrite with new value" % (self))
        if self.status in [DOStates.INITIALIZED, DOStates.WRITING]:
            raise Exception("DataObject %s is still not fully written, cannot manually set a checksum type yet" % (self))
        self._checksumType = value

    @property
    def oid(self):
        """
        The DataObject's Object ID (OID). OIDs are unique identifiers given to
        semantically different DataObjects (and by consequence the data they
        represent). This means that different DataObjects that point to the same
        data semantically speaking, either in the same or in a different
        storage, will share the same OID.
        """
        return self._oid

    @property
    def uid(self):
        """
        The DataObject's Unique ID (UID). Unlike the OID, the UID is globally
        different for all DataObject instances, regardless of the data they
        point to.
        """
        return self._uid

    @property
    def executionMode(self):
        """
        The execution mode of this DataObject. If `ExecutionMode.DO` it means
        that this DataObject will automatically trigger the execution of all its
        consumers. If `ExecutionMode.EXTERNAL` it means that this DataObject
        will *not* trigger its consumers, and therefore an external entity will
        have to do it.
        """
        return self._executionMode

    def subscribe(self, callback, eventType=None):
        """
        Adds a new subscription to events fired from this DataObject.
        """
        self._bcaster.subscribe(callback, eventType=eventType)

    def unsubscribe(self, callback, eventType=None):
        """
        Removes a subscription from events fired from this DataObject.
        """
        self._bcaster.unsubscribe(callback, eventType=eventType)

    def _fire(self, eventType, **kwargs):
        kwargs['oid'] = self.oid
        kwargs['uid'] = self.uid
        self._bcaster.fire(eventType, **kwargs)

    def isReplicable(self):
        return True

    @property
    def phase(self):
        return self._phase

    @phase.setter
    def phase(self, phase):
        self._phase = phase

    @property
    def expirationDate(self):
        return self._expirationDate

    @property
    def size(self):
        """
        The size of the data pointed by this DataObject. Its value is
        automatically calculated if the data was actually written through this
        DataObject (using the `self.write()` method directly or indirectly). In
        the case that the data has been externally written, the size can be set
        externally after the DataObject has been moved to COMPLETED or beyond.
        """
        return self._size

    @size.setter
    def size(self, size):
        if self._size is not None:
            raise Exception("The size of DataObject %s is already calculated, cannot overwrite with new value" % (self))
        if self.status in [DOStates.INITIALIZED, DOStates.WRITING]:
            raise Exception("DataObject %s is still not fully written, cannot manually set a size yet" % (self))
        self._size = size

    @property
    def precious(self):
        """
        Whether this DataObject should be considered as 'precious' or not
        """
        return self._precious

    @property
    def status(self):
        """
        The current status of this DataObject.
        """
        with self._statusLock:
            return self._status

    @status.setter
    def status(self, value):
        with self._statusLock:
            # if we are already in the state that is requested then do nothing
            if value == self._status:
                return
            self._status = value

        self._fire('status', status = value)

    @property
    def parent(self):
        return self._parent

    @parent.setter
    def parent(self, parent):
        if self._parent and parent:
            warnings.warn("A parent is already set in DataObject %s/%s, overwriting with new value" % (self._oid, self._uid))
        if parent:
            prevParent = self._parent
            self._parent = parent # a parent is a container
            if hasattr(parent, 'addChild'):
                try:
                    parent.addChild(self)
                except:
                    self._parent = prevParent

    @property
    def consumers(self):
        """
        The list of 'normal' consumers held by this DataObject.

        :see: `self.addConsumer()`
        """
        return self._consumers[:]

    def addConsumer(self, consumer):
        """
        Adds a consumer to this DataObject.

        Consumers are a particular kind of subscriber that are only interested
        on the status change of DataObjects to COMPLETED. When the expected
        status change occurs, the consumers' `consume()` method is invoked with
        a reference to the DataObject that changed state.

        This is one of the key mechanisms by which the DataObject graph is
        executed automatically. If DataObject C consumes DataObject B, and B
        consumes C, then as soon as A transitions to COMPLETED B will consume A,
        and when B is finally COMPLETED C is triggered.
        """

        # Consumers have a "consume" method that gets invoked when
        # this DO moves to COMPLETED
        if not hasattr(consumer, 'dataObjectCompleted'):
            raise Exception("The consumer %s doesn't have a 'dataObjectCompleted' method, cannot add to %s" % (consumer, self))

        # An object cannot be a normal and immediate consumer at the same time,
        # see the comment in the __init__ method
        if consumer in self._immediateConsumers:
            raise Exception("Consumer %s is already registered as an immediate consumer" % (consumer))

        # Add if not already present
        # Add the reverse reference too automatically
        if consumer in self._consumers:
            return
        _logger.debug('Adding new consumer for DataObject %s/%s: %s' %(self.oid, self.uid, consumer))
        self._consumers.append(consumer)

        # Automatic back-reference
        if hasattr(consumer, 'addInput'):
            consumer.addInput(self)

        # Only trigger consumers automatically if the DataObject graph's
        # execution is driven by the DataObjects themselves
        if self._executionMode == ExecutionMode.EXTERNAL:
            return

        def dataObjectCompleted(e):
            if e.status != DOStates.COMPLETED:
                if _logger.isEnabledFor(logging.DEBUG):
                    _logger.debug('Skipping event for consumer %s: %s' %(consumer, str(e.__dict__)) )
                return
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug('Triggering consumer %s: %s' %(consumer, str(e.__dict__)))
            consumer.dataObjectCompleted(self.uid)
        self.subscribe(dataObjectCompleted, eventType='status')

    @property
    def producer(self):
        """
        The producer for which this DataObject acts as a 'normal' consumer, if
        any.

        :see: `self.addConsumer()`
        """
        return self._producer

    @producer.setter
    def producer(self, producer):
        if self._producer == producer:
            return
        if self._producer and producer:
            warnings.warn("A producer is already set in DataObject %s/%s, overwriting with new value" % (self._oid, self._uid))
        if producer:
            self._producer = producer
            if hasattr(producer, 'addOutput'):
                producer.addOutput(self)

    @property
    def immediateConsumers(self):
        """
        The list of 'immediate' consumers held by this DataObject.

        :see: `self.addImmediateConsumer()`
        """
        return self._immediateConsumers[:]

    def addImmediateConsumer(self, immediateConsumer):
        """
        Adds an immediate immediateConsumer to this DataObject.

        Immediate consumers are objects that receive the data written into this
        DataObject *as it gets written*, and therefore do not need to wait until
        this DataObject has been moved to the COMPLETED state.
        """

        # Consumers have a "consume" method that gets invoked when
        # this DO moves to COMPLETED
        if not hasattr(immediateConsumer, 'dataObjectCompleted') or not hasattr(immediateConsumer, 'dataWritten'):
            raise Exception("The immediate consumer %r doesn't have a 'dataObjectCompleted' and/or 'dataWritten' method" % (immediateConsumer))

        # An object cannot be a normal and immediate immediateConsumer at the same time,
        # see the comment in the __init__ method
        if immediateConsumer in self._consumers:
            raise Exception("Consumer %s is already registered as a normal consumer" % (immediateConsumer))

        # Add if not already present
        # Add the reverse reference too automatically
        if immediateConsumer in self._immediateConsumers:
            return
        _logger.debug('Adding new immediate immediate consumer for DataObject %s/%s: %s' %(self.oid, self.uid, immediateConsumer))
        self._immediateConsumers.append(immediateConsumer)

        # Automatic back-reference
        if hasattr(immediateConsumer, 'addImmediateInput'):
            immediateConsumer.addImmediateInput(self)

    @property
    def immediateProducer(self):
        """
        The producer for which this DataObject acts as an 'immediate' consumer,
        if any.

        :see: `self.addImmediateConsumer()`
        """
        return self._immediateProducer

    @immediateProducer.setter
    def immediateProducer(self, immediateProducer):
        if self._immediateProducer and immediateProducer:
            warnings.warn("A immediate producer is already set in DataObject %s/%s, overwriting with new value" % (self._oid, self._uid))
        if immediateProducer:
            self._producer = immediateProducer

    def setCompleted(self):
        '''
        Manually moves this DO to the COMPLETED state. This can be used when
        not all the expected data has arrived for a given DO, but it should
        still be moved to COMPLETED, or when the expected amount of data
        held by a DataObject is not known in advanced.
        '''
        if self.status not in [DOStates.INITIALIZED, DOStates.WRITING]:
            raise Exception("DataObject %s/%s not in INITIALIZED or WRITING state (%s), cannot setComplete()" % (self._oid, self._uid, self.status))

        # Close our writing IO instance.
        # If written externally, self._wio will have remained None
        if self._wio:
            self._wio.close()

        if _logger.isEnabledFor(logging.INFO):
            _logger.info("Moving DataObject %s/%s to COMPLETED" % (self._oid, self._uid))
        self.status = DOStates.COMPLETED

        # Signal our immediate consumers that the show is over
        for ic in self._immediateConsumers:
            ic.dataObjectCompleted(self.uid)

    def isCompleted(self):
        '''
        Checks whether this DO is currently in the COMPLETED state or not
        '''
        # Mind you we're not accessing _status, but status. This way we use the
        # lock in status() to access _status
        return (self.status == DOStates.COMPLETED)

    @property
    def location(self):
        """
        return where the "actual" data is located
        the location could be a Compute node or a Island or just the buffer URL
        """
        if (self._location is not None):
            return self._location
        else:
            return ''

    @location.setter
    def location(self, value):
        """
        This should be set when the physical graph was built
        """
        self._location = value

    @property
    def uri(self):
        return self._uri

    @uri.setter
    def uri(self, uri):
        self._uri = uri

    def ping(self):
        """
        This is for testing purpose
        """
        return 'OK. My oid = %s, and my uid = %s' % (self.oid, self.uid)

class FileDataObject(AbstractDataObject):

    def initialize(self, **kwargs):
        """
        File data object-specific initialization.
        """
        self._root = self._getArg(kwargs, 'dirname', '/tmp/sdp_dfms')
        if (not os.path.exists(self._root)):
            os.mkdir(self._root)
        self._root = os.path.abspath(self._root)

        # TODO: Make sure the parts that make up the filename are composed
        #       of valid filename characters; otherwise encode them
        self._fnm = self._root + os.sep + self._oid + '___' + self.uid
        if os.path.isfile(self._fnm):
            warnings.warn('File %s already exists, overwriting' % (self._fnm))

        self._wio = None

    def getIO(self):
        return FileIO(self._fnm)

    @property
    def path(self):
        return self._fnm

    @property
    def dataURL(self):
        hostname = os.uname()[1] # TODO: change when necessary
        return "file://" + hostname + self._fnm

class NgasDataObject(AbstractDataObject):
    '''
    A DataObject whose data is finally stored into NGAS. Since NGAS doesn't
    support appending data to existing files, we store all the data temporarily
    in a file on the local filesystem and then move it to the NGAS destination
    '''

    def initialize(self, **kwargs):

        # Check we actually can write NGAMS clients
        try:
            from ngamsPClient import ngamsPClient  # @UnusedImport @UnresolvedImport
        except:
            warnings.warn("No NGAMS client libs found, cannot use NgasDataObjects")
            raise

        self._ngasSrv            = self._getArg(kwargs, 'ngasSrv', 'localhost')
        self._ngasPort           = int(self._getArg(kwargs, 'ngasPort', 7777))
        # TODO: The NGAS client doesn't differentiate between these, it should
        self._ngasTimeout        = int(self._getArg(kwargs, 'ngasConnectTimeout', 2))
        self._ngasConnectTimeout = int(self._getArg(kwargs, 'ngasTimeout', 2))

        # The NGAS client API doesn't have a way to continually feed an ARCHIVE
        # request with data. Thus the only way we can currently archive data
        # into NGAS is by accumulating it all on our side and finally
        # sending it over.
        self._buf = ''

    def _getClient(self):
        from ngamsPClient import ngamsPClient  # @UnresolvedImport
        return ngamsPClient.ngamsPClient(self._ngasSrv, self._ngasPort, self._ngasTimeout)

    def getIO(self):
        return NgasIO(self._ngasSrv, self.uid, port=self._ngasPort,
                      ngasConnectTimeout=self._ngasConnectTimeout,
                      ngasTimeout=self._ngasTimeout)

    @property
    def dataURL(self):
        return "ngas://%s:%d/%s" % (self._ngasSrv, self._ngasPort, self.uid)

class InMemoryDataObject(AbstractDataObject):

    def initialize(self, **kwargs):
        self._wio = None
        self._buf = None

    def getIO(self):
        if self._buf is None:
            self._buf = StringIO()
        return MemoryIO(self._buf)

    @property
    def dataURL(self):
        hostname = os.uname()[1]
        return "mem://%s/%d/%d" % (hostname, os.getpid(), id(self._buf))

class NullDataObject(AbstractDataObject):
    """
    A DataObject that stores no data
    """

    def getIO(self):
        return NullIO()

    @property
    def dataURL(self):
        return "null://"

class ContainerDataObject(AbstractDataObject):
    """
    A DataObject that doesn't directly point to some piece of data, but instead
    holds references to other DataObjects, and from them its own internal state
    is deduced.

    Because of its nature, ContainerDataObjects cannot be written to directly,
    and likewise they cannot be read from directly. One instead has to pay
    attention to its "children" DataObjects if I/O must be performed.
    """

    def initialize(self, **kwargs):
        super(ContainerDataObject, self).initialize(**kwargs)
        self._children = []

    #===========================================================================
    # No data-related operations should actually be called in Container DOs
    #===========================================================================
    def getIO(self):
        return ErrorIO()
    def dataURL(self):
        raise NotImplementedError()

    def addChild(self, child):

        # Avoid circular dependencies between Containers
        if child == self.parent:
            raise Exception("Circular dependency between DataObjects %s/%s and %s/%s" % (self.oid, self.uid, child.oid, child.uid))

        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug("Adding new child for ContainerDataObject %s/%s: %s" % (self.oid, self.uid, child.uid))

        self._children.append(child)
        child.parent = self

    def delete(self):
        # TODO: this needs more thinking. Probably a separate method to perform
        #       this recursive deletion will be needed, while this delete method
        #       will go hand-to-hand with the rest of the I/O methods above,
        #       which are currently raise a NotImplementedError
        for c in [c for c in self._children if c.exists()]:
            c.delete()

    @property
    def expirationDate(self):
        if self._children:
            return heapq.nlargest(1, [c.expirationDate for c in self._children])[0]
        return None

    @property
    def children(self):
        return self._children[:]

    def exists(self):
        # TODO: Or should it be __and__? Depends on what the exact contract of
        #       "exists" is
        return reduce(__or__, [c.exists() for c in self._children])

    def isReplicable(self):
        return False


class DirectoryContainer(ContainerDataObject):
    """
    A ContainerDataObject that represents a filesystem directory. It only allows
    FileDataObjects and DirectoryContainers to be added as children. Children
    can only be added if they are placed directly within the directory
    represented by this DirectoryContainer.
    """

    def initialize(self, **kwargs):
        ContainerDataObject.initialize(self, **kwargs)

        if 'dirname' not in kwargs:
            raise Exception('DirectoryContainer needs a "directory" optional ')

        directory = kwargs['dirname']
        if not os.path.isdir(directory):
            raise Exception('%s is not a directory' % (directory))

        self._path = os.path.abspath(directory)

    def addChild(self, child):
        if isinstance(child, (FileDataObject, DirectoryContainer)):
            path = child.path
            if os.path.dirname(path) != self.path:
                raise Exception('Child DataObject is not under %s' % (self.path))
            ContainerDataObject.addChild(self, child)
        else:
            raise TypeError('Child data object is not of type FileDataObject or DirectoryContainer')

    @property
    def path(self):
        return self._path


#===============================================================================
# AppDataObject classes follow
#===============================================================================


class AppDataObject(ContainerDataObject):

    __metaclass__ = ABCMeta

    '''
    An AppDataObject is a DataObject that reads data from one or more
    DataObjects (its inputs), processes it, and writes data onto one or more
    DataObjects (its outputs). Once it has finished writing each output, it
    moves the output's state to COMPLETED.
    '''

    def initialize(self, **kwargs):

        super(AppDataObject, self).initialize(**kwargs)

        # Inputs and Outputs are the DataObjects that get read from and written
        # to by this AppDataObject, respectively. An input DataObject will see
        # this AppDataObject as one of its consumers, while an output DataObject
        # will see this AppDataObject as its single producer.
        #
        # Input objects will 
        self._inputs  = collections.OrderedDict()
        self._outputs = collections.OrderedDict()

        # Same as above, only that these correspond to the 'immediate' version
        # of the consumers and producers.
        self._immediateInputs  = {}

    def addInput(self, inputDataObject):
        if inputDataObject not in self._inputs.values():
            uid = inputDataObject.uid
            self._inputs[uid] = inputDataObject
            inputDataObject.addConsumer(self)

    @property
    def inputs(self):
        return self._inputs.values()

    def addOutput(self, outputDataObject):
        if outputDataObject is self:
            raise Exception('Cannot add an AppConsumer as its own output')
        if outputDataObject not in self._outputs.values():
            uid = outputDataObject.uid
            self._outputs[uid] = outputDataObject
            outputDataObject.producer = self

    @property
    def outputs(self):
        return self._outputs.values()

    def addImmediateInput(self, immediateInputDO):
        if immediateInputDO not in self._immediateInputs.values():
            uid = immediateInputDO.uid
            self._immediateInputs[uid] = immediateInputDO
            immediateInputDO.addImmediateConsumer(self)

    @property
    def immediateInputs(self):
        return self._immediateInputs.values()

    def dataObjectCompleted(self, uid):
        """
        Callback invoked when `dataObject` (which is one of the inputs of this
        AppDataObject) has moved to the COMPLETED state. By default no action is
        performed
        """

    def dataWritten(self, uid, data):
        """
        Callback invoked when `data` has been written into the DataObject with
        UID `uid` (which is one of the immediate inputs of this AppDataObject).
        By default no action is performed
        """

class BarrierAppDataObject(AppDataObject):
    """
    An AppDataObject that implements a barrier. It accepts no 'immediate' inputs
    and it waits until all its inputs have been moved to COMPLETED to execute
    its 'run' method.
    """

    def initialize(self, **kwargs):
        super(BarrierAppDataObject, self).initialize(**kwargs)
        self._completedInputs = []

    def addImmediateInput(self, immediateInputDO):
        raise Exception("BarrierAppDataObjects don't accept streaming inputs")

    def dataObjectCompleted(self, uid):
        super(BarrierAppDataObject, self).dataObjectCompleted(uid)
        self._completedInputs.append(uid)
        if len(self._completedInputs) == len(self._inputs):
            # TODO: This is temporary and needs to be defined more clearly
            self.setCompleted()
            self.run()

    # TODO: another thing we need to check
    def exists(self):
        return True

    def run(self):
        """
        Run this application. It can be safely assumed that at this point all
        the required inputs are COMPLETED.
        """

class CRCAppDataObject(BarrierAppDataObject):
    '''
    An BarrierAppDataObject that calculates the CRC of the single DataObject it
    consumes. It assumes the DataObject being consumed is not a container.
    This is a simple example of an BarrierAppDataObject being implemented, and
    not something really intended to be used in a production system
    '''

    def run(self):
        if len(self._inputs) != 1:
            raise Exception("This application read only from one DataObject")
        if len(self._outputs) != 1:
            raise Exception("This application writes only one DataObject")

        inputDO = self._inputs.values()[0]
        outputDO = self._outputs.values()[0]

        bufsize = 4 * 1024 ** 2
        desc = inputDO.open()
        buf = inputDO.read(desc, bufsize)
        crc = 0
        while buf:
            crc = crc32(buf, crc)
            buf = inputDO.read(desc, bufsize)
        inputDO.close(desc)

        # Rely on whatever implementation we decide to use
        # for storing our data
        outputDO.write(str(crc))

        # That's the only data we write; after that we are complete
        outputDO.setCompleted()


#===============================================================================
# SocketListener class and mix-ins follow
#===============================================================================

_socketListenerCounter = 0
_socketListenerLock = threading.RLock()
class SocketListener(object):
    '''
    A class that listens on a socket for data. The server-side socket expects
    only one client, and assumes that the client will close the connection after
    all its data has been sent.
    '''

    # By modifying this value before creating an instance of this object
    # we can actually allow to create dummy SocketListeners that don't actually
    # open the server-side socket (and therefore don't receive data). This can
    # be useful during testing, which is why it defaults to False.
    _dryRun = False

    def __init__(self, *args, **kwargs):
        super(SocketListener, self).__init__(*args, **kwargs)
        self.createSocket(**kwargs)

    def createSocket(self, **kwargs):
        host = None
        port = None
        if 'host' in kwargs:
            host = kwargs['host']
        if 'port' in kwargs:
            port = int(kwargs['port'])

        if not host:
            host = '127.0.0.1'
        if not port:
            port = 1111

        with _socketListenerLock:
            global _socketListenerCounter
            counter = _socketListenerCounter
            _socketListenerCounter += 1

        # Don't really listen for data if running dry
        if self._dryRun:
            return

        # Accept one connection at most
        serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        serverSocket.bind((host, port))
        serverSocket.listen(1)
        self._listenerThread = threading.Thread(None, self.processData, "Socket_Listener_%s" % (counter), [serverSocket])
        self._listenerThread.setDaemon(1)
        self._listenerThread.start()
        # TODO: we still need to join this thread, or at least try

        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug('Successfully listening for requests on %s:%d' % (host, port))

    def processData(self, serverSocket):
        clientSocket, address = serverSocket.accept()
        serverSocket.close()
        if _logger.isEnabledFor(logging.INFO):
            _logger.info('Accepted connection from %s:%d' % (address[0], address[1]))

        while True:
            data = clientSocket.recv(4096)
            if not data:
                break
            self.write(data)
        clientSocket.close()
        self.setCompleted()

class InMemorySocketListenerDataObject(SocketListener, InMemoryDataObject): pass
class FileSocketListenerDataObject(SocketListener, FileDataObject): pass