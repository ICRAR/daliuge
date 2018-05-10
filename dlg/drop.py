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
"""
Module containing the core DROP classes.
"""

from abc import ABCMeta, abstractmethod
import base64
import collections
import contextlib
import errno
import heapq
import importlib
import logging
import math
import os
import random
import shutil
import threading
import time
import re

import six
from six import BytesIO

from .ddap_protocol import ExecutionMode, ChecksumTypes, AppDROPStates, \
    DROPLinkType, DROPPhases, DROPStates, DROPRel
from .event import EventFirer
from .exceptions import InvalidDropException, InvalidRelationshipException
from .io import OpenMode, FileIO, MemoryIO, NgasIO, ErrorIO, NullIO, ShoreIO
from .utils import prepare_sql, createDirIfMissing, isabs, object_tracking


try:
    from crc32c import crc32  # @UnusedImport
    _checksumType = ChecksumTypes.CRC_32C
except:
    from binascii import crc32  # @Reimport
    _checksumType = ChecksumTypes.CRC_32


logger = logging.getLogger(__name__)

class ListAsDict(list):
    """A list that adds drop UIDs to a set as they get appended to the list"""
    def __init__(self, my_set):
        self.set = my_set
    def append(self, drop):
        super(ListAsDict, self).append(drop)
        self.set.add(drop.uid)


track_current_drop = object_tracking('drop')

#===============================================================================
# DROP classes follow
#===============================================================================


class AbstractDROP(EventFirer):
    """
    Base class for all DROP implementations.

    A DROP is a representation of a piece of data. DROPs are created,
    written once, potentially read many times, and they finally potentially
    expire and get deleted. Subclasses implement different storage mechanisms
    to hold the data represented by the DROP.

    If the data represented by this DROP is written *through* this object
    (i.e., calling the ``write`` method), this DROP will keep track of the
    data's size and checksum. If the data is written externally, the size and
    checksum can be fed into this object for future reference.

    DROPs can have consumers attached to them. 'Normal' consumers will
    wait until the DROP they 'consume' (their 'input') moves to the
    COMPLETED state and then will consume it, most typically by opening it
    and reading its contents, but any other operation could also be performed.
    How the consumption is triggered depends on the producer's `executionMode`
    flag, which dictates whether it should trigger the consumption itself or
    if it should be manually triggered by an external entity. On the other hand,
    streaming consumers receive the data that is written into its input
    *as it gets written*. This mechanism is driven always by the DROP that
    acts as a streaming input. Apart from receiving the data as it gets
    written into the DROP, streaming consumers are also notified when the
    DROPs moves to the COMPLETED state, at which point no more data should
    be expected to arrive at the consumer side.

    DROPs' data can be expired automatically by the system after the DROP has
    transitioned to the COMPLETED state if they are created by a DROP Manager.
    Expiration can either be triggered by an interval relative to the creation
    time of the DROP (via the `lifespan` keyword), or by specifying that the
    DROP should be expired after all its consumers have finished (via the
    `expireAfterUse` keyword). These two methods are mutually exclusive. If none
    is specified no expiration occurs.
    """

    # This ensures that:
    #  - This class cannot be instantiated
    #  - Subclasses implement methods decorated with @abstractmethod
    __metaclass__ = ABCMeta

    @track_current_drop
    def __init__(self, oid, uid, **kwargs):
        """
        Creates a DROP. The only mandatory argument are the Object ID
        (`oid`) and the Unique ID (`uid`) of the new object (see the `self.oid`
        and `self.uid` methods for more information). Any extra arguments must
        be keyed, and will be processed either by this method, or by the
        `initialize` method.

        This method should not be overwritten by subclasses. For any specific
        initialization logic, the `initialize` method should be overwritten
        instead. This allows us to move to the INITIALIZED state only after any
        specific initialization has occurred in the subclasses.
        """

        super(AbstractDROP, self).__init__()

        # Copy it since we're going to modify it
        kwargs = dict(kwargs)

        # So far only these three are mandatory
        self._oid = str(oid)
        self._uid = str(uid)

        # The Session owning this drop, if any
        # In most real-world situations this attribute will be set, but in
        # general it cannot be assumed it will (e.g., unit tests create drops
        # directly outside the context of a session).
        self._dlg_session = self._getArg(kwargs, 'dlg_session', None)

        # A simple name that the Drop might receive
        # This is usually set in the Logical Graph Editor,
        # but is not necessarily always there
        self.name = self._getArg(kwargs, 'nm', "")

        # 1-to-N relationship: one DROP may have many consumers and producers.
        # The potential consumers and producers are always AppDROPs instances
        # We keep the UIDs in a set for O(1) "x in set" operations
        # Obviously the normal way of doing this is using a dictionary, but
        # for the time being and while testing the integration with TBU's ceda
        # library we need to expose a list.
        self._consumers_uids = set()
        self._consumers = ListAsDict(self._consumers_uids)
        self._producers_uids = set()
        self._producers = ListAsDict(self._producers_uids)

        # Set holding the state of the producers that have finished their
        # execution. Once all producers have finished, this DROP moves
        # itself to the COMPLETED state
        self._finishedProducers = []
        self._finishedProducersLock = threading.Lock()

        # Streaming consumers are objects that consume the data written in
        # this DROP *as it gets written*, and therefore don't have to
        # wait until this DROP has moved to COMPLETED.
        # An object cannot be a streaming consumers and a 'normal' consumer
        # at the same time, although this rule is imposed simply to enforce
        # efficiency (why would a consumer want to consume the data twice?) and
        # not because it's technically impossible.
        # See comment above in self._consumers/self._producers for separate set
        # with uids
        self._streamingConsumers_uids = set()
        self._streamingConsumers = ListAsDict(self._streamingConsumers_uids)

        self._refCount = 0
        self._refLock  = threading.Lock()
        self._location = None
        self._parent   = None
        self._status   = None
        self._statusLock = threading.RLock()

        # Current and target phases.
        # Phases represent the resiliency of data. An initial phase of PLASMA
        # is set on each DROP representing its lack of non-transient storage
        # support. A target phase is also set to hint the Data Lifecycle Manager
        # about the level of resilience that this DROP should achieve.
        self._phase = DROPPhases.PLASMA
        self._targetPhase = self._getArg(kwargs, 'targetPhase', DROPPhases.GAS)

        # Calculating the checksum and maintaining the data size internally
        # implies that the data represented by this DROP is written
        # *through* this DROP. This might not always be the case though,
        # since data could be written externally and the DROP simply be
        # moved to COMPLETED at the end of the process. In this case we return a
        # None checksum and size (when requested), signaling that we don't have
        # this information.
        # Note also that the setters of these two properties also allow to set
        # a value on them, but only if they are None
        self._checksum     = None
        self._checksumType = None
        self._size         = None

        # The DataIO instance we use in our write method. It's initialized to
        # None because it's lazily initialized in the write method, since data
        # might be written externally and not through this DROP
        self._wio = None

        # DataIO objects used for reading.
        # Instead of passing file objects or more complex data types in our
        # open/read/close calls we use integers, mainly because Pyro doesn't
        # handle file types and other classes (like StringIO) well, but also
        # because it requires less transport.
        # TODO: Make these threadsafe, no lock around them yet
        self._rios = {}

        # The execution mode.
        # When set to DROP (the default) the graph execution will be driven by
        # DROPs themselves by firing and listening to events, and reacting
        # accordingly by executing themselves or moving to the COMPLETED state.
        # When set to EXTERNAL, DROPs do no react to these events, and remain
        # in the state they currently are. In this case an external entity must
        # listen to the events and decide when to trigger the execution of the
        # applications.
        self._executionMode = self._getArg(kwargs, 'executionMode', ExecutionMode.DROP)

        # The physical node where this DROP resides.
        # This piece of information is mandatory when submitting the physical
        # graph via the DataIslandManager, but in simpler scenarios such as
        # tests or graph submissions via the NodeManager it might be
        # missing.
        self._node = self._getArg(kwargs, 'node', None)

        # The host representing the Data Island where this DROP resides
        # This piece of information is mandatory when submitting the physical
        # graph via the MasterManager, but in simpler scenarios such as tests or
        # graphs submissions via the DataIslandManager or NodeManager it might
        # missing.
        self._dataIsland = self._getArg(kwargs, 'island', None)

        # DROP expiration.
        # Expiration can be time-driven or usage-driven, which are mutually
        # exclusive methods. If time-driven, a relative lifespan is assigned to
        # the DROP.
        # Expected lifespan for this object, used by to expire them
        if 'lifespan' in kwargs and 'expireAfterUse' in kwargs:
            raise InvalidDropException(self, "%r specifies both `lifespan` and `expireAfterUse`" \
                             "but they are mutually exclusive" % (self,))

        self._expireAfterUse = self._getArg(kwargs, 'expireAfterUse', False)
        self._expirationDate = -1
        if not self._expireAfterUse:
            lifespan = float(self._getArg(kwargs, 'lifespan', -1))
            if lifespan != -1:
                self._expirationDate = time.time() + lifespan

        # Expected data size, used to automatically move the DROP to COMPLETED
        # after successive calls to write()
        self._expectedSize = -1
        if 'expectedSize' in kwargs and kwargs['expectedSize']:
            self._expectedSize = int(kwargs.pop('expectedSize'))

        # All DROPs are precious unless stated otherwise; used for replication
        self._precious = self._getArg(kwargs, 'precious', True)

        # Sub-class initialization; mark ourselves as INITIALIZED after that
        self.initialize(**kwargs)
        self._status = DROPStates.INITIALIZED # no need to use synchronised self.status here

    def _getArg(self, kwargs, key, default):
        val = default
        if key in kwargs:
            val = kwargs.pop(key)
        elif logger.isEnabledFor(logging.DEBUG):
            logger.debug("Defaulting %s to %s in %r" % (key, str(val), self))
        return val

    def __hash__(self):
        return hash(self._uid)

    def __repr__(self):
        return "<%s oid=%s, uid=%s>" % (self.__class__.__name__, self.oid, self.uid)

    def initialize(self, **kwargs):
        """
        Performs any specific subclass initialization.

        `kwargs` contains all the keyword arguments given at construction time,
        except those used by the constructor itself. Implementations of this
        method should make sure that arguments in the `kwargs` dictionary are
        removed once they are interpreted so they are not interpreted by
        accident by another method implementations that might reside in the call
        hierarchy (in the case that a subclass implementation calls the parent
        method implementation, which is usually the case).
        """

    def incrRefCount(self):
        """
        Increments the reference count of this DROP by one atomically.
        """
        with self._refLock:
            self._refCount += 1

    def decrRefCount(self):
        """
        Decrements the reference count of this DROP by one atomically.
        """
        with self._refLock:
            self._refCount -= 1

    @track_current_drop
    def open(self, **kwargs):
        """
        Opens the DROP for reading, and returns a "DROP descriptor"
        that must be used when invoking the read() and close() methods.
        DROPs maintain a internal reference count based on the number
        of times they are opened for reading; because of that after a successful
        call to this method the corresponding close() method must eventually be
        invoked. Failing to do so will result in DROPs not expiring and
        getting deleted.
        """
        if self.status != DROPStates.COMPLETED:
            raise Exception("%r is in state %s (!=COMPLETED), cannot be opened for reading" % (self, self.status,))

        io = self.getIO()
        io.open(OpenMode.OPEN_READ, **kwargs)

        # Save the IO object in the dictionary and return its descriptor instead
        while True:
            descriptor = random.SystemRandom().randint(-six.MAXSIZE - 1, six.MAXSIZE)
            if descriptor not in self._rios:
                break
        self._rios[descriptor] = io

        # This occurs only after a successful opening
        self.incrRefCount()
        self._fire('open')

        return descriptor

    @track_current_drop
    def close(self, descriptor, **kwargs):
        """
        Closes the given DROP descriptor, decreasing the DROP's
        internal reference count and releasing the underlying resources
        associated to the descriptor.
        """
        self._checkStateAndDescriptor(descriptor)

        # Decrement counter and then actually close
        self.decrRefCount()
        io = self._rios.pop(descriptor)
        io.close(**kwargs)

    def read(self, descriptor, count=4096, **kwargs):
        """
        Reads `count` bytes from the given DROP `descriptor`.
        """
        self._checkStateAndDescriptor(descriptor)
        io = self._rios[descriptor]
        return io.read(count, **kwargs)

    def _checkStateAndDescriptor(self, descriptor):
        if self.status != DROPStates.COMPLETED:
            raise Exception("%r is in state %s (!=COMPLETED), cannot be read" % (self.status,))
        if descriptor not in self._rios:
            raise Exception("Illegal descriptor %d given, remember to open() first" % (descriptor))

    def isBeingRead(self):
        """
        Returns `True` if the DROP is currently being read; `False`
        otherwise
        """
        with self._refLock:
            return self._refCount > 0

    @track_current_drop
    def write(self, data, **kwargs):
        '''
        Writes the given `data` into this DROP. This method is only meant
        to be called while the DROP is in INITIALIZED or WRITING state;
        once the DROP is COMPLETE or beyond only reading is allowed.
        The underlying storage mechanism is responsible for implementing the
        final writing logic via the `self.writeMeta()` method.
        '''

        if self.status not in [DROPStates.INITIALIZED, DROPStates.WRITING]:
            raise Exception("No more writing expected")

        if not isinstance(data, six.binary_type):
            raise Exception("Data type not of binary type: %s", type(data).__name__)

        # We lazily initialize our writing IO instance because the data of this
        # DROP might not be written through this DROP
        if not self._wio:
            self._wio = self.getIO()
            self._wio.open(OpenMode.OPEN_WRITE)
        nbytes = self._wio.write(data)

        dataLen = len(data)
        if nbytes != dataLen:
            # TODO: Maybe this should be an actual error?
            logger.warning('Not all data was correctly written by %s (%d/%d bytes written)' % (self, nbytes, dataLen))

        # see __init__ for the initialization to None
        if self._size is None:
            self._size = 0
        self._size += nbytes

        # Trigger our streaming consumers
        if self._streamingConsumers:
            for streamingConsumer in self._streamingConsumers:
                streamingConsumer.dataWritten(self.uid, data)

        # Update our internal checksum
        self._updateChecksum(data)

        # If we know how much data we'll receive, keep track of it and
        # automatically switch to COMPLETED
        if self._expectedSize > 0:
            remaining = self._expectedSize - self._size
            if remaining > 0:
                self.status = DROPStates.WRITING
            else:
                if remaining < 0:
                    logger.warning("Received and wrote more bytes than expected: " + str(-remaining))
                logger.debug("Automatically moving %r to COMPLETED, all expected data arrived" % (self,))
                self.setCompleted()
        else:
            self.status = DROPStates.WRITING

        return nbytes

    @abstractmethod
    def getIO(self):
        """
        Returns an instance of one of the `dlg.io.DataIO` instances that
        handles the data contents of this DROP.
        """

    def delete(self):
        """
        Deletes the data represented by this DROP.
        """
        self.getIO().delete()

    def exists(self):
        """
        Returns `True` if the data represented by this DROP exists indeed
        in the underlying storage mechanism
        """
        return self.getIO().exists()

    @abstractmethod
    def dataURL(self):
        """
        A URL that points to the data referenced by this DROP. Different
        DROP implementations will use different URI schemes.
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
        The checksum value for the data represented by this DROP. Its
        value is automatically calculated if the data was actually written
        through this DROP (using the `self.write()` method directly or
        indirectly). In the case that the data has been externally written, the
        checksum can be set externally after the DROP has been moved to
        COMPLETED or beyond.

        :see: `self.checksumType`
        """
        return self._checksum

    @checksum.setter
    def checksum(self, value):
        if self._checksum is not None:
            raise Exception("The checksum for DROP %s is already calculated, cannot overwrite with new value" % (self))
        if self.status in [DROPStates.INITIALIZED, DROPStates.WRITING]:
            raise Exception("DROP %s is still not fully written, cannot manually set a checksum yet" % (self))
        self._checksum = value

    @property
    def checksumType(self):
        """
        The algorithm used to compute this DROP's data checksum. Its value
        if automatically set if the data was actually written through this
        DROP (using the `self.write()` method directly or indirectly). In
        the case that the data has been externally written, the checksum type
        can be set externally after the DROP has been moved to COMPLETED
        or beyond.

        :see: `self.checksum`
        """
        return self._checksumType

    @checksumType.setter
    def checksumType(self, value):
        if self._checksumType is not None:
            raise Exception("The checksum type for DROP %s is already set, cannot overwrite with new value" % (self))
        if self.status in [DROPStates.INITIALIZED, DROPStates.WRITING]:
            raise Exception("DROP %s is still not fully written, cannot manually set a checksum type yet" % (self))
        self._checksumType = value

    @property
    def oid(self):
        """
        The DROP's Object ID (OID). OIDs are unique identifiers given to
        semantically different DROPs (and by consequence the data they
        represent). This means that different DROPs that point to the same
        data semantically speaking, either in the same or in a different
        storage, will share the same OID.
        """
        return self._oid

    @property
    def uid(self):
        """
        The DROP's Unique ID (UID). Unlike the OID, the UID is globally
        different for all DROP instances, regardless of the data they
        point to.
        """
        return self._uid

    @property
    def executionMode(self):
        """
        The execution mode of this DROP. If `ExecutionMode.DROP` it means
        that this DROP will automatically trigger the execution of all its
        consumers. If `ExecutionMode.EXTERNAL` it means that this DROP
        will *not* trigger its consumers, and therefore an external entity will
        have to do it.
        """
        return self._executionMode

    def handleInterest(self, drop):
        """
        Main mechanism through which a DROP handles its interest in a
        second DROP it isn't directly related to.

        A call to this method should be expected for each DROP this
        DROP is interested in. The default implementation does nothing,
        but implementations are free to perform any action, such as subscribing
        to events or storing information.

        At this layer only the handling of such an interest exists. The
        expression of such interest, and the invocation of this method wherever
        necessary, is currently left as a responsibility of the entity creating
        the DROPs. In the case of a Session in a DROPManager for
        example this step would be performed using deployment-time information
        contained in the dropspec dictionaries held in the session.
        """

    def _fire(self, eventType, **kwargs):
        """
        Delivers an event of `eventType` to all interested listeners.

        All the key-value pairs contained in `attrs` are set as attributes of
        the event being sent. On top of that, the `uid` and `oid` attributes are
        also added, carrying the uid and oid of the current DROP, respectively.
        """
        kwargs['oid'] = self.oid
        kwargs['uid'] = self.uid
        self._fireEvent(eventType, **kwargs)

    @property
    def phase(self):
        """
        This DROP's phase. The phase indicates the resilience of a DROP.
        """
        return self._phase

    @phase.setter
    def phase(self, phase):
        self._phase = phase

    @property
    def targetPhase(self):
        return self._targetPhase

    @property
    def expirationDate(self):
        return self._expirationDate

    @property
    def expireAfterUse(self):
        return self._expireAfterUse

    @property
    def size(self):
        """
        The size of the data pointed by this DROP. Its value is
        automatically calculated if the data was actually written through this
        DROP (using the `self.write()` method directly or indirectly). In
        the case that the data has been externally written, the size can be set
        externally after the DROP has been moved to COMPLETED or beyond.
        """
        return self._size

    @size.setter
    def size(self, size):
        if self._size is not None:
            raise Exception("The size of DROP %s is already calculated, cannot overwrite with new value" % (self))
        if self.status in [DROPStates.INITIALIZED, DROPStates.WRITING]:
            raise Exception("DROP %s is still not fully written, cannot manually set a size yet" % (self))
        self._size = size

    @property
    def precious(self):
        """
        Whether this DROP should be considered as 'precious' or not
        """
        return self._precious

    @property
    def status(self):
        """
        The current status of this DROP.
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
        """
        The DROP that acts as the parent of the current one. This
        parent/child relationship is created by ContainerDROPs, which are
        a specific kind of DROP.
        """
        return self._parent

    @parent.setter
    @track_current_drop
    def parent(self, parent):
        if self._parent and parent:
            logger.warning("A parent is already set in %r, overwriting with new value" % (self,))
        if parent:
            prevParent = self._parent
            self._parent = parent # a parent is a container
            if hasattr(parent, 'addChild') and self not in parent.children:
                try:
                    parent.addChild(self)
                except:
                    self._parent = prevParent

    @property
    def consumers(self):
        """
        The list of 'normal' consumers held by this DROP.

        :see: `self.addConsumer()`
        """
        return self._consumers[:]

    @track_current_drop
    def addConsumer(self, consumer, back=True):
        """
        Adds a consumer to this DROP.

        Consumers are normally (but not necessarily) AppDROPs that get
        notified when this DROP moves into the COMPLETED or ERROR states.
        This is done by firing an event of type `dropCompleted` to which the
        consumer subscribes to.

        This is one of the key mechanisms by which the DROP graph is
        executed automatically. If AppDROP B consumes DROP A, then
        as soon as A transitions to COMPLETED B will be notified and will
        probably start its execution.
        """

        # An object cannot be a normal and streaming consumer at the same time,
        # see the comment in the __init__ method
        cuid = consumer.uid
        if cuid in self._streamingConsumers_uids:
            raise InvalidRelationshipException(DROPRel(consumer, DROPLinkType.CONSUMER, self),
                                               "Consumer already registered as a streaming consumer")

        # Add if not already present
        # Add the reverse reference too automatically
        if cuid in self._consumers_uids:
            return
        logger.debug('Adding new consumer %r to %r', consumer, self)
        self._consumers.append(consumer)

        # Subscribe the consumer to events sent when this DROP moves to
        # COMPLETED. This way the consumer will be notified that its input has
        # finished.
        # This only happens if this DROP's execution mode is 'DROP'; otherwise
        # an external entity will trigger the execution of the consumer at the
        # right time
        if self.executionMode == ExecutionMode.DROP:
            self.subscribe(consumer, 'dropCompleted')

        # Automatic back-reference
        if back and hasattr(consumer, 'addInput'):
            logger.debug("Adding back %r as input of %r", self, consumer)
            consumer.addInput(self, False)

    @property
    def producers(self):
        """
        The list of producers that write to this DROP

        :see: `self.addProducer()`
        """
        return self._producers[:]

    @track_current_drop
    def addProducer(self, producer, back=True):
        """
        Adds a producer to this DROP.

        Producers are AppDROPs that write into this DROP; from the
        producers' point of view, this DROP is one of its many outputs.

        When a producer has finished its execution, this DROP will be
        notified via the self.producerFinished() method.
        """

        # Don't add twice
        puid = producer.uid
        if puid in self._producers_uids:
            return

        self._producers.append(producer)

        # Automatic back-reference
        if back and hasattr(producer, 'addOutput'):
            producer.addOutput(self, False)

    @track_current_drop
    def handleEvent(self, e):
        """
        Handles the arrival of a new event. Events are delivered from those
        objects this DROP is subscribed to.
        """
        if e.type == 'producerFinished':
            self.producerFinished(e.uid, e.status)

    @track_current_drop
    def producerFinished(self, uid, drop_state):
        """
        Method called each time one of the producers of this DROP finishes
        its execution. Once all producers have finished this DROP moves to the
        COMPLETED state (or to ERROR if one of the producers is on the ERROR
        state).

        This is one of the key mechanisms through which the execution of a
        DROP graph is accomplished. If AppDROP A produces DROP
        B, as soon as A finishes its execution B will be notified and will move
        itself to COMPLETED.
        """

        finished = False
        with self._finishedProducersLock:
            self._finishedProducers.append(drop_state)
            nFinished = len(self._finishedProducers)
            nProd = len(self._producers)

            if nFinished > nProd:
                raise Exception("More producers finished that registered in DROP %r: %d > %d" % (self, nFinished, nProd))
            elif nFinished == nProd:
                finished = True

        if finished:
            logger.debug("All producers finished for DROP %r", self)

            # decided that if any producer fails then fail the data drop
            if DROPStates.ERROR in self._finishedProducers:
                self.setError()
            else:
                self.setCompleted()

    @property
    def streamingConsumers(self):
        """
        The list of 'streaming' consumers held by this DROP.

        :see: `self.addStreamingConsumer()`
        """
        return self._streamingConsumers[:]

    @track_current_drop
    def addStreamingConsumer(self, streamingConsumer, back=True):
        """
        Adds a streaming consumer to this DROP.

        Streaming consumers are AppDROPs that receive the data written
        into this DROP *as it gets written*, and therefore do not need to
        wait until this DROP has been moved to the COMPLETED state.
        """

        # Consumers have a "consume" method that gets invoked when
        # this DROP moves to COMPLETED
        if not hasattr(streamingConsumer, 'dropCompleted') or not hasattr(streamingConsumer, 'dataWritten'):
            raise InvalidRelationshipException(DROPRel(streamingConsumer, DROPLinkType.STREAMING_CONSUMER, self),
                                               "The streaming consumer doesn't have a 'dropCompleted' and/or 'dataWritten' method")

        # An object cannot be a normal and streaming streamingConsumer at the same time,
        # see the comment in the __init__ method
        scuid = streamingConsumer.uid
        if scuid in self._consumers_uids:
            raise InvalidRelationshipException(DROPRel(streamingConsumer, DROPLinkType.STREAMING_CONSUMER, self),
                                               "Consumer is already registered as a normal consumer")

        # Add if not already present
        if scuid in self._streamingConsumers_uids:
            return
        logger.debug('Adding new streaming streaming consumer for %r: %s' %(self, streamingConsumer))
        self._streamingConsumers.append(streamingConsumer)

        # Automatic back-reference
        if back and hasattr(streamingConsumer, 'addStreamingInput'):
            streamingConsumer.addStreamingInput(self, False)

        # Subscribe the streaming consumer to events sent when this DROP moves
        # to COMPLETED. This way the streaming consumer will be notified that
        # its input has finished
        # This only happens if this DROP's execution mode is 'DROP'; otherwise
        # an external entity will trigger the execution of the consumer at the
        # right time
        if self.executionMode == ExecutionMode.DROP:
            self.subscribe(streamingConsumer, 'dropCompleted')

    @track_current_drop
    def setError(self):
        '''
        Moves this DROP to the ERROR state.
        '''
        # Close our writing IO instance.
        # If written externally, self._wio will have remained None
        if self._wio:
            self._wio.close()

        logger.info("Moving %r to ERROR", self)
        self.status = DROPStates.ERROR

        # Signal our subscribers that the show is over
        self._fire('dropCompleted', status=DROPStates.ERROR)

    @track_current_drop
    def setCompleted(self):
        '''
        Moves this DROP to the COMPLETED state. This can be used when not all the
        expected data has arrived for a given DROP, but it should still be moved
        to COMPLETED, or when the expected amount of data held by a DROP
        is not known in advanced.
        '''
        if self.status not in [DROPStates.INITIALIZED, DROPStates.WRITING]:
            raise Exception("%r not in INITIALIZED or WRITING state (%s), cannot setComplete()" % (self, self.status))

        # Close our writing IO instance.
        # If written externally, self._wio will have remained None
        if self._wio:
            self._wio.close()

        logger.debug("Moving %r to COMPLETED", self)
        self.status = DROPStates.COMPLETED

        # Signal our subscribers that the show is over
        self._fire('dropCompleted', status=DROPStates.COMPLETED)

    def isCompleted(self):
        '''
        Checks whether this DROP is currently in the COMPLETED state or not
        '''
        # Mind you we're not accessing _status, but status. This way we use the
        # lock in status() to access _status
        return (self.status == DROPStates.COMPLETED)

    @property
    def node(self):
        return self._node

    @property
    def dataIsland(self):
        return self._dataIsland

class PathBasedDrop(object):
    """Base class for data drops that handle paths (i.e., file and directory drops)"""

    def initialize(self, **kwargs):
        self._path = None
        PathBasedDrop.initialize(self, **kwargs)

    def get_dir(self, dirname):

        if isabs(dirname):
            return dirname

        # dirname will be based on the current working directory
        # If we have a session, it goes into the path as well
        # (most times we should have a session BTW, we should expect *not* to
        # have one only during testing)
        parts = []
        if self._dlg_session:
            parts.append('.')
            parts.append(self._dlg_session.sessionId)
        else:
            parts.append('/tmp/daliuge_tfiles')
        if dirname:
            parts.append(dirname)

        the_dir = os.path.abspath(os.path.normpath(os.path.join(*parts)))
        createDirIfMissing(the_dir)
        return the_dir

    @property
    def path(self):
        return self._path

class FileDROP(AbstractDROP, PathBasedDrop):
    """
    A DROP that points to data stored in a mounted filesystem.

    Users can (but usually don't need to) specify both a `filepath` and a
    `dirname` parameter for each FileDrop. The combination of these two parameters
    will determine the final location of the file backed up by this drop on the
    underlying filesystem. When no ``filepath`` is provided, the drop's UID will be
    used as a filename. When a relative filepath is provided, it is relative to
    ``dirname``. When an absolute ``filepath`` is given, it is used as-is.
    When a relative ``dirname`` is provided, it is relative to the base directory
    of the currently running session (i.e., a directory with the session ID as a
    name, placed within the currently working directory of the Node Manager
    hosting that session). If ``dirname`` is absolute, it is used as-is.

    In some cases drops are created **outside** the context of a session, most
    notably during unit tests. In these cases the base directory is a fixed
    location under ``/tmp``.

    The following table summarizes the calculation of the final path used by
    the ``FileDrop`` class depending on its parameters:

    ============ ===================== ===================== ==========
         .                               filepath
    ------------ ------------------------------------------------------
    dirname      empty                 relative              absolute
    ============ ===================== ===================== ==========
    **empty**    /``$B``/``$u``        /``$B``/``$f``        /``$f``
    **relative** /``$B``/``$d``/``$u`` /``$B``/``$d``/``$f`` **ERROR**
    **absolute** /``$d``/``$u``        /``$d``/``$f``        **ERROR**
    ============ ===================== ===================== ==========

    In the table, ``$f`` is the value of ``filepath``, ``$d`` is the value of
    ``dirname``, ``$u`` is the drop's UID and ``$B`` is the base directory for
    this drop's session, namelly ``/the/cwd/$session_id``.
    """

    def sanitize_paths(self, filepath, dirname):

        # No filepath has been given, there's nothing to sanitize
        if not filepath:
            return filepath, dirname

        # All is good, return unchanged
        filepath_b = os.path.basename(filepath)
        if filepath_b == filepath:
            return filepath, dirname

        # Extract the dirname from filepath and append it to dirname
        filepath_d = os.path.dirname(filepath)
        if dirname:
            filepath_d = os.path.join(dirname, filepath_d)
        return filepath_b, filepath_d

    non_fname_chars = re.compile(r':|%s' % os.sep)
    def initialize(self, **kwargs):
        """
        FileDROP-specific initialization.
        """
        self._delete_parent_dir = self._getArg(kwargs, 'delete_parent_directory', False)

        # The two pieces of information we offer users to tweak
        # These are very intermingled but are not exactly the same, see below
        filepath = self._getArg(kwargs, 'filepath', None)
        dirname = self._getArg(kwargs, 'dirname', None)

        # Duh!
        if isabs(filepath) and dirname:
            raise InvalidDropException(self, 'An absolute filepath does not allow a dirname to be specified')

        # Sanitize filepath/dirname into proper directories-only and
        # filename-only components (e.g., dirname='lala' and filename='1/2'
        # results in dirname='lala/1' and filename='2'
        filepath, dirname = self.sanitize_paths(filepath, dirname)

        # We later check if the file exists, but only if the user has specified
        # an absolute dirname/filepath (otherwise it doesn't make sense, since
        # we create our own filenames/dirnames dynamically as necessary
        check = False
        if (isabs(dirname) and filepath):
            check = self._getArg(kwargs, 'check_filepath_exists', False)

        # Default filepath to drop UID and dirname to per-session directory
        if not filepath:
            filepath = self.non_fname_chars.sub('_', self.uid)
        dirname = self.get_dir(dirname)

        self._root = dirname
        self._path = os.path.join(dirname, filepath)
        if check and not os.path.isfile(self._path):
            raise InvalidDropException(self, 'File does not exist or is not a file: %s' % self._path)

        self._wio = None

    def getIO(self):
        return FileIO(self._path)

    def delete(self):
        AbstractDROP.delete(self)
        if self._delete_parent_dir:
            try:
                os.rmdir(self._root)
            except OSError as e:
                # Silently ignore "Directory not empty" errors
                if e.errno != errno.ENOTEMPTY:
                    raise

    @property
    def dataURL(self):
        hostname = os.uname()[1] # TODO: change when necessary
        return "file://" + hostname + self._path

class ShoreDROP(AbstractDROP):
    def initialize(self, **kwargs):
        self._doid = self._getArg(kwargs, 'doid', 'test_data_object')
        self._column = self._getArg(kwargs, 'column', 'test_column')
        self._row = self._getArg(kwargs, 'row', 0)
        self._rows = self._getArg(kwargs, 'rows', 1)
        self._address = self._getArg(kwargs, 'address', None)
    def getIO(self):
        return ShoreIO(self._doid, self._column, self._row, self._rows, self._address)
    @property
    def dataURL(self):
        return self._address


class NgasDROP(AbstractDROP):
    '''
    A DROP that points to data stored in an NGAS server
    '''

    def initialize(self, **kwargs):
        self._ngasSrv            = self._getArg(kwargs, 'ngasSrv', 'localhost')
        self._ngasPort           = int(self._getArg(kwargs, 'ngasPort', 7777))
        self._ngasTimeout        = int(self._getArg(kwargs, 'ngasTimeout', 2))
        self._ngasConnectTimeout = int(self._getArg(kwargs, 'ngasConnectTimeout', 2))

    def getIO(self):
        return NgasIO(self._ngasSrv, self.uid, port=self._ngasPort,
                      ngasConnectTimeout=self._ngasConnectTimeout,
                      ngasTimeout=self._ngasTimeout)

    @property
    def dataURL(self):
        return "ngas://%s:%d/%s" % (self._ngasSrv, self._ngasPort, self.uid)

class InMemoryDROP(AbstractDROP):
    """
    A DROP that points data stored in memory.
    """

    def initialize(self, **kwargs):
        args = []
        if 'pydata' in kwargs:
            pydata = kwargs.pop('pydata')
            if isinstance(pydata, six.string_types):
                pydata = six.b(pydata)
            args.append(base64.b64decode(pydata))
        self._buf = BytesIO(*args)

    def getIO(self):
        return MemoryIO(self._buf)

    @property
    def dataURL(self):
        hostname = os.uname()[1]
        return "mem://%s/%d/%d" % (hostname, os.getpid(), id(self._buf))

class NullDROP(AbstractDROP):
    """
    A DROP that doesn't store any data.
    """

    def getIO(self):
        return NullIO()

    @property
    def dataURL(self):
        return "null://"

class RDBMSDrop(AbstractDROP):
    """
    A Drop that stores data in a table of a relational database
    """

    def initialize(self, **kwargs):
        AbstractDROP.initialize(self, **kwargs)

        if 'dbmodule' not in kwargs:
            raise InvalidDropException(self, '%r needs a "dbmodule" parameter' % (self,))
        if 'dbtable' not in kwargs:
            raise InvalidDropException(self, '%r needs a "dbtable" parameter' % (self,))

        # The DB-API 2.0 module
        dbmodname = kwargs.pop('dbmodule')
        self._db_drv = importlib.import_module(dbmodname)

        # The table this Drop points at
        self._db_table = kwargs.pop('dbtable')

        # Optional connection parameters
        self._db_params = self._getArg(kwargs, 'dbparams', {})

    def getIO(self):
        # This Drop cannot be accessed directly
        return ErrorIO()

    def _connection(self):
        return contextlib.closing(self._db_drv.connect(**self._db_params))

    def _cursor(self, conn):
        return contextlib.closing(conn.cursor())

    def insert(self, vals):
        """
        Inserts the values contained in the ``vals`` dictionary into the
        underlying table. The keys of ``vals`` are used as the column names.
        """
        with self._connection() as c:
            with self._cursor(c) as cur:

                # vals is a dictionary, its keys are the column names and its
                # values are the values to insert
                sql = "INSERT into %s (%s) VALUES (%s)" % (self._db_table, ','.join(vals.keys()), ','.join(['{}']*len(vals)))
                sql, vals = prepare_sql(sql, self._db_drv.paramstyle, list(vals.values()))
                logger.debug('Executing SQL with parameters: %s / %r', sql, vals)
                cur.execute(sql, vals)
                c.commit()

    def select(self, columns=None, condition=None, vals=()):
        """
        Returns the selected values from the table. Users can constrain the
        result set by specifying a list of ``columns`` to be returned (otherwise
        all table columns are returned) and a ``condition`` to be applied,
        in which case a list of ``vals`` to be applied as query parameters can
        also be given.
        """
        with self._connection() as c:
            with self._cursor(c) as cur:

                # Build up SQL with optional columns and conditions
                columns = columns or ("*",)
                sql = ["SELECT %s FROM %s" % (','.join(columns), self._db_table,)]
                if condition:
                    sql.append(" WHERE ")
                    sql.append(condition)

                # Go, go, go!
                sql, vals = prepare_sql(''.join(sql), self._db_drv.paramstyle, vals)
                logger.debug('Executing SQL with parameters: %s / %r', sql, vals)
                cur.execute(sql, vals)
                if cur.description:
                    return cur.fetchall()
                return []

    @property
    def dataURL(self):
        return "rdbms://%s/%s/%r" % (self._db_drv.__name__, self._db_table, self._db_params)

class ContainerDROP(AbstractDROP):
    """
    A DROP that doesn't directly point to some piece of data, but instead
    holds references to other DROPs (its children), and from them its own
    internal state is deduced.

    Because of its nature, ContainerDROPs cannot be written to directly,
    and likewise they cannot be read from directly. One instead has to pay
    attention to its "children" DROPs if I/O must be performed.
    """

    def initialize(self, **kwargs):
        super(ContainerDROP, self).initialize(**kwargs)
        self._children = []

    #===========================================================================
    # No data-related operations should actually be called in Container DROPs
    #===========================================================================
    def getIO(self):
        return ErrorIO()
    def dataURL(self):
        raise NotImplementedError()

    def addChild(self, child):

        # Avoid circular dependencies between Containers
        if child == self.parent:
            raise InvalidRelationshipException(DROPRel(child, DROPLinkType.CHILD, self),
                                               "Circular dependency found")

        logger.debug("Adding new child for %r: %r", self, child)

        self._children.append(child)
        child.parent = self

    def delete(self):
        # TODO: this needs more thinking. Probably a separate method to perform
        #       this recursive deletion will be needed, while this delete method
        #       will go hand-to-hand with the rest of the I/O methods above,
        #       which are currently raise a NotImplementedError
        if self._children:
            for c in [c for c in self._children if c.exists()]:
                c.delete()

    @property
    def expirationDate(self):
        if self._children:
            return heapq.nlargest(1, [c.expirationDate for c in self._children])[0]
        return self._expirationDate

    @property
    def children(self):
        return self._children[:]

    def exists(self):
        if self._children:
            # TODO: Or should it be all()? Depends on what the exact contract of
            #       "exists" is
            return any([c.exists() for c in  self._children])
        return True

class DirectoryContainer(PathBasedDrop, ContainerDROP):
    """
    A ContainerDROP that represents a filesystem directory. It only allows
    FileDROPs and DirectoryContainers to be added as children. Children
    can only be added if they are placed directly within the directory
    represented by this DirectoryContainer.
    """

    def initialize(self, **kwargs):
        ContainerDROP.initialize(self, **kwargs)

        if 'dirname' not in kwargs:
            raise InvalidDropException(self, 'DirectoryContainer needs a "dirname" parameter')

        directory = kwargs['dirname']

        check_exists = self._getArg(kwargs, 'check_exists', True)
        if check_exists is True:
            if not os.path.isdir(directory):
                raise InvalidDropException(self, '%s is not a directory' % (directory))

        self._path = self.get_dir(directory)

    def addChild(self, child):
        if isinstance(child, (FileDROP, DirectoryContainer)):
            path = child.path
            if os.path.dirname(path) != self.path:
                raise InvalidRelationshipException(DROPRel(child, DROPLinkType.CHILD, self),
                                                   'Child DROP is not under %s' % (self.path))
            ContainerDROP.addChild(self, child)
        else:
            raise TypeError('Child DROP is not of type FileDROP or DirectoryContainer')

    def delete(self):
        shutil.rmtree(self._path)

    def exists(self):
        return os.path.isdir(self._path)

#===============================================================================
# AppDROP classes follow
#===============================================================================


class AppDROP(ContainerDROP):

    '''
    An AppDROP is a DROP representing an application that reads data
    from one or more DROPs (its inputs), and writes data onto one or more
    DROPs (its outputs).

    AppDROPs accept two different kind of inputs: "normal" and "streaming"
    inputs. Normal inputs are DROPs that must be on the COMPLETED state
    (and therefore their data must be fully written) before this application is
    run, while streaming inputs are DROPs that feed chunks of data into
    this application as the data gets written into them.

    This class contains two methods that should be overwritten as needed by
    subclasses: `dropCompleted`, invoked when input DROPs move to
    COMPLETED, and `dataWritten`, invoked with the data coming from streaming
    inputs.

    How and when applications are executed is completely up to the user, and is
    not enforced by this base class. Some applications might need to be run at
    `initialize` time, while other might start during the first invocation of
    `dataWritten`. A common scenario anyway is to start an application only
    after all its inputs have moved to COMPLETED (implying that none of them is
    an streaming input); for these cases see the `BarrierAppDROP`.
    '''

    def initialize(self, **kwargs):

        super(AppDROP, self).initialize(**kwargs)

        # Inputs and Outputs are the DROPs that get read from and written
        # to by this AppDROP, respectively. An input DROP will see
        # this AppDROP as one of its consumers, while an output DROP
        # will see this AppDROP as one of its producers.
        #
        # Input and output objects are later referenced by their *index*
        # (relative to the order in which they were added to this object)
        # Therefore we use an ordered dict to keep the insertion order.
        self._inputs  = collections.OrderedDict()
        self._outputs = collections.OrderedDict()

        # Same as above, only that these correspond to the 'streaming' version
        # of the consumers
        self._streamingInputs  = collections.OrderedDict()

        # An AppDROP has a second, separate state machine indicating its
        # execution status.
        self._execStatus = AppDROPStates.NOT_RUN

    @track_current_drop
    def addInput(self, inputDrop, back=True):
        uid = inputDrop.uid
        if uid not in self._inputs:
            self._inputs[uid] = inputDrop
            if back:
                inputDrop.addConsumer(self, False)

    @property
    def inputs(self):
        """
        The list of inputs set into this AppDROP
        """
        return list(self._inputs.values())

    @track_current_drop
    def addOutput(self, outputDrop, back=True):
        if outputDrop is self:
            raise InvalidRelationshipException(DROPRel(outputDrop, DROPLinkType.OUTPUT, self),
                                               'Cannot add an AppConsumer as its own output')
        uid = outputDrop.uid
        if uid not in self._outputs:
            self._outputs[uid] = outputDrop

            if back:
                outputDrop.addProducer(self, False)

            # Subscribe the output DROP to events sent by this AppDROP when it
            # finishes its execution.
            self.subscribe(outputDrop, 'producerFinished')

    @property
    def outputs(self):
        """
        The list of outputs set into this AppDROP
        """
        return list(self._outputs.values())

    def addStreamingInput(self, streamingInputDrop, back=True):
        if streamingInputDrop not in self._streamingInputs.values():
            uid = streamingInputDrop.uid
            self._streamingInputs[uid] = streamingInputDrop
            if back:
                streamingInputDrop.addStreamingConsumer(self, False)

    @property
    def streamingInputs(self):
        """
        The list of streaming inputs set into this AppDROP
        """
        return list(self._streamingInputs.values())

    def handleEvent(self, e):
        """
        Handles the arrival of a new event. Events are delivered from those
        objects this DROP is subscribed to.
        """
        if e.type == 'dropCompleted':
            self.dropCompleted(e.uid, e.status)

    def dropCompleted(self, uid, drop_state):
        """
        Callback invoked when the DROP with UID `uid` (which is either a
        normal or a streaming input of this AppDROP) has moved to the
        COMPLETED or ERROR state. By default no action is performed.
        """

    def dataWritten(self, uid, data):
        """
        Callback invoked when `data` has been written into the DROP with
        UID `uid` (which is one of the streaming inputs of this AppDROP).
        By default no action is performed
        """

    @property
    def execStatus(self):
        """
        The execution status of this AppDROP
        """
        return self._execStatus

    @execStatus.setter
    def execStatus(self, execStatus):
        if self._execStatus == execStatus:
            return
        self._execStatus = execStatus
        self._fire('execStatus', execStatus=execStatus)

    def _notifyAppIsFinished(self):
        """
        Method invoked by subclasses when the execution of the application is
        over. Subclasses must make sure that both the status and execStatus
        properties are set to their correct values correctly before invoking
        this method.
        """
        logger.debug("Moving %r to %s", self, "FINISHED" if self._execStatus is AppDROPStates.FINISHED else "ERROR")
        self._fire('producerFinished', status=self.status, execStatus=self.execStatus)

class InputFiredAppDROP(AppDROP):
    """
    An InputFiredAppDROP accepts no streaming inputs and waits until a given
    amount of inputs (called *effective inputs*) have moved to COMPLETED to
    execute its 'run' method, which must be overwritten by subclasses. This way,
    this application allows to continue the execution of the graph given a
    minimum amount of inputs being ready. The transitions of subsequent inputs
    to the COMPLETED state have no effect.

    Normally only one call to the `run` method will happen per application.
    However users can override this by specifying a different number of tries
    before finally giving up.

    The amount of effective inputs must be less or equal to the amount of inputs
    added to this application once the graph is being executed. The special
    value of -1 means that all inputs are considered as effective, in which case
    this class acts as a BarrierAppDROP, effectively blocking until all its
    inputs have moved to the COMPLETED state.

    An input error threshold controls the behavior of the application given an
    error in one or more of its inputs (i.e., a DROP moving to the ERROR state).
    The threshold is a value within 0 and 100 that indicates the tolerance
    to erroneous effective inputs, and after which the application will not be
    run but moved to the ERROR state itself instead.
    """
    def initialize(self, **kwargs):
        super(InputFiredAppDROP, self).initialize(**kwargs)
        self._completedInputs = []
        self._errorInputs = []

        # Error threshold must be within 0 and 100
        self._input_error_threshold = int(self._getArg(kwargs, 'input_error_threshold', 0))
        if self._input_error_threshold < 0 or self._input_error_threshold > 100:
            raise InvalidDropException(self, "%r: input_error_threshold not within [0,100]" % (self,))

        # Amount of effective inputs
        if 'n_effective_inputs' not in kwargs:
            raise InvalidDropException(self, "%r: n_effective_inputs is mandatory" % (self,))
        self._n_effective_inputs = int(kwargs['n_effective_inputs'])
        if self._n_effective_inputs < -1 or self._n_effective_inputs == 0:
            raise InvalidDropException(self, "%r: n_effective_inputs must be > 0 or equals to -1" % (self,))

        # Number of tries
        self._n_tries = int(self._getArg(kwargs, 'n_tries', 1))
        if self._n_tries < 1:
            raise InvalidDropException(self, 'Invalid n_tries, must be a positive number')

    def addStreamingInput(self, streamingInputDrop, back=True):
        raise InvalidRelationshipException(DROPRel(streamingInputDrop, DROPLinkType.STREAMING_INPUT, self),
                                           "InputFiredAppDROPs don't accept streaming inputs")

    def dropCompleted(self, uid, drop_state):
        super(InputFiredAppDROP, self).dropCompleted(uid, drop_state)

        logger.debug("Received notification from input drop: uid=%s, state=%d", uid, drop_state)

        # A value of -1 means all inputs
        n_inputs = len(self._inputs)
        n_eff_inputs = self._n_effective_inputs
        if n_eff_inputs == -1:
            n_eff_inputs = n_inputs

        # More effective inputs than inputs, this is a horror
        if n_eff_inputs > n_inputs:
            raise Exception("%r: More effective inputs (%d) than inputs (%d)" % \
                            (self, self._n_effective_inputs, n_inputs))

        if drop_state == DROPStates.ERROR:
            self._errorInputs.append(uid)
        elif drop_state == DROPStates.COMPLETED:
            self._completedInputs.append(uid)
        else:
            raise Exception('Invalid DROP state in dropCompleted: %s' % drop_state)

        error_len = len(self._errorInputs)
        ok_len = len(self._completedInputs)

        if (error_len + ok_len) == n_eff_inputs:
            # calculate the number of errors that have already occurred
            percent_failed = math.floor((error_len/float(n_eff_inputs)) * 100)

            logger.debug("Error on inputs for %r: %d/%d", self, percent_failed, self._input_error_threshold)

            # if we hit the input error threshold then ERROR the drop and move on
            if percent_failed > self._input_error_threshold:
                logger.info("Error threshold reached on %r, not executing it: %d/%d", self, percent_failed, self._input_error_threshold)

                self.execStatus = AppDROPStates.ERROR
                self.status =  DROPStates.ERROR
                self._notifyAppIsFinished()
            else:
                self.async_execute()

    def async_execute(self):
        # Return immediately, but schedule the execution of this app
        # If we have been given a thread pool use that
        if hasattr(self, '_tp'):
            self._tp.apply_async(self.execute)
        else:
            t = threading.Thread(target=self.execute)
            t.daemon = 1
            t.start()

    @track_current_drop
    def execute(self):
        """
        Manually trigger the execution of this application.

        This method is normally invoked internally when the application detects
        all its inputs are COMPLETED.
        """

        # TODO: We need to be defined more clearly how the state is set in
        #       applications, for the time being they follow their execState.

        # Run at most self._n_tries if there are errors during the execution
        logger.debug("Executing %r", self)
        tries = 0
        drop_state = DROPStates.COMPLETED
        self.execStatus = AppDROPStates.RUNNING
        while tries < self._n_tries:
            try:
                self.run()
                self.execStatus = AppDROPStates.FINISHED
                break
            except:
                tries += 1
                logger.exception('Error while executing %r (try %d/%d)' % (self, tries, self._n_tries))

        # We gave up running the application, go to error
        if tries == self._n_tries:
            self.execStatus = AppDROPStates.ERROR
            drop_state = DROPStates.ERROR

        self.status = drop_state
        self._notifyAppIsFinished()

    def run(self):
        """
        Run this application. It can be safely assumed that at this point all
        the required inputs are COMPLETED.
        """

    # TODO: another thing we need to check
    def exists(self):
        return True

class BarrierAppDROP(InputFiredAppDROP):
    """
    A BarrierAppDROP is an InputFireAppDROP that waits for all its inputs to
    complete, effectively blocking the flow of the graph execution.
    """
    def initialize(self, **kwargs):
        # Blindly override existing value if any
        kwargs['n_effective_inputs'] = -1
        super(BarrierAppDROP, self).initialize(**kwargs)


class dropdict(dict):
    """
    An intermediate representation of a DROP that can be easily serialized
    into a transport format such as JSON or XML.

    This dictionary holds all the important information needed to call any given
    DROP constructor. The most essential pieces of information are the
    DROP's OID, and its type (which determines the class to instantiate).
    Depending on the type more fields will be required. This class doesn't
    enforce these requirements though, as it only acts as an information
    container.

    This class also offers a few utility methods to make it look more like an
    actual DROP class. This way, users can use the same set of methods
    both to create DROPs representations (i.e., instances of this class)
    and actual DROP instances.

    Users of this class are, for example, the graph_loader module which deals
    with JSON -> DROP representation transformations, and the different
    repositories where graph templates are expected to be found by the
    DROPManager.
    """
    def _addSomething(self, other, key):
        if key not in self:
            self[key] = []
        if other['oid'] not in self[key]:
            self[key].append(other['oid'])

    def addConsumer(self, other):
        self._addSomething(other, 'consumers')
    def addStreamingConsumer(self, other):
        self._addSomething(other, 'streamingConsumers')
    def addInput(self, other):
        self._addSomething(other, 'inputs')
    def addStreamingInput(self, other):
        self._addSomething(other, 'streamingInputs')
    def addOutput(self, other):
        self._addSomething(other, 'outputs')
    def addProducer(self, other):
        self._addSomething(other, 'producers')


# Dictionary mapping 1-to-many DROPLinkType constants to the corresponding methods
# used to append a a DROP into a relationship collection of another
# (e.g., one uses `addConsumer` to add a DROPLinkeType.CONSUMER DROP into
# another)
LINKTYPE_1TON_APPEND_METHOD = {
    DROPLinkType.CONSUMER:           'addConsumer',
    DROPLinkType.STREAMING_CONSUMER: 'addStreamingConsumer',
    DROPLinkType.INPUT:              'addInput',
    DROPLinkType.STREAMING_INPUT:    'addStreamingInput',
    DROPLinkType.OUTPUT:             'addOutput',
    DROPLinkType.CHILD:              'addChild',
    DROPLinkType.PRODUCER:           'addProducer'
}

# Same as above, but for N-to-1 relationships, in which case we indicate not a
# method but a property
LINKTYPE_NTO1_PROPERTY = {
    DROPLinkType.PARENT: 'parent'
}

LINKTYPE_1TON_BACK_APPEND_METHOD = {
    DROPLinkType.CONSUMER:           'addInput',
    DROPLinkType.STREAMING_CONSUMER: 'addStreamingInput',
    DROPLinkType.INPUT:              'addConsumer',
    DROPLinkType.STREAMING_INPUT:    'addStreamingConsumer',
    DROPLinkType.OUTPUT:             'addProducer',
    DROPLinkType.CHILD:              'setParent',
    DROPLinkType.PRODUCER:           'addOutput'
}

LINKTYPE_NTO1_BACK_APPEND_METHOD = {
    DROPLinkType.PARENT: 'addChild'
}
