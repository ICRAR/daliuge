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
import threading
import random
from IN import INT16_MAX, INT16_MIN
import warnings
"""
Data object is the centre of the data-driven architecture
It should be based on the UML class diagram
"""

from abc import ABCMeta, abstractmethod
import heapq
import logging
from operator import __or__
import os, time

from ddap_protocol import DOStates


try:
    from crc32c import crc32
except:
    from binascii import crc32


_logger = logging.getLogger(__name__)



#===============================================================================
# DataObject classes follow
#===============================================================================


class AbstractDataObject(object):
    '''
    Base class for all DataObject implementations.

    A DataObject is a representation of a piece of data. DataObjects are created,
    written once, potentially read many times, and they finally potentially
    expire and get deleted. Subclasses implement different storage mechanisms
    to hold the data represented by the DataObject.
    '''

    # This ensures that:
    #  - This class cannot be instantiated
    #  - Subclasses implement methods decorated with @abstractmethod
    __metaclass__ = ABCMeta

    def __init__(self, oid, uid, eventbc, subs=[], **kwargs):
        """
        Constructor
        oid:    object id (string)
        uid:    uuid    (string)
        """

        self._bcaster = eventbc

        self._oid = oid
        self._uid = uid
        self._consumers = []# could be (1) real component (if I am a real data object consumed by them)
                            #       or (2) real data objects (if I am a real component that produce them)
        self._producers = []# could be (1) real component (if I am a real data object produced by them)
                            #       or (2) real data objects (if I am a real component that consume them)

        self._refCount = 0
        self._refLock  = threading.RLock()
        self._location = None
        self._parent   = None
        self._status   = None
        self._statusLock = threading.RLock()
        self._phase    = None
        self._checksum = 0
        self._size     = 0

        # "DataObject descriptors" used for reading.
        # Instead of passing file objects or more complex data types in our
        # open/read/close calls we use integers, mainly because Pyro doesn't
        # handle file types and other classes (like StringIO) well, but also
        # because it requires less transport.
        # TODO: Make these threadsafe, no lock around them yet
        self._readDescriptors = {}

        # TODO: Do we really want to introduce this dependency here?
        self._dom = None
        if kwargs.has_key('dom'):
            self._dom = kwargs['dom'] # hold a reference to data object manager

        # Expected lifespan for this object, used by to expire them
        lifespan = 10
        if kwargs.has_key('lifespan'):
            lifespan = float(kwargs['lifespan'])
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

        for s in subs:
            self.subscribe(s)

        try:
            self.initialize(**kwargs)
            if hasattr(self, 'appInitialize'):
                self.appInitialize(**kwargs)
            self.status = DOStates.INITIALIZED

        except Exception as e:
            self.status = DOStates.FAILED
            raise e

    def __hash__(self):
        return hash(self._uid)

    def __str__(self):
        re = "{0}/{1}".format(self.oid, self.uid)
        if (self.location is not None):
            re += "@{0}".format(self.location)
        return re

    def initialize(self, **kwargs):
        """
        Hook for subclass initialization.
        """
        pass

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
        descriptor = self.openMeta(**kwargs)

        # Save the real descriptor in the dictionary and return its key instead
        while True:
            key = random.SystemRandom().randint(INT16_MIN, INT16_MAX)
            if key not in self._readDescriptors:
                break
        self._readDescriptors[key] = descriptor

        # This occurs only after a successful opening
        with self._refLock:
            self._refCount += 1
        self.fire('open')

        return key

    def close(self, descriptor, **kwargs):

        self._checkStateAndDescriptor(descriptor)

        # Decrement counter and then actually close
        with self._refLock:
            if self._refCount <= 0:
                raise Exception("Invalid call to close(), no respective open() detected")
            self._refCount -= 1
        self.closeMeta(self._readDescriptors.pop(descriptor), **kwargs)

    def read(self, descriptor, count=4096, **kwargs):
        """
        Reads count bytes from the given DataObject descriptor.
        """
        self._checkStateAndDescriptor(descriptor)
        return self.readMeta(self._readDescriptors[descriptor], count, **kwargs)

    def _checkStateAndDescriptor(self, descriptor):
        if self.status != DOStates.COMPLETED:
            raise Exception("DataObject %s/%s is in state %s (!=COMPLETED), cannot be read" % (self._oid, self._uid, self.status))
        if descriptor not in self._readDescriptors:
            raise Exception("Illegal descriptor %d given, remember to open() first" % (descriptor))

    def isBeingRead(self):
        '''
        Returns True if the DataObject is currently being read; False otherwise
        '''
        with self._refLock:
            return self._refCount > 0

    def write(self, data, **kwargs):
        '''
        Writes the given data into this DataObject. This method is only meant to
        be called while the DataObject is in INITIALIZED or WRITING state; once
        the DataObject is COMPLETE or beyond only reading is allowed.
        The underlying storage mechanism is responsible for implementing the
        final writing logic via the writeMeta() method.
        '''

        if self.status not in [DOStates.INITIALIZED, DOStates.WRITING]:
            raise Exception("No more writing expected")

        nbytes = self.writeMeta(data, **kwargs)
        if nbytes:
            self._size += nbytes

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
    def openMeta(self, **kwargs):
        """
        Hook for subclass open. It returns a "DataObject descriptor", used by
        the readMeta() and closeMeta() methods. This way parallel readings can
        be performed over the same DataObject.
        """
        pass

    @abstractmethod
    def closeMeta(self, descriptor, **kwargs):
        """
        Hook for subclass close. It closes the given descriptor, thus freeing
        underlying resources. The descriptor is that returned by the openMeta()
        method.
        """
        pass

    @abstractmethod
    def readMeta(self, descriptor, count, **kwargs):
        """
        Hook for subclass read. It reads at most count bytes from the given
        descriptor. The descriptor is that returned by the openMeta() method.
        """
        pass

    @abstractmethod
    def writeMeta(self, data, **kwargs):
        """
        Hook for subclass write. It writes the data represented by this
        DataObject into the underlying media.
        """
        pass

    def delete(self):
        '''
        Deletes the data represented by this DO.
        '''
        pass

    def _updateChecksum(self, chunk):
        self._checksum = crc32(chunk, self._checksum)

    @property
    def checksum(self):
        return self._checksum

    @checksum.setter
    def checksum(self, value):
        self._checksum = value

    @property
    def oid(self):
        return self._oid

    @property
    def uid(self):
        return self._uid

    def subscribe(self, callback):
        self._bcaster.subscribe(self._uid, callback)

    def unsubscribe(self, callback):
        self._bcaster.unsubscribe(self._uid, callback)

    def fire(self, eventType, **attrs):
        attrs['oid'] = self.oid
        attrs['uid'] = self.uid
        self._bcaster.fire(eventType, **attrs)

    def exists(self):
        return True

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
        return self._size

    @property
    def precious(self):
        return self._precious

    @property
    def status(self):
        with self._statusLock:
            return self._status

    @status.setter
    def status(self, value):
        with self._statusLock:
            # if we are already in the state that is requested then do nothing
            if value == self._status:
                return
            self._status = value

        # fire off event
        self.fire('status', status = value)

    @property
    def parent(self):
        return self._parent

    @parent.setter
    def parent(self, value):
        if self._parent and value:
            warnings.warn("A parent is already set in DataObject %s/%s, overwriting with new value" % (self._oid, self._uid))
        if value:
            self._parent = value # a parent is a container

    @property
    def consumers(self):
        return self._consumers[:]

    @consumers.setter
    def consumers(self, consumers):
        """
        set a list of consumers (replace the existing ones)
        """
        for c in consumers:
            self.addConsumer(c)

    def addConsumer(self, consumer):
        """
        Adds a consumer to this DataObject.

        Consumers are a particular kind of subscriber that are only interested
        on the status change of DataObjects to COMPLETED. When the expected
        status change occurs, the consumers' consume() method is invoked with
        a reference to the DataObject that changed state.

        This is one of the key mechanisms by which the DataObject graph is
        executed automatically. If DataObject C consumes DataObject B, and B
        consumes C, then as soon as A transitions to COMPLETED B will consume A,
        and when B is finally COMPLETED C is triggered.
        """

        # Consumers have a "consume" method that gets invoked when
        # this DO moves to COMPLETED
        if not hasattr(consumer, 'consume'):
            raise Exception("The consumer %s doesn't have a 'consume' method" % (consumer))

        # Add if not already present
        # Add the reverse reference too automatically
        if consumer in self._consumers:
            return
        _logger.debug('Adding new consumer for DataObject %s/%s: %s' %(self.oid, self.uid, consumer))
        self._consumers.append(consumer)

        # Automatic back-reference
        if hasattr(consumer, 'addProducer'):
            consumer.addProducer(self)

        def consumeCompleted(e):
            if not hasattr(e, 'status') or e.status != DOStates.COMPLETED:
                _logger.debug('Skipping event for consumer %s: %s' %(consumer, str(e.__dict__)) )
                return
            _logger.debug('Triggering consumer %s: %s' %(consumer, str(e.__dict__)))
            consumer.consume(self)
        self.subscribe(consumeCompleted)

    @property
    def producers(self):
        return self._producers[:]

    def addProducer(self, producer):
        if producer not in self._producers:
            self._producers.append(producer)
            # Automatic back-reference
            if hasattr(producer, 'addConsumer'):
                producer.addConsumer(self)

    def setCompleted(self):
        '''
        Manually moves this DO to the COMPLETED state. This can be used when
        not all the expected data has arrived for a given DO, but it should
        still be moved to COMPLETED, or when the expected amount of data
        held by a DataObject is not known in advanced.
        '''
        if self.status not in [DOStates.INITIALIZED, DOStates.WRITING]:
            raise Exception("DataObject %s/%s not in INITIALIZED or WRITING state (%s), cannot setComplete()" % (self._oid, self._uid, self.status))

        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug("Moving DataObject %s/%s to COMPLETED" % (self._oid, self._uid))
        self.status = DOStates.COMPLETED

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

    def _type_code(self):
        return 4

    def get_upstream_objects(self):
        return self.producers

    def to_json_obj(self, jsobj_out=None):
        """
        json serialisation of the data object
        """
        jsobj = dict()
        jsobj['type'] = self._type_code()
        jsobj['loc'] = self.location
        inputQueue = []
        for uobj in self.get_upstream_objects():
            iqd = dict()
            iqd['oid'] = uobj.oid
            inputQueue.append(iqd)
        if (len(inputQueue) > 0):
            jsobj['inputQueue'] = inputQueue

        create_dict = jsobj_out is None
        if (create_dict):
            m_jsobj_out = dict()
            m_jsobj_out[self.oid] = jsobj
        else:
            jsobj_out[self.oid] = jsobj

        next = self.consumers
        if (self.parent is not None):
            next.append(self.parent)
        if (create_dict):
            param_jsobj_out = m_jsobj_out
        else:
            param_jsobj_out = jsobj_out
        for dob in next:
            dob.to_json_obj(param_jsobj_out)

        if (create_dict):
            return m_jsobj_out

    def _getDataObject(self, dataObject):
        return dataObject

class FileDataObject(AbstractDataObject):

    def initialize(self, **kwargs):
        """
        File data object-specific initialization.
        """
        self._root = '/tmp/sdp_dfms'
        if (not os.path.exists(self._root)):
            os.mkdir(self._root)

        # TODO: Make sure the parts that make up the filename are composed
        #       of valid filename characters; otherwise encode them
        self._fnm = self._root + os.sep + self._oid + '___' + self.uid
        if os.path.isfile(self._fnm):
            warnings.warn('File %s already exists, overwriting' % (self._fnm))
        self._fo = open(self._fnm, "w")

    def openMeta(self, **kwargs):
        return open(self._fnm, 'r')

    def readMeta(self, descriptor, count=4096, **kwargs):
        return descriptor.read(count)

    def writeMeta(self, data, **kwargs):
        """
        Each chunk written to a file object
        will be written to the file system

        this is NOT thread safe (assuming we will single threaded event loop)

        """
        self._fo.write(data)
        return len(data)

    def setCompleted(self):
        self._fo.close()
        AbstractDataObject.setCompleted(self)

    def closeMeta(self, descriptor, **kwargs):
        descriptor.close()

    def seek(self, **kwargs):
        pass

    def delete(self):
        os.unlink(self._fnm)

    def exists(self):
        return os.path.isfile(self._fnm)

class NgasDataObject(AbstractDataObject):
    '''
    A DataObject whose data is finally stored into NGAS. Since NGAS doesn't
    support appending data to existing files, we store all the data temporarily
    in a file on the local filesystem and then move it to the NGAS destination
    '''

    def initialize(self, **kwargs):

        # Check we actually can write NGAMS clients
        try:
            from ngamsPClient import ngamsPClient
        except Exception, e:
            _logger.exception("No NGAMS client libs found, cannot use NgasDataObjects")
            raise e

        if not kwargs.has_key('ngasSrv'):
            raise Exception('ngasSrv option must be supplied at DO construction time')
        self._ngasSrv = kwargs['ngasSrv']

        self._ngasPort = 7777
        if kwargs.has_key('ngasPort'):
            self._ngasPort = int(kwargs['ngasPort'])

        self._buf = ''

    def openMeta(self, **kwargs):
        return self._getClient()

    def _getClient(self):
        from ngamsPClient import ngamsPClient
        return ngamsPClient.ngamsPClient(self._ngasSrv, self._ngasPort)

    def writeMeta(self, data, **kwargs):
        self._buf += data
        return len(data)
        # TODO: When all the expected data has arrived we move it from the
        # buffer into NGAS

    def closeMeta(self, descriptor):
        del descriptor

    def readMeta(self, descriptor, count, **kwargs):
        '''
        :param ngamsPClient.ngamsPClient descriptor:
        '''
        # Read data from NGAS and give it back to our reader
        descriptor.retrieve2File(self.uid, cmd="QRETRIEVE")

    def exists(self):
        self._getClient().status(fileId=self.uid)

class InMemoryDataObject(AbstractDataObject):

    def initialize(self, **kwargs):
        self._buf = ''

    def openMeta(self, **kwargs):
        from cStringIO import StringIO
        return StringIO(self._buf)

    def writeMeta(self, data, **kwargs):
        self._buf += data
        return len(data)

    def readMeta(self, descriptor, count=4096, **kwargs):
        return descriptor.read(count)

    def closeMeta(self, descriptor, **kwargs):
        descriptor.close()

    def delete(self):
        del self._buf
        self._buf = None

    def exists(self):
        return self._buf is not None

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
        """
        Hook for subclass initialization.
        """
        self._children = []
        self._complete_map = {} #key - child oid, value - completed yet (bool)?

    #===========================================================================
    # No data-related operations should actually be called in Container DOs
    #===========================================================================
    def closeMeta(self, descriptor, **kwargs):
        raise NotImplementedError()
    def openMeta(self, **kwargs):
        raise NotImplementedError()
    def readMeta(self, descriptor, count, **kwargs):
        raise NotImplementedError()
    def writeMeta(self, descriptor, data, **kwargs):
        raise NotImplementedError()

    def check_join_condition(self, event):

        if ("status" != event.type):
            return

        if (event.status != DOStates.COMPLETED):
            return

        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug("ContainerDataObject %s/%s joined COMPLETED child DataObject %s/%s" % (self._oid, self._uid, event.oid, event.uid))

        self._complete_map[event.oid] = True

        # check if each child is completed
        # TODO: We should also consider the case in which one child takes so
        #       long that the quicker children expire in the meanwhile.
        if not all(self._complete_map.itervalues()):
            return

        # move the container as a whole to COMPLETED
        self.setCompleted()

    def addChild(self, child):

        # Avoid circular dependencies between Containers
        if child == self.parent:
            raise Exception("Circular dependency between DataObjects %s/%s and %s/%s" % (self.oid, self.uid, child.oid, child.uid))

        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug("Adding new child for ContainerDataObject %s/%s: %s" % (self.oid, self.uid, child.uid))

        child.subscribe(self.check_join_condition)
        self._children.append(child)
        child.parent = self
        self._complete_map[child.oid] = child.isCompleted()

    def _type_code(self):
        return 2

    def get_upstream_objects(self):
        return self.producers + self._children

    def delete(self):
        for c in [c for c in self._children if c.exists()]:
            c.delete()

    @property
    def expirationDate(self):
        return heapq.nlargest(1, [c.expirationDate for c in self._children])[0]

    @property
    def children(self):
        return self._children[:]

    def exists(self):
        # TODO: Or should it be __and__? Depends on what the exact contract of
        #       "exists" is
        return reduce(__or__, [c.exists() for c in self._children])

    def isReplicable(self):
        return False


#===============================================================================
# AppConsumer classes follow
#===============================================================================


class AppConsumer(object):
    '''
    An AppConsumer is an object implementing the "consume" method, invoked by
    DataObjects when they change to the COMPLETED state.

    Although consumers in general can be any kind of object, this AppConsumer
    assumes that itself implements the setCompleted() method, and subclasses
    also assume that there is a write() method. In other words, the AppConsumer
    classes are meant to be mixed in with the basic DataObject classes to create
    DataObjects that react on other DOs who transit to COMPLETED, and that
    represent the output of a computation done over the COMPLETED DO. This is
    then the mechanism through which a DataObject graph will be able to progress
    through its execution.

    Different AppConsumer implementations might decide whether they accept or
    not to consume ContainerDataObjects, which are a special case of DOs where
    I/O is not performed directly via the DO, and requires special navigation
    through its children.
    '''

    def appInitialize(self, **kwargs):
        """
        Hooks for sub class
        """
        pass

    def consume(self, dataObject):
        """
        Execute the tasks
        """
        self.run(self._getDataObject(dataObject))

    def run(self, dataObject):
        """
        Hooks for sub class
        Must return a dictionary: key: parameter name, val: argument value
        which will then be used as the **kwargs for calling consumers (i.e. "real" AbstractDataObjects)
        """
        pass

    def _type_code(self):
        return 1

class CRCResultConsumer(AppConsumer):
    '''
    An AppConsumer that calculates the CRC of the DataObject it consumes.
    It assumes the DataObject being consumed is not a container.
    This is a simple example of an AppConsumer being implemented, and not
    something really intended to be used in a production system
    '''

    def run(self, dataObject):
        if isinstance(dataObject, ContainerDataObject):
            raise Exception("This consumer doesn't consume Container DataObjects")

        bufsize = 4 * 1024 ** 2
        desc = dataObject.open()
        buf = dataObject.read(desc, bufsize)
        crc = 0
        while buf:
            crc = crc32(buf, crc)
            buf = dataObject.read(desc, bufsize)
        dataObject.close(desc)

        # Rely on whatever implementation we decide to use
        # for storing our data
        self.write(str(crc))

        # That's the only data we write; after that we are complete
        self.setCompleted()

class FileCRCResultDataObject(CRCResultConsumer,
                              FileDataObject):
    '''
    A CRCResultConsumer that exposes its result as a FileDataObject
    '''
    pass

class NgasCRCResultDataObject(CRCResultConsumer,
                              NgasDataObject):
    '''
    A CRCResultConsumer that exposes its result as an NgasDataObject
    '''
    pass

class InMemoryCRCResultDataObject(CRCResultConsumer,
                                  InMemoryDataObject):
    '''
    A CRCResultConsumer that exposes its result as an InMemoryDataObject
    '''
    pass

class ContainerAppConsumer(AppConsumer,
                           ContainerDataObject):
    '''
    An AppConsumer that is in turn a ContainerDataObject. This implies that
    the consumption of the data of the producer object yields more than one
    DataObject. Objects inheriting from this class will be able to attach
    children to them, and to invoke their individual write() methods when
    consuming the data coming from the producer DataObject.
    '''
    pass