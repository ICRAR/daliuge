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
from dfms.lifecycle.registry import DataObject
"""
Data object is the centre of the data-driven architecture
It should be based on the UML class diagram
"""

from abc import ABCMeta, abstractmethod
from cStringIO import StringIO
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

    """
    The AbstractDataObject
    It should be split into abstract, container
    but we mix them into a single one for the time being

    TODO - to support stream and iterative processing
    """
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
        self._location = None
        self._parent   = None
        self._status   = None
        self._phase    = None
        self._checksum = 0
        self._size     = 0

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
        re = "oid:{0}".format(self.oid)
        if (self.location is not None):
            loc = "@{0}".format(self.location)
        else:
            loc = ""
        return "{0}{1}".format(re, loc)

    def initialize(self, **kwargs):
        """
        Hook for subclass initialization.
        """
        pass

    def open(self, **kwargs):
        """
        Refer to Activity Diagram (Data Lifecycle / Open Data Object)
        """
        descriptor = self.openMeta(**kwargs)
        self._refCount += 1
        self.fire('open')
        return descriptor

    def close(self, descriptor, **kwargs):
        self.closeMeta(descriptor, **kwargs)
        self._refCount -= 1

    def write(self, data, **kwargs):

        if self.status not in [DOStates.INITIALIZED, DOStates.WRITING]:
            raise Exception("No more writing expected")

        nbytes = self.writeMeta(data, **kwargs)
        if nbytes:
            self._size += nbytes

        # Update our internal checksum
        self._computeChecksum(data)

        # If we know how much data we'll receive, keep track of it and
        # automatically switch to COMPLETED
        if self._expectedSize > 0:
            remaining = self._expectedSize - self._size
            if remaining > 0:
                self.status = DOStates.WRITING
            else:
                if remaining < 0:
                    _logger.warning("Received and wrote more bytes than expected: " + str(-remaining))
                _logger.debug("Automatically DataObject %s/%s moving to COMPLETED, all expected data arrived" % (self.oid, self.uid))
                self.setCompleted()
        else:
            self.status = DOStates.WRITING

        return nbytes

    @abstractmethod
    def openMeta(self, **kwargs):
        """
        Hook for subclass open
        """
        pass

    @abstractmethod
    def closeMeta(self, descriptor, **kwargs):
        """
        Hook for subclass close
        """
        pass

    @abstractmethod
    def read(self, descriptor, count, **kwargs):
        pass

    @abstractmethod
    def writeMeta(self, data, **kwargs):
        """
        Hook for subclass write
        """
        pass

    def delete(self):
        '''
        Deletes the data represented by this DO
        '''
        pass

    def _computeChecksum(self, chunk):
        self._checksum = crc32(chunk, self._checksum)
        return self._checksum

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
        return self._status

    @status.setter
    def status(self, value):
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
        if (value): # only real data object has parent, and we currently only have up to 1 parent
            self._parent = value # a parent is a container

    @property
    def consumers(self):
        return self._consumers

    @consumers.setter
    def consumers(self, consumers):
        """
        set a list of consumers (replace the existing ones)
        """
        for c in consumers:
            self.addConsumer(c)

    def addConsumer(self, consumer):

        # Consumers have a "consume" method that gets invoked when
        # this DO moves to COMPLETED
        if not consumer.consume:
            raise Exception("The consumer doesn't have a 'consume' method")

        # Add if not already present
        # Add the reverse reference too automatically
        if consumer in self._consumers:
            return
        _logger.debug('Adding new consumer for DataObject %s/%s: %s' %(self.oid, self.uid, consumer.uid))
        self._consumers.append(consumer)
        if hasattr(consumer, 'addProducer'):
            consumer.addProducer(self)

        def consumeCompleted(e):
            if not hasattr(e, 'status') or e.status != DOStates.COMPLETED:
                _logger.debug('Skipping event for consumer %s: %s' %(consumer.uid, str(e.__dict__)) )
                return
            _logger.debug('Triggering consumer %s: %s' %(consumer.uid, str(e.__dict__)))
            consumer.consume(self)
        self.subscribe(consumeCompleted)

    @property
    def producers(self):
        return self._producers

    def addProducer(self, producer):
        if producer not in self._producers:
            self._producers.append(producer)
            if hasattr(producer, 'addConsumer'):
                producer.addConsumer(self)

    def setCompleted(self):
        '''
        Manually moves this DO to the COMPLETED state. This can be used when
        not all the expected data has arrived for a given DO, but it should
        still be moved to COMPLETED
        '''
        if self._status not in [DOStates.INITIALIZED, DOStates.WRITING]:
            raise Exception("This method can only be called while the DO is still in INITIALIZED or DIRTY state")

        self.status = DOStates.COMPLETED

    def isCompleted(self):
        '''
        Checks whether this DO is currently in the COMPLETED state or not
        '''
        return (self._status == DOStates.COMPLETED)

    def isContainer(self):
        return (hasattr(self, '_children') and len(self._children) > 0)

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

    def getDataObject(self, dataObject):
        return dataObject

class FileDataObject(AbstractDataObject):

    def initialize(self, **kwargs):
        """
        File data object-specific initialization.
        """
        self._root = '/tmp/sdp_dfms'
        if (not os.path.exists(self._root)):
            os.mkdir(self._root)

        self._fnm = ''.join([self._root, os.sep, self._oid])
        self._fo = open(self._fnm, "w")

    def openMeta(self, **kwargs):
        return open(self._fnm, 'r')

    def read(self, descriptor, count=4096, **kwargs):
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
        # TODO: When all the expected data has arrived we move it from the
        # buffer into NGAS

    def closeMeta(self, descriptor):
        del descriptor

    def read(self, descriptor, count, **kwargs):
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
        return StringIO(self._buf)

    def writeMeta(self, data, **kwargs):
        self._buf += data
        return len(data)

    def read(self, descriptor, count=4096, **kwargs):
        return descriptor.read(count)

    def closeMeta(self, descriptor, **kwargs):
        descriptor.close()

    def delete(self):
        del self._buf
        self._buf = None

    def exists(self):
        return bool(self._buf)

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
    def read(self, descriptor, count, **kwargs):
        raise NotImplementedError()
    def writeMeta(self, descriptor, data, **kwargs):
        raise NotImplementedError()

    def check_join_condition(self, event):

        if ("status" != event.type):
            return

        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug("Join condition event from {0}: {1} = {2}".format(event.oid, event.type, event.status))

        if (event.status != DOStates.COMPLETED):
            return

        self._complete_map[event.oid] = True
        # check if each child is completed
        for c_yet in self._complete_map.itervalues():
            if (not c_yet):
                return

        # move the container as a whole to COMPLETED
        self.setCompleted()

    def addChild(self, child):
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

    def appInitialize(self, **kwargs):
        """
        Hooks for sub class
        """
        pass

    def consume(self, dataObject):
        """
        Execute the tasks
        """
        return self.run(dataObject)

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
    It assumes the DataObject being consumed is not a container
    '''

    def run(self, dataObject):
        if dataObject.isContainer():
            raise Exception("This consumer doesn't consume Container DataObjects")

        dataObject = self.getDataObject(dataObject)
        bufsize = 4 * 1024 ** 2
        desc = dataObject.open()
        buf = dataObject.read(desc, bufsize)
        crc = 0
        while (buf != ""):
                crc = crc32(buf, crc)
                buf = dataObject.read(desc, bufsize)
        dataObject.close(desc)

        # Rely on whatever implementation we decide to use
        # for storing our data
        self.write(str(crc))
        self.setCompleted()



class FileCRCResultDataObject(CRCResultConsumer,
                              FileDataObject):
    pass

class NgasCRCResultDataObject(CRCResultConsumer,
                              NgasDataObject):
    pass

class InMemoryCRCResultDataObject(CRCResultConsumer,
                                  InMemoryDataObject):
    pass