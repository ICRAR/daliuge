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
import heapq
import logging
from operator import __or__
from abc import ABCMeta, abstractmethod

"""
Data object is the centre of the data-driven architecture
It should be based on the UML class diagram
"""
import os, time

try:
    from crc32c import crc32
except:
    from binascii import crc32

from ddap_protocol import DOStates

_logger = logging.getLogger(__name__)

class AbstractDataObject(object):

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

        #self._stateEHlist = [] # state event handler list

        self._location = None
        self._parent = None
        self._status = None
        self._phase  = None
        self._checksum = 0
        self._size     = -1

        if kwargs.has_key('dom'):
            self._dom = kwargs['dom'] # hold a reference to data object manager

        lifespan = 10
        if kwargs.has_key('lifespan'):
            lifespan = float(kwargs['lifespan'])
        self._expirationDate = time.time() + lifespan

        self._precious = True
        if kwargs.has_key('precious'):
            self._precious = bool(kwargs['precious'])

        for s in subs:
            self.subscribe(s)

        try:
            self.initialize(**kwargs)
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
        self.fire(type='open', uid = self._uid, oid = self._oid)
        return descriptor

    @abstractmethod
    def openMeta(self, **kwargs):
        """
        Hook for subclass open
        """
        pass

    def close(self, descriptor, **kwargs):
        self.closeMeta(descriptor, **kwargs)
        #self.setStatus(DOStates.CLOSED)

    @abstractmethod
    def closeMeta(self, descriptor, **kwargs):
        """
        Hook for subclass close
        """
        pass

    @abstractmethod
    def read(self, descriptor, **kwargs):
        pass

    def write(self, data, **kwargs):

        if self.status != DOStates.INITIALIZED:
            raise Exception("No more writing expected")

        nbytes = self.writeMeta(data, **kwargs)
        if nbytes:
            self._size += nbytes

        if (self._status == DOStates.COMPLETED):
            pass
            #if (self._parent):
            #    self._parent.onCompleted(self)

        elif (self._status == DOStates.FAILED):
            pass

        else:
            self.status = DOStates.DIRTY

        return nbytes

    @abstractmethod
    def writeMeta(self, data, **kwargs):
        """
        Hook for subclass write
        """
        pass

    def delete(self):
        pass

    def computeChecksum(self, chunk):
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

    def fire(self, **attrs):
        self._bcaster.fire(**attrs)

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
        self.fire(type='status', status = value, uid = self._uid, oid = self._oid)


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
        self._consumers = consumers

    def addConsumer(self, consumer):
        self._consumers.append(consumer)

    @property
    def producers(self):
        return self._producers

    def addProducer(self, producer):
        self._producers.append(producer)

    def isCompleted(self):
        return (self._status == DOStates.COMPLETED)

    def isContainer(self):
        return (len(self._children) > 0)

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

    def trigger_consumers(self, **kwargs):
        """
        A helper function to trigger consumers
        Should be "parallel for"
        """
        for cs_id, cs in enumerate(self._consumers):
            kwargs['cs_index'] = cs_id
            cs._run(**kwargs)

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

class AppDataObject(AbstractDataObject):

    def initialize(self, **kwargs):

        self.appInitialize(**kwargs)

    def appInitialize(self, **kwargs):
        """
        Hooks for sub class
        """
        pass

    def writeMeta(self, data, **kwargs):
        """
        So that AppDataObject can be called by service handlers in the same way as
        "pure" data object if necessary
        """
        self._run(**kwargs)

    def _run(self, **kwargs):
        """
        Execute the tasks
        """
        kwdict = self.run(**kwargs)
        if (kwdict is None):
            kwdict = {}
        # TODO - this should be in another process/thread or as a continuation
        for cs_id, cs in enumerate(self.consumers):
            kwdict['cs_index'] = cs_id
            cs.write(**kwdict)

    def run(self, **kwargs):
        """
        Hooks for sub class
        Must return a dictionary: key: parameter name, val: argument value
        which will then be used as the **kwargs for calling consumers (i.e. "real" AbstractDataObjects)
        """
        pass

    def _type_code(self):
        return 1

    def isReplicable(self):
        return False

class ComputeStreamChecksum(AppDataObject):

    def appInitialize(self, **kwargs):
        pass

    def run(self, **kwargs):
        chunk = kwargs['chunk']
        self._checksum = crc32(chunk, self._checksum)

        self.checksum = self._checksum

        # put the checksum in the output
        kwargs['checksum'] = self.checksum

        return kwargs

class ComputeFileChecksum(AppDataObject):

    def appInitialize(self, **kwargs):
        self._bufsize = 4 * 1024 ** 2

    def run(self, **kwargs):
        #cs_index = cs_id, file_name = self._fnm, file_length = self._fleng
        filename = kwargs['file_name']
        with open(filename, "r") as fo:
            buf = fo.read(self._bufsize)
            crc = 0
            while (buf != ""):
                crc = crc32(buf, crc)
                buf = fo.read(self._bufsize)

        self.checksum = crc

        # put the checksum in the output
        kwargs['checksum'] = self.checksum

        return kwargs

class FileDataObject(AbstractDataObject):

    def initialize(self, **kwargs):
        """
        File data object-specific initialization.
        """
        self._root = '/tmp/sdp_dfms'
        if (not os.path.exists(self._root)):
            os.mkdir(self._root)

        self._fnm = ''.join([self._root, os.sep, self._oid])
        if (kwargs.has_key('file_length')):
            self._fleng = kwargs['file_length']
        else:
            raise Exception("Must specify the length of the file: file_length")

        self._fo = open(self._fnm, "w")
        self._fwritten = 0

    def openMeta(self, **kwargs):
        """
        """
        if (kwargs.has_key('mode')):
            mode = kwargs['mode']
        else:
            mode = 'wb'

        return open(self._fnm, mode)

    def read(self, fd, bufsize=4096):
        return fd.read(bufsize)

    def writeMeta(self, data, **kwargs):
        """
        Each chunk written to a file object
        will be written to the file system

        this is NOT thread safe (assuming we will single threaded event loop)

        """
        self._fo.write(data)
        self._fwritten += len(data)
        if (self._fwritten == self._fleng):
            self._fo.flush()
            self.status = DOStates.COMPLETED

        return len(data)

    def closeMeta(self, descriptor, **kwargs):
        """
        Closing the file object will trigger its consumer (AppDataObject) to run
        """
        descriptor.close()
        # use helper function to trigger consumers:
        self.trigger_consumers(file_name=self._fnm, file_length=self._fleng)

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
        except:
            _logger.exception("No NGAMS client libs found, cannot use NgasDataObjects")

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

    def writeMeta(self, data):
        self._buf += data
        # TODO: When all the expected data has arrived we move it from the
        # buffer into NGAS

    def closeMeta(self, descriptor):
        del descriptor

    def read(self, descriptor):
        '''
        :param ngamsPClient.ngamsPClient descriptor:
        '''
        # Read data from NGAS and give it back to our reader
        descriptor.retrieve2File(self.uid, cmd="QRETRIEVE")

    def exists(self):
        self._getClient().status(fileId=self.uid)

class StreamDataObject(AbstractDataObject):

    def initialize(self, **kwargs):
        """
        Hook for subclass initialization.
        """
        self._buf = ''

    def openMeta(self, **kwargs):
        """
        This is not thread safe, because we assume everything is inside a single thread!
        """
        return self._buf

    def writeMeta(self, data, **kwargs):
        """
        Each chunk written to a stream object
        will be immediately streamed to its consumer

        TODO - use an internal buffer, only trigger when it is full
        """
        self._buf = data
        self.trigger_consumers(chunk=self._buf)
        self.status = DOStates.COMPLETED

        return len(data)

    def stream(self, **kwargs):
        pass

class InMemoryDataObject(AbstractDataObject):

    def initialize(self, **kwargs):
        self._buf = ''

    def openMeta(self, **kwargs):
        return self._buf

    @abstractmethod
    def writeMeta(self, data, **kwargs):
        self._buf += data
        return len(data)

    @abstractmethod
    def read(self, descriptor, **kwargs):
        return descriptor

    @abstractmethod
    def closeMeta(self, descriptor, **kwargs):
        pass

class ContainerDataObject(AbstractDataObject):

    """
    Container data object has children data objects
    """
    def initialize(self, **kwargs):
        """
        Hook for subclass initialization.
        """
        self._children = []
        self._complete_map = {} #key - child oid, value - completed yet (bool)?

    def consumer_params(self):
        """
        Sub-class to add more parameters
        """
        pass

    #===========================================================================
    # No data-related operations should actually be called in Container DOs
    #===========================================================================
    def closeMeta(self, **kwargs):
        raise NotImplementedError()
    def openMeta(self, **kwargs):
        raise NotImplementedError()
    def read(self, **kwargs):
        raise NotImplementedError()
    def writeMeta(self, **kwargs):
        raise NotImplementedError()

    def check_join_condition(self, event):

        if ("status" != event.type.lower()):
            return

        print "Join condition event from {0}: {1} = {2}".format(event.oid, event.type, event.status)
        if (event.status != DOStates.COMPLETED):
            return

        self._complete_map[event.oid] = True
        # check if each child is completed
        for k, c_yet in self._complete_map.iteritems():
            if (not c_yet):
                return

        # invoke consumers if any
        add_params = self.consumer_params()
        if not add_params:
            add_params = {}
        self.trigger_consumers(**add_params)

        # notify my parent (if any) via setStatus, which fires an event
        self.status = DOStates.COMPLETED

    def addChild(self, child):
        child.subscribe(self.check_join_condition)
        self._children.append(child)
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

    def exists(self):
        # TODO: Or should it be __and__? Depends on what the exact contract of
        #       "exists" is
        return reduce(__or__, [c.exists() for c in self._children])

    def isReplicable(self):
        return False