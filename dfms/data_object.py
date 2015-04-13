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
import time, os

try:
    from crc32c import crc32
except:
    from binascii import crc32

from ddap_protocol import DOStates


class AbstractDataObject(object):
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
        self._checksum = 0
        if kwargs.has_key('dom'):
            self._dom = kwargs['dom'] # hold a reference to data object manager


        for s in subs:
            self.subscribe(s)

        try:
            self.initialize(**kwargs)
            self.status = DOStates.INITIALIZED
            
        except Exception as e:
            self.status = DOStates.FAILED
            raise e
            

    def initialize(self, **kwargs):
        """
        Hook for subclass initialization.
        """
        pass

    def open(self, **kwargs):
        """
        Refer to Activity Diagram (Data Lifecycle / Open Data Object)
        """
        self.openMeta(**kwargs)

    def openMeta(self, **kwargs):
        """
        Hook for subclass open
        """
        pass

    def close(self, **kwargs):
        self.closeMeta(**kwargs)
        #self.setStatus(DOStates.CLOSED)

    def closeMeta(self, **kwargs):
        """
        Hook for subclass close
        """
        pass

    def read(self, **kwargs):
        pass

    def write(self, producer, **kwargs):

        nbytes = self.writeMeta(producer, **kwargs)

        if (self._status == DOStates.COMPLETED):
            pass
            #if (self._parent):
            #    self._parent.onCompleted(self)

        elif (self._status == DOStates.FAILED):
            pass

        else:
            self.status = DOStates.DIRTY

        return nbytes

    def writeMeta(self, producer, **kwargs):
        """
        Hook for subclass write
        """
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
        self.fire(type='setStatus', status=value, uid=self._uid, oid=self._oid)


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
        return self._location
    
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


class AppDataObject(AbstractDataObject):

    def initialize(self, **kwargs):

        self.appInitialize(**kwargs)

    def appInitialize(self, **kwargs):
        """
        Hooks for sub class
        """
        pass

    def writeMeta(self, producer, **kwargs):
        """
        So that AppDataObject can be called by service handlers in the same way as
        "pure" data object if necessary
        """
        self._run(**kwargs)

    def _run(self, producer, **kwargs):
        """
        Execute the tasks
        """
        kwdict = self.run(producer, **kwargs)
        # TODO - this should be in another process/thread or as a continuation
        for cs_id, cs in enumerate(self._consumers):
            kwdict['cs_index'] = cs_id
            cs.write(self, **kwdict)

    def run(self, producer, **kwargs):
        """
        Hooks for sub class
        Must return a dictionary: key: parameter name, val: argument value
        which will then be used as the **kwargs for calling consumers (i.e. "real" AbstractDataObjects)
        """
        pass

class ComputeStreamChecksum(AppDataObject):

    def appInitialize(self, **kwargs):
        pass

    def run(self, producer, **kwargs):
        chunk = kwargs['chunk']
        self._checksum = crc32(chunk, self._checksum)
        producer.checksum = self._checksum
        return kwargs

class ComputeFileChecksum(AppDataObject):

    def appInitialize(self, **kwargs):
        self._bufsize = 4 * 1024 ** 2

    def run(self, producer, **kwargs):
        #cs_index = cs_id, file_name = self._fnm, file_length = self._fleng
        filename = kwargs['file_name']
        fo = open(filename, "r")
        buf = fo.read(self._bufsize)
        crc = 0
        while (buf != ""):
            crc = crc32(buf, crc)
            buf = fo.read(self._bufsize)
        fo.close()
        producer.checksum = crc
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
        self._fwritten = 0

    def openMeta(self, **kwargs):
        """
        """
        if (kwargs.has_key('mode')):
            mode = kwargs['mode']
        else:
            mode = 'wb'
        self._fo = open(self._fnm, mode)
        return self._fo

    def writeMeta(self, producer, **kwargs):
        """
        Each chunk written to a file object
        will be written to the file system

        producer:    is the AppDataObject

        this is NOT thread safe (assuming we will single threaded event loop)

        """
        chunk = kwargs['chunk']
        self._fo.write(chunk)
        self._fwritten += len(chunk)
        if (self._fwritten == self._fleng):
            self._fo.flush()
            self.status = DOStates.COMPLETED

        return len(chunk)

    def closeMeta(self, **kwargs):
        """
        Closing the file object will trigger its consumer (AppDataObject) to run
        """
        self._fo.close()
        for cs_id, cs in enumerate(self._consumers):
            cs._run(self, cs_index = cs_id, file_name = self._fnm, file_length = self._fleng)


    def seek(self, **kwargs):
        pass

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

    def writeMeta(self, producer, **kwargs):
        """
        Each chunk written to a stream object
        will be immediately streamed to its consumer

        TODO - use an internal buffer, only trigger when it is full
        """
        self._buf = kwargs['chunk']
        #doms_handler = kwargs['doms_handler']
        for cs_id, cs in enumerate(self._consumers):
            cs._run(self, cs_index = cs_id, chunk = self._buf)

        self.status = DOStates.COMPLETED

        return len(self._buf)

    def stream(self, **kwargs):
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

    def check_join_condition(self, event):
        if ("setStatus" != event.type):
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
        for cs_id, cs in enumerate(self._consumers):
            cs._run(self, cs_index = cs_id) #TODO: this should be done in parallel

        # notify my parent (if any) via setStatus, which fires an event
        self.status = DOStates.COMPLETED

    def addChild(self, child):
        child.subscribe(self.check_join_condition)
        self._children.append(child)
        self._complete_map[child.oid] = child.isCompleted()


