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
from dfms.ddap_protocol import DOStates

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
    def __init__(self, oid, uid, **kwargs):
        """
        Constructor
        oid:    object id (string)
        uid:    uuid    (string)
        """
        self._oid = oid
        self._uid = uid
        self._consumers = []# could be (1) real component (if I am a real data object consumed by them)
                            #       or (2) real data objects (if I am a real component that produce them)
        self._producers = []# could be (1) real component (if I am a real data object produced by them)
                            #       or (2) real data objects (if I am a real component that consume them)
        self._children = [] # now, we only allow real data object to have children
                            # but real component can have children too (in the future)
        self._stateEHlist = [] # state event handler list
     
        self._location = None
        self._parent = None
        self._status = DOStates.CLOSED
        self._checksum = 0
        if kwargs.has_key('dom'):
            self._dom = kwargs['dom'] # hold a reference to data object manager
        self.initialize(**kwargs)
    
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
        self.setStatus(DOStates.CLOSED)
        
    def closeMeta(self, **kwargs):
        """
        Hook for subclass close
        """
        pass
    
    def read(self, **kwargs):
        pass
    
    def write(self, producer, **kwargs):
        if (self._status == DOStates.CLOSED):
            self.open()

        nbytes = self.writeMeta(producer, **kwargs)
        
        # notify parent object that its completed
        if (self._status == DOStates.COMPLETED):
            if (self._parent):
                self._parent.onCompleted(self)
        
        if (self._status != DOStates.DIRTY):
            self.setStatus(DOStates.DIRTY)
        
        return nbytes
    
    def writeMeta(self, producer, **kwargs):
        """
        Hook for subclass write
        """
        pass
    
    def computeChecksum(self, chunk):
        self._checksum = crc32(chunk, self._checksum)
        return self._checksum
    
    def getChecksum(self):
        return self._checksum
    
    def setChecksum(self, checksum):
        self._checksum = checksum
    
    def getOid(self):
        return self._oid
    
    def setStatus(self, status):
        olds = self._status
        self._status = status
        # fire events TODO - start a new thread with proper exception handling:
        for eh in self._stateEHlist:
            if (eh.filterStateChange(olds, status)):
                eh.onStateChange(self.getOid(), olds, status)
       
    def setParent(self, parent):
        if (parent): # only real data object has parent, and we currently only have up to 1 parent
            self._parent = parent # a parent is a container
    
    def getParent(self):
        return self._parent
    
    def setConsumers(self, consumers):
        """
        set a list of consumers (replace the existing ones)
        """
        self._consumers = consumers
        
    def getConsumers(self):
        return self._consumers 
        
    def addConsumer(self, consumer):
        self._consumers.append(consumer)
        
    def addProducer(self, producer):
        self._producers.append(producer)
        
    def addChild(self, child):
        self._children.append(child)
    
    def isCompleted(self):
        return (self._status == DOStates.COMPLETED)
    
    def isContainer(self):
        return (len(self._children) > 0)
    
    def onCompleted(self, child):
        """
        Callback when data (ingestion) is completed
        This is called ONLY by one of the children data objects
        In this case, the current data object is a "container"
        
        child:    The child of the container, i.e. myself
        """
        print "I am %s, I am notified of the completion by my child: %s" % (self.getOid(), child.getOid())
        # wait for all children if any
        for mychild in self._children:
            if (not mychild.isCompleted()): #TODO - this is somehow wrong
                return
        
        # invoke consumers if any
        for cs_id, cs in enumerate(self._consumers):
            #cs.run(self, cs_index = cs_id) #TODO: this should be done in parallel
            cs.postCompleteEvent(self, cs_index = cs_id)
            
        # notify my parent if any
        if (self._parent):
            self._parent.onCompleted(self)
        
        self.setStatus(DOStates.COMPLETED)
    
    def setLocation(self, location):
        """
        This should be set when the physical graph was built
        """
        self._location = location
    
    def getLocation(self):
        """
        return where the "actual" data is located
        the location could be a Compute node or a Island or just the buffer URL
        """
        return self._location
    
    def setURI(self, uri):
        self._uri = uri
    
    def getURI(self):
        return self._uri
    
    def ping(self):
        """
        This is for testing purpose
        """
        return 'OK. My oid = %s, and my uid = %s' % (self.getOid(), self._uid)
    
    def subscribeStateChange(self, eventHandler):
        """
        subscribe to state change event
        eventHandler:  an instance of DOStateEventHandler
        """
        self._stateEHlist.append(eventHandler)
        return
    
    def unsubscribeStateChange(self, eventHandler):
        self._stateEHlist.remove(eventHandler) # assuming no duplicates for now

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
        #self.run(**kwargs)
        pass
    
    '''def run(self, producer, **kwargs):
        """
        Execute the tasks
        """
        kwdict = self.runCallBack(producer, **kwargs)
        # TODO - this should be in another process/thread or as a continuation
        for cs_id, cs in enumerate(self._consumers):
            kwdict['cs_index'] = cs_id
            cs.write(self, **kwdict)'''
    
    def postBufferEvent(self, producer, **kwargs):
        """
        Execute the tasks
        """
        kwdict = self.onBuffer(producer, **kwargs)
        if kwdict:
            # TODO - this should be in another process/thread or as a continuation
            for cs_id, cs in enumerate(self._consumers):
                kwdict['cs_index'] = cs_id
                cs.write(self, **kwdict)
    
    def postInitEvent(self, producer, **kwargs):
        """
        Execute the tasks
        """
        kwdict = self.onInit(producer, **kwargs)
        if kwdict:
            # TODO - this should be in another process/thread or as a continuation
            for cs_id, cs in enumerate(self._consumers):
                kwdict['cs_index'] = cs_id
                cs.write(self, **kwdict)
    
    def postCompleteEvent(self, producer, **kwargs):
        """
        Execute the tasks
        """
        kwdict = self.onComplete(producer, **kwargs)
        if kwdict:
            # TODO - this should be in another process/thread or as a continuation
            for cs_id, cs in enumerate(self._consumers):
                kwdict['cs_index'] = cs_id
                cs.write(self, **kwdict)
    
    def postEntryEvent(self, producer, **kwargs):
        """
        Execute the tasks
        """
        kwdict = self.onEntry(producer, **kwargs)
        if kwdict:
            # TODO - this should be in another process/thread or as a continuation
            for cs_id, cs in enumerate(self._consumers):
                kwdict['cs_index'] = cs_id
                cs.write(self, **kwdict)
    
    '''def runCallBack(self, producer, **kwargs):
        """
        Hooks for sub class
        Must return a dictionary: key: parameter name, val: argument value
        which will then be used as the **kwargs for calling consumers (i.e. "real" AbstractDataObjects)
        """
        pass'''

class ComputeStreamChecksum(AppDataObject):
    
    def appInitialize(self, **kwargs):
        pass
    
    
    def onBuffer(self, producer, **kwargs):
    #def runCallBack(self, producer, **kwargs):
        chunk = kwargs['chunk']
        self._checksum = crc32(chunk, self._checksum)
        producer.setChecksum(self._checksum)
        return kwargs

class ComputeFileChecksum(AppDataObject):
    
    def appInitialize(self, **kwargs):
        self._bufsize = 4 * 1024 ** 2
    
    def onComplete(self, producer, **kwargs):
    #def runCallBack(self, producer, **kwargs):
        #cs_index = cs_id, file_name = self._fnm, file_length = self._fleng
        filename = kwargs['file_name']
        fo = open(filename, "r")
        buf = fo.read(self._bufsize)
        crc = 0
        while (buf != ""):
            crc = crc32(buf, crc)
            buf = fo.read(self._bufsize)
        fo.close()        
        producer.setChecksum(crc)
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
            self.setStatus(DOStates.COMPLETED)
            # tell my parent (if any) that I am completed 
            #if (self._parent):
            #    self._parent.onCompleted(self)
                
        return len(chunk)
    
    def closeMeta(self, **kwargs):
        """
        Closing the file object will trigger its consumer (AppDataObject) to run
        """
        self._fo.close()
        for cs_id, cs in enumerate(self._consumers):
            cs.postCompleteEvent(self, cs_index = cs_id, file_name = self._fnm, file_length = self._fleng)
            #cs.run(self, cs_index = cs_id, file_name = self._fnm, file_length = self._fleng)
        
    
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
            #cs.run(self, cs_index = cs_id, chunk = self._buf)
            cs.postBufferEvent(self, cs_index = cs_id, chunk = self._buf)
            
        self.setStatus(DOStates.COMPLETED)
        # notify my parent if any
        #if (self._parent):
        #    self._parent.onCompleted(self)
        return len(self._buf)
    
    def stream(self, **kwargs):
        pass


    