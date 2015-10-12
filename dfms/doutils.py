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
Utility methods and classes to be used when interacting with DataObjects

@author: rtobar, July 3, 2015
'''

import inspect
import logging
import threading
import types

import Pyro4

from dfms.data_object import AppDataObject
from dfms.io import IOForURL, OpenMode


logger = logging.getLogger(__name__)

class EvtConsumer(object):
    '''
    Small utility class that sets the internal flag of the given threading.Event
    object when consuming a DO. Used throughout the tests as a barrier to wait
    until all DOs of a given graph have executed
    '''
    def __init__(self, evt):
        self._evt = evt
    def dataObjectCompleted(self, do):
        self._evt.set()


class DOWaiterCtx(object):
    """
    Class used by unit tests to trigger the execution of a physical graph and
    wait until the given set of DataObjects have reached its COMPLETED status.

    It does so by appending an EvtConsumer consumer to each DO before they are
    used in the execution, and finally checking that the events have been set.
    It should be used like this inside a test class:

    # There is a physical graph that looks like: a -> b -> c
    with DOWaiterCtx(self, c):
        a.write('a')
        a.setCompleted()
    """

    def __init__(self, test, dos, timeout=1):
        self._dos = listify(dos)
        self._test = test
        self._timeout = timeout
        self._evts = []
    def __enter__(self):
        for do in self._dos:
            evt = threading.Event()
            do.addConsumer(EvtConsumer(evt))
            self._evts.append(evt)
        return self
    def __exit__(self, typ, value, traceback):
        to = self._timeout
        for evt in self._evts:
            self._test.assertTrue(evt.wait(to), "Waiting for DO failed with timeout %d" % to)


class EvtConsumerProxyCtx(object):
    """
    Class used by unit tests to trigger the execution of a remote physical graph
    and wait until the given set of nodes have reached its COMPLETED status. In
    summary, this class is similar to DOWaiterCtx, but works for remote objects
    (i.e., Pyro proxies).

    Since the graph is remote (i.e., it is hosted by a DOM), the DataObjects
    given to this class are actually Pyro proxies to the real DataObjects, and
    therefore the consumer that is appended into them is hosted by a Pyro Daemon
    local to this class. This class creates the daemon, starts a separate thread
    to listen for incoming requests, waits until the DataObjects have reached
    the COMPLETED state, stops the daemon, its listening thread, and uses the
    test class to assert basic facts.
    """

    def __init__(self, test, dos, timeout=1):
        self._dos = listify(dos)
        self._test = test
        self._timeout = timeout
        self._evts = []

    def __enter__(self):

        # Create the daemon and listen for requests
        daemon = Pyro4.Daemon()
        t = threading.Thread(None, lambda: daemon.requestLoop())
        t.daemon = 1
        t.start()

        # Attach a (proxy) EvtConsumer to each (proxy) do
        for do in self._dos:
            evt = threading.Event()
            consumer = EvtConsumer(evt)
            uri = daemon.register(consumer)
            consumerProxy = Pyro4.Proxy(uri)
            do.addConsumer(consumerProxy)
            self._evts.append(evt)

        self.daemon = daemon
        self.t = t
        return self

    def __exit__(self, typ, value, traceback):

        to = self._timeout
        allFine = True

        # Check now that all events have been set. If everything went fine, we
        # also check that the thread hosting the daemon is dead.
        try:
            for evt in self._evts:
                self._test.assertTrue(evt.wait(to), "Waiting for DO failed with timeout %d" % (to))
        except:
            allFine = False
            raise
        finally:
            self.daemon.shutdown()
            self.t.join(to)
            if allFine:
                self._test.assertFalse(self.t.isAlive())



def allDataObjectContents(dataObject):
    '''
    Returns all the data contained in a given dataObject
    '''
    desc = dataObject.open()
    buf = dataObject.read(desc)
    allContents = buf
    while buf:
        buf = dataObject.read(desc)
        allContents += buf
    dataObject.close(desc)
    return allContents

def copyDataObjectContents(source, target, bufsize=4096):
    '''
    Manually copies data from one DataObject into another, in bufsize steps
    '''
    desc = source.open()
    buf = source.read(desc, bufsize)
    while buf:
        target.write(buf)
        buf = source.read(desc, bufsize)
    source.close(desc)

def getUpstreamObjects(dataObject):
    """
    Returns a list of all direct "upstream" DataObjects for the given
    DataObject. An DataObject A is "upstream" with respect to DataObject B if
    any of the following conditions are true:
     * A is a producer of B (therefore A is an AppDataObject)
     * A is a normal or streaming input of B (and B is therefore an AppDataObject)

    In practice if A is an upstream DataObject of B means that it must be moved
    to the COMPLETED state before B can do so.
    """
    upObjs = []
    if isinstance(dataObject, AppDataObject):
        upObjs += dataObject.inputs
        upObjs += dataObject.streamingInputs
    else:
        upObjs += dataObject.producers
    return upObjs

def getDownstreamObjects(dataObject):
    """
    Returns a list of all direct "downstream" DataObjects for the given
    DataObject. An DataObject A is "downstream" with respect to DataObject B if
    any of the following conditions are true:
     * A is an output of B (therefore B is an AppDataObject)
     * A is a normal or streaming consumer of B (and A is therefore an AppDataObject)

    In practice if A is a downstream DataObject of B means that it cannot
    advance to the COMPLETED state until B does so.
    """
    downObjs = []
    if isinstance(dataObject, AppDataObject):
        downObjs += dataObject.outputs
    else:
        downObjs += dataObject.consumers
        downObjs += dataObject.streamingConsumers
    return downObjs

def getLeafNodes(nodes):
    """
    Returns a list of all the "leaf nodes" of the graph pointed by `nodes`.
    `nodes` is either a single DataObject, or a list of DataObjects.
    """

    nodes = listify(nodes)

    # To be executed when visiting each node
    endNodes = []
    def addLeafNode(n):
        if not getDownstreamObjects(n):
            endNodes.append(n)

    breadFirstTraverse(nodes, addLeafNode)
    return endNodes

def depthFirstTraverse(node, func = None, visited = []):
    """
    Depth-first traversal of a DataObject graph. For each node in the graph the
    function func, if given, is executed with the current DataObject as the only
    argument. The visited argument maintains the list of nodes already visited.
    This implementation is recursive.
    """

    if func:
        func(node)
    visited.append(node)

    dependencies = getDownstreamObjects(node)
    if dependencies:
        for do in [d for d in dependencies if d not in visited]:
            depthFirstTraverse(do, func, visited)

def breadFirstTraverse(toVisit, func = None):
    """
    Breadth-first traversal of a DataObject graph.

    For each node in the graph the function `func` is executed, if given. `func`
    must accept at least one argument, the DataObject being currently visited.
    If two arguments are specified, the second argument will be a list of
    nodes that will be visited subsequently; `func` can alter this list in order
    to remove certain nodes from the traversal process.

    This implementation is non-recursive.
    """

    toVisit = listify(toVisit)[:]
    found = toVisit[:]

    # See how many arguments we should used when calling func
    if func:
        nArgs = len(inspect.getargspec(func).args)
        if type(func) == types.MethodType:
            nArgs -= 1

    while toVisit:

        # Pay the node a visit
        node = toVisit.pop(0)
        dependencies = getDownstreamObjects(node)
        if func:
            if nArgs == 1:
                func(node)
            elif nArgs == 2:
                func(node, dependencies)
            else:
                raise Exception("Unsupported number of arguments for function %r: %d. Expected 1 or 2" % (func, nArgs))

        # Enqueue its dependencies, making sure they are enqueued only once
        nextVisits = [do for do in dependencies if do not in found]
        toVisit += nextVisits
        found += nextVisits

def listify(o):
    """
    If `o` is already a list return it as is; if `o` is a tuple returns a list
    containing the elements contained in the tuple; otherwise returns a list
    with `o` being its only element
    """
    if isinstance(o, list):
        return o
    if isinstance(o, tuple):
        return list(o)
    return [o]

class DOFile(object):
    """
    A file-like object (currently only supporting the read() operation, more to
    be added in the future) that wraps the DataObject given at construction
    time.

    Depending on the underlying storage of the data the file-like object
    returned by this method will directly access the data pointed by the
    DataObject if possible, or will access it through the DataObject methods
    instead.

    Objects of this class will automatically close themselves when no referenced
    anymore (i.e., when __del__ is called), but users should still try to invoke
    `close()` eagerly to free underlying resources.

    Objects of this class can also be used in a `with` context.
    """
    def __init__(self, do):
        self._do = do
        self._io = IOForURL(do.dataURL)

    def open(self):
        if self._io:
            self._io.open(OpenMode.OPEN_READ)

            # TODO: This is still very insufficient, since when we `open` a DO
            #       for reading we don't only increment its reference count,
            #       but also check that it's in a proper state, and we also
            #       fire an 'open' event. We then should have two explicitly
            #       different mechanisms to open a DO, one actually opening the
            #       underlying storage and the other not doing it (because we
            #       do it here).
            #       The same concerns are valid for the close() operation
            self._do.incrRefCount()
        else:
            self._fd = self._do.open()
        self._isClosed = False

    @property
    def closed(self):
        return self._isClosed

    def close(self):
        if self._isClosed:
            return

        if self._io:
            self._io.close()

            # See the comment above regarding the call to do.incrRefCount()
            self._do.decrRefCount()
        else:
            self._do.close(self._fd)
        self._isClosed = True

    def read(self, size=4096):
        if self._io:
            return self._io.read(size)
        return self._do.read(self._fd, size)

    # Support for the `with` keyword
    def __enter__(self):
        self.open()
        return self
    def __exit__(self, typ, value, traceback):
        self.close()

    def __del__(self):
        if not self._isClosed:
            self.close()
