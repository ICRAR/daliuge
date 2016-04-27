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
Utility methods and classes to be used when interacting with DROPs
'''

import inspect
import logging
import threading
import traceback
import types

import Pyro4

from dfms import utils
from dfms.ddap_protocol import DROPStates
from dfms.drop import AppDROP
from dfms.io import IOForURL, OpenMode


logger = logging.getLogger(__name__)

class EvtConsumer(utils.noopctx):
    '''
    Small utility class that sets the internal flag of the given threading.Event
    object when consuming a DROP. Used throughout the tests as a barrier to wait
    until all DROPs of a given graph have executed.
    '''
    def __init__(self, evt):
        self._evt = evt
    def handleEvent(self, e):
        if e.status in (DROPStates.COMPLETED, DROPStates.ERROR):
            self._evt.set()


class DROPWaiterCtx(object):
    """
    Class used by unit tests to trigger the execution of a physical graph and
    wait until the given set of DROPs have reached its COMPLETED status.

    It does so by appending an EvtConsumer consumer to each DROP before they are
    used in the execution, and finally checking that the events have been set.
    It should be used like this inside a test class::

     # There is a physical graph that looks like: a -> b -> c
     with DROPWaiterCtx(self, c):
         a.write('a')
         a.setCompleted()
    """

    def __init__(self, test, drops, timeout=1):
        self._drops = listify(drops)
        self._test = test
        self._timeout = timeout
        self._evts = []
    def __enter__(self):
        for drop in self._drops:
            evt = threading.Event()
            drop.subscribe(EvtConsumer(evt), 'status')
            self._evts.append(evt)
        return self
    def __exit__(self, typ, value, tb):
        if typ is not None:
            traceback.print_tb(tb)
            self._test.fail('%r' % (value,))
        to = self._timeout
        for evt in self._evts:
            self._test.assertTrue(evt.wait(to), "Waiting for DROP failed with timeout %d" % to)


class EvtConsumerProxyCtx(object):
    """
    Class used by unit tests to trigger the execution of a remote physical graph
    and wait until the given set of nodes have reached its COMPLETED status. In
    summary, this class is similar to DROPWaiterCtx, but works for remote objects
    (i.e., Pyro proxies).

    Since the graph is remote (i.e., it is hosted by a DM), the DROPs
    given to this class are actually Pyro proxies to the real DROPs, and
    therefore the consumer that is appended into them is hosted by a Pyro Daemon
    local to this class. This class creates the daemon, starts a separate thread
    to listen for incoming requests, waits until the DROPs have reached
    the COMPLETED state, stops the daemon, its listening thread, and uses the
    test class to assert basic facts.
    """

    def __init__(self, test, drops, timeout=1):
        self._drops = listify(drops)
        self._test = test
        self._timeout = timeout
        self._evts = []

    def __enter__(self):

        # Create the daemon and listen for requests
        daemon = Pyro4.Daemon()
        t = threading.Thread(None, lambda: daemon.requestLoop())
        t.daemon = 1
        t.start()

        # Attach a (proxy) EvtConsumer to each (proxy) drop
        for drop in self._drops:
            evt = threading.Event()
            consumer = EvtConsumer(evt)
            uri = daemon.register(consumer)
            consumerProxy = Pyro4.Proxy(uri)
            drop.subscribe(consumerProxy, 'status')
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
                self._test.assertTrue(evt.wait(to), "Waiting for DROP failed with timeout %d" % (to))
        except:
            allFine = False
            raise
        finally:
            self.daemon.shutdown()
            self.t.join(to)
            if allFine:
                self._test.assertFalse(self.t.isAlive())



def allDropContents(drop):
    '''
    Returns all the data contained in a given DROP
    '''
    desc = drop.open()
    buf = drop.read(desc)
    allContents = buf
    while buf:
        buf = drop.read(desc)
        allContents += buf
    drop.close(desc)
    return allContents

def copyDropContents(source, target, bufsize=4096):
    '''
    Manually copies data from one DROP into another, in bufsize steps
    '''
    desc = source.open()
    buf = source.read(desc, bufsize)
    while buf:
        target.write(buf)
        buf = source.read(desc, bufsize)
    source.close(desc)

def getUpstreamObjects(drop):
    """
    Returns a list of all direct "upstream" DROPs for the given
    DROP. An DROP A is "upstream" with respect to DROP B if
    any of the following conditions are true:
    * A is a producer of B (therefore A is an AppDROP)
    * A is a normal or streaming input of B (and B is therefore an AppDROP)

    In practice if A is an upstream DROP of B means that it must be moved
    to the COMPLETED state before B can do so.
    """
    upObjs = []
    if isinstance(drop, AppDROP):
        upObjs += drop.inputs
        upObjs += drop.streamingInputs
    else:
        upObjs += drop.producers
    return upObjs

def getDownstreamObjects(drop):
    """
    Returns a list of all direct "downstream" DROPs for the given
    DROP. An DROP A is "downstream" with respect to DROP B if
    any of the following conditions are true:
    * A is an output of B (therefore B is an AppDROP)
    * A is a normal or streaming consumer of B (and A is therefore an AppDROP)

    In practice if A is a downstream DROP of B means that it cannot
    advance to the COMPLETED state until B does so.
    """
    downObjs = []
    if isinstance(drop, AppDROP):
        downObjs += drop.outputs
    else:
        downObjs += drop.consumers
        downObjs += drop.streamingConsumers
    return downObjs

def getLeafNodes(nodes):
    """
    Returns a list of all the "leaf nodes" of the graph pointed by `nodes`.
    `nodes` is either a single DROP, or a list of DROPs.
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
    Depth-first traversal of a DROP graph. For each node in the graph the
    function func, if given, is executed with the current DROP as the only
    argument. The visited argument maintains the list of nodes already visited.
    This implementation is recursive.
    """

    if func:
        func(node)
    visited.append(node)

    dependencies = getDownstreamObjects(node)
    if dependencies:
        for drop in [d for d in dependencies if d not in visited]:
            depthFirstTraverse(drop, func, visited)

def breadFirstTraverse(toVisit, func = None):
    """
    Breadth-first traversal of a DROP graph.

    For each node in the graph the function `func` is executed, if given. `func`
    must accept at least one argument, the DROP being currently visited.
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
        nextVisits = [drop for drop in dependencies if drop not in found]
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

class DROPFile(object):
    """
    A file-like object (currently only supporting the read() operation, more to
    be added in the future) that wraps the DROP given at construction
    time.

    Depending on the underlying storage of the data the file-like object
    returned by this method will directly access the data pointed by the
    DROP if possible, or will access it through the DROP methods
    instead.

    Objects of this class will automatically close themselves when no referenced
    anymore (i.e., when __del__ is called), but users should still try to invoke
    `close()` eagerly to free underlying resources.

    Objects of this class can also be used in a `with` context.
    """
    def __init__(self, drop):
        self._drop = drop
        self._io = IOForURL(drop.dataURL)

    def open(self):
        if self._io:
            self._io.open(OpenMode.OPEN_READ)

            # TODO: This is still very insufficient, since when we `open` a DROP
            #       for reading we don't only increment its reference count,
            #       but also check that it's in a proper state, and we also
            #       fire an 'open' event. We then should have two explicitly
            #       different mechanisms to open a DROP, one actually opening the
            #       underlying storage and the other not doing it (because we
            #       do it here).
            #       The same concerns are valid for the close() operation
            self._drop.incrRefCount()
        else:
            self._fd = self._drop.open()
        self._isClosed = False

    @property
    def closed(self):
        return self._isClosed

    def close(self):
        if self._isClosed:
            return

        if self._io:
            self._io.close()

            # See the comment above regarding the call to drop.incrRefCount()
            self._drop.decrRefCount()
        else:
            self._drop.close(self._fd)
        self._isClosed = True

    def read(self, size=4096):
        if self._io:
            return self._io.read(size)
        return self._drop.read(self._fd, size)

    # Support for the `with` keyword
    def __enter__(self):
        self.open()
        return self
    def __exit__(self, typ, value, traceback):
        self.close()

    def __del__(self):
        if not self._isClosed:
            self.close()

def replace_path_placeholders(cmd, inputs, outputs):
    """
    Replaces any placeholder found in ``cmd`` with the path of the respective
    input or output Drop from ``inputs`` or ``outputs``.

    Placeholders have the different formats:
    * ``%iN``, with N starting from 0, indicates the path of the N-th element
      from the ``inputs`` argument; likewise for ``%oN``.
    * ``%i[X]`` indicates the path of the input with UID ``X``; likewise for
      ``%o[X]``.
    """

    for x,i in enumerate(inputs):
        cmd = cmd.replace("%%i%d" % (x), i.path)
        cmd = cmd.replace("%%i[%s]" % (i.uid), i.path)
    for x,o in enumerate(outputs):
        cmd = cmd.replace("%%o%d" % (x), o.path)
        cmd = cmd.replace("%%o[%s]" % (o.uid), o.path)

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("Command after path placeholder replacement is: %s" % (cmd))

    return cmd

def replace_dataurl_placeholders(cmd, inputs, outputs):
    """
    Replaces any placeholder found in ``cmd`` with the dataURL property of the
    respective input or output Drop from ``inputs`` or ``outputs``.

    Placeholders have the different formats:
    * ``%iDataURLN``, with N starting from 0, indicates the path of the N-th
      element from the ``inputs`` argument; likewise for ``%oDataURLN``.
    * ``%iDataURL[X]`` indicates the path of the input with UID ``X``; likewise
      for ``%oDataURL[X]``.
    """

    # Inputs/outputs that are not FileDROPs or DirectoryContainers can't
    # bind their data via volumes into the docker container. Instead they
    # communicate their dataURL via command-line replacement
    for x,i in enumerate(inputs):
        cmd = cmd.replace("%%iDataURL%d" % (x), i.dataURL)
        cmd = cmd.replace("%%iDataURL[%s]" % (i.uid), i.dataURL)
    for x,o in enumerate(outputs):
        cmd = cmd.replace("%%oDataURL%d" % (x), o.dataURL)
        cmd = cmd.replace("%%oDataURL[%s]" % (o.uid), o.dataURL)

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("Command after data URL placeholder replacement is: %s" % (cmd))

    return cmd

def get_roots(pg_spec):
    """
    Returns a list with the dropspecs that are the roots of the given physical
    graph specification.
    """

    # Find dropspecs with no links to the previous drops
    candidates = []
    for dropspec in pg_spec:
        if dropspec['type'] == 'app' and \
           (('inputs' in dropspec and dropspec['inputs']) or \
            ('streamingInputs' in dropspec and dropspec['streamingInputs'])):
            continue
        elif dropspec['type'] == 'plain' and \
           'producers' in dropspec and dropspec['producers']:
            continue
        candidates.append(dropspec)

    # They might still be referenced by potential neighbors
    # (e.g., an app might not reference its inputs, but they might reference it
    # as its consumer)
    roots = []
    for candidate in candidates:
        is_root = True
        typ = candidate['type']
        oid = candidate['oid']
        if typ == 'plain':
            for dropspec in pg_spec:
                if dropspec['type'] == 'app' and \
                  'outputs' in dropspec and oid in dropspec['outputs']:
                    is_root = False
                    break
        elif typ == 'app':
            for dropspec in pg_spec:
                if dropspec['type'] == 'plain' and \
                  (('consumers' in dropspec and oid in dropspec['consumers']) or \
                   ('streamingConsumers' in dropspec and oid in dropspec['streamingConsumers'])):
                    is_root = False
                    break

        if is_root:
            roots.append(candidate)

    return roots