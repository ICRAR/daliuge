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

import collections
import logging
import re
import threading
import traceback

import six

from .ddap_protocol import DROPStates
from .drop import AppDROP
from .io import IOForURL, OpenMode


logger = logging.getLogger(__name__)

# Used to check whether a command specifies via UID reference the path or
# data URL of an input or output
indexed_ipath_pattern    = re.compile(r".*%i\[.+\].*")
indexed_opath_pattern    = re.compile(r".*%o\[.+\].*")
indexed_idataurl_pattern = re.compile(r".*%iDataURL\[.+\].*")
indexed_odataurl_pattern = re.compile(r".*%oDataURL\[.+\].*")

class EvtConsumer(object):
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


def allDropContents(drop, bufsize=4096):
    '''
    Returns all the data contained in a given DROP
    '''
    buf = six.BytesIO()
    desc = drop.open()
    read = drop.read

    while True:
        data = read(desc, bufsize)
        if not data:
            break
        buf.write(data)
    drop.close(desc)
    return buf.getvalue()

def copyDropContents(source, target, bufsize=4096):
    '''
    Manually copies data from one DROP into another, in bufsize steps
    '''
    desc = source.open()
    read = source.read
    buf = read(desc, bufsize)
    while buf:
        target.write(buf)
        buf = read(desc, bufsize)
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
    return [drop for drop,_ in breadFirstTraverse(nodes) if not getDownstreamObjects(drop)]

def depthFirstTraverse(node, visited = []):
    """
    Depth-first iterator for a DROP graph.

    This iterator yields a tuple where the first item is the node being visited,
    and the second is a list of nodes that will be visited subsequently.
    Callers can alter this list in order to remove certain nodes from the
    graph traversal process.

    This implementation is recursive.
    """

    dependencies = getDownstreamObjects(node)
    yield node, dependencies
    visited.append(node)

    for drop in [d for d in dependencies if d not in visited]:
        for x in depthFirstTraverse(drop, visited):
            yield x

def breadFirstTraverse(toVisit):
    """
    Breadth-first iterator for a DROP graph.

    This iterator yields a tuple where the first item is the node being visited,
    and the second is a list of nodes that will be visited subsequently.
    Callers can alter this list in order to remove certain nodes from the
    graph traversal process.

    This implementation is non-recursive.
    """

    toVisit_list = listify(toVisit)[:]
    toVisit = collections.deque(toVisit_list)
    visited = set(toVisit_list)

    # See how many arguments we should used when calling func
    while toVisit:

        # Pay the node a visit
        node = toVisit.popleft()
        dependencies = getDownstreamObjects(node)
        yield node, dependencies

        # Enqueue its dependencies, making sure they are enqueued only once
        next_visits = [n for n in dependencies if n not in visited]
        visited.update(next_visits)
        toVisit += next_visits

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

def has_path(x):
    """Returns `True` if `x` has a `path` attribute"""
    try:
        getattr(x, 'path')
        return True
    except:
        return False

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

    logger.debug('Replacing cmd %s with placeholders with I/O uids: %r, %r',
                 cmd, inputs.keys(), outputs.keys())

    for x,i in enumerate(inputs.values()):
        pathRef = "%%i%d" % (x,)
        if pathRef in cmd:
            cmd = cmd.replace(pathRef, i.path)
    for x,o in enumerate(outputs.values()):
        pathRef = "%%o%d" % (x)
        if pathRef in cmd:
            cmd = cmd.replace(pathRef, o.path)

    for uid, i in inputs.items():
        pathRef = "%%i[%s]" % (uid,)
        if pathRef in cmd:
            cmd = cmd.replace(pathRef, i.path)
    for uid, o in outputs.items():
        pathRef = "%%o[%s]" % (uid,)
        if pathRef in cmd:
            cmd = cmd.replace(pathRef, o.path)

    logger.debug("Command after path placeholder replacement is: %s", cmd)

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

    for x,i in enumerate(inputs.values()):
        dataUrlRef = "%%iDataURL%d" % (x,)
        if dataUrlRef in cmd:
            cmd = cmd.replace(dataUrlRef, i.dataURL)
    for x,o in enumerate(outputs.values()):
        dataUrlRef = "%%oDataURL%d" % (x,)
        if dataUrlRef in cmd:
            cmd = cmd.replace(dataUrlRef, o.dataURL)

    for uid,i in inputs.items():
        dataURLRef = "%%iDataURL[%s]" % (uid,)
        if dataURLRef in cmd:
            cmd = cmd.replace(dataURLRef, i.dataURL)
    for uid,o in outputs.items():
        dataURLRef = "%%oDataURL[%s]" % (uid,)
        if dataURLRef in cmd:
            cmd = cmd.replace(dataUrlRef, o.dataURL)

    logger.debug("Command after data URL placeholder replacement is: %s", cmd)

    return cmd

def get_roots(pg_spec):
    """
    Returns a set with the OIDs of the dropspecs that are the roots of the given physical
    graph specification.
    """

    # We find all the nonroots first, which are easy to spot.
    # The rest are the roots
    all_oids = set()
    nonroots = set()
    for dropspec in pg_spec:

        oid = dropspec['oid']
        all_oids.add(oid)

        if dropspec['type'] in ('app', 'socket'):
            if dropspec.get('inputs', None) or dropspec.get('streamingInputs', None):
                nonroots.add(oid)
            if dropspec.get('outputs', None):
                nonroots |= set(dropspec['outputs'])
        elif dropspec['type'] == 'plain':
            if dropspec.get('producers', None):
                nonroots.add(oid)
            if dropspec.get('consumers', None):
                nonroots |= set(dropspec['consumers'])
            if dropspec.get('streamingConsumers', None):
                nonroots |= set(dropspec['streamingConsumers'])

    return all_oids - nonroots

def get_leaves(pg_spec):
    """
    Returns a set with the OIDs of the dropspecs that are the leaves of the given physical
    graph specification.
    """

    # We find all the nonleaves first, which are easy to spot.
    # The rest are the leaves
    all_oids = set()
    nonleaves = set()
    for dropspec in pg_spec:

        oid = dropspec['oid']
        all_oids.add(oid)

        if dropspec['type'] == 'app':
            if dropspec.get('outputs', None):
                nonleaves.add(oid)
            if dropspec.get('streamingInputs', None):
                nonleaves |= set(dropspec['streamingInputs'])
            if dropspec.get('inputs', None):
                nonleaves |= set(dropspec['inputs'])
        elif dropspec['type'] == 'plain':
            if dropspec.get('producers', None):
                nonleaves |= set(dropspec['producers'])
            if dropspec.get('consumers', None) or dropspec.get('streamingConsumers', None):
                nonleaves.add(oid)

    return all_oids - nonleaves