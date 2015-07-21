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
import logging
from dfms.data_object import ContainerAppConsumer, ContainerDataObject

'''
Utility methods and classes to be used when interacting with DataObjects

@author: rtobar, July 3, 2015
'''

_logger = logging.getLogger(__name__)

class EvtConsumer(object):
    '''
    Small utility class that sets the internal flag of the given threading.Event
    object when consuming a DO. Used throughout the tests as a barrier to wait
    until all DOs of a given graph have executed
    '''
    def __init__(self, evt):
        self._evt = evt
    def consume(self, do):
        self._evt.set()

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
     * B is a consumer of A (and therefore, A is a producer respect to B)
     * B is a child of A, and A is a ContainerAppConsumer
     * B is a ContainerDataObject (but not a ContainerAppConsumer) and A is a
       child of B

    In practice if A is an upstream DataObject of B means that it must be moved
    to the COMPLETED state before B can do so.
    """
    upObjs = []
    if dataObject.producer:
        upObjs.append(dataObject.producer)
    if _logger.isEnabledFor(logging.DEBUG):
        parent = dataObject.parent
        _logger.debug("Has parent? " + str(bool(parent)))
        if parent:
            _logger.debug("Parent details: %s/%s, type=%s" % (parent.oid, parent.uid, parent.__class__))
            _logger.debug("Is parent a ContainerAppConsumer? " + str(bool(isinstance(parent, ContainerAppConsumer))))
    if dataObject.parent and isinstance(dataObject.parent, ContainerAppConsumer):
        upObjs.append(dataObject.parent)
    elif isinstance(dataObject, ContainerDataObject) and not isinstance(dataObject, ContainerAppConsumer):
        upObjs += [dob for dob in dataObject._children]
    return upObjs

def getDownstreamObjects(dataObject):
    """
    Returns a list of all direct "downstream" DataObjects for the given
    DataObject. An DataObject A is "downstream" with respect to DataObject B if
    any of the following conditions are true:
     * A is a consumer of B (and therefore, B is a producer respect to A)
     * A is a child of B, and B is a ContainerAppConsumer
     * A is a ContainerDataObject (but not a ContainerAppConsumer) and B is a
       child of A

    In practice if A is a downstream DataObject of B means that it cannot
    advance to the COMPLETED state until B does so.
    """
    downObjs = dataObject.consumers
    if _logger.isEnabledFor(logging.DEBUG):
        parent = dataObject.parent
        _logger.debug("Has parent? " + str(bool(parent)))
        if parent:
            _logger.debug("Parent details: %s/%s, type=%s" % (parent.oid, parent.uid, parent.__class__))
            _logger.debug("Is parent a ContainerAppConsumer? " + str(bool(isinstance(parent, ContainerAppConsumer))))
    if isinstance(dataObject, ContainerAppConsumer):
        downObjs += [dob for dob in dataObject._children]
    elif dataObject.parent and isinstance(dataObject.parent, ContainerDataObject) and \
         not isinstance(dataObject.parent, ContainerAppConsumer):
        downObjs.append(dataObject.parent)
    return downObjs

def getEndNodes(nodes):
    """
    Returns a list of all the "end nodes" of the graph pointed by nodes.
    nodes is either a single DataObject, or a list of DataObjects.
    """

    if not isinstance(nodes, list):
        nodes = [nodes]

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
    Breadth-first traversal of a DataObject graph. For each node in the graph
    the function func, if given, is executed with the current DataObject as the
    only argument.
    This implementation is non-recursive.
    """

    if not isinstance(toVisit, list):
        toVisit = [toVisit]

    found = toVisit[:]
    while toVisit:

        # Pay the node a visit
        node = toVisit.pop(0)
        if func:
            func(node)

        # Enqueue its dependencies, making sure they are enqueued only one
        dependencies = getDownstreamObjects(node)
        nextVisits = [do for do in dependencies if do not in found]
        toVisit += nextVisits
        found += nextVisits