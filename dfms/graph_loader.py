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
import collections

"""
Module containing functions to load a fully-functional DataObject graph from its
full JSON representation.
"""

import importlib
import json
import logging

from dfms import doutils
from dfms.data_object import ContainerDataObject, InMemoryDataObject, \
    FileDataObject, NgasDataObject, SocketListener, ContainerAppConsumer
from dfms.events.event_broadcaster import LocalEventBroadcaster


STORAGE_TYPES = {
    'memory': InMemoryDataObject,
    'file'  : FileDataObject,
    'ngas'  : NgasDataObject
}

logger = logging.getLogger(__name__)

def readObjectGraph(fileObj):
    """
    Loads the DataObject definitions from file-like object `fileObj`, creating
    all DataObjects, establishing their relationships, and returns the root
    nodes of the graph represented by the DataObjects.
    """
    return _createDataObjectGraph(json.load(fileObj))

def readObjectGraphS(s):
    """
    Loads the DataObject definitions from the string `s`, creating all
    DataObjects, establishing their relationships, and returns the root nodes of
    the graph represented by the DataObjects.
    """
    return _createDataObjectGraph(json.loads(s))

def _createDataObjectGraph(doSpecList):
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("Loaded %d DO definitions" % (len(doSpecList)))

    # Step #1: create the actual DataObjects
    dataObjects = collections.OrderedDict()
    for doSpec in doSpecList:

        # 'type' is mandatory
        doType = doSpec['type']

        # container DOs
        if doType == "plain":
            do = _createPlain(doSpec)
        elif doType == 'container':
            do = _createContainer(doSpec)
        elif doType == 'containerapp':
            do = _createContainerApp(doSpec)
        elif doType == 'socket':
            do = _createSocket(doSpec)
        elif doType == 'app':
            do = _createApp(doSpec)
        else:
            raise Exception("Unknown DataObject type: %s" % (doType))

        dataObjects[do.oid] = do

    # Step #2: establish relationships
    for doSpec in doSpecList:

        # 'oid' is mandatory
        oid = doSpec['oid']
        dataObject = dataObjects[oid]

        if doSpec.has_key('consumers'):
            for consumerOid in doSpec['consumers']:
                immediateConsumer = dataObjects[consumerOid]
                dataObject.addConsumer(immediateConsumer)

        if doSpec.has_key('immediateConsumers'):
            for consumerOid in doSpec['immediateConsumers']:
                immediateConsumer = dataObjects[consumerOid]
                dataObject.addImmediateConsumer(immediateConsumer)

        if doSpec.has_key('children'):
            for childOid in doSpec['children']:
                child = dataObjects[childOid]
                dataObject.addChild(child)

    # We're done! Return the roots of the graph to the caller
    roots = []
    for do in dataObjects.itervalues():
        if not doutils.getUpstreamObjects(do):
            roots.append(do)

    return roots

def _createPlain(doSpec):
    oid, uid = _getIds(doSpec)
    kwargs   = _getKwargs(doSpec)

    # 'storage' is mandatory
    storageType = STORAGE_TYPES[doSpec['storage']]
    return storageType(oid, uid, LocalEventBroadcaster(), **kwargs)

def _createContainer(doSpec):
    oid, uid = _getIds(doSpec)
    kwargs   = _getKwargs(doSpec)
    return ContainerDataObject(oid, uid, LocalEventBroadcaster(), **kwargs)

def _createContainerApp(doSpec):
    oid, uid = _getIds(doSpec)
    kwargs   = _getKwargs(doSpec)
    del kwargs['app']

    appName = doSpec['app']
    parts   = appName.split('.')
    module  = importlib.import_module('.'.join(parts[:-1]))
    appType = getattr(module, parts[-1])

    class doType(appType, ContainerAppConsumer): pass

    return doType(oid, uid, LocalEventBroadcaster(), **kwargs)

def _createSocket(doSpec):
    oid, uid = _getIds(doSpec)
    kwargs   = _getKwargs(doSpec)

    # 'storage' is mandatory
    storageType = STORAGE_TYPES[doSpec['storage']]
    class socketType(SocketListener, storageType): pass

    return socketType(oid, uid, LocalEventBroadcaster(), **kwargs)

def _createApp(doSpec):
    oid, uid = _getIds(doSpec)
    kwargs   = _getKwargs(doSpec)
    del kwargs['app']

    appName = doSpec['app']
    parts   = appName.split('.')
    module  = importlib.import_module('.'.join(parts[:-1]))
    appType = getattr(module, parts[-1])

    # 'storage' is mandatory
    storageType = STORAGE_TYPES[doSpec['storage']]
    class doType(appType, storageType): pass

    return doType(oid, uid, LocalEventBroadcaster(), **kwargs)

def _getIds(doSpec):
    # uid is copied from oid if not explicitly given
    oid = doSpec['oid']
    uid = oid
    if doSpec.has_key('uid'):
        uid = doSpec['uid']
    return oid, uid

def _getKwargs(doSpec):
    kwargs = dict(doSpec)
    del kwargs['oid']
    if kwargs.has_key('uid'):
        del kwargs['uid']
    return kwargs