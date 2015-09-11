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
"""
Module containing functions to load a fully-functional DataObject graph from its
full JSON representation.
"""

import collections
import importlib
import json
import logging

from dfms import doutils
from dfms.data_object import ContainerDataObject, InMemoryDataObject, \
    FileDataObject, NgasDataObject, SocketListener


STORAGE_TYPES = {
    'memory': InMemoryDataObject,
    'file'  : FileDataObject,
    'ngas'  : NgasDataObject
}

# 1-to-N relationships between DataObjects in (jsonName, DOBindingMethodName) form
__ONE_TO_N_RELS = [('consumers', 'addConsumer'), ('streamingConsumers', 'addStreamingConsumer'),
                   ('inputs', 'addInput'), ('streamingInputs', 'addStreamingInput'),
                   ('outputs', 'addOutput'), ('children', 'addChild')]

# N-to-1 relationships between DataObjects. Their json name matches the attribute
# name at the DataObject level
__N_TO_ONE_RELS = ['producer', 'parent']

logger = logging.getLogger(__name__)

def readObjectGraph(fileObj):
    """
    Loads the DataObject definitions from file-like object `fileObj`, creating
    all DataObjects, establishing their relationships, and returns the root
    nodes of the graph represented by the DataObjects.
    """
    return createGraphFromDOSpecList(json.load(fileObj))

def readObjectGraphS(s):
    """
    Loads the DataObject definitions from the string `s`, creating all
    DataObjects, establishing their relationships, and returns the root nodes of
    the graph represented by the DataObjects.
    """
    return createGraphFromDOSpecList(json.loads(s))

def loadDataObjectSpecs(fileObj):
    """
    Loads the DataObject definitions from the file-like object `fileObj`, checks
    that the DataObjects are correctly specified, and return a dictionary
    containing all DataObject specifications (i.e., a dictionary of
    dictionaries) keyed on the OID of each DataObject. Unlike `readObjectGraph`
    and `readObjectGraphS`, this method doesn't actually create the DataObjects
    themselves.
    """
    return _loadDataObjectSpecs(json.load(fileObj))

def loadDataObjectSpecsS(s):
    """
    Loads the DataObject definitions from the string `s`, checks that
    the DataObjects are correctly specified, and return a dictionary containing
    all DataObject specifications (i.e., a dictionary of dictionaries) keyed on
    the OID of each DataObject. Unlike `readObjectGraph` and `readObjectGraphS`,
    this method doesn't actually create the DataObjects themselves.
    """
    return _loadDataObjectSpecs(json.loads(s))

def _loadDataObjectSpecs(doSpecList):

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("Found %d DO definitions" % (len(doSpecList)))

    # Step #1: Check the DO specs and collect them
    doSpecs = {}
    for doSpec in doSpecList:

        # 'type' is mandatory
        doType = doSpec['type']

        cf = __CREATION_FUNCTIONS[doType]
        cf(doSpec, dryRun=True)
        doSpecs[doSpec['oid']] = doSpec

    # Step #2: check relationships
    for doSpec in doSpecList:

        # 1-N relationships
        for rel,_ in __ONE_TO_N_RELS:
            if rel in doSpec:
                # A KeyError will be raised if a oid has been specified in the
                # relationship list but doesn't exist in the list of DOs
                for oid in doSpec[rel]: doSpecs[oid]

        # N-1 relationships
        for rel in __N_TO_ONE_RELS:
            if rel in doSpec:
                # See comment above
                doSpecs[doSpec[rel]]

    # Done!
    return doSpecs

def createGraphFromDOSpecList(doSpecList):

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("Found %d DO definitions" % (len(doSpecList)))

    # Step #1: create the actual DataObjects
    dataObjects = collections.OrderedDict()
    for doSpec in doSpecList:

        # 'type' is mandatory
        doType = doSpec['type']

        cf = __CREATION_FUNCTIONS[doType]
        do = cf(doSpec)
        dataObjects[do.oid] = do

    # Step #2: establish relationships
    for doSpec in doSpecList:

        # 'oid' is mandatory
        oid = doSpec['oid']
        dataObject = dataObjects[oid]

        # 1-N relationships
        for rel, relFuncName in __ONE_TO_N_RELS:
            if rel in doSpec:
                for oid in doSpec[rel]:
                    lhDO = dataObjects[oid]
                    relFunc = getattr(dataObject, relFuncName)
                    relFunc(lhDO)

        # N-1 relationships
        for rel in __N_TO_ONE_RELS:
            if rel in doSpec:
                lhDO = dataObjects[doSpec[rel]]
                setattr(dataObject, rel, lhDO)

    # We're done! Return the roots of the graph to the caller
    roots = []
    for do in dataObjects.itervalues():
        if not doutils.getUpstreamObjects(do):
            roots.append(do)

    return roots

def _createPlain(doSpec, dryRun=False):
    oid, uid = _getIds(doSpec)
    kwargs   = _getKwargs(doSpec)

    # 'storage' is mandatory
    storageType = STORAGE_TYPES[doSpec['storage']]
    if dryRun:
        return
    return storageType(oid, uid, **kwargs)

def _createContainer(doSpec, dryRun=False):
    oid, uid = _getIds(doSpec)
    kwargs   = _getKwargs(doSpec)
    if dryRun:
        return
    return ContainerDataObject(oid, uid, **kwargs)

def _createSocket(doSpec, dryRun=False):
    oid, uid = _getIds(doSpec)
    kwargs   = _getKwargs(doSpec)

    # 'storage' is mandatory
    storageType = STORAGE_TYPES[doSpec['storage']]
    class socketType(SocketListener, storageType): pass

    if dryRun:
        return
    return socketType(oid, uid, **kwargs)

def _createApp(doSpec, dryRun=False):
    oid, uid = _getIds(doSpec)
    kwargs   = _getKwargs(doSpec)
    del kwargs['app']

    appName = doSpec['app']
    parts   = appName.split('.')
    module  = importlib.import_module('.'.join(parts[:-1]))
    appType = getattr(module, parts[-1])

    if dryRun:
        return
    return appType(oid, uid, **kwargs)

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

__CREATION_FUNCTIONS = {
    'plain': _createPlain,
    'container': _createContainer,
    'app': _createApp,
    'socket': _createSocket
}