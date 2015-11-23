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
from dfms.apps.socket_listener import SocketListenerApp
"""
Module containing functions to load a fully-functional DROP graph from its
full JSON representation.
"""

import collections
import importlib
import json
import logging

from dfms import doutils
from dfms.data_object import ContainerDataObject, InMemoryDataObject, \
    FileDataObject, NgasDataObject, LINKTYPE_NTO1_PROPERTY, \
    LINKTYPE_1TON_APPEND_METHOD, NullDataObject
from dfms.ddap_protocol import DORel, DOLinkType


STORAGE_TYPES = {
    'memory': InMemoryDataObject,
    'file'  : FileDataObject,
    'ngas'  : NgasDataObject,
    'null'  : NullDataObject
}

# Dictionary for the key used to store 1-to-N relationships between DROPs
# in the the DROP specification format
__ONE_TO_N_RELS = {
    DOLinkType.CONSUMER:           'consumers',
    DOLinkType.STREAMING_CONSUMER: 'streamingConsumers',
    DOLinkType.INPUT:              'inputs',
    DOLinkType.STREAMING_INPUT:    'streamingInputs',
    DOLinkType.OUTPUT:             'outputs',
    DOLinkType.CHILD:              'children',
    DOLinkType.PRODUCER:           'producers'
}

# Same for above, but for n-to-1 relationships
__N_TO_ONE_RELS = {
    DOLinkType.PARENT: 'parent'
}

logger = logging.getLogger(__name__)

def addLink(linkType, lhDOSpec, rhOID, force=False):
    """
    Adds a link from `lhDOSpec` to point to `rhOID`. The link type (e.g., a
    consumer) is signaled by `linkType`.
    """

    lhOID = lhDOSpec['oid']

    # 1-N relationship
    if linkType in __ONE_TO_N_RELS:
        rel = __ONE_TO_N_RELS[linkType]
        if not rel in lhDOSpec:
            relList = []
            lhDOSpec[rel] = relList
        else:
            relList = lhDOSpec[rel]
        if rhOID not in relList:
            relList.append(rhOID)
        else:
            raise Exception("DROP %s is already part of %s's %s" % (rhOID, lhOID, rel))
    # N-1 relationship, overwrite existing relationship only if `force` is specified
    elif linkType in __N_TO_ONE_RELS:
        rel = __N_TO_ONE_RELS[linkType]
        if rel and not force:
            raise Exception("DROP %s already has a '%s', use 'force' to override" % (lhOID, rel))
        lhDOSpec[rel] = rhOID
    else:
        raise ValueError("Cannot handle link type %d" % (linkType))

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("Successfully linked %s and %s via '%s'" % (lhOID, rhOID, rel))


def removeUnmetRelationships(doSpecList):
    unmetRelationships = []

    # Step #1: Index DROP specs
    doSpecsDict = {}
    [doSpecsDict.__setitem__(doSpec['oid'], doSpec) for doSpec in doSpecList]

    # Step #2: find unmet relationships and remove them from the original
    # DROP spec, keeping track of them
    for doSpec in doSpecList:

        # 1-N relationships
        for link,rel in __ONE_TO_N_RELS.viewitems():
            if rel in doSpec:
                # Find missing OIDs in relationship and keep track of them
                missingOids = [oid for oid in doSpec[rel] if oid not in doSpecsDict]
                for oid in missingOids:
                    unmetRelationships.append(DORel(oid, link, doSpec['oid']))
                # Remove them from the current DROP spec
                [doSpec[rel].remove(oid) for oid in missingOids]
                # Remove the relationship list entirely if it has no elements
                if not doSpec[rel]: del doSpec[rel]

        # N-1 relationships
        for link,rel in __N_TO_ONE_RELS.viewitems():
            if rel in doSpec:
                # Check if OID is missing
                oid = doSpec[rel]
                if oid in doSpecsDict:
                    continue
                # Keep track of missing relationship
                unmetRelationships.append(DORel(oid, link, doSpec['oid']))
                # Remove relationship from current DROP spec
                del doSpec[rel]

    return unmetRelationships

def readObjectGraph(fileObj):
    """
    Loads the DROP definitions from file-like object `fileObj`, creating
    all DROPs, establishing their relationships, and returns the root
    nodes of the graph represented by the DROPs.
    """
    return createGraphFromDOSpecList(json.load(fileObj))

def readObjectGraphS(s):
    """
    Loads the DROP definitions from the string `s`, creating all
    DROPs, establishing their relationships, and returns the root nodes of
    the graph represented by the DROPs.
    """
    return createGraphFromDOSpecList(json.loads(s))

def loadDataObjectSpecs(doSpecList):
    """
    Loads the DROP definitions from `doSpectList`, checks that
    the DROPs are correctly specified, and return a dictionary containing
    all DROP specifications (i.e., a dictionary of dictionaries) keyed on
    the OID of each DROP. Unlike `readObjectGraph` and `readObjectGraphS`,
    this method doesn't actually create the DROPs themselves.
    """

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("Found %d DROP definitions" % (len(doSpecList)))

    # Step #1: Check the DROP specs and collect them
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
        for rel in __ONE_TO_N_RELS.viewvalues():
            if rel in doSpec:
                # A KeyError will be raised if a oid has been specified in the
                # relationship list but doesn't exist in the list of DROPs
                for oid in doSpec[rel]: doSpecs[oid]

        # N-1 relationships
        for rel in __N_TO_ONE_RELS.viewvalues():
            if rel in doSpec:
                # See comment above
                doSpecs[doSpec[rel]]

    # Done!
    return doSpecs

def createGraphFromDOSpecList(doSpecList):

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("Found %d DROP definitions" % (len(doSpecList)))

    # Step #1: create the actual DROPs
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
        for link,rel in __ONE_TO_N_RELS.viewitems():
            if rel in doSpec:
                for oid in doSpec[rel]:
                    lhDO = dataObjects[oid]
                    relFuncName = LINKTYPE_1TON_APPEND_METHOD[link]
                    relFunc = getattr(dataObject, relFuncName)
                    relFunc(lhDO)

        # N-1 relationships
        for link,rel in __N_TO_ONE_RELS.viewitems():
            if rel in doSpec:
                lhDO = dataObjects[doSpec[rel]]
                propName = LINKTYPE_NTO1_PROPERTY[link]
                setattr(dataObject, propName, lhDO)

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

    # if no 'container' is specified, we default to ContainerDataObject
    if 'container' in doSpec:
        containerTypeName = doSpec['container']
        parts = containerTypeName.split('.')
        module  = importlib.import_module('.'.join(parts[:-1]))
        containerType = getattr(module, parts[-1])
    else:
        containerType = ContainerDataObject

    if dryRun:
        return

    return containerType(oid, uid, **kwargs)

def _createSocket(doSpec, dryRun=False):
    oid, uid = _getIds(doSpec)
    kwargs   = _getKwargs(doSpec)

    if dryRun:
        return
    return SocketListenerApp(oid, uid, **kwargs)

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