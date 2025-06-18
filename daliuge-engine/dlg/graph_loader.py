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
Module containing functions to load a fully-functional DROP graph from its
full JSON representation.
"""

import collections
import importlib
import logging
from typing import List, Optional

from dlg.common.reproducibility.constants import ReproducibilityFlags

from . import droputils
from .apps.socket_listener import SocketListenerApp
from .ddap_protocol import DROPRel, DROPLinkType
from .drop import (
    AbstractDROP,
    LINKTYPE_NTO1_PROPERTY,
    LINKTYPE_1TON_APPEND_METHOD,
)

from dlg.data.drops import InMemoryDROP, SharedMemoryDROP, FileDROP, NgasDROP
from dlg.data.drops.data_base import NullDROP
from dlg.data.drops.environmentvar_drop import EnvironmentVarDROP
from dlg.data.drops.parset_drop import ParameterSetDROP
from dlg.data.drops.container import ContainerDROP
from dlg.exceptions import InvalidGraphException
from dlg.data.drops.json_drop import JsonDROP


class CategoryType:
    DATA = "dropclass"

    # Dictionary for the key used to store 1-to-N relationships between DROPs


# in the the DROP specification format
__TOMANY = {
    DROPLinkType.CONSUMER: "consumers",
    DROPLinkType.STREAMING_CONSUMER: "streamingConsumers",
    DROPLinkType.INPUT: "inputs",
    DROPLinkType.STREAMING_INPUT: "streamingInputs",
    DROPLinkType.OUTPUT: "outputs",
    DROPLinkType.CHILD: "children",
    DROPLinkType.PRODUCER: "producers",
}

# Same for above, but for n-to-1 relationships
__TOONE = {DROPLinkType.PARENT: "parent"}

# Both also contain the reverse mapping
__TOMANY.update({v: k for k, v in __TOMANY.items()})
__TOONE.update({v: k for k, v in __TOONE.items()})

logger = logging.getLogger(f"dlg.{__name__}")


def addLink(linkType, lhDropSpec, rhOID, force=False):
    """
    Adds a link from `lhDropSpec` to point to `rhOID`. The link type (e.g., a
    consumer) is signaled by `linkType`.
    """

    lhOID = lhDropSpec["oid"]

    # 1-N relationship
    if linkType in __TOMANY:
        rel = __TOMANY[linkType]
        if not rel in lhDropSpec:
            relList = []
            lhDropSpec[rel] = relList
        else:
            relList = lhDropSpec[rel]
        if rhOID not in relList:
            relList.append(rhOID)
        else:
            raise RuntimeError("DROP %s is already part of %s's %s" % (rhOID, lhOID, rel))
    # N-1 relationship, overwrite existing relationship only if `force` is specified
    elif linkType in __TOONE:
        rel = __TOONE[linkType]
        if rel and not force:
            raise RuntimeError(
                "DROP %s already has a '%s', use 'force' to override" % (lhOID, rel)
            )
        lhDropSpec[rel] = rhOID
    else:
        raise ValueError("Cannot handle link type %d" % (linkType))

    logger.debug("Successfully linked %s and %s via '%s'", lhOID, rhOID, rel)


def removeUnmetRelationships(dropSpecList):
    unmetRelationships = []

    normalise_oid = lambda oid: next(iter(oid)) if isinstance(oid, dict) else oid

    # Step #1: Get all OIDs
    oids = set()
    for dropSpec in dropSpecList:
        oid = normalise_oid(dropSpec["oid"])
        oids.add(oid)

    # Step #2: find unmet relationships and remove them from the original
    # DROP spec, keeping track of them
    for dropSpec in dropSpecList:
        this_oid = normalise_oid(dropSpec["oid"])
        to_delete = []

        for rel in dropSpec:
            # 1-N relationships
            if rel in __TOMANY:
                link = __TOMANY[rel]

                # Find missing OIDs in this relationship and keep track of them,
                # removing them from the current DROP spec
                ds = dropSpec[rel]
                # TODO: In principle all of the ds should be dicts, but they are not
                # in a loop. Need to check the generation
                # ds = [next(iter(d)) if isinstance(d, dict) else d for d in ds]
                if isinstance(ds[0], dict):
                    ds = [next(iter(d)) for d in ds]
                else:
                    logger.debug(">>> ds[0] not a dict: %s", ds[0])
                #                ds = [normalise_oid(d) for d in ds]
                missingOids = [oid for oid in ds if oid not in oids]
                for oid in missingOids:
                    unmetRelationships.append(DROPRel(oid, link, this_oid))
                    ds.remove(oid)

                # Remove the relationship list entirely if it has no elements
                if not ds:
                    to_delete.append(rel)

            # N-1 relationships
            elif rel in __TOONE:
                link = __TOONE[rel]

                # Check if OID is missing
                oid = normalise_oid(dropSpec[rel])
                if oid in oids:
                    continue

                # Keep track of missing relationship
                unmetRelationships.append(DROPRel(oid, link, this_oid))

                # Remove relationship from current DROP spec
                to_delete.append(rel)

        for rel in to_delete:
            del dropSpec[rel]

    return unmetRelationships


def check_dropspec(n, dropSpec):
    if "oid" not in dropSpec:
        raise InvalidGraphException(
            "Drop #%d is missing its 'oid' argument: %r" % (n, dropSpec)
        )
    if "categoryType" not in dropSpec:
        raise InvalidGraphException(
            "Drop %s is missing its 'categoryType' argument" % (dropSpec)
        )


def loadDropSpecs(dropSpecList):
    """
    Loads the DROP definitions from `dropSpectList`, checks that
    the DROPs are correctly specified, and return a dictionary containing
    all DROP specifications (i.e., a dictionary of dictionaries) keyed on
    the OID of each DROP. Unlike `readObjectGraph` and `readObjectGraphS`,
    this method doesn't actually create the DROPs themselves.

    Slices off graph-wise reproducibility data for later use
    """

    # Step #1: Check the DROP specs and collect them
    dropSpecs = {}
    reprodata = None
    if dropSpecList is None:
        raise InvalidGraphException("DropSpec is empty %r" % dropSpecList)
    if dropSpecList[-1].get("rmode") or not dropSpecList[-1]:
        reprodata = dropSpecList.pop()
    for n, dropSpec in enumerate(dropSpecList):
        # "categoryType" and 'oid' are mandatory
        check_dropspec(n, dropSpec)
        dropSpec.pop("input_parser", None)
        dropSpec.pop("output_parser", None)
        dropType = dropSpec["categoryType"].lower()

        cf = __CREATION_FUNCTIONS[dropType]
        cf(dropSpec, dryRun=True)
        dropSpecs[dropSpec["oid"]] = dropSpec

    # logger.debug("Found DROP definitions for oids: %s", dropSpecs)

    # Step #2: check relationships
    # TODO: shouldn't this loop be done the other way around, going through all __TOMANY
    # and __TOONE and directly address the respective dropSpec attribute?
    for dropSpec in dropSpecList:
        # 1-N relationships
        for rel in dropSpec:
            if rel in __TOMANY:
                # A KeyError will be raised if a oid has been specified in the
                # relationship list but doesn't exist in the list of DROPs
                for oid in dropSpec[rel]:
                    oid = list(oid.keys())[0] if isinstance(oid, dict) else oid
                    if oid not in dropSpecs:
                        logger.error("OID: %s not found!", oid)
                        continue

    # Done!
    return dropSpecs, reprodata


def createGraphFromDropSpecList(
    dropSpecList: List[dict], session: Optional["Session"] = None
) -> List[AbstractDROP]:
    logger.debug("Found %d DROP definitions", len(dropSpecList))

    # Step #1: create the actual DROPs
    drops = collections.OrderedDict()
    logger.info("Creating %d drops", len(dropSpecList))
    if dropSpecList[-1].get("rmode") or not dropSpecList[-1]:
        dropSpecList.pop()
    for n, dropSpec in enumerate(dropSpecList):
        check_dropspec(n, dropSpec)
        #        dropType = dropSpec.pop("categoryType")
        # backwards compatibility
        dropSpec.pop("input_parser", None)
        dropSpec.pop("output_parser", None)
        dropType = dropSpec["categoryType"]
        # if dropType.lower() in ["application", "app"]:
        #     dropType = "dropclass"
        # if dropType.lower() == "data":
        #     dropType = "dropclass"

        cf = __CREATION_FUNCTIONS[dropType.lower()]
        session_id = session.sessionId if session else ""
        drop = cf(dropSpec, session_id=session_id)
        if session is not None:
            # Now using per-drop reproducibility setting.
            drop.reproducibility_level = ReproducibilityFlags(
                int(dropSpec.get("reprodata", {}).get("rmode", "0"))
            )
            # session.reprodata['rmode']
        drops[drop.oid] = drop

    # Step #2: establish relationships
    logger.info("Establishing relationships between drops")
    for dropSpec in dropSpecList:
        # 'oid' is mandatory
        oid = dropSpec["oid"]
        drop = drops[oid]

        for attr in dropSpec:
            # 1-N relationships
            if attr in __TOMANY:
                link = __TOMANY[attr]
                for rel in dropSpec[attr]:
                    oid = list(rel.keys())[0] if isinstance(rel, dict) else rel
                    if oid in drops:
                        lhDrop = drops[oid]
                    else:
                        continue
                    relFuncName = LINKTYPE_1TON_APPEND_METHOD[link]
                    try:
                        relFunc = getattr(drop, relFuncName)
                    except AttributeError:
                        logger.error(
                            '%r cannot be linked to %r due to missing method "%s"',
                            drop,
                            lhDrop,
                            relFuncName,
                        )
                        raise
                    relFunc(lhDrop)

            # N-1 relationships
            elif attr in __TOONE:
                link = __TOONE[attr]
                rel = dropSpec[attr]
                rel = list(rel.keys())[0] if isinstance(rel, dict) else rel
                lhDrop = drops[rel]
                propName = LINKTYPE_NTO1_PROPERTY[link]
                setattr(drop, propName, lhDrop)

    # We're done! Return the roots of the graph to the caller
    logger.info("Calculating graph roots")
    roots: List[AbstractDROP] = [
        drop for drop in drops.values() if not droputils.getUpstreamObjects(drop)
    ]
    logger.info("%d graph roots found, bye-bye!", len(roots))
    return roots


def _createData(dropSpec, dryRun=False, session_id=None):
    oid, uid = _getIds(dropSpec)
    kwargs = _getKwargs(dropSpec)

    if dropSpec["categoryType"] == "Data":
        dataClassName = dropSpec["dropclass"]
        parts = dataClassName.split(".")
        module = importlib.import_module(".".join(parts[:-1]))
        storageType = getattr(module, parts[-1])
    else:
        # STORAGE_TYPES are deprecated, but here for backwards compatibility

        # Fall back to old behaviour or to FileDROP
        # if nothing else is specified
        STORAGE_TYPES = {
            "Memory": InMemoryDROP,
            "SharedMemory": SharedMemoryDROP,
            "File": FileDROP,
            "NGAS": NgasDROP,
            "null": NullDROP,
            "json": JsonDROP,
            "ParameterSet": ParameterSetDROP,
            "EnvironmentVariables": EnvironmentVarDROP,
        }

        try:
            from dlg.data.drops.s3_drop import S3DROP

            STORAGE_TYPES["S3"] = S3DROP
        except ImportError:
            pass
        if "storage" in dropSpec:
            storageType = STORAGE_TYPES[dropSpec["storage"]]
            # pass
        else:
            storageType = FileDROP
    if dryRun:
        return
    if "self" in kwargs:
        kwargs.pop("self")
    return storageType(oid, uid, dlg_session_id=session_id, **kwargs)


def _createContainer(dropSpec, dryRun=False, session_id=None):
    oid, uid = _getIds(dropSpec)
    kwargs = _getKwargs(dropSpec)

    # if no 'container' is specified, we default to ContainerDROP
    if "dropclass" in dropSpec:
        containerTypeName = dropSpec["dropclass"]
        parts = containerTypeName.split(".")

        # Support old "dfms..." package names (pre-Oct2017)
        if parts[0] == "dfms":
            parts[0] = "dlg"

        module = importlib.import_module(".".join(parts[:-1]))
        containerType = getattr(module, parts[-1])
    else:
        containerType = ContainerDROP

    if dryRun:
        return

    return containerType(oid, uid, dlg_session_id=session_id, **kwargs)


def _createSocket(dropSpec, dryRun=False, session_id=None):
    oid, uid = _getIds(dropSpec)
    kwargs = _getKwargs(dropSpec)

    if dryRun:
        return
    return SocketListenerApp(oid, uid, dlg_session_id=session_id, **kwargs)


def _createApp(dropSpec, dryRun=False, session_id=None):
    oid, uid = _getIds(dropSpec)
    kwargs = _getKwargs(dropSpec)
    appName = dropSpec.get("dropclass", "")
    if not appName:
        dropSpec.get("Application", "")
    parts = appName.split(".")

    # Support old "dfms..." package names (pre-Oct2017)
    if parts[0] == "dfms":
        parts[0] = "dlg"

    try:
        module = importlib.import_module(".".join(parts[:-1]))
        appType = getattr(module, parts[-1])
    except (ImportError, AttributeError, ValueError) as e:
        raise InvalidGraphException(
            "drop %s specifies non-existent application: %s" % (oid, appName)
        ) from e

    if dryRun:
        return
    return appType(oid, uid, dlg_session_id=session_id, **kwargs)


def _getIds(dropSpec):
    # uid is copied from oid if not explicitly given
    oid = dropSpec["oid"]
    uid = oid
    if "uid" in dropSpec:
        uid = dropSpec["uid"]
    return oid, uid


def _getKwargs(dropSpec):
    kwargs = dict(dropSpec)

    REMOVE = [
        "oid",
        "uid",
        "Application",
        "dropclass",
        "appclass",
        "dataclass",
        "data",
        "Data",
    ]
    for kw in REMOVE:
        if kw in kwargs:
            del kwargs[kw]

    RENAME = {
        "precious": "persist",
    }
    for find, replace in RENAME.items():
        if find in kwargs:
            kwargs[replace] = kwargs[find]
            del kwargs[find]

    for name, spec in dropSpec.get("applicationArgs", dict()).items():
        kwargs[name] = spec["value"]

    return kwargs


__CREATION_FUNCTIONS = {
    "socket": _createSocket,
    "data": _createData,
    "application": _createApp,
    "app": _createApp,
    "container": _createContainer,
}
