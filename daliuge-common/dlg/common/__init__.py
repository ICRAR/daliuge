#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2020
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
from enum import Enum
from dataclasses import dataclass, field, asdict
import logging

"""Common utilities used by daliuge packages"""
from .osutils import terminate_or_kill, wait_or_kill
from .network import check_port, connect_to, portIsClosed, portIsOpen, write_to
from .streams import ZlibCompressedStream, JSONStream

logger = logging.getLogger(__name__)


class CategoryType(str, Enum):
    DATA = "Data"
    APPLICATION = "Application"
    CONSTRUCT = "Construct"
    GROUP = "Group"
    UNKNOWN = "Unknown"
    SERVICE = "Service"
    CONTAINER = "Container"
    SOCKET = "Socket"
    CONTROL = "Control"
    OTHER = "Other"


def b2s(b, enc="utf8"):
    "Converts bytes into a string"
    return b.decode(enc)


class dropdict(dict):
    """
    An intermediate representation of a DROP that can be easily serialized
    into a transport format such as JSON or XML.

    This dictionary holds all the important information needed to call any given
    DROP constructor. The most essential pieces of information are the
    DROP's OID, and its type (which determines the class to instantiate).
    Depending on the type more fields will be required. This class doesn't
    enforce these requirements though, as it only acts as an information
    container.

    This class also offers a few utility methods to make it look more like an
    actual DROP class. This way, users can use the same set of methods
    both to create DROPs representations (i.e., instances of this class)
    and actual DROP instances.

    Users of this class are, for example, the graph_loader module which deals
    with JSON -> DROP representation transformations, and the different
    repositories where graph templates are expected to be found by the
    DROPManager.
    """

    def __init__(self, init_dict=None):
        if init_dict is None:
            init_dict = {
                "oid": None,
                "categoryType": "Unknown",
            }

        self.update(init_dict)
        if "oid" not in self:
            self.update({"oid": None})
        return super().__init_subclass__()

    def _addSomething(self, other, key, name=None):
        if key not in self:
            self[key] = []
        if other["oid"] not in self[key]:
            # TODO: Returning just the other drop OID instead of the named
            #       port list is not a good solution. Required for the dask
            #       tests.
            append = {other["oid"]: name} if name else other["oid"]
            # if name is None:
            # raise ValueError
            self[key].append(append)

    def addConsumer(self, other, name=None):
        self._addSomething(other, "consumers", name=name)

    def addStreamingConsumer(self, other, name=None):
        self._addSomething(other, "streamingConsumers", name=name)

    def addInput(self, other, name=None):
        self._addSomething(other, "inputs", name=name)

    def addStreamingInput(self, other, name=None):
        self._addSomething(other, "streamingInputs", name=name)

    def addOutput(self, other, name=None):
        self._addSomething(other, "outputs", name=name)

    def addProducer(self, other, name=None):
        self._addSomething(other, "producers", name=name)


def _sanitize_links(links):
    """
    Links can now be dictionaries, but we only need
    the key.
    """
    if isinstance(links, list):
        nlinks = []
        for l in links:
            if isinstance(l, dict):  # could be a list of dicts
                nlinks.extend(list(l.keys()))
            else:
                nlinks.extend(l) if isinstance(l, list) else nlinks.append(l)
        return nlinks
    elif isinstance(links, dict):
        return list(links.keys()) if isinstance(links, dict) else links


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
        # Assumed to be reprodata / other non-drop elements
        #
        # TODO (rtobar): Note that this should be a temporary measure.
        # In principle the pg_spec given here should be a graph, which (until
        # recently) consisted on drop specifications only. The fact that repro
        # data is now appended at the end of some graphs highlights the need for
        # a more formal specification of graphs and other pieces of data that we
        # move through the system.
        if "oid" not in dropspec:
            continue

        oid = dropspec["oid"]
        all_oids.add(oid)
        ctype = (
            dropspec["categoryType"]
            if "categoryType" in dropspec
            else dropspec["type"]
        )
        if ctype in (
            CategoryType.APPLICATION,
            CategoryType.SOCKET,
            "app",
        ):
            if dropspec.get("inputs", None) or dropspec.get(
                "streamingInputs", None
            ):
                nonroots.add(oid)
            if dropspec.get("outputs", None):
                do = _sanitize_links(dropspec["outputs"])
                nonroots |= set(do)
        elif ctype == CategoryType.DATA:
            if dropspec.get("producers", None):
                nonroots.add(oid)
            if dropspec.get("consumers", None):
                dc = _sanitize_links(dropspec["consumers"])
                nonroots |= set(dc)
            if dropspec.get("streamingConsumers", None):
                dsc = _sanitize_links(dropspec["streamingConsumers"])
                nonroots |= set(dsc)

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
        oid = dropspec["oid"]
        all_oids.add(oid)
        ctype = (
            dropspec["categoryType"]
            if "categoryType" in dropspec
            else dropspec["type"]
        )

        if ctype in [CategoryType.APPLICATION, "app"]:
            if dropspec.get("outputs", None):
                nonleaves.add(oid)
            if dropspec.get("streamingInputs", None):
                dsi = _sanitize_links(dropspec["streamingInputs"])
                nonleaves |= set(dsi)
            if dropspec.get("inputs", None):
                di = _sanitize_links(dropspec["inputs"])
                nonleaves |= set(di)
        if ctype in [CategoryType.SERVICE, "socket"]:
            nonleaves.add(oid)  # services are never leaves
            if dropspec.get("streamingInputs", None):
                dsi = _sanitize_links(dropspec["streamingInputs"])
                nonleaves |= set(dsi)
            if dropspec.get("inputs", None):
                di = _sanitize_links(dropspec["inputs"])
                nonleaves |= set(di)
        elif ctype in [CategoryType.DATA, "data"]:
            if dropspec.get("producers", None):
                dp = _sanitize_links(dropspec["producers"])
                nonleaves |= set(dp)
            if dropspec.get("consumers", None) or dropspec.get(
                "streamingConsumers", None
            ):
                nonleaves.add(oid)

    return all_oids - nonleaves
