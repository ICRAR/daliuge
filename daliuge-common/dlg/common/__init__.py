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
"""Common utilities used by daliuge packages"""
from .osutils import terminate_or_kill, wait_or_kill
from .network import check_port, connect_to, portIsClosed, portIsOpen, write_to
from .streams import ZlibCompressedStream, JSONStream


class Categories:
    START = "Start"
    END = "End"

    MEMORY = "Memory"
    SHMEM = "SharedMemory"
    FILE = "File"
    NGAS = "NGAS"
    NULL = "null"
    JSON = "json"
    S3 = "S3"
    PLASMA = "Plasma"
    PLASMAFLIGHT = "PlasmaFlight"
    PARSET = "ParameterSet"
    ENVIRONMENTVARS = "EnvironmentVars"

    MKN = "MKN"
    SCATTER = "Scatter"
    GATHER = "Gather"
    GROUP_BY = "GroupBy"
    LOOP = "Loop"
    VARIABLES = "Variables"

    BRANCH = "Branch"
    DATA = "Data"
    COMPONENT = "Component"
    PYTHON_APP = "PythonApp"
    BASH_SHELL_APP = "BashShellApp"
    MPI = "Mpi"
    DYNLIB_APP = "DynlibApp"
    DOCKER = "Docker"
    DYNLIB_PROC_APP = "DynlibProcApp"
    SERVICE = "Service"

    COMMENT = "Comment"
    DESCRIPTION = "Description"


STORAGE_TYPES = {
    Categories.MEMORY,
    Categories.SHMEM,
    Categories.FILE,
    Categories.NGAS,
    Categories.NULL,
    Categories.END,
    Categories.JSON,
    Categories.PLASMA,
    Categories.PLASMAFLIGHT,
    Categories.PARSET,
    Categories.ENVIRONMENTVARS
}
APP_DROP_TYPES = [
    Categories.COMPONENT,
    Categories.PYTHON_APP,
    Categories.BRANCH,
    Categories.BASH_SHELL_APP,
    Categories.MPI,
    Categories.DYNLIB_APP,
    Categories.DOCKER,
    Categories.DYNLIB_PROC_APP,
    Categories.SERVICE,
]


class DropType:
    PLAIN = "plain"
    SOCKET = "socket"
    APP = "app"  # Application drop that terminates onces executed
    SERVICE_APP = "serviceapp"  # App drop that runs continously
    CONTAINER = "container"  # Drop that contains other drops


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

    def _addSomething(self, other, key):
        if key not in self:
            self[key] = []
        if other["oid"] not in self[key]:
            self[key].append(other["oid"])

    def addConsumer(self, other):
        self._addSomething(other, "consumers")

    def addStreamingConsumer(self, other):
        self._addSomething(other, "streamingConsumers")

    def addInput(self, other):
        self._addSomething(other, "inputs")

    def addStreamingInput(self, other):
        self._addSomething(other, "streamingInputs")

    def addOutput(self, other):
        self._addSomething(other, "outputs")

    def addProducer(self, other):
        self._addSomething(other, "producers")


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

        oid = dropspec["oid"]
        all_oids.add(oid)

        if dropspec["type"] in (DropType.APP, DropType.SOCKET):
            if dropspec.get("inputs", None) or dropspec.get("streamingInputs", None):
                nonroots.add(oid)
            if dropspec.get("outputs", None):
                nonroots |= set(dropspec["outputs"])
        elif dropspec["type"] == DropType.PLAIN:
            if dropspec.get("producers", None):
                nonroots.add(oid)
            if dropspec.get("consumers", None):
                nonroots |= set(dropspec["consumers"])
            if dropspec.get("streamingConsumers", None):
                nonroots |= set(dropspec["streamingConsumers"])

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

        if dropspec["type"] == DropType.APP:
            if dropspec.get("outputs", None):
                nonleaves.add(oid)
            if dropspec.get("streamingInputs", None):
                nonleaves |= set(dropspec["streamingInputs"])
            if dropspec.get("inputs", None):
                nonleaves |= set(dropspec["inputs"])
        if dropspec["type"] == DropType.SERVICE_APP:
            nonleaves.add(oid)  # services are never leaves
            if dropspec.get("streamingInputs", None):
                nonleaves |= set(dropspec["streamingInputs"])
            if dropspec.get("inputs", None):
                nonleaves |= set(dropspec["inputs"])
        elif dropspec["type"] == DropType.PLAIN:
            if dropspec.get("producers", None):
                nonleaves |= set(dropspec["producers"])
            if dropspec.get("consumers", None) or dropspec.get(
                "streamingConsumers", None
            ):
                nonleaves.add(oid)

    return all_oids - nonleaves
