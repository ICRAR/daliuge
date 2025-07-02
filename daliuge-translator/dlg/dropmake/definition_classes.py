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
Helper classes to aid the translation
"""
from dataclasses import dataclass


class Categories:
    """
    TODO: The explicit treatment of many of these Categories (Components)
    needs to disappear from the translator. It should be sufficient to do
    most of this treatment by using the CategoryTypes instead.
    """

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
    ENVIRONMENTVARS = "EnvironmentVariables"

    MKN = "MKN"
    SCATTER = "Scatter"
    GATHER = "Gather"
    GROUP_BY = "GroupBy"
    LOOP = "Loop"
    VARIABLES = "Variables"
    SUBGRAPH = "SubGraph"

    BRANCH = "Branch"
    DATA = "Data"
    COMPONENT = "Component"
    PYTHON_APP = "PythonApp"
    DALIUGE_APP = "DALiuGEApp"
    BASH_SHELL_APP = "BashShellApp"
    MPI = "Mpi"
    DYNLIB_APP = "DynlibApp"
    DOCKER = "Docker"
    DYNLIB_PROC_APP = "DynlibProcApp"
    SERVICE = "Service"

    COMMENT = "Comment"
    DESCRIPTION = "Description"


DATA_TYPES = [
    Categories.FILE,
    Categories.MEMORY,
    Categories.S3,
    Categories.NGAS,
    Categories.JSON,
    Categories.SHMEM,
    Categories.DATA,
    Categories.NULL,
    Categories.PLASMA,
    Categories.PLASMAFLIGHT,
    Categories.PARSET,
    Categories.ENVIRONMENTVARS,
    Categories.START,
    Categories.END,
]

APP_TYPES = [
    Categories.BRANCH,
    Categories.DATA,
    Categories.COMPONENT,
    Categories.PYTHON_APP,
    Categories.DALIUGE_APP,
    Categories.BASH_SHELL_APP,
    Categories.MPI,
    Categories.DYNLIB_APP,
    Categories.DYNLIB_PROC_APP,
    Categories.DOCKER,
    Categories.SERVICE,
]

CONSTRUCT_TYPES = [
    Categories.SCATTER,
    Categories.GATHER,
    Categories.GROUP_BY,
    Categories.LOOP,
    Categories.MKN,
    Categories.SERVICE,
    Categories.SUBGRAPH
]


@dataclass
class ConstructTypes:
    SCATTER = Categories.SCATTER
    GATHER = Categories.GATHER
    GROUP_BY = Categories.GROUP_BY
    LOOP = Categories.LOOP
    MKN = Categories.MKN
    SERVICE = Categories.SERVICE
    SUBGRAPH = Categories.SUBGRAPH
