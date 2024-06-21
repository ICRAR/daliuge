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
This package contains several general-purpose data stores in form of
DROPs that we have developed as examples and for real-life use. Most of them
are based on the :class:`DataDROP`.
"""

__all__ = [
    "DirectoryContainer",
    "FileDROP",
    "InMemoryDROP",
    "SharedMemoryDROP",
    "NgasDROP",
    "RDBMSDrop",
    "ParameterSetDROP",
    "EnvironmentVarDROP",
    "S3DROP",
    "DataDROP",
    "NullDROP",
]

from dlg.data.drops.directorycontainer import DirectoryContainer
from dlg.data.drops.file import FileDROP
from dlg.data.drops.memory import InMemoryDROP, SharedMemoryDROP
from dlg.data.drops.ngas import NgasDROP
from dlg.data.drops.rdbms import RDBMSDrop
from dlg.data.drops.parset_drop import ParameterSetDROP
from dlg.data.drops.environmentvar_drop import EnvironmentVarDROP
from dlg.data.drops.s3_drop import S3DROP
from dlg.data.drops.data_base import DataDROP
from dlg.data.drops.data_base import NullDROP
