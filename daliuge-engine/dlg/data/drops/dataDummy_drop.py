#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia
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
import errno
import os
import re

from dlg.common.reproducibility.reproducibility import common_hash
from dlg.ddap_protocol import DROPStates
from dlg.drop import DataDROP, PathBasedDrop, logger, track_current_drop
from dlg.exceptions import InvalidDropException
from dlg.data.io import FileIO
from dlg.meta import dlg_bool_param
from dlg.utils import isabs


##
# @brief DataDummyDROP
# @details A placeholder Data Component to aid construction of new data components.
# This is mainly useful (and used) when starting a new workflow from scratch.
# @par EAGLE_START
# @param category File
# @param tag template
# @param dataclass Application Class//String/ComponentParameter/readonly//False/False/Data class
# @param data_volume Data volume/5/Float/ComponentParameter/readwrite//False/False/Estimated size of the data contained in this node
# @param group_end Group end/False/Boolean/ComponentParameter/readwrite//False/False/Is this node the end of a group?
# @param streaming Streaming/False/Boolean/ComponentParameter/readwrite//False/False/Specifies whether this data component streams input and output data 
# @param persist Persist/False/Boolean/ComponentParameter/readwrite//False/False/Specifies whether this data component contains data that should not be deleted after execution
# @param dummy dummy//Object/InputPort/readwrite//False/False/Dummy input port
# @param dummy dummy//Object/OutputPort/readwrite//False/False/Dummy output port
# @par EAGLE_END
class DataDummyDROP(DataDROP, PathBasedDrop):
    """
    A Data Component placeholder that aids the construction of palettes from 
    scratch
    """
    pass