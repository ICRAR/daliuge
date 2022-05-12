#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2014
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
import io
import os
from abc import abstractmethod

from dlg.drop import DataDROP, DEFAULT_INTERNAL_PARAMETERS
from dlg.io import MemoryIO
from dlg.meta import dlg_string_param


##
# @brief ParameterSet
# @details A set of parameters, wholly specified in EAGLE
# @par EAGLE_START
# @param category ParameterSet
# @param tag template
# @param[in] cparam/data_volume Data volume/5/Float/readwrite/False//False/Estimated size of the data contained in this node
# @param[in] cparam/group_end Group end/False/Boolean/readwrite/False//False/Is this node the end of a group?
# @param[in] cparam/mode Parset mode/"YANDA"/String/readonly/False//False/To what standard DALiuGE should filter and serialize the parameters.
# @param[in] cparam/config_data ConfigData/""/String/readwrite/False//False/Additional configuration information to be mixed in with the initial data
# @param[out] port/Config ConfigFile/File/The output configuration file
# @par EAGLE_END
class ParameterSetDROP(DataDROP):
    """
    A generic configuration file template wrapper
    This drop opens an (optional) file containing some initial configuration information, then
    appends any additional specified parameters to it, finally serving it as a data object.
    """

    config_data = b""

    mode = dlg_string_param("mode", None)

    @abstractmethod
    def serialize_parameters(self, parameters: dict, mode):
        """
        Returns a string representing a serialization of the parameters.
        """
        if mode == "YANDA":
            # TODO: Add more complex value checking
            return "\n".join(f"{x}={y}" for x, y in parameters.items())
        # Add more formats (.ini for example)
        return "\n".join(f"{x}={y}" for x, y in parameters.items())

    @abstractmethod
    def filter_parameters(self, parameters: dict, mode):
        """
        Returns a dictionary of parameters, with daliuge-internal or other parameters filtered out
        """
        if mode == "YANDA":
            forbidden_params = list(DEFAULT_INTERNAL_PARAMETERS)
            if parameters["config_data"] == "":
                forbidden_params.append("configData")
            return {
                key: val
                for key, val in parameters.items()
                if key not in DEFAULT_INTERNAL_PARAMETERS
            }
        return parameters

    def initialize(self, **kwargs):
        """
        TODO: Open input file
        """
        self.config_data = self.serialize_parameters(
            self.filter_parameters(self.parameters, self.mode), self.mode
        ).encode("utf-8")

    def getIO(self):
        return MemoryIO(io.BytesIO(self.config_data))

    @property
    def dataURL(self) -> str:
        hostname = os.uname()[1]
        return f"config://{hostname}/{os.getpid()}/{id(self.config_data)}"
