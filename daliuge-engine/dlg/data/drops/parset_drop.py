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
import base64
import io
import logging
import os

from benedict import benedict

from dlg.data.drops.data_base import DataDROP
from dlg.drop import DEFAULT_INTERNAL_PARAMETERS
from dlg.data.io import MemoryIO
from dlg.meta import dlg_string_param

logger = logging.getLogger(f"dlg.{__name__}")


##
# @brief ParameterSet
# @details A set of parameters, which can be set and modified in EAGLE and thus is part of the graph. Multiple serialisation formats are available.
# @par EAGLE_START
# @param category ParameterSet
# @param tag daliuge
# @param data_volume 5/Float/ConstraintParameter/NoPort/ReadWrite//False/False/Estimated size of the data contained in this node
# @param group_end False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Is this node the end of a group?
# @param mode "YANDA"/Select/ComponentParameter/NoPort/ReadWrite/YANDA,ini,yaml,json,toml,pickle/False/False/Serialisation method.
# @param config_data ""/String/ComponentParameter/NoPort/ReadWrite//False/False/Additional configuration information to be mixed in with the initial data
# @param dropclass dlg.data.drops.parset_drop.ParameterSetDROP/String/ComponentParameter/NoPort/ReadWrite//False/False/Drop class
# @param base_name parset_drop/String/ComponentParameter/NoPort/ReadOnly//False/False/Base name of application class
# @param streaming False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Specifies whether this data component streams input and output data
# @param persist False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Specifies whether this data component contains data that should not be deleted after execution
# @param block_skip False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/If set the drop will block a skipping chain until the last producer has finished and is not also skipped.
# @param Config /Object.File/ApplicationArgument/OutputPort/ReadWrite//False/False/The output configuration file
# @par EAGLE_END
class ParameterSetDROP(DataDROP):
    """
    A generic configuration drop template wrapper
    This drop opens an (optional) file containing some initial configuration information, then
    appends any additional specified parameters to it, finally serving it as a data object.
    """

    config_data = b""

    mode = dlg_string_param("mode", None)

    def serialize_parameters(self, parameters: dict, mode):
        """
        Returns a string representing a serialization of the parameters.
        """
        if mode not in ["ini", "yaml", "json", "toml", "YANDA", "pickle"]:
            mode = "yaml"
        if mode == "YANDA":
            # TODO: Add more complex value checking
            serialp = "\n".join(f"{x}={y.value}" for x, y in parameters.items())
            serialp = serialp.encode("utf-8")
        # Add more formats (.ini for example)
        elif mode == "ini":
            serialp = parameters.to_ini().encode("utf-8")
        elif mode == "yaml":
            serialp = parameters.to_yaml().encode("utf-8")
        elif mode == "json":
            serialp = parameters.to_json().encode("utf-8")
        elif mode == "toml":
            serialp = parameters.to_toml().encode("utf-8")
        elif mode == "pickle":
            serialp = base64.b64decode(parameters.to_pickle())
        return serialp

    def filter_parameters(self, parameters: dict, mode):
        """
        Returns a dictionary of parameters, with daliuge-internal or other parameters filtered out
        """
        if mode == "YANDA_DEPRECATED":  # we now return all applicationArgs
            forbidden_params = list(DEFAULT_INTERNAL_PARAMETERS)
            if parameters["config_data"] == b"":
                forbidden_params.append("configData")
            return {
                key: val
                for key, val in parameters.items()
                if key not in DEFAULT_INTERNAL_PARAMETERS
            }
        # we use benedict to get access to the serializers.
        return benedict(
            {
                k: benedict(
                    {
                        "value": v.value,
                        "description": v.description,
                        "type": v.type,
                        "default": v.defaultValue,
                    },
                    keypath_separator=None,
                )
                for k, v in benedict(
                    parameters["applicationArgs"], keypath_separator=None
                ).items()
            },
            keypath_separator=None,  # support "." in keyword name
        )

    def initialize(self, **kwargs):
        """
        TODO: Open input file
        """
        logger.debug(
            "Application arguments found: %s", self.parameters["applicationArgs"]
        )
        self.config_data = self.serialize_parameters(
            self.filter_parameters(self.parameters, self.mode), self.mode
        )

    def getIO(self):
        return MemoryIO(io.BytesIO(self.config_data))

    @property
    def dataURL(self) -> str:
        hostname = os.uname()[1]
        return f"config://{hostname}/{os.getpid()}/{id(self.config_data)}"
