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
import abc
import io
import os
import json

from dlg.drop import AbstractDROP, DEFAULT_INTERNAL_PARAMETERS
from dlg.data.io import MemoryIO


class KeyValueDROP:
    @abc.abstractmethod
    def get(self, key):
        """
        Returns the value stored by this drop for the given key. Returns None if not present
        """

    @abc.abstractmethod
    def get_multiple(self, keys: list):
        """
        Returns a list of values stored by this drop. Maintains order returning None for any keys
        not present.
        """

    # TODO: Implement Set(key, value) operations
    @abc.abstractmethod
    def set(self, key, value):
        """
        Should update a value in the key-val store or add a new values if not present.
        """
        # Will be difficult to handle shared memory considerations.
        # For such a job, using a REDIS or other distributed key-val store may be more appropriate.


def _filter_parameters(parameters: dict):
    return {
        key: val
        for key, val in parameters.items()
        if key not in DEFAULT_INTERNAL_PARAMETERS
    }


##
# @brief EnvironmentVariables
# @details A set of environment variables, wholly specified in EAGLE and accessible to all drops.
# @par EAGLE_START
# @param category EnvironmentVariables
# @param tag daliuge
# @param data_volume 5/Float/ConstraintParameter/NoPort/ReadWrite//False/False/Estimated size of the data contained in this node
# @param dropclass dlg.data.drops.environmentvar_drop.EnvironmentVarDROP/String/ComponentParameter/NoPort/ReadWrite//False/False/Drop class
# @param base_name environmentvar_drop/String/ComponentParameter/NoPort/ReadOnly//False/False/Base name of application class
# @param streaming False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Specifies whether this data component streams input and output data
# @param persist False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Specifies whether this data component contains data that should not be deleted after execution
# @param output /Object/ApplicationArgument/OutputPort/ReadWrite//False/False/Output port
# @par EAGLE_END
class EnvironmentVarDROP(AbstractDROP, KeyValueDROP):
    """
    Drop storing static variables for access by all drops.
    Functions effectively like a globally-available Python dictionary
    """

    def initialize(self, **kwargs):
        """
        Runs through all parameters, putting each into this drop's variable dict
        """
        super(EnvironmentVarDROP, self).initialize(**kwargs)
        self._variables = dict()
        self._variables.update(_filter_parameters(self.parameters))

    def getIO(self):
        return MemoryIO(io.BytesIO(json.dumps(self._variables).encode("utf-8")))

    def get(self, key):
        """
        Fetches key from internal store if present.
        If not present, attempts to fetch variable from environment
        """
        value = self._variables.get(key)
        if value is None:
            value = os.environ.get(key)
        return value

    def get_multiple(self, keys: list):
        return_vars = []
        for key in keys:
            return_vars.append(self._variables.get(key))
        return return_vars

    def set(self, key, value):
        raise NotImplementedError(
            "Setting EnvironmentVariables mid-execution is not currently implemented"
        )

    @property
    def dataURL(self) -> str:
        hostname = os.uname()[1]
        return f"config://{hostname}/{os.getpid()}/{id(self._variables)}"
