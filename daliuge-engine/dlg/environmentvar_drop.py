import abc
import io
import os
import json

from dlg.drop import AbstractDROP, DEFAULT_INTERNAL_PARAMETERS
from dlg.io import MemoryIO


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


def _filter_parameters(parameters: dict):
    return {key: val for key, val in parameters.items() if
            key not in DEFAULT_INTERNAL_PARAMETERS}


##
# @brief Environment variables
# @details A set of environment variables, wholly specified in EAGLE and accessible to all drops.
# @par EAGLE_START
# @param category EnvironmentVars
# @par EAGLE_END
class EnvironmentVarDROP(AbstractDROP, KeyValueDROP):
    """
    Drop storing static variables for access by all drops.
    Functions effectively like a globally-available Python dictionary
    """

    variables = {}

    def initialize(self, **kwargs):
        """
        Runs through all parameters, putting each into this drop's variable dict
        """
        self.variables.update(_filter_parameters(self.parameters))

    def getIO(self):
        return MemoryIO(io.BytesIO(json.dumps(self.variables).encode('utf-8')))

    def get(self, key):
        return self.variables.get(key)

    def get_multiple(self, keys: list):
        return_vars = []
        for key in keys:
            return_vars.append(self.variables.get(key))
        return return_vars

    @property
    def dataURL(self):
        hostname = os.uname()[1]
        return f"config://{hostname}/{os.getpid()}/{id(self.variables)}"
