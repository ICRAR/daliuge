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
import base64
import binascii
import builtins
from json import JSONDecodeError
from pickle import PickleError

import dill
import io
import json
import os
import random
import string
import sys
from typing import Union

from dlg.common.reproducibility.reproducibility import common_hash
from dlg.data.drops.data_base import DataDROP, logger
from dlg.data.io import SharedMemoryIO, MemoryIO
from dlg.drop import track_current_drop


def get_builtins()-> dict:
    """
    Get a tuple of buitlin types to compare pydata with.
    """
    builtin_types = tuple(getattr(builtins, t) for t in dir(builtins) if isinstance(getattr(builtins, t), type))
    builtin_types = builtin_types[builtin_types.index(bool):]
    builtin_names = [b.__name__ for b in builtin_types]
    return dict(zip(builtin_names, builtin_types))


def parse_pydata(pd: Union[bytes, dict]) -> bytes:
    """
    Parse and evaluate the pydata argument to populate memory during initialization

    :param pd: either the pydata dictionary from the graph node or the value directly

    This function attempts to identify the type of data so it can appropriately be stored
    in the right IO buffer (currently supporting BytesIO and StringIO).

    If there is nothing in pydata (e.g. the empty string, ''), then technically the
    type of the input data is String. However, if this is because the memory drop is
    expecting data from an InputPort, then this is not a good assumption.

    :returns a byte encoded value
    """
    pd_dict = pd if isinstance(pd, dict) else {"value":pd, "type":"raw"}
    pydata = pd_dict["value"]
    logger.debug("pydata value provided: '%s' with type '%s'", pydata, type(pydata))
    empty_strings = ["None", ""]
    if pd_dict["type"].lower() in ["string", "str"]:
        pass
    if pydata in empty_strings:
        pydata = bytes() # Treat None/Empty objects as empty object data.
        pd_dict["type"] = 'object'
    builtin_types = get_builtins()
    if pd_dict["type"] != "raw" and type(pydata) in builtin_types.values() and pd_dict["type"] not in builtin_types.keys():
        logger.warning("Type of pydata %s provided differs from specified type: %s", type(pydata).__name__, pd_dict["type"])
        pd_dict["type"] = type(pydata).__name__
    if pd_dict["type"].lower() == "json":
        try:
            pydata = json.loads(pydata)
        except JSONDecodeError:
            pydata = pydata.encode()
    if pd_dict["type"].lower() == "eval":
        # try:
        pydata = eval(pydata) # pylint: disable=eval-used
        # except Exception:
        #     pydata = pydata.encode()
    elif pd_dict["type"].lower() == "int" or isinstance(pydata, int):
        try:
            pydata = int(pydata)
            pd_dict["type"] = "int"
        except JSONDecodeError:
            pydata = pydata.encode()
    elif pd_dict["type"].lower() == "float" or isinstance(pydata, float):
        try:
            pydata = float(pydata)
            pd_dict["type"] = "float"
        except JSONDecodeError:
            pydata = pydata.encode()
    elif pd_dict["type"].lower() == "boolean" or isinstance(pydata, bool):
        try:
            pydata = bool(pydata)
            pd_dict["type"] = "bool"
        except JSONDecodeError:
            pydata = pydata.encode()
    elif pd_dict["type"].lower() == "object":
        if pydata:
            pydata = base64.b64decode(pydata.encode())
            try:
                pydata = dill.loads(pydata)
            except dill.PickleError as e:
                logger.error("Issue loading pydata object with pickle %s", pydata)
                raise dill.PickleError from e
    elif pd_dict["type"].lower() == "raw":
        pydata = dill.loads(base64.b64decode(pydata))
        logger.debug("Returning pydata of type: %s", type(pydata))
        # return pydata
    return dill.dumps(pydata)


##
# @brief Memory
# @details In-memory storage of intermediate data products
# @par EAGLE_START
# @param category Memory
# @param tag daliuge
# @param pydata None/Object/ApplicationArgument/NoPort/ReadWrite//False/False/Data to be loaded into memory
# @param io /Object/ApplicationArgument/InputOutput/ReadWrite//False/False/Input Output port
# @param block_skip False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/If set the drop will block a skipping chain until the last producer has finished and is not also skipped.
# @param persist False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Specifies whether this data component contains data that should not be deleted after execution
# @param data_volume 5/Float/ConstraintParameter/NoPort/ReadWrite//False/False/Estimated size of the data contained in this node
# @param group_end False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Is this node the end of a group?
# @param streaming False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Specifies whether this data component streams input and output data
# @param dropclass dlg.data.drops.memory.InMemoryDROP/String/ComponentParameter/NoPort/ReadOnly//False/False/Drop class
# @param base_name memory/String/ComponentParameter/NoPort/ReadOnly//False/False/Base name of application class
# @par EAGLE_END
class InMemoryDROP(DataDROP):
    """
    A DROP that points data stored in memory.
    """

    # Allow in-memory drops to be automatically removed by default

    @track_current_drop
    def __init__(self, *args, **kwargs):
        if "persist" not in kwargs:
            kwargs["persist"] = False
        if "expireAfterUse" not in kwargs:
            kwargs["expireAfterUse"] = True
        if kwargs["persist"]:
            kwargs["expireAfterUse"] = False
        super().__init__(*args, **kwargs)

    def initialize(self, **kwargs):
        """
        If there is a pydata argument use that to populate the DROP

        Note: It is essential to have the type of the initial PyData correct, or it could
        cause issues with the type of IO system that is used.

        If parse_pydata() determines that the data passed via 'pydata' is a String, it
        will use StringIO as the buffer. This can have ramifications for none-string
        encodings used further down the track.
        """
        args = []
        pydata = None
        # pdict = {}
        pdict = {"type": "raw"}  # initialize this value to enforce BytesIO
        self.data_type = pdict["type"]
        field_names = (
            [f["name"] for f in kwargs["fields"]] if "fields" in kwargs else []
        )
        if "pydata" in kwargs and not (
            "fields" in kwargs and "pydata" in field_names
        ):  # means that is was passed directly (e.g. from tests)
            pydata = kwargs.pop("pydata")
            pdict["value"] = pydata
            pydata = parse_pydata(pdict)
        elif "fields" in kwargs and "pydata" in field_names:
            data_pos = field_names.index("pydata")
            pdict = kwargs["fields"][data_pos]
            pydata = parse_pydata(pdict)
        if pdict and pdict["type"].lower() in ["str","string"]:
            self.data_type =  "String" if pydata else "raw"
        else:
            self.data_type = pdict["type"] if pdict else ""
        if pydata:
            args.append(pydata)
            logger.debug("Loaded into memory: %s, %s, %s", pydata, self.data_type, type(pydata))
        self._buf = io.BytesIO(*args) if self.data_type != "String" else io.StringIO(
            dill.loads(*args)) # pylint: disable = no-value-for-parameter
        self.size = len(pydata) if pydata else 0

    def getIO(self):
        if (
            hasattr(self, "_tp")
            and hasattr(self, "_sessionId")
            and sys.version_info >= (3, 8)
        ):
            return SharedMemoryIO(self.oid, self._sessionId)
        else:
            return MemoryIO(self._buf)

    @property
    def dataURL(self) -> str:
        hostname = os.uname()[1]
        return "mem://%s/%d/%d" % (hostname, os.getpid(), id(self._buf))

    # Override
    def generate_reproduce_data(self):
        from dlg.droputils import allDropContents

        data = b""
        try:
            data = allDropContents(self, self.size)
        except IOError:
            logger.debug("Could not read drop reproduce data")
        return {"data_hash": common_hash(data)}


##
# @brief PythonObject
# @details A Python object stored in memory
# @par EAGLE_START
# @param category PythonObject
# @param tag daliuge
# @param self /Object/ComponentParameter/InputOutput/ReadWrite//False/False/Reference to object
# @param persist False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Object should be serialized
# @param data_volume 5/Float/ConstraintParameter/NoPort/ReadWrite//False/False/Estimated size of the data contained in the object
# @param dropclass dlg.data.drops.memory.InMemoryDROP/String/ComponentParameter/NoPort/ReadOnly//False/False/Drop class
# @param base_name memory/String/ComponentParameter/NoPort/ReadOnly//False/False/Base name of class
# @param streaming False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Specifies whether this data component streams input and output data
# @par EAGLE_END
class PythonObjectDROP(InMemoryDROP):
    """
    A python object is an InMemoryDROP but has a special category of PythonObject.

    This is only here to force the creation of the component for the palette.
    """


##
# @brief SharedMemory
# @details Data stored in shared memory
# @par EAGLE_START
# @param category SharedMemory
# @param tag daliuge
# @param pydata None/String/ApplicationArgument/NoPort/ReadWrite//False/False/Data to be loaded into memory
# @param io /Object/ApplicationArgument/InputOutput/ReadWrite//False/False/Input Output port
# @param persist False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Specifies whether this data component contains data that should not be deleted after execution
# @param block_skip False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/If set the drop will block a skipping chain until the last producer has finished and is not also skipped.
# @param data_volume 5/Float/ConstraintParameter/NoPort/ReadWrite//False/False/Estimated size of the data contained in this node
# @param group_end False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Is this node the end of a group?
# @param streaming False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Specifies whether this data component streams input and output data
# @param dropclass dlg.data.drops.memory.SharedMemoryDROP/String/ComponentParameter/NoPort/ReadOnly//False/False/Drop class
# @param base_name memory/String/ComponentParameter/NoPort/ReadOnly//False/False/Base name of application class
# @par EAGLE_END
class SharedMemoryDROP(DataDROP):
    """
    A DROP that points to data stored in shared memory.
    This drop is functionality equivalent to an InMemory drop running in a concurrent environment.
    In this case however, the requirement for shared memory is explicit.

    @WARNING Currently implemented as writing to shmem and there is no backup behaviour.
    """

    def initialize(self, **kwargs):
        args = []
        pydata = None
        if "pydata" in kwargs and not (
            "nodeAttributes" in kwargs and "pydata" in kwargs["nodeAttributes"]
        ):  # means that is was passed directly
            pydata = kwargs.pop("pydata")
            logger.debug("pydata value provided: %s", pydata)
            try:  # test whether given value is valid
                _ = dill.loads(base64.b64decode(pydata.encode("latin1")))
                pydata = base64.b64decode(pydata.encode("latin1"))
            except PickleError:
                pydata = None
                logger.warning("Unable to load using pickle, default to None")
            except binascii.Error:
                pydata = None
                logger.warning("Unable to load using base64, defaulting to None")
        elif "nodeAttributes" in kwargs and "pydata" in kwargs["nodeAttributes"]:
            pydata = parse_pydata(kwargs["nodeAttributes"]["pydata"])
        args.append(pydata)
        self._buf = io.BytesIO(*args)

    def getIO(self):
        if sys.version_info >= (3, 8):
            if hasattr(self, "_sessionId"):
                return SharedMemoryIO(self.oid, self._sessionId)
            else:
                # Using Drop without manager, just generate a random name.
                sess_id = "".join(
                    random.choices(string.ascii_uppercase + string.digits, k=10)
                )
                return SharedMemoryIO(self.oid, sess_id)
        else:
            raise NotImplementedError(
                "Shared memory is only available with Python >= 3.8"
            )

    @property
    def dataURL(self) -> str:
        hostname = os.uname()[1]
        return f"shmem://{hostname}/{os.getpid()}/{id(self._buf)}"
