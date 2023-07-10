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
import io
import json
import os
import pickle
import random
import string
import sys

from dlg.common.reproducibility.reproducibility import common_hash
from dlg.data.drops.data_base import DataDROP, logger
from dlg.data.io import SharedMemoryIO, MemoryIO


##
# @brief Memory
# @details In-memory storage of intermediate data products
# @par EAGLE_START
# @param category Memory
# @param tag daliuge
# @param dropclass dlg.data.drops.memory.InMemoryDROP/String/ComponentParameter/NoPort/ReadWrite//False/False/Drop class
# @param data_volume 5/Float/ComponentParameter/NoPort/ReadWrite//False/False/Estimated size of the data contained in this node
# @param group_end False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Is this node the end of a group?
# @param streaming False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Specifies whether this data component streams input and output data
# @param persist False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Specifies whether this data component contains data that should not be deleted after execution
# @param dummy /Object/ApplicationArgument/InputOutput/ReadWrite//False/False/Dummy port
# @par EAGLE_END
class InMemoryDROP(DataDROP):
    """
    A DROP that points data stored in memory.
    """

    # Allow in-memory drops to be automatically removed by default
    def __init__(self, *args, **kwargs):
        if "persist" not in kwargs:
            kwargs["persist"] = False
        if "expireAfterUse" not in kwargs:
            kwargs["expireAfterUse"] = True
        if kwargs["persist"]:
            kwargs["expireAfterUse"] = False
        super().__init__(*args, **kwargs)

    def initialize(self, **kwargs):
        args = []
        if "pydata" in kwargs:
            pydata = kwargs.pop("pydata")
            # TODO: Should be able to pass in data without base64, but
            # currently this is blocking the dask delayed tests.
            # if isinstance(pydata, str):
            #     try:
            #         pydata = pickle.dumps(json.loads(pydata))
            #     except:
            #         pydata = pydata.encode("utf8")
            # else:
            #     pydata = base64.b64decode(pydata)
            if isinstance(pydata, str):
                pydata = pydata.encode("utf8")
            args.append(base64.b64decode(pydata))
        self._buf = io.BytesIO(*args)

    def getIO(self):
        if (
            hasattr(self, "_tp")
            and hasattr(self, "_sessID")
            and sys.version_info >= (3, 8)
        ):
            return SharedMemoryIO(self.oid, self._sessID)
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
        except Exception:
            logger.debug("Could not read drop reproduce data")
        return {"data_hash": common_hash(data)}


##
# @brief SharedMemory
# @details Data stored in shared memory
# @par EAGLE_START
# @param category SharedMemory
# @param tag daliuge
# @param dropclass dlg.data.drops.memory.SharedMemoryDROP/String/ComponentParameter/NoPort/ReadWrite//False/False/Drop class
# @param data_volume 5/Float/ComponentParameter/NoPort/ReadWrite//False/False/Estimated size of the data contained in this node
# @param group_end False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Is this node the end of a group?
# @param streaming False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Specifies whether this data component streams input and output data
# @param persist False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Specifies whether this data component contains data that should not be deleted after execution
# @param dummy /Object/ApplicationArgument/InputOutput/ReadWrite//False/False/Dummy port
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
        if "pydata" in kwargs:
            pydata = kwargs.pop("pydata")
            if isinstance(pydata, str):
                pydata = pydata.encode("utf8")
            args.append(base64.b64decode(pydata))
        self._buf = io.BytesIO(*args)

    def getIO(self):
        if sys.version_info >= (3, 8):
            if hasattr(self, "_sessID"):
                return SharedMemoryIO(self.oid, self._sessID)
            else:
                # Using Drop without manager, just generate a random name.
                sess_id = "".join(
                    random.choices(
                        string.ascii_uppercase + string.digits, k=10
                    )
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
