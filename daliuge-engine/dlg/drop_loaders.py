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
Utility functions for DROP I/O. This has been factored out from droputils
to avoid cyclic imports.
"""
import base64
import dill
import io
import logging
import pickle
import re
from typing import Any
import numpy as np

from dlg.data.io import OpenMode

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dlg.data.drops.data_base import DataDROP

logger = logging.getLogger(f"dlg.{__name__}")

# Used to check whether a command specifies via UID reference the path or
# data URL of an input or output
indexed_ipath_pattern = re.compile(r".*%i\[.+\].*")
indexed_opath_pattern = re.compile(r".*%o\[.+\].*")
indexed_idataurl_pattern = re.compile(r".*%iDataURL\[.+\].*")
indexed_odataurl_pattern = re.compile(r".*%oDataURL\[.+\].*")


def save_pickle(drop: "DataDROP", data: Any):
    """Saves a python object in pkl format"""
    pickle.dump(data, drop)


def load_pickle(drop: "DataDROP") -> Any:
    """Loads a pkl formatted data object stored in a DataDROP.
    Note: does not support streaming mode.
    """
    if drop.size == 0:
        return None
    buf = io.BytesIO()
    desc = drop.open()
    while True:
        data = drop.read(desc)
        if not data:
            break
        buf.write(data)
    drop.close(desc)
    return pickle.loads(buf.getvalue())


def save_npy(drop: "DataDROP", ndarray: np.ndarray, allow_pickle=False):
    """
    Saves a numpy ndarray to a drop in npy format
    """
    bio = io.BytesIO()
    # np.save accepts a "file-like" object which basically just requires
    # a .write() method. Try np.save(drop, array)
    np.save(bio, ndarray, allow_pickle=allow_pickle)
    dropio = drop.getIO()
    dropio.open(OpenMode.OPEN_WRITE)
    dropio.write(bio.getbuffer())
    dropio.close()


def save_numpy(drop: "DataDROP", ndarray: np.ndarray):
    save_npy(drop, ndarray)


def load_npy(drop: "DataDROP", allow_pickle=True) -> np.ndarray:
    """
    Loads a numpy ndarray from a drop in npy format
    """
    dropio = drop.getIO()
    dropio.open(OpenMode.OPEN_READ)
    res = np.load(io.BytesIO(dropio.buffer()), allow_pickle=allow_pickle)
    dropio.close()
    return res


def load_numpy(drop: "DataDROP", allow_pickle=True):
    return load_npy(drop, allow_pickle=allow_pickle)


def load_dill(drop: "DataDROP"):
    """
    Load dill
    """
    buf = io.BytesIO()
    desc = drop.open()
    if drop.size == 0:
        return None
    while True:
        data = drop.read(desc)
        if not data:
            break
        if isinstance(data, str):
            data = base64.b64decode(data)
        buf.write(data)
    drop.close(desc)
    return dill.loads(buf.getvalue())


def load_binary(drop: "DataDROP"):
    """
    Load binary
    """
    buf = io.BytesIO()
    desc = drop.open()
    read = True
    while read:
        data = drop.read(desc)
        if data:
            buf.write(data)
            drop.close(desc)
            return buf.getvalue().decode()

        return 0


def save_binary(drop: "DataDROP", data: bytes):
    """
    Save binary
    """
    bytes_data = io.BytesIO(data)
    dropio = drop.getIO()
    dropio.open(OpenMode.OPEN_WRITE)
    dropio.write(bytes_data.getbuffer())
    dropio.close()


def load_utf8(drop: "DataDROP"):
    """
    Loads data from a drop and converts it to a UTF8 encoded string.
    """
    dropio = drop.getIO()
    dropio.open(OpenMode.OPEN_READ)
    res = dropio.buffer()
    dropio.close()
    return res
