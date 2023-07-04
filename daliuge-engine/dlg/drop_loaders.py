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
Utility methods and classes to be used when interacting with DROPs
"""

import collections
import io
import time
import logging
import pickle
import re
import threading
import traceback
from typing import Any, Tuple
import numpy as np

from dlg.ddap_protocol import DROPStates
from dlg.data.io import IOForURL, OpenMode
from dlg import common

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dlg.drop import AbstractDROP
    from dlg.apps.app_base import AppDROP
    from dlg.data.drops.data_base import DataDROP

logger = logging.getLogger(__name__)

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
    buf = io.BytesIO()
    desc = drop.open()
    while True:
        data = drop.read(desc)
        if not data:
            break
        buf.write(data)
    drop.close(desc)
    return pickle.loads(buf.getbuffer())


# async def save_pickle_iter(drop: DataDROP, data: Iterable[Any]):
#     for obj in data:
#         yield drop.write(obj)


# async def load_pickle_iter(drop: PathBasedDrop) -> AsyncIterable:
#     with open(drop.path, 'rb') as p:
#         while p.peek(1):
#             yield pickle.load(p)


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


def load_npy(drop: "DataDROP", allow_pickle=False) -> np.ndarray:
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


# def save_jsonp(drop: PathBasedDrop, data: Dict[str, object]):
#     with open(drop.path, 'r') as f:
#         json.dump(data, f)


# def save_json(drop: DataDROP, data: Dict[str, object]):
#     # TODO: support BinaryIO or TextIO interface from DataIO?
#     json_string = json.dumps(data)
#     drop.write(json_string.encode('UTF-8'))


# def load_json(drop: DataDROP) -> dict:
#     dropio = drop.getIO()
#     dropio.open(OpenMode.OPEN_READ)
#     data = json.loads(dropio.buffer())
#     dropio.close()
#     return data
