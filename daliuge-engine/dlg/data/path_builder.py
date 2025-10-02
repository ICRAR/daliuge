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

"""
Module with utility methods to create runtime file paths for the DALiuGE Engine.

The Engine supports implicit filepath naming if FileDROPs have no user-defined name in
the Logical Graph Template. This helps when multiple FileDROPs are created in a
construct (e.g. Scatter or Loop) and we don't run the risk of overwriting them.

This module provides the methods that support different filepath naming heuristics to
facilitate implicit filepath naming at runtime.
"""
import datetime
import logging
import re
import os
import uuid

from pathlib import Path
from enum import Enum, auto

NON_FILENAME_CHARACTERS = re.compile(fr":|{os.sep}")

class PathType(Enum):

    File = auto()
    Directory = auto()


def construct_map(**kwargs):
    """
    Get the default map for the FSTRING replacement keywords
    """
    map = {
        "dlg": "DALIGUE",
        "datetime": datetime.date.today().strftime("%Y-%m-%d"),
        "uid": str(uuid.uuid4()),
        "auto": base_uid_pathname(kwargs.get("uid"), kwargs.get("humanKey"))
    }
    map.update(**kwargs)
    return map

def base_uid_pathname(uid: str, humanKey: str):
    """
    This a basic filename generator, using the UID and humandReadableKey. The function
    returns only the name of the file, and expects the full filepath to be handled by
    the function caller.

    Parameters
    ----------
    uid: str, the UID of the DROP
    humanKey: str, a human-readable key (typically a truncated UID) for user-facing
        display

    Notes
    -----
    An example DROP UID would be:

        '2024-10-30T12:01:57_0140555b-8c23-4d6a-9e24-e16c15555e8c_0'

    This approach will lead to an abridged UID along with the humanKey appended:

       '2024-10-30T12:01:57_0140555b_0/1/1'

    In this case, 0/1/1 is the "humanKey", derived from the DALiuGE Translator
    constructs (For more information on humanKey generation, refer to
    dlg.common.truncateUidToKey).

    The final part of the method replaces characters that cannot be in filenames from
    the UIDs (e.g. "/").

    Returns
    -------
        str: Base filename
    """
    if not uid:
        return None
    if not humanKey:
        humanKey = ""
    fn = str(uid).split("_")[0] + "_" + str(humanKey)
    return NON_FILENAME_CHARACTERS.sub("_", fn)

def find_dlg_fstrings(filename: str) -> list[str]:
    """
    Find f-string style braces intended for replacement by the Engine and return a list
    of the placeholder strings.

    Supports escaping the braces by using two in a row (e.g. {{example}}).

    :param filename: filename with possible f-string brackets for keyword replacement

    .. note::
        The following cases are supported by this method:
        1. No braces:
            "unique_file.txt"  # []

        2. Standard {} replacement :
            "gather_{uid}.py" -> return ['uid']

        3. Escaped braces:
            "gather_{{uid}}.py"  -> []

        4. Multiple braces
            "prefix_{uid}_{datetime}.dat"  # ['uid', 'datetime']

    :return: list of placeholder strings (incl. {})
    """
    opts = []
    try:
        opts.extend(re.findall(r'\{([^{}]+)\}', filename))
        return opts
    except TypeError as e:
        logging.warning("Data not in expected format %s",e)
        return opts


def filepath_from_string(path: str, path_type: PathType, dirname: str = "",
                         **kwargs) -> (
        str):
    """
    Attempts to construct a path from path, dirname, and possible string replacements.
    The replacements are built from a combination of FSTRING_MAP and **kwargs.

    If path_type is PathType.Directory and we do not have a 'path', we do not create a
    base_uid_pathname, for we do not need to create a filename. Instead, we use dirname
    only to construct the final path.

    Returns
    -------
    path: The provisional path provided. This may be empty, a file-path, or a directory path.
    path_type: An indicator of what
    dirname
    """

    opts = []
    fstring_map = construct_map(**kwargs)
    if path_type == PathType.Directory and not path:
        path = "" # The path we want is a directory DROP and we do not care about
    elif not path and "humanKey" in fstring_map:
        return base_uid_pathname(fstring_map["uid"], fstring_map["humanKey"])
    elif not path:
        return path

    expanded_path = os.path.expandvars(path)
    expanded_dirname = os.path.expandvars(dirname)

    if expanded_path == path and "$" in expanded_path:
        raise RuntimeError(f"Environment variable in path {path} not set!")
    if expanded_dirname == dirname and "$" in expanded_dirname:
        raise RuntimeError(f"Environment variable in path {dirname} not set!")

    opts.extend(find_dlg_fstrings(path))
    for fp in opts:
        expanded_path = expanded_path.replace(f"{{{fp}}}", fstring_map[fp])

    if Path(expanded_path).is_absolute():
        return expanded_path
    else:
        return f"{expanded_dirname}/{expanded_path}" if expanded_dirname else expanded_path
