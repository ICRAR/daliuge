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

NON_FILENAME_CHARACTERS = re.compile(fr":|{os.sep}")

def default_map():
    """
    Get the default map for the FSTRING replacement keywords
    """
    return {
        "dlg": "DALIGUE",
        "datetime": datetime.date.today().strftime("%Y-%m-%d"),
        "uid": str(uuid.uuid4()),
    } 

def base_uid_filename(uid: str, humanKey: str):
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


def filepath_from_string(filename: str, dirname: str = "", **kwargs) -> str:
    """
    Attempts to construct a filename from filename and possible mappings, which are
    built from a combination of FSTRING_MAP and **kwargs.

    Returns
    -------
    filename
    """

    opts = []
    fstring_map = default_map() 
    fstring_map.update(kwargs)
    if not filename and "humanKey" in fstring_map:
        return base_uid_filename(fstring_map["uid"], fstring_map["humanKey"])
    elif not filename:
        return filename

    opts.extend(find_dlg_fstrings(filename))
    for fp in opts:
        filename = filename.replace(f"{{{fp}}}", fstring_map[fp])

    return f"{dirname}/{filename}" if dirname else filename
