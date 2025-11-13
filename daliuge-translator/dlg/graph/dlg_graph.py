#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2025
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

from dlg.dropmake.dm_utils import (
    LG_APPREF,
    get_lg_ver_type,
    convert_construct,
    convert_fields,
    convert_subgraphs,
    LG_VER_EAGLE,
    LG_VER_EAGLE_CONVERTED,
    GraphException,
    GInvalidLink,
    GInvalidNode,
    load_lg,
    extract_globals
)


class DaliugeGraph:
    """
    An object representation of the DALiuGE graph
    """

    def __init__(self, f, ssid=None, apply_config=True):
        """
        parse JSON into LG object graph first
        """
        self._g_var = []
        lg = load_lg(f)
        if ssid is None:
            ts = time.time()
            ssid = datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%dT%H:%M:%S")
        self._session_id = ssid
        self._loop_aware_set = set()

        # key - gather drop oid, value - a tuple with two elements
        # input drops list and output drops list
        self._gather_cache = {}

        lgver = get_lg_ver_type(lg)
        logger.info("Loading graph: %s", lg["modelData"]["filePath"])
        logger.info("Found LG version: %s", lgver)

