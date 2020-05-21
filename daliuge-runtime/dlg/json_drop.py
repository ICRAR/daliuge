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
"""
A DROP for a JSON file
"""
import json
import logging

from .drop import FileDROP

LOG = logging.getLogger(__name__)


class JsonDROP(FileDROP):
    def __init__(self, oid, uid, **kwargs):
        self._data = None
        super(JsonDROP, self).__init__(oid, uid, **kwargs)

    def __getitem__(self, item):
        if self._data is None:
            self._load()

        return self._data[item]

    def _load(self):
        with open(self.path, mode='r') as json_file:
            self._data = json.load(json_file)
