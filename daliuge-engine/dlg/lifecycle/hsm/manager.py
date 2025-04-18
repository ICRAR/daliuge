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
A very simple implementation of an HSM, without knowing much actually about how
HSMs work and what they actually offer in terms of APIs.

@author: rtobar
"""

import logging

from ..hsm import store

logger = logging.getLogger(f"dlg.{__name__}")


class HierarchicalStorageManager(object):
    def __init__(self):
        self._stores = []
        self.addStore(store.MemoryStore())
        self.addStore(store.FileSystemStore("/", "/tmp/daliuge_tfiles"))

    def addStore(self, newStore):
        """
        @param newStore store.AbstractStore
        """
        logger.debug("Adding store to HSM: %s", str(newStore))
        self._stores.append(newStore)

    def getSlowestStore(self):
        """
        :return store.AbstractStore:
        """
        return self._stores[-1]
