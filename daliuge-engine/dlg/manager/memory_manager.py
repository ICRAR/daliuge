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
Module contains shared memory manager which handles shared memory for a multi-threaded NodeManager.
"""

import logging
from multiprocessing import current_process
from multiprocessing.shared_memory import SharedMemory

logger = logging.getLogger(__name__)


class SharedMemoryManager(object):

    def __init__(self):
        self._blocks = {}
        self._shutdown = False

    def start(self):
        pass

    def resize(self, name, size):
        # TODO: Implement resizing memory blocks

        raise NotImplementedError("Cannot resize yet")
        pass

    def open_write(self, name, size=4096):
        if name not in self._blocks.keys():
            try:
                self._blocks[name] = SharedMemory(name, create=True, size=size)
            except FileExistsError:
                self._blocks[name] = SharedMemory(name)
        else:
            if size > self._blocks[name].buf.size:
                self.resize(name, size)
        return self._blocks[name]

    def open_read(self, name):
        if name not in self._blocks.keys():
            return self.open_write(name)
        else:
            return self._blocks[name]

    def close(self, name):
        if name in self._blocks.keys():
            try:
                self._blocks[name].close()
            except BufferError:
                pass

    def shutdown(self):
        self._shutdown = True
        for k, v in self._blocks.items():
            v.close()
            v.unlink()

    def close_all(self):
        for k, v in self._blocks.items():
            v.close()

    def __del__(self):
        if current_process().name == 'MainProcess':
            if not self._shutdown:
                self.shutdown()
        else:
            self.close_all()
