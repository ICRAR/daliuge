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

from dlg.shared_memory import DlgSharedMemory

logger = logging.getLogger(f"dlg.{__name__}")


def _cleanup_block(session_id, name):
    mem = DlgSharedMemory(f"{session_id}_{name}")
    mem.close()
    mem.unlink()  # It is unlinking that is critical to freeing resources from the OS


def _close_block(session_id, name):
    mem = DlgSharedMemory(f"{session_id}_{name}")
    mem.close()


class DlgSharedMemoryManager:
    """
    Light class used by a NodeManager to log the existence of sharedmemory blocks.
    Unlinks all objects when requested
    """

    def __init__(self):
        self.drop_names = {}

    def register_session(self, name):
        """
        Registers a session with this memory manager
        """
        if str(name) in self.drop_names.keys():
            return
        self.drop_names[str(name)] = set()  # Handles duplicates

    def register_drop(self, name, session_id):
        """
        Adds a drop to the list of known shared memory blocks
        """
        if str(session_id) not in self.drop_names.keys():
            self.register_session(session_id)
        self.drop_names[str(session_id)].add(str(name))

    def shutdown_session(self, session_id):
        """
        Unlinks all memory blocks associated with a particular session.
        """
        if session_id in self.drop_names.keys():
            for drop in self.drop_names[session_id]:
                _cleanup_block(session_id, drop)

    def destroy_session(self, session_id):
        if session_id in self.drop_names.keys():
            for drop in self.drop_names[session_id]:
                _cleanup_block(session_id, drop)

    def shutdown_all(self):
        """
        Unlinks all shared memory blocks associated with this memory manager
        """
        for session_id in self.drop_names:
            self.destroy_session(session_id)
        self.drop_names = {}

    def __del__(self):
        if len(self.drop_names) > 0:
            self.shutdown_all()
