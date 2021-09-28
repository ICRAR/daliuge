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
import mmap
import os
import logging
import _posixshmem  # Does not work on Windows
import secrets

logger = logging.getLogger(__name__)

_O_CREX = os.O_CREAT | os.O_EXCL
_MAXNAMELENGTH = 14  # Apparently FreeBSD have this limitation


def _make_filename(name):
    if type(name) is not str:
        name = str(name)
    if name[0] != '/':
        name = '/' + name
    if len(name) > _MAXNAMELENGTH:
        return name[:_MAXNAMELENGTH]
    return name


def _make_random_filename():
    nbytes = (_MAXNAMELENGTH - len('/')) // 2
    assert nbytes >= 2, '_SHM_NAME_PREFIX too long'
    name = '/' + secrets.token_hex(nbytes)
    assert len(name) <= _MAXNAMELENGTH
    return name


class DlgSharedMemory:
    """
    A re-implementation of multiprocessing.shared_memory for more direct usage in daliuge.
    Writes directly to shared-memory, automatically handles resizing and fetching of pre-existing
    blocks.
    Based heavily on Python's own shared memory implementation
    (https://github.com/python/cpython/blob/3.9/Lib/multiprocessing/shared_memory.py)
    """
    _name = None
    _fd = -1
    _mmap = None
    _buf = None
    _flags = os.O_RDWR
    _mode = 0o600

    def __init__(self, name, size=4096):
        """
        Tries to create a file with name provided. If this file exists, returns existing file.
        If name is not provided, a random name is created.
        """
        if not size > 0:
            raise ValueError("'size' must be positive")
        if name is None:
            while True:
                name = _make_random_filename()
                try:
                    self._fd = _posixshmem.shm_open(
                        name,
                        self._flags,
                        mode=self._mode
                    )
                except FileExistsError:
                    continue
                self._name = name
                break
        else:
            name = _make_filename(name)
            self._flags = _O_CREX | os.O_RDWR
            try:
                self._fd = _posixshmem.shm_open(
                    name,
                    self._flags,
                    mode=self._mode
                )
            except FileExistsError:
                self._flags = os.O_RDWR
                self._fd = _posixshmem.shm_open(
                    name,
                    self._flags,
                    mode=self._mode
                )
                # Find the size of the written file
                size = os.lseek(self._fd, 0, os.SEEK_END)
                # Return to start for operations
                os.lseek(self._fd, 0, os.SEEK_SET)
            self._name = name
        try:
            os.ftruncate(self._fd, size)
            stats = os.fstat(self._fd)
            size = stats.st_size
            self._mmap = mmap.mmap(self._fd, size)
        except OSError:
            self.unlink()
            raise

        self._size = size
        self._buf = memoryview(self._mmap)

    def __del__(self):
        try:
            self.close()
        except OSError:
            pass

    def __repr__(self):
        return f'{self.__class__.__name__}({self._name!r}, size={self._size})'

    @property
    def buf(self):
        """A memoryview of contents of the shared memory block."""
        return self._buf

    @property
    def name(self):
        """Unique name that identifies the shared memory block."""
        return self._name

    @property
    def size(self):
        """Size in bytes."""
        return self._size

    def close(self):
        """
        Closes access to the shared memory but does not destroy it.
        """
        if self._buf is not None:
            self._buf.release()
            self._buf = None
        if self._mmap is not None:
            try:
                self._mmap.close()
            except BufferError:  # TODO: Possibly quite dodge
                pass
            self._mmap = None
        if self._fd > -1:
            os.close(self._fd)
            self._fd = -1

    def unlink(self):
        """
        Requests destruction of this memory block.
        Unlink should be called once and only once across all processes
        """
        _posixshmem.shm_unlink(self._name)

    def resize(self, new_size):
        """
        Replaces current file with larger block, containing old data.
        """
        old_data = bytes(self._buf)
        self.close()
        self.unlink()
        self.__init__(self._name, new_size)
        self._buf[0:len(old_data)] = old_data


def _cleanup_block(session_id, name):
    mem = DlgSharedMemory(f'{session_id}_{name}')
    mem.close()
    mem.unlink()  # It is unlinking that is critical to freeing resources from the OS


class DlgSharedMemoryManager:
    """
    Lite class used by a NodeManager to log the existance of sharedmemory objects.
    Unlinks all objects when requested
    """

    def __init__(self):
        self.drop_names = {}

    def register_session(self, name):
        # TODO: handle duplicate sessions
        self.drop_names[str(name)] = set()  # Handles duplicates

    def register_drop(self, name, session_id):
        """
        Adds a drop to the list of known shared memory blocks
        """
        # TODO: Handle unregistered session
        self.drop_names[str(session_id)].add(str(name))

    def shutdown_session(self, session_id):
        """
        Unlinks all memory blocks associated with a particular session.
        """
        if session_id in self.drop_names.keys():
            for drop in self.drop_names[session_id]:
                _cleanup_block(session_id, drop)

    def shutdown_all(self):
        """
        Unlinks all shared memory blocks associated with this memory manager
        """
        for session_id in self.drop_names.keys():
            self.shutdown_session(session_id)
