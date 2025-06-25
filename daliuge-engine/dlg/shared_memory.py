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
Module contains shared memory class which creates shared memory blocks for use by drops or other
DALiuGE components.
"""

import logging
import mmap
import os
import secrets
import warnings

import _posixshmem  # Does not work on Windows

logger = logging.getLogger(f"dlg.{__name__}")

_O_CREX = os.O_CREAT | os.O_EXCL
_MAXNAMELENGTH = 14  # Apparently FreeBSD has this limitation


def _make_filename(name):
    if isinstance(name, str):
        name = str(name)
    if name[0] != "/":
        name = "/" + name
    if len(name) > _MAXNAMELENGTH:
        return name[:_MAXNAMELENGTH]
    return name


def _make_random_filename():
    nbytes = (_MAXNAMELENGTH - len("/")) // 2
    assert nbytes >= 2, "_SHM_NAME_PREFIX too long"
    name = "/" + secrets.token_hex(nbytes)
    assert len(name) <= _MAXNAMELENGTH
    return name


class DlgSharedMemory:
    """
    A re-implementation of multiprocessing.shared_memory for more direct usage in daliuge.
    Writes directly to shared-memory, automatically handles resizing and fetching of pre-existing
    blocks.
    If given a name, will attempt to create a new file or return a pre-existing one.
    Based heavily on Python's own shared memory implementation
    (https://github.com/python/cpython/blob/3.9/Lib/multiprocessing/shared_memory.py)
    """

    _name = None
    _fd = -1
    _mmap = None
    _buf = None
    _flags = os.O_RDWR
    _mode = 0o600

    def __init__(self, name, size=65536):
        """
        Tries to create a file with name provided. If this file exists, returns existing file.
        If name is not provided, a random name is created.
        """
        if size <= 0:
            raise ValueError("'size' must be positive")
        if name is None:
            while True:
                # Try make a random shared file
                name = _make_random_filename()
                try:
                    self._fd = _posixshmem.shm_open(
                        name, self._flags | _O_CREX, mode=self._mode
                    )
                except FileExistsError:  # Collision with other name
                    continue
                self._name = name
                break
        else:
            # Attempt to create a named file
            name = _make_filename(name)
            self._flags = _O_CREX | os.O_RDWR
            try:
                self._fd = _posixshmem.shm_open(name, self._flags, mode=self._mode)
            except FileExistsError:
                # File already exists, attempt to open in read/write mode
                self._flags = os.O_RDWR
                try:
                    self._fd = _posixshmem.shm_open(name, self._flags, mode=self._mode)
                except FileNotFoundError:
                    self.__init__(name, size) # pylint: disable=non-parent-init-called
                # Find the size of the written file
                # Needs to be set so that the file is truncated down to the correct size.
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
        return f"{self.__class__.__name__}({self._name!r}, size={self._size})"

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
            except BufferError:
                # WARNING: Possibly quite dodge but I cannot see a way around this.
                # If a child process creates a memory block then closes it, there will be a
                # buffer error as the parent process (child in the view of the memory) will still
                # have access.
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
        try:
            _posixshmem.shm_unlink(self._name)
        except FileNotFoundError:
            logger.debug("%s tried to unlink twice", self.name)
            warnings.warn("Cannot unlink a shared block twice", RuntimeWarning)

    def resize(self, new_size):
        """
        Replaces current file with larger block, containing old data.
        """
        old_data = bytes(self._buf)
        self.close()
        self.unlink()
        self.__init__(self._name, new_size)
        if new_size < len(old_data):
            warnings.warn("Shrinking shared block, may lose data", BytesWarning)
            self._buf[:] = old_data[0:new_size]
        else:
            self._buf[0 : len(old_data)] = old_data
