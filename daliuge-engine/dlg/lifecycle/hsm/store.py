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
Implementation of the different storage layers that are then used by the HSM to
store data. Each layer keeps track of its used space, and knows how to create
DROPs that use that layer as its storage mechanism.

@author: rtobar
"""

import json
import logging
import os
from abc import ABCMeta, abstractmethod

import psutil

from dlg.data.drops.memory import InMemoryDROP
from dlg.data.drops.ngas import NgasDROP
from dlg.data.drops.file import FileDROP

logger = logging.getLogger(f"dlg.{__name__}")


class AbstractStore(object):
    """
    The abstract store implementation, see the subclasses for details
    """

    __metaclass__ = ABCMeta

    def __init__(self):
        super(AbstractStore, self).__init__()
        self._setTotalSpace(0)
        self._setAvailableSpace(0)
        self._setWritingSpeed(0)
        self._setReadingSpeed(0)

    def updateSpaces(self):
        self._updateSpaces()
        if logger.isEnabledFor(logging.DEBUG):
            avail = self.getAvailableSpace()
            total = self.getTotalSpace()
            perc = avail * 100.0 / total
            logger.debug(
                "Available/Total space on %s: %d/%d (%.2f %%)", self, avail, total, perc
            )

    def _setTotalSpace(self, totalSpace):
        self._totalSpace = totalSpace

    def _setAvailableSpace(self, availableSpace):
        self._availableSpace = availableSpace

    def _setWritingSpeed(self, writingSpeed):
        self._writingSpeed = writingSpeed

    def _setReadingSpeed(self, readingSpeed):
        self._readingSpeed = readingSpeed

    def getAvailableSpace(self):
        return self._availableSpace

    def getTotalSpace(self):
        return self._totalSpace

    @abstractmethod
    def createDrop(self, oid, uid, **kwargs):
        pass

    @abstractmethod
    def _updateSpaces(self):
        pass


class FileSystemStore(AbstractStore):
    """
    A filesystem store implementation. It requires a mount point at construction
    time which is used as the root of the store; thus this store uses a mounted
    device fully. It creates FileDROPs that live directly in the root of
    the filesystem, and monitors the usage of the filesystem.
    """

    def __init__(self, mountPoint, savingDir=None):
        super(FileSystemStore, self).__init__()

        if not mountPoint:
            raise Exception("No mount point given when creating FileSystemStore")

        if not os.path.isdir(mountPoint):
            raise Exception("Mount point '" + mountPoint + "' is not a directory")

        if not os.path.ismount(mountPoint):
            raise Exception("'" + mountPoint + "' is not a mount point")

        self._mountPoint = mountPoint
        self._savingDir = savingDir or mountPoint
        if not os.path.exists(self._savingDir):
            os.mkdir(self._savingDir)
        self.updateSpaces()

    def _updateSpaces(self):
        stat = os.statvfs(self._mountPoint)
        blocks = stat.f_blocks
        blockSize = stat.f_bsize
        freeBlocks = stat.f_bfree
        fragmentSize = stat.f_bsize

        totalSpace = blocks * fragmentSize
        availableSpace = freeBlocks * blockSize
        self._setTotalSpace(totalSpace)
        self._setAvailableSpace(availableSpace)

    def createDrop(self, oid, uid, **kwargs):
        kwargs["dirname"] = self._savingDir
        return FileDROP(oid, uid, **kwargs)

    def __str__(self):
        return self._mountPoint


class MemoryStore(AbstractStore):
    """
    A store that uses RAM memory as its storage mechanism. It creates
    InMemoryDROPs and monitors the RAM usage of the system.
    """

    def __init__(self):
        super(MemoryStore, self).__init__()
        self.updateSpaces()

    def _updateSpaces(self):
        vmem = psutil.virtual_memory()
        self._setTotalSpace(vmem.total)
        self._setAvailableSpace(vmem.free)

    def createDrop(self, oid, uid, **kwargs):
        return InMemoryDROP(oid, uid, **kwargs)

    def __str__(self):
        return "Memory"


class NgasStore(AbstractStore):
    """
    A store that a given NGAS server as its storage mechanism. It creates
    NgasDROPs and monitors the disks usage of the NGAS system.
    """

    def __init__(self, host=None, port=None):
        super(NgasStore, self).__init__()
        try:
            from ngamsPClient import ngamsPClient  # pylint: disable=unused-import
        except:
            logger.error("NGAMS client libs not found, cannot use NGAMS as a store")
            raise

        # Some sane defaults
        if not host:
            host = "localhost"
            logger.debug("Defaulting NGAS host to %s", host)
        if not port:
            port = 7777
            logger.debug("Defaulting NGAS port to %d", port)

        self._host = host
        self._port = port

        self.updateSpaces()

    def _updateSpaces(self):
        client = self._getClient()
        data = client.sendCmd(
            "QUERY", pars=[["query", "disks_list"], ["format", "json"]]
        ).getData()
        disks = json.loads(data)["disks_list"]
        totalAvailable = 0
        totalStored = 0
        for disk in disks:
            # col13 = available_mb
            # col14 = bytes_stored
            totalAvailable += float(disk["col13"])
            totalStored += int(disk["col14"])
        totalAvailable *= 1024**2  # to bytes

        # TODO: Check if these computations are correct, I'm not sure if the
        #       quantities stored by NGAS should be interpreted like this, or
        #       if "available" should be read as "total"
        self._setTotalSpace(totalAvailable + totalStored)
        self._setAvailableSpace(totalAvailable)

    def createDrop(self, oid, uid, **kwargs):
        kwargs["ngasSrv"] = self._host
        kwargs["ngasPort"] = self._port
        return NgasDROP(oid, uid, **kwargs)

    def _getClient(self):
        from ngamsPClient import ngamsPClient

        return ngamsPClient.ngamsPClient(self._host, self._port)

    def __str__(self):
        return "NGAS@%s:%d" % (self._host, self._port)


class DirectoryStore(AbstractStore):
    """
    A store similar to the FileSystemStore that doesn't actually act on
    a mount point (and thus, most probably on a whole disk) but instead treats
    a directory sitting on the filesystem as the root of the (recurisve) store.
    The "available" size of the directory is determined by the content of the
    SIZES file that must be present at the root level, while the currently used
    space is determined by summing up the individual sizes of all files within
    the directory, recursively.
    This store creates FileDROPs that live inside the store's directory.
    """

    __SIZE_FILE = "SIZE"

    def __init__(self, dirName, initialize=False):
        super(DirectoryStore, self).__init__()
        if not dirName:
            raise Exception("No directory given to DirectoryStore")

        if not initialize and not os.path.isdir(dirName):
            raise Exception("%s doesn't exist or is not a directory" % (dirName))

        sizeFile = os.path.join(dirName, self.__SIZE_FILE)
        if not initialize and not os.path.isfile(sizeFile):
            raise Exception(
                "No %s file under %s, cannot determine available space for DirectoryStore"
                % (self.__SIZE_FILE, dirName)
            )
        else:
            # Should be used only for testing
            size = 1024**3
            logger.info(
                "Initializing %s with size %d. THIS SHOULD ONLY BE USED DURING TESTING",
                sizeFile,
                size,
            )
            self.prepareDirectory(dirName, size)

        with open(sizeFile) as f:
            self._setTotalSpace(int(f.read()))

        self._dirName = dirName
        self.updateSpaces()

    def createDrop(self, oid, uid, **kwargs):
        kwargs["dirname"] = self._dirName
        return FileDROP(oid, uid, **kwargs)

    def _updateSpaces(self):
        used = self._dirUsage(self._dirName)
        self._setAvailableSpace(self.getTotalSpace() - used)

    def _dirUsage(self, dirName):
        total = 0
        for f in os.listdir(dirName):
            if os.path.isdir(f):
                total += self._dirUsage(os.path.join(dirName, f))
            elif os.path.isfile(f):
                # Don't count our special file
                if f != self.__SIZE_FILE or dirName != self._dirName:
                    total += os.stat(os.path.join(dirName, f))
        return total

    @staticmethod
    def prepareDirectory(dirName, size):
        if not dirName:
            raise Exception("No directory given to prepareDirectory")

        if not os.path.isdir(dirName):
            os.makedirs(dirName)

        sizeFile = os.path.join(dirName, DirectoryStore.__SIZE_FILE)
        if not os.path.exists(sizeFile):
            with open(sizeFile, "w") as f:
                f.write(str(size))

    def __str__(self):
        return "dir:%s" % (self._dirName)
