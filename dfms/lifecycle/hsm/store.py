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
from abc import ABCMeta, abstractmethod
import psutil
import warnings
import json
'''
Created on 22 Jun 2015

@author: rtobar
'''

import logging
import os
from dfms.data_object import FileDataObject, InMemoryDataObject, NgasDataObject

_logger = logging.getLogger(__name__)

class AbstractStore(object):

    __metaclass__ = ABCMeta

    def __init__(self, *args, **kwargs):
        super(AbstractStore, self).__init__()
        self._setTotalSpace(0)
        self._setAvailableSpace(0)
        self._setWritingSpeed(0)
        self._setReadingSpeed(0)

    def updateSpaces(self):
        self._updateSpaces()
        if _logger.isEnabledFor(logging.DEBUG):
            avail = self.getAvailableSpace()
            total = self.getTotalSpace()
            perc  = avail*100./total
            _logger.debug("Available/Total space on %s: %d/%d (%.2f %%)" % (self, avail, total, perc))
        pass

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
    def createDataObject(self, oid, uid, broadcaster, **kwargs):
        pass

    @abstractmethod
    def _updateSpaces(self):
        pass

class FileSystemStore(AbstractStore):

    def __init__(self, mountPoint):
        super(FileSystemStore, self).__init__()

        if not mountPoint:
            raise Exception("No mount point given when creating FileSystemStore")

        if not os.path.isdir(mountPoint):
            raise Exception("Mount point '" + mountPoint + "' is not a directory")

        if not os.path.ismount(mountPoint):
            raise Exception("'" + mountPoint + "' is not a mount point")

        self._mountPoint = mountPoint
        self.updateSpaces()

    def _updateSpaces(self):
        stat = os.statvfs(self._mountPoint)
        blocks = stat.f_blocks
        blockSize = stat.f_bsize
        freeBlocks = stat.f_bfree
        fragmentSize = stat.f_bsize

        totalSpace = blocks * fragmentSize
        availableSpace=freeBlocks * blockSize
        self._setTotalSpace(totalSpace)
        self._setAvailableSpace(availableSpace)

    def createDataObject(self, oid, uid, broadcaster, **kwargs):
        return FileDataObject(oid, uid, broadcaster, **kwargs)

    def __str__(self):
        return self._mountPoint

class MemoryStore(AbstractStore):

    def __init__(self):
        super(MemoryStore, self).__init__()
        self.updateSpaces()

    def _updateSpaces(self):
        vmem = psutil.virtual_memory()
        self._setTotalSpace(vmem.total)
        self._setAvailableSpace(vmem.free)

    def createDataObject(self, oid, uid, broadcaster, **kwargs):
        return InMemoryDataObject(oid, uid, broadcaster, **kwargs)

    def __str__(self):
        return 'Memory'

class NgasStore(AbstractStore):

    def __init__(self, host=None, port=None, initialCheck=True):

        try:
            from ngamsPClient import ngamsPClient  # @UnusedImport
        except:
            warnings.warn("NGAMS client libs not found, cannot use NGAMS as a store")
            raise

        # Some sane defaults
        if not host:
            host = 'localhost'
            warnings.warn('Defaulting NGAS host to %s' % (host))
        if not port:
            port = 7777
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug('Defaulting NGAS port to %d' % (port))

        self._host = host
        self._port = port

        self.updateSpaces()

    def _updateSpaces(self):
        client = self._getClient()
        data = client.sendCmd("QUERY", pars=[['query', 'disks_list'], ['format', 'json']]).getData()
        disks = json.loads(data)['disks_list']
        totalAvailable = 0
        totalStored = 0
        for disk in disks:
            # col13 = available_mb
            # col14 = bytes_stored
            totalAvailable += float(disk['col13'])
            totalStored += int(disk['col14'])
        totalAvailable *= 1024**2 # to bytes

        # TODO: Check if these computations are correct, I'm not sure if the
        #       quantities stored by NGAS should be interpreted like this, or
        #       if "available" should be read as "total"
        self._setTotalSpace(totalAvailable + totalStored)
        self._setAvailableSpace(totalAvailable)

    def createDataObject(self, oid, uid, broadcaster, *args, **kwargs):
        kwargs['ngasSrv']  = self._host
        kwargs['ngasPort'] = self._port
        return NgasDataObject(oid, uid, broadcaster, **kwargs)

    def _getClient(self):
        from ngamsPClient import ngamsPClient
        return ngamsPClient.ngamsPClient(self._host, self._port)

    def __str__(self):
        return "NGAS@%s:%d" % (self._host, self._port)