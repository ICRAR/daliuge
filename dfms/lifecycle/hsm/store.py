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
'''
Created on 22 Jun 2015

@author: rtobar
'''

import logging
import os
from dfms.data_object import FileDataObject

_logger = logging.getLogger(__name__)

class AbstractStore(object):

    __metaclass__ = ABCMeta

    def __init__(self):
        super(AbstractStore, self).__init__()
        self._setTotalSpace(0)
        self._setAvailableSpace(0)
        self._setWritingSpeed(0)
        self._setReadingSpeed(0)

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

    @abstractmethod
    def createDataObject(self, oid, uid, broadcaster, **kwargs):
        pass

class FileSystemStore(AbstractStore):

    def __init__(self, mountPoint):
        super(FileSystemStore, self).__init__()

        if not mountPoint:
            raise Exception("No mount point given when creating FileSystemStore")

        if not os.path.isdir(mountPoint):
            raise Exception("Mount point '" + mountPoint + "' is not a directory")

        self._mountPoint = mountPoint
        self.updateSpaces()

    def updateSpaces(self):
        stat = os.statvfs(self._mountPoint)
        blocks = stat.f_blocks
        blockSize = stat.f_bsize
        freeBlocks = stat.f_bfree
        fragmentSize = stat.f_bsize

        totalSpace = blocks * fragmentSize
        availableSpace=freeBlocks * blockSize
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug("Available/Total space on %s: %d/%d" % (self._mountPoint, availableSpace, totalSpace))
        self._setTotalSpace(totalSpace)
        self._setAvailableSpace(availableSpace)

    def createDataObject(self, oid, uid, broadcaster, **kwargs):
        return FileDataObject(oid, uid, broadcaster, **kwargs)

    def __str__(self):
        return self._mountPoint