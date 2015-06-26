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
'''
Module containing the base class and a simple implementation of the registry
used by the DLM to keep track of which DOs are where, and therefore in which
phase they currently are

@author: rtobar
'''

import logging
import time
from abc import abstractmethod, ABCMeta
from dfms.ddap_protocol import DOPhases

_logger = logging.getLogger(__name__)

class DataObject(object):
    oid         = None
    phase       = DOPhases.GAS
    instances   = []
    accessTimes = []

class DataObjectInstance(object):
    oid     = None
    uid     = None
    storage = None


class Registry():
    '''
    Base class that imposes a given structure, subclasses should implement all
    NotImplementedError methods. Is this the
    '''

    __metaclass__ = ABCMeta

    @abstractmethod
    def addDataObject(self, dataObject):
        pass

    @abstractmethod
    def addDataObjectInstance(self, dataObject):
        pass

    @abstractmethod
    def getDataObjectUids(self, dataObject):
        pass

    @abstractmethod
    def setDataObjectPhase(self, dataObject, phase):
        pass

    @abstractmethod
    def removeDataObject(self, dataObject):
        pass

    @abstractmethod
    def recordNewAccess(self, doi):
        pass

    def _checkDOIsInRegistry(self, oid):
        if not self._dos.has_key(oid):
            raise Exception('DataObject %s is not present in the registry' % (oid))

class InMemoryRegistry(Registry):

    def __init__(self):
        super(InMemoryRegistry, self).__init__()
        self._dos= {}

    def addDataObject(self, dataObject):
        '''
        :param dfms.data_object.AbstractDataObject dataObject:
        '''
        # Check that the DO is not in the registry
        doRow = DataObject()
        doRow.oid       = dataObject.oid
        doRow.phase     = dataObject.phase
        doRow.instances = {dataObject.uid: dataObject}
        self._dos[doRow.oid] = doRow

    def addDataObjectInstance(self, dataObject):
        '''
        :param dfms.data_object.AbstractDataObject dataObject:
        '''
        self._checkDOIsInRegistry(dataObject.oid)
        if self._dos[dataObject.oid].instances.has_key(dataObject.uid):
            raise Exception('DataObject %s/%s already present in registry' % (dataObject.oid, dataObject.uid))
        self._dos[dataObject.oid].instances[dataObject.uid] = dataObject

    def getDataObjectUids(self, dataObject):
        self._checkDOIsInRegistry(dataObject.oid)
        return self._dos[dataObject.oid].instances.keys()

    def setDataObjectPhase(self, dataObject, phase):
        self._checkDOIsInRegistry(dataObject.oid)
        self._dos[dataObject.oid].phase = phase

    def removeDataObject(self, dataObject):
        self._checkDOIsInRegistry(dataObject.oid)
        del self._dos[dataObject.oid]

    def recordNewAccess(self, oid):
        self._checkDOIsInRegistry(oid)
        self._dos[oid].accessTimes.append(time.time())