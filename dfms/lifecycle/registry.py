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

The registry simply (for the time being) keeps a record of:
 * Which DataObjects (i.e., which oids) are out there
 * For each DataObject, which instances(i.e., which uids) are out there

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
        """
        Adds a new DataObject to the registry
        """

    @abstractmethod
    def addDataObjectInstance(self, dataObject):
        """
        Adds a new DataObject instance to the registry. The registry should have
        a record already for the DataObject that the new instance belongs to
        """

    @abstractmethod
    def getDataObjectUids(self, dataObject):
        """
        Returns a list with the UIDs of all known instances of the given
        DataObject
        """

    @abstractmethod
    def setDataObjectPhase(self, dataObject, phase):
        """
        Records the phase of the given DataObject
        """

    @abstractmethod
    def recordNewAccess(self, oid):
        """
        Appends a new record to the list of access times of the given DataObject
        (i.e., when it has been accessed for reading)
        """

    @abstractmethod
    def getLastAccess(self, oid):
        """
        Returns the last access time for the given DataObject, or -1 if it has
        never been accessed
        """

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

    def recordNewAccess(self, oid):
        self._checkDOIsInRegistry(oid)
        self._dos[oid].accessTimes.append(time.time())

    def getLastAccess(self, oid):
        if oid in self._dos and self._dos[oid].accessTimes:
            return self._dos[oid].accesTimes[-1]
        else:
            return -1