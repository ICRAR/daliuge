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
from dfms.ddap_protocol import DOPhases

_logger = logging.getLogger(__name__)

class DataObject(object):
    oid       = None
    phase     = DOPhases.GAS
    instances = []

class DataObjectInstance(object):
    oid     = None
    uid     = None
    storage = None

class Registry(object):
    def addDataObject(self, dataObject): raise NotImplementedError()
    def removeDataObject(self, dataObject): raise NotImplementedError()
    def addInstance(self, dataObjectInstance): raise NotImplementedError()

class InMemoryRegistry(Registry):

    def __init__(self):
        super(InMemoryRegistry, self).__init__()
        self._dos= {}

    def addDataObject(self, dataObject):
        '''
        :param dfms.data_object.AbstractDataObject dataObject:
        '''
        doRow = DataObject()
        doRow.oid       = dataObject.oid()
        doRow.phase     = dataObject.phase()
        doRow.instances = {dataObject.uid(): dataObject}
        self._dos[doRow.oid] = doRow
