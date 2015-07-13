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
Utility methods and classes to be used when interacting with DataObjects

@author: rtobar, July 3, 2015
'''

class EvtConsumer(object):
    '''
    Small utility class that sets the internal flag of the given threading.Event
    object when consuming a DO. Used throughout the tests as a barrier to wait
    until all DOs of a given graph have executed
    '''
    def __init__(self, evt):
        self._evt = evt
    def consume(self, do):
        self._evt.set()

def allDataObjectContents(dataObject):
    '''
    Returns all the data contained in a given dataObject
    '''
    desc = dataObject.open()
    buf = dataObject.read(desc)
    allContents = buf
    while buf:
        buf = dataObject.read(desc)
        allContents += buf
    dataObject.close(desc)
    return allContents

def copyDataObjectContents(source, target, bufsize=4096):
    '''
    Manually copies data from one DataObject into another, in bufsize steps
    '''
    desc = source.open()
    buf = source.read(desc, bufsize)
    while buf:
        target.write(buf)
        buf = source.read(desc, bufsize)
    source.close(desc)