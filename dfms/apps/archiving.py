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
from dfms.data_object import BarrierAppDataObject, ContainerDataObject
from dfms.io import NgasIO, OpenMode
import warnings
from dfms.doutils import DOFile

class NgasArchivingApp(BarrierAppDataObject):
    '''
    Module containing an archiving application class that takes a given DataObject
    and archives it in an NGAS server. It currently deals with non-container
    DataObjects only.

    The archiving to NGAS occurs through the framework and not by spawning a
    new NGAS client process. This way we can read the different storage types
    supported by the framework, and not only filesystem objects.
    '''

    def initialize(self, **kwargs):
        BarrierAppDataObject.initialize(self, **kwargs)

        # Check we actually can write NGAMS clients
        try:
            from ngamsPClient import ngamsPClient  # @UnusedImport @UnresolvedImport
        except:
            warnings.warn("No NGAMS client libs found, cannot use NgasDataObjects")
            raise

        self._ngasSrv            = self._getArg(kwargs, 'ngasSrv', 'localhost')
        self._ngasPort           = int(self._getArg(kwargs, 'ngasPort', 7777))
        # TODO: The NGAS client doesn't differentiate between these, it should
        self._ngasTimeout        = int(self._getArg(kwargs, 'ngasConnectTimeout', 2))
        self._ngasConnectTimeout = int(self._getArg(kwargs, 'ngasTimeout', 2))

    def run(self):

        if self.outputs:
            raise Exception("No outputs should be declared for this application")
        if len(self.inputs) != 1:
            raise Exception("Only one input is expected by this application")

        inDO = self.inputs[0]
        if isinstance(inDO, ContainerDataObject):
            raise Exception("ContainerDataObjects are not supported as inputs for this application")

        ngasIO = NgasIO(self._ngasSrv, inDO.uid, self._ngasPort, self._ngasConnectTimeout, self._ngasTimeout)
        ngasIO.open(OpenMode.OPEN_WRITE)

        # Copy in blocks of 4096 bytes
        with DOFile(inDO) as f:
            while True:
                buff = f.read(4096)
                ngasIO.write(buff)
                if len(buff) != 4096:
                    break
        ngasIO.close()