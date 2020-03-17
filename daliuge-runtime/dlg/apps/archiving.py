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
from ..drop import BarrierAppDROP, ContainerDROP
from ..droputils import DROPFile
from ..io import NgasIO, OpenMode, NgasLiteIO
from ..meta import dlg_string_param, dlg_float_param, dlg_int_param, \
    dlg_component, dlg_batch_input, dlg_batch_output, dlg_streaming_input


class ExternalStoreApp(BarrierAppDROP):
    """
    An application that takes its input DROP (which must be one, and only
    one) and creates a copy of it in a completely external store, from the point
    of view of the DALiuGE framework.

    Because this application copies the data to an external location, it also
    shouldn't contain any output, making it a leaf node of the physical graph
    where it resides.
    """
    compontent_meta = dlg_component('ExternalStoreApp', 'An application that takes its input DROP (which must be one, and only one) '
                                    'and creates a copy of it in a completely external store, from the point '
                                    'of view of the DALiuGE framework.',
                                    [dlg_batch_input('binary/*', [])],
                                    [dlg_batch_output('binary/*', [])],
                                    [dlg_streaming_input('binary/*')])

    def run(self):

        # Check that the constrains are correct
        if self.outputs:
            raise Exception("No outputs should be declared for this application")
        if len(self.inputs) != 1:
            raise Exception("Only one input is expected by this application")

        # ... and go!
        inDrop = self.inputs[0]
        self.store(inDrop)

    def store(self, inputDrop):
        """
        Method implemented by subclasses. It should stores the contents of
        `inputDrop` into an external store.
        """

class NgasArchivingApp(ExternalStoreApp):
    '''
    An ExternalStoreApp class that takes its input DROP and archives it in
    an NGAS server. It currently deals with non-container DROPs only.

    The archiving to NGAS occurs through the framework and not by spawning a
    new NGAS client process. This way we can read the different storage types
    supported by the framework, and not only filesystem objects.
    '''
    compontent_meta = dlg_component('NgasArchivingApp', 'An ExternalStoreApp class that takes its input DROP and archives it in '
                                    'an NGAS server. It currently deals with non-container DROPs only.',
                                    [dlg_batch_input('binary/*', [])],
                                    [dlg_batch_output('binary/*', [])],
                                    [dlg_streaming_input('binary/*')])

    ngasSrv = dlg_string_param('NGAS hostname', 'localhost')
    ngasPort = dlg_int_param('NGAS Port', 7777)
    ngasConnectTimeout = dlg_float_param('Connect Timeout', 2.)
    ngasTimeout = dlg_float_param('Timeout', 2.)

    def initialize(self, **kwargs):
        super(NgasArchivingApp, self).initialize(**kwargs)

    def store(self, inDrop):
        if isinstance(inDrop, ContainerDROP):
            raise Exception("ContainerDROPs are not supported as inputs for this application")

        size = -1 if inDrop.size is None else inDrop.size
        try:
            ngasIO = NgasIO(self.ngasSrv, inDrop.uid, self.ngasPort, self.ngasConnectTimeout, self.ngasTimeout, size)
        except ImportError:
            ngasIO = NgasLiteIO(self.ngasSrv, inDrop.uid, self.ngasPort, self.ngasConnectTimeout, self.ngasTimeout, size)

        ngasIO.open(OpenMode.OPEN_WRITE)

        # Copy in blocks of 4096 bytes
        with DROPFile(inDrop) as f:
            while True:
                buff = f.read(4096)
                ngasIO.write(buff)
                if len(buff) != 4096:
                    break
        ngasIO.close()