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
from dfms import remote
from dfms.data_object import BarrierAppDataObject, FileDataObject, \
    DirectoryContainer


class ScpApp(BarrierAppDataObject):
    """
    A BarrierAppDataObject that copies the content of its single input onto its
    single output via SSH's scp protocol.

    Because of the nature of the scp protocol, the input and output DataObjects
    of this application must both be filesystem-based; i.e., they must be an
    instance of FileDataObject or of DirectoryContainer.

    Depending on the physical location of each DataObject (this application, and
    its input and outputs) this application will copy data FROM another host or
    TO other host. This application's node must thus coincide with one of the
    two I/O DataObjects.
    """

    def initialize(self, **kwargs):
        BarrierAppDataObject.initialize(self, **kwargs)
        self._remoteUser = self._getArg(kwargs, 'remoteUser', None)

    def run(self):

        # Check inputs/outputs are of a valid type
        for i in self.inputs + self.outputs:
            if not isinstance(i, (FileDataObject, DirectoryContainer)):
                raise Exception("%r is not supported by the ScpApp" % (i))

        # Only one input and one output are supported
        if len(self.inputs) != 1:
            raise Exception("Only one input is supported by the ScpApp, %d given" % (len(self.inputs)))
        if len(self.outputs) != 1:
            raise Exception("Only one output is supported by the ScpApp, %d given" % (len(self.outputs)))

        # Input and output must be of the same type
        inp = self.inputs[0]
        out = self.outputs[0]
        if inp.__class__ != out.__class__:
            raise Exception("Input and output must be of the same type")

        # This app's location must be equal to at least one of the I/O
        if self.node != inp.node and self.node != out.node:
            raise Exception("%r is deployed in a node different from its input AND its output" % (self,))

        recursive = isinstance(inp, DirectoryContainer)
        if self.node == inp.node:
            remote.copyTo(self._toHost, inp.path, remotePath=out.path, recursive=recursive, username=self._remoteUser)
        else:
            remote.copyFrom(self._toHost, inp.path, localPath=out.path, recursive=recursive, username=self._remoteUser)