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
from .. import remote
from ..drop import BarrierAppDROP


class ScpApp(BarrierAppDROP):
    """
    A BarrierAppDROP that copies the content of its single input onto its
    single output via SSH's scp protocol.

    Because of the nature of the scp protocol, the input and output DROPs
    of this application must both be filesystem-based; i.e., they must be an
    instance of FileDROP or of DirectoryContainer.

    Depending on the physical location of each DROP (this application, and
    its input and outputs) this application will copy data FROM another host or
    TO other host. This application's node must thus coincide with one of the
    two I/O DROPs.
    """

    def initialize(self, **kwargs):
        BarrierAppDROP.initialize(self, **kwargs)
        self._remoteUser = self._getArg(kwargs, 'remoteUser', None)
        self._pkeyPath   = self._getArg(kwargs, 'pkeyPath', None)
        self._timeout    = self._getArg(kwargs, 'timeout', None)

    def run(self):

        # Check inputs/outputs are of a valid type
        for i in self.inputs + self.outputs:
            # The current only way to check if we are handling a FileDROP
            # or a DirectoryContainer is by checking if they have a `path`
            # attribute. Calling `isinstance(i, (FileDROP, DirectoryContainer))`
            # doesn't work because the input/output might be a proxy object
            # that fails the test
            if not hasattr(i, 'path'):
                raise Exception("%r is not supported by the ScpApp" % (i))

        # Only one input and one output are supported
        if len(self.inputs) != 1:
            raise Exception("Only one input is supported by the ScpApp, %d given" % (len(self.inputs)))
        if len(self.outputs) != 1:
            raise Exception("Only one output is supported by the ScpApp, %d given" % (len(self.outputs)))

        inp = self.inputs[0]
        out = self.outputs[0]

        # Input and output must be of the same type
        # See comment above regarding identification of DROP types, and why we
        # can't simply do:
        # if inp.__class__ != out.__class__:
        if hasattr(inp, 'children') != hasattr(out, 'children'):
            raise Exception("Input and output must be of the same type")

        # This app's location must be equal to at least one of the I/O
        if self.node != inp.node and self.node != out.node:
            raise Exception("%r is deployed in a node different from its input AND its output" % (self,))

        # See comment above regarding identification of File/Directory DROPs and
        # why we can't simply do:
        # recursive = isinstance(inp, DirectoryContainer)
        recursive = hasattr(inp, 'children')
        if self.node == inp.node:
            remote.copyTo(out.node, inp.path, remotePath=out.path, recursive=recursive, username=self._remoteUser, pkeyPath=self._pkeyPath, timeout=self._timeout)
        else:
            remote.copyFrom(inp.node, inp.path, localPath=out.path, recursive=recursive, username=self._remoteUser, pkeyPath=self._pkeyPath, timeout=self._timeout)