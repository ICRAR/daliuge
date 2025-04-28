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
from dlg.remote import copyTo, copyFrom
from dlg.apps.app_base import BarrierAppDROP
from dlg.data.drops.data_base import NullDROP
from dlg.data.drops.container import ContainerDROP
from dlg.data.drops.rdbms import RDBMSDrop
from dlg.data.drops.memory import InMemoryDROP, SharedMemoryDROP
from dlg.data.drops.ngas import NgasDROP
from dlg.meta import (
    dlg_string_param,
    dlg_float_param,
    dlg_component,
    dlg_batch_input,
    dlg_batch_output,
    dlg_streaming_input,
)


##
# @brief ScpApp
# @details A BarrierAppDROP that copies the content of its single input onto its single output via SSH's scp protocol.
# @par EAGLE_START
# @param category DALiuGEApp
# @param tag daliuge
# @param remoteUser /String/ApplicationArgument/NoPort/ReadWrite//False/False/Remote user address
# @param pkeyPath /String/ApplicationArgument/NoPort/ReadWrite//False/False/Private key path
# @param timeout 60/Float/ApplicationArgument/NoPort/ReadWrite//False/False/Connection timeout in seconds
# @param file /Object.PathBasedDrop/ApplicationArgument/InputOutput/ReadWrite//False/False/File path
# @param log_level "NOTSET"/Select/ComponentParameter/NoPort/ReadWrite/NOTSET,DEBUG,INFO,WARNING,ERROR,CRITICAL/False/False/Set the log level for this drop
# @param dropclass dlg.apps.scp.ScpApp/String/ComponentParameter/NoPort/ReadOnly//False/False/Application class
# @param base_name scp/String/ComponentParameter/NoPort/ReadOnly//False/False/Base name of application class
# @param execution_time 5/Float/ConstraintParameter/NoPort/ReadOnly//False/False/Estimated execution time
# @param num_cpus 1/Integer/ConstraintParameter/NoPort/ReadOnly//False/False/Number of cores used
# @param group_start False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Is this node the start of a group?
# @param input_error_threshold 0/Integer/ComponentParameter/NoPort/ReadWrite//False/False/the allowed failure rate of the inputs (in percent), before this component goes to ERROR state and is not executed
# @param n_tries 1/Integer/ComponentParameter/NoPort/ReadWrite//False/False/Specifies the number of times the 'run' method will be executed before finally giving up
# @par EAGLE_END
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

    component_meta = dlg_component(
        "ScpApp",
        "A BarrierAppDROP that copies the content of its single "
        "input onto its single output via SSHs scp protocol.",
        [
            dlg_batch_input(
                "binary/*",
                [
                    NgasDROP,
                    InMemoryDROP,
                    SharedMemoryDROP,
                    NullDROP,
                    RDBMSDrop,
                    ContainerDROP,
                ],
            )
        ],
        [
            dlg_batch_output(
                "binary/*",
                [
                    NgasDROP,
                    InMemoryDROP,
                    SharedMemoryDROP,
                    NullDROP,
                    RDBMSDrop,
                    ContainerDROP,
                ],
            )
        ],
        [dlg_streaming_input("binary/*")],
    )

    remoteUser = dlg_string_param("remoteUser", None)
    pkeyPath = dlg_string_param("pkeyPath", None)
    timeout = dlg_float_param("timeout", None)

    def initialize(self, **kwargs):
        BarrierAppDROP.initialize(self, **kwargs)

    def run(self):
        # Check inputs/outputs are of a valid type
        for i in self.inputs + self.outputs:
            # The current only way to check if we are handling a FileDROP
            # or a DirectoryContainer is by checking if they have a `path`
            # attribute. Calling `isinstance(i, (FileDROP, DirectoryContainer))`
            # doesn't work because the input/output might be a proxy object
            # that fails the test
            if not hasattr(i, "path"):
                raise Exception("%r is not supported by the ScpApp" % (i))

        # Only one input and one output are supported
        if len(self.inputs) != 1:
            raise Exception(
                "Only one input is supported by the ScpApp, %d given"
                % (len(self.inputs))
            )
        if len(self.outputs) != 1:
            raise Exception(
                "Only one output is supported by the ScpApp, %d given"
                % (len(self.outputs))
            )

        inp = self.inputs[0]
        out = self.outputs[0]

        # Input and output must be of the same type
        # See comment above regarding identification of DROP types, and why we
        # can't simply do:
        # if inp.__class__ != out.__class__:
        if hasattr(inp, "children") != hasattr(out, "children"):
            raise Exception("Input and output must be of the same type")

        # This app's location must be equal to at least one of the I/O
        if self.node != inp.node and self.node != out.node:
            raise Exception(
                "%r is deployed in a node different from its input AND its output"
                % (self,)
            )

        # See comment above regarding identification of File/Directory DROPs and
        # why we can't simply do:
        # recursive = isinstance(inp, DirectoryContainer)
        recursive = hasattr(inp, "children")
        if self.node == inp.node:
            copyTo(
                out.node,
                inp.path,
                remotePath=out.path,
                recursive=recursive,
                username=self.remoteUser,
                pkeyPath=self.pkeyPath,
                timeout=self.timeout,
            )
        else:
            copyFrom(
                inp.node,
                inp.path,
                localPath=out.path,
                recursive=recursive,
                username=self.remoteUser,
                pkeyPath=self.pkeyPath,
                timeout=self.timeout,
            )
