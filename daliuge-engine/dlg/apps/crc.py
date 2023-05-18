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
"""
Module containing an example application that calculates a CRC value
"""

from ..apps.app_base import BarrierAppDROP, AppDROP
from dlg.ddap_protocol import AppDROPStates

from ..meta import (
    dlg_component,
    dlg_batch_input,
    dlg_batch_output,
    dlg_streaming_input,
)

try:
    from crc32c import crc32c as crc32  # @UnusedImport
except:
    from binascii import crc32  # @Reimport


class CRCApp(BarrierAppDROP):
    """
    An BarrierAppDROP that calculates the CRC of the single DROP it
    consumes. It assumes the DROP being consumed is not a container.
    This is a simple example of an BarrierAppDROP being implemented, and
    not something really intended to be used in a production system
    """

    component_meta = dlg_component(
        "CRCApp",
        "A BarrierAppDROP that calculates the "
        "CRC of the single DROP it consumes",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

    def run(self):
        if len(self.inputs) != 1:
            raise Exception("This application read only from one DROP")
        if len(self.outputs) != 1:
            raise Exception("This application writes only one DROP")

        inputDrop = self.inputs[0]
        outputDrop = self.outputs[0]

        bufsize = 4 * 1024**2
        desc = inputDrop.open()
        buf = inputDrop.read(desc, bufsize)
        crc = 0
        while buf:
            crc = crc32(buf, crc)
            buf = inputDrop.read(desc, bufsize)
        inputDrop.close(desc)

        # Rely on whatever implementation we decide to use
        # for storing our data
        outputDrop.write(str(crc).encode("utf8"))


##
# @brief CRCStreamApp
# @details Calculate CRC in the streaming mode
# i.e. A "streamingConsumer" of its predecessor in the graph
# @par EAGLE_START
# @param category PythonApp
# @param tag daliuge
# @param dropclass Application Class/dlg.apps.crc.CRCStreamApp/String/ComponentParameter/readonly//False/False/Application class
# @param input_parser Input Parser/pickle/Select/ApplicationArgument/readwrite/raw,pickle,eval,npy,path,dataurl/False/False/Input port parsing technique
# @param output_parser Output Parser/pickle/Select/ApplicationArgument/readwrite/raw,pickle,eval,npy,path,dataurl/False/False/Output port parsing technique
# @param execution_time Execution Time/5/Float/ComponentParameter/readonly//False/False/Estimated execution time
# @param num_cpus No. of CPUs/1/Integer/ComponentParameter/readonly//False/False/Number of cores used
# @param group_start Group start/False/Boolean/ComponentParameter/readwrite//False/False/Is this node the start of a group?
# @param input_error_threshold "Input error rate (%)"/0/Integer/ComponentParameter/readwrite//False/False/the allowed failure rate of the inputs (in percent), before this component goes to ERROR state and is not executed
# @param n_tries Number of tries/1/Integer/ComponentParameter/readwrite//False/False/Specifies the number of times the 'run' method will be executed before finally giving up
# @param data Data//String/OutputPort/readwrite//False/False/Input data stream
# @par EAGLE_END
class CRCStreamApp(AppDROP):
    """
    Calculate CRC in the streaming mode
    i.e. A "streamingConsumer" of its predecessor in the graph
    """

    component_meta = dlg_component(
        "CRCStreamApp",
        "Calculate CRC in the streaming mode.",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

    def initialize(self, **kwargs):
        super(CRCStreamApp, self).initialize(**kwargs)
        self._crc = 0

    def dataWritten(self, uid, data):
        self.execStatus = AppDROPStates.RUNNING
        self._crc = crc32(data, self._crc)

    def dropCompleted(self, uid, status):
        outputDrop = self.outputs[0]
        outputDrop.write(str(self._crc).encode("utf8"))
        self.execStatus = AppDROPStates.FINISHED
        self._notifyAppIsFinished()
