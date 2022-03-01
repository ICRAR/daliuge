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
import logging
from ..drop import BarrierAppDROP, ContainerDROP
from ..droputils import DROPFile
from ..io import NgasIO, OpenMode, NgasLiteIO
from ..meta import (
    dlg_string_param,
    dlg_float_param,
    dlg_int_param,
    dlg_component,
    dlg_batch_input,
    dlg_batch_output,
    dlg_streaming_input,
)

logger = logging.getLogger(__name__)


class ExternalStoreApp(BarrierAppDROP):
    """
    An application that takes its input DROP (which must be one, and only
    one) and creates a copy of it in a completely external store, from the point
    of view of the DALiuGE framework.

    Because this application copies the data to an external location, it also
    shouldn't contain any output, making it a leaf node of the physical graph
    where it resides.
    """

    component_meta = dlg_component(
        "ExternalStoreApp",
        "An application that takes its input DROP (which must be one, and only one) "
        "and creates a copy of it in a completely external store, from the point "
        "of view of the DALiuGE framework.",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

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


##
# @brief NgasArchivingApp
# @details Takes an input and archives it in an NGAS server.
# @par EAGLE_START
# @param category PythonApp
# @param tag daliuge
# @param[in] cparam/appclass Application class/dlg.apps.archiving.NgasArchivingApp/String/readonly/False//False/
#     \~English Application class
# @param[in] cparam/execution_time Execution Time/5/Float/readonly/False//False/
#     \~English Estimated execution time
# @param[in] cparam/num_cpus No. of CPUs/1/Integer/readonly/False//False/
#     \~English Number of cores used
# @param[in] cparam/group_start Group start/False/Boolean/readwrite/False//False/
#     \~English Is this node the start of a group?
# @param[in] cparam/input_error_threshold "Input error rate (%)"/0/Integer/readwrite/False//False/
#     \~English the allowed failure rate of the inputs (in percent), before this component goes to ERROR state and is not executed
# @param[in] cparam/n_tries Number of tries/1/Integer/readwrite/False//False/
#     \~English Specifies the number of times the 'run' method will be executed before finally giving up
# @param[in] aparam/ngasSrv NGAS Server URL/localhost/String/readwrite/False//False/
#     \~English URL of the NGAS Server
# @param[in] aparam/ngasPort NGAS Server Port/7777/Integer/readwrite/False//False/
#     \~English TCP/IP Port on the NGAS Server
# @param[in] aparam/ngasMime NGAS Mime Type/"application/octet-stream"/String/readwrite/False//False/
#     \~English Mime-type of the NGAS payload
# @param[in] aparam/ngasTimeout NGAS Server Timeout/2/Integer/readonly/False//False/
#     \~English Archiving request timeout
# @param[in] aparam/ngasConnectTimeout NGAS Server Connect Timeout/2/Integer/readonly/False//False/
#     \~English NGAS Server connection timeout
# @param[in] port/fileObject File Object/File/
#     \~English Input File Object
# @par EAGLE_END
class NgasArchivingApp(ExternalStoreApp):
    """
    An ExternalStoreApp class that takes its input DROP and archives it in
    an NGAS server. It currently deals with non-container DROPs only.

    The archiving to NGAS occurs through the framework and not by spawning a
    new NGAS client process. This way we can read the different storage types
    supported by the framework, and not only filesystem objects.
    """

    component_meta = dlg_component(
        "NgasArchivingApp",
        "An ExternalStoreApp class that takes its input DROP and archives it in "
        "an NGAS server. It currently deals with non-container DROPs only.",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

    ngasSrv = dlg_string_param("ngasSrv", "localhost")
    ngasPort = dlg_int_param("ngasPort", 7777)
    ngasMime = dlg_string_param("ngasMime", "application/octet-stream")
    ngasTimeout = dlg_int_param("ngasTimeout", 2)
    ngasConnectTimeout = dlg_int_param("ngasConnectTimeout", 2)

    def initialize(self, **kwargs):
        super(NgasArchivingApp, self).initialize(**kwargs)

    def store(self, inDrop):
        logger.debug("NGAS Server %s", self.ngasSrv)
        logger.debug("NGAS Port %s", self.ngasPort)

        if isinstance(inDrop, ContainerDROP):
            raise Exception(
                "ContainerDROPs are not supported as inputs for this application"
            )

        if inDrop.size is None or inDrop.size < 0:
            logger.error(
                "NGAS requires content-length to be know, but the given input does not provide a size."
            )
            size = None
        else:
            size = inDrop.size
            logger.debug("Content-length %s", size)
        try:
            ngasIO = NgasIO(
                self.ngasSrv,
                inDrop.uid,
                self.ngasPort,
                self.ngasConnectTimeout,
                self.ngasTimeout,
                size,
                mimeType=self.ngasMime,
            )
        except ImportError:
            logger.warning("NgasIO library not available, falling back to NgasLiteIO.")
            ngasIO = NgasLiteIO(
                self.ngasSrv,
                inDrop.uid,
                self.ngasPort,
                self.ngasConnectTimeout,
                self.ngasTimeout,
                size,
                mimeType=self.ngasMime,
            )

        ngasIO.open(OpenMode.OPEN_WRITE)

        # Copy in blocks of 4096 bytes
        with DROPFile(inDrop) as f:
            while True:
                buff = f.read(4096)
                ngasIO.write(buff)
                if len(buff) != 4096:
                    break
        ngasIO.close()
