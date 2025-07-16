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
from dlg.apps.app_base import BarrierAppDROP
from dlg.data.drops.container import ContainerDROP
from ..droputils import DROPFile
from dlg.data.io import NgasIO, OpenMode, NgasLiteIO
from ..meta import (
    dlg_string_param,
    dlg_int_param,
    dlg_component,
    dlg_batch_input,
    dlg_batch_output,
    dlg_streaming_input,
)

logger = logging.getLogger(f"dlg.{__name__}")


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
# @param category DALiuGEApp
# @param tag daliuge
# @param ngasSrv localhost/String/ApplicationArgument/NoPort/ReadWrite//False/False/URL of the NGAS Server
# @param ngasPort 7777/Integer/ApplicationArgument/NoPort/ReadWrite//False/False/"TCP/IP Port on the NGAS Server"
# @param ngasMime "application/octet-stream"/String/ApplicationArgument/NoPort/ReadWrite//False/False/Mime-type of the NGAS payload
# @param ngasTimeout 2/Integer/ApplicationArgument/NoPort/ReadOnly//False/False/Archiving request timeout
# @param ngasConnectTimeout 2/Integer/ApplicationArgument/NoPort/ReadOnly//False/False/NGAS Server connection timeout
# @param fileObject /Object.File/ApplicationArgument/InputPort/ReadWrite//False/False/Input File Object
# @param log_level "NOTSET"/Select/ComponentParameter/NoPort/ReadWrite/NOTSET,DEBUG,INFO,WARNING,ERROR,CRITICAL/False/False/Set the log level for this drop
# @param dropclass dlg.apps.archiving.NgasArchivingApp/String/ComponentParameter/NoPort/ReadOnly//False/False/Application class
# @param base_name archiving/String/ComponentParameter/NoPort/ReadOnly//False/False/Base name of application class
# @param execution_time 5/Float/ConstraintParameter/NoPort/ReadOnly//False/False/Estimated execution time
# @param num_cpus 1/Integer/ConstraintParameter/NoPort/ReadOnly//False/False/Number of cores used
# @param group_start False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Is this node the start of a group?
# @param input_error_threshold 0/Integer/ComponentParameter/NoPort/ReadWrite//False/False/the allowed failure rate of the inputs (in percent), before this component goes to ERROR state and is not executed
# @param n_tries 1/Integer/ComponentParameter/NoPort/ReadWrite//False/False/Specifies the number of times the 'run' method will be executed before finally giving up
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


    def store(self, inputDrop):
        logger.debug("NGAS Server %s", self.ngasSrv)
        logger.debug("NGAS Port %s", self.ngasPort)

        if isinstance(inputDrop, ContainerDROP):
            raise Exception(
                "ContainerDROPs are not supported as inputs for this application"
            )

        if inputDrop.size is None or inputDrop.size < 0:
            logger.error(
                "NGAS requires content-length to be know, but the given input does not provide a size."
            )
            size = None
        else:
            size = inputDrop.size
            logger.debug("Content-length %s", size)
        try:
            ngasIO = NgasIO(
                self.ngasSrv,
                inputDrop.uid,
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
                inputDrop.uid,
                self.ngasPort,
                self.ngasConnectTimeout,
                self.ngasTimeout,
                size,
                mimeType=self.ngasMime,
            )

        ngasIO.open(OpenMode.OPEN_WRITE)

        # Copy in blocks of 4096 bytes
        with DROPFile(inputDrop) as f:
            while True:
                buff = f.read(4096)
                ngasIO.write(buff)
                if len(buff) != 4096:
                    break
        ngasIO.close()
