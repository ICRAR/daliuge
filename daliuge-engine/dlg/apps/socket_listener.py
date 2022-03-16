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
Module containing the SocketListenerApp, a simple application that listens for
incoming data in a TCP socket.
"""

import contextlib
import logging
import socket

from ..ddap_protocol import DROPRel, DROPLinkType
from ..drop import BarrierAppDROP
from ..exceptions import InvalidRelationshipException
from ..meta import (
    dlg_string_param,
    dlg_int_param,
    dlg_bool_param,
    dlg_component,
    dlg_batch_output,
    dlg_batch_input,
    dlg_streaming_input,
)

logger = logging.getLogger(__name__)


##
# @brief SocketListenerApp
# @details A BarrierAppDROP that listens on a socket for data. The server-side
# socket expects only one client, and assumes that the client will close the
# connection after all its data has been sent.
# This application expects no input DROPs, and therefore raises an
# exception whenever one is added. On the output side, one or more outputs
# can be specified with the restriction that they are not ContainerDROPs
# so data can be written into them through the framework.
# @par EAGLE_START
# @param category PythonApp
# @param tag daliuge
# @param[in] cparam/appclass Application Class/dlg.apps.socket_listener.SocketListener/String/readonly/False//False/
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
# @param[in] aparam/host Host/127.0.0.1/String/readwrite/False//False/
#     \~English Host address
# @param[in] aparam/port Port/1111/Integer/readwrite/False//False/
#     \~English Host port
# @param[in] aparam/bufsize Buffer Size/4096/String/readwrite/False//False/
#     \~English Receive buffer size
# @param[in] aparam/reuseAddr Reuse Address/False/Boolean/readwrite/False//False/
#     \~English
# @param[out] port/data Data/String/
# @par EAGLE_END
class SocketListenerApp(BarrierAppDROP):
    """
    A BarrierAppDROP that listens on a socket for data. The server-side
    socket expects only one client, and assumes that the client will close the
    connection after all its data has been sent.

    This application expects no input DROPs, and therefore raises an
    exception whenever one is added. On the output side, one or more outputs
    can be specified with the restriction that they are not ContainerDROPs
    so data can be written into them through the framework.
    """

    _dryRun = False

    component_meta = dlg_component(
        "SocketListenerApp",
        "A BarrierAppDROP that listens on a socket for data",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

    host = dlg_string_param("host", "127.0.0.1")
    port = dlg_int_param("port", 1111)
    bufsize = dlg_int_param("bufsize", 4096)
    reuseAddr = dlg_bool_param("reuseAddr", False)

    def initialize(self, **kwargs):
        super(SocketListenerApp, self).initialize(**kwargs)

    def run(self):
        # At least one output should have been added
        outs = self.outputs
        if len(outs) < 1:
            raise Exception("At least one output should have been added to %r" % self)

        # Don't really listen for data if running dry
        if self._dryRun:
            return

        # Accept one connection at most
        serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        with contextlib.closing(serverSocket):
            if self.reuseAddr:
                serverSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            serverSocket.bind((self.host, self.port))
            serverSocket.listen(1)
            logger.debug(
                "Listening for a TCP connection on %s:%d", self.host, self.port
            )
            clientSocket, address = serverSocket.accept()
            logger.info("Accepted connection from %s:%d", address[0], address[1])

        # Simply write the data we receive into our outputs
        n = 0
        with contextlib.closing(clientSocket):
            while True:
                data = clientSocket.recv(self.bufsize)
                if not data:
                    break
                n += len(data)
                for out in outs:
                    out.write(data)
        logger.info("TCP receiver received %d bytes of data", n)

    # Avoid inputs
    def addInput(self, inputDrop, back=True):
        raise InvalidRelationshipException(
            DROPRel(inputDrop.uid, DROPLinkType.INPUT, self.uid),
            "SocketListenerApp should have no inputs",
        )

    def addStreamingInput(self, streamingInputDrop, back=True):
        raise InvalidRelationshipException(
            DROPRel(streamingInputDrop.uid, DROPLinkType.STREAMING_INPUT, self.uid),
            "SocketListenerApp should have no inputs",
        )
