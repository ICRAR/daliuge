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


logger = logging.getLogger(__name__)

class SocketListenerApp(BarrierAppDROP):
    '''
    A BarrierAppDROP that listens on a socket for data. The server-side
    socket expects only one client, and assumes that the client will close the
    connection after all its data has been sent.

    This application expects no input DROPs, and therefore raises an
    exception whenever one is added. On the output side, one or more outputs
    can be specified with the restriction that they are not ContainerDROPs
    so data can be written into them through the framework.
    '''

    _dryRun = False

    def initialize(self, **kwargs):
        super(SocketListenerApp, self).initialize(**kwargs)

        host = None
        port = None
        if 'host' in kwargs:
            host = kwargs.pop('host')
        if 'port' in kwargs:
            port = int(kwargs.pop('port'))
        if not host:
            host = '127.0.0.1'
        if not port:
            port = 1111

        self._host = host
        self._port = port
        self._reuseAddr = self._getArg(kwargs, 'reuseAddr', False)

    def run(self):

        # At least one output should have been added
        outs = self.outputs
        if len(outs) < 1:
            raise Exception('At least one output should have been added to %r' % (self))

        # Don't really listen for data if running dry
        if self._dryRun:
            return

        # Accept one connection at most
        serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        with contextlib.closing(serverSocket):
            if self._reuseAddr:
                serverSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            serverSocket.bind((self.host, self.port))
            serverSocket.listen(1)
            logger.debug('Listening for a TCP connection on %s:%d', self.host, self.port)
            clientSocket, address = serverSocket.accept()
            logger.info('Accepted connection from %s:%d', address[0], address[1])

        # Simply write the data we receive into our outputs
        with contextlib.closing(clientSocket):
            while True:
                data = clientSocket.recv(4096)
                if not data:
                    break
                for out in outs:
                    out.write(data)

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    # Avoid inputs
    def addInput(self, inputDrop, back=True):
        raise InvalidRelationshipException(DROPRel(inputDrop.uid, DROPLinkType.INPUT, self.uid),
                                           "SocketListenerApp should have no inputs")
    def addStreamingInput(self, streamingInputDrop, back=True):
        raise InvalidRelationshipException(DROPRel(streamingInputDrop.uid, DROPLinkType.STREAMING_INPUT, self.uid),
                                           "SocketListenerApp should have no inputs")