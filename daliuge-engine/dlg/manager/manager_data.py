#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2024
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
This module contains classes and helper-methods to support the various manager classes
"""

from dataclasses import dataclass

from dlg import constants

from enum import IntEnum


class NodeProtocolPosition(IntEnum):
    HOST = 0
    PORT = 1
    RPC_PORT = 2
    EVENTS_PORT = 3


class Node:
    """
    Class for encapsulating compute node information to standardise
    inter-node communication.
    """

    def __init__(self, host: str):
        chunks = host.split(':')
        num_chunks = len(chunks)
        self.host = constants.NODE_DEFAULT_HOSTNAME
        self.port = constants.NODE_DEFAULT_REST_PORT
        self.rpc_port = constants.NODE_DEFAULT_RPC_PORT
        self.events_port = constants.NODE_DEFAULT_RPC_PORT
        self._rest_port_specified = False

        if num_chunks >= 1:
            self.host = chunks[NodeProtocolPosition.HOST]
        if num_chunks >= 2:
            self.port = int(chunks[NodeProtocolPosition.PORT])
            self._rest_port_specified = True
        if num_chunks >= 3:
            self.rpc_port = int(chunks[NodeProtocolPosition.RPC_PORT])
        if num_chunks >= 4:
            self.events_port = int(chunks[NodeProtocolPosition.EVENTS_PORT])

    def serialize(self):
        """
        Convert to the expect string representation of our Node using the
        following 'protocol':

            "host:port:rpc_port:event_port"

        :return: str
        """
        return f"{self.host}:{self.port}:{self.rpc_port}:{self.events_port}"

    @property
    def rest_port_specified(self):
        """
        Returns True if we specified a Node REST port when passing the list of nodes to
        the DIM at startup.
        """
        return self._rest_port_specified

    def __str__(self):
        """
        Make our serialized Node the string.
        :return: str
        """
        return self.serialize()

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        if isinstance(other, Node):
            return hash(self) == hash(other)
        if isinstance(other, str):
            return hash(self) == hash(other)

    def __hash__(self):
        return hash(str(self))
