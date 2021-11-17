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

# Constants used throughout the manager package
DEFAULT_PORTS = {
    "NODE_DEFAULT_REST_PORT": 8000,
    "ISLAND_DEFAULT_REST_PORT": 8001,
    "MASTER_DEFAULT_REST_PORT": 8002,
    "REPLAY_DEFAULT_REST_PORT": 8500,
    "DAEMON_DEFAULT_REST_PORT": 9000,
    "NODE_DEFAULT_EVENTS_PORT": 5555,
    "NODE_DEFAULT_RPC_PORT": 6666,
}

#  just for backwards compatibility
NODE_DEFAULT_REST_PORT = DEFAULT_PORTS["NODE_DEFAULT_REST_PORT"]
ISLAND_DEFAULT_REST_PORT = DEFAULT_PORTS["ISLAND_DEFAULT_REST_PORT"]
MASTER_DEFAULT_REST_PORT = DEFAULT_PORTS["MASTER_DEFAULT_REST_PORT"]

REPLAY_DEFAULT_REST_PORT = DEFAULT_PORTS["REPLAY_DEFAULT_REST_PORT"]

DAEMON_DEFAULT_REST_PORT = DEFAULT_PORTS["DAEMON_DEFAULT_REST_PORT"]


# Others ports used by the Node Managers
NODE_DEFAULT_EVENTS_PORT = DEFAULT_PORTS["NODE_DEFAULT_EVENTS_PORT"]
NODE_DEFAULT_RPC_PORT = DEFAULT_PORTS["NODE_DEFAULT_RPC_PORT"]
