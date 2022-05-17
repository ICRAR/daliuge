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
import hashlib
from io import BytesIO
import logging
from typing import Optional

import pyarrow
import pyarrow.flight as paf
import pyarrow.plasma as plasma

logger = logging.getLogger(__name__)


class PlasmaFlightClient:
    """
    Client for accessing plasma-backed arrow flight data server.
    """

    def __init__(
        self,
        socket: str,
        scheme: str = "grpc+tcp",
        connection_args: Optional[dict] = None,
    ):
        """
        Args:
            socket (str): The socket of the local plasma store
            scheme (str, optional): [description]. Defaults to "grpc+tcp".
            connection_args (dict, optional): [description]. Defaults to {}.
        """
        self.plasma_client: plasma.PlasmaClient = plasma.connect(socket)
        self._scheme = scheme
        self._connection_args = {} if connection_args is None else connection_args

    def list_flights(self, location: str):
        """
        Retrieves a list of flights
        """
        flight_client = paf.FlightClient(
            f"{self._scheme}://{location}", **self._connection_args
        )
        return flight_client.list_flights()

    def get_flight(
        self, object_id: plasma.ObjectID, location: str
    ) -> paf.FlightStreamReader:
        """
        Retreives an flight object stream
        """
        descriptor = paf.FlightDescriptor.for_path(
            object_id.binary().hex().encode("utf-8")
        )

        logger.debug(
            f"connecting to {self._scheme}://{location} with descriptor {descriptor}"
        )
        flight_client = paf.FlightClient(
            f"{self._scheme}://{location}", **self._connection_args
        )
        info = flight_client.get_flight_info(descriptor)
        for endpoint in info.endpoints:
            logger.debug("using endpoint locations %s", endpoint.locations)
            return flight_client.do_get(endpoint.ticket)

    def create(self, object_id: plasma.ObjectID, size: int) -> plasma.PlasmaBuffer:
        """
        Creates an empty plasma buffer
        """
        return self.plasma_client.create(object_id, size)

    def seal(self, object_id: plasma.ObjectID):
        """Seals the plasma buffer marking it as readonly"""
        self.plasma_client.seal(object_id)

    def put_raw_buffer(self, data: memoryview, object_id: plasma.ObjectID):
        """Puts  """
        self.plasma_client.put_raw_buffer(data, object_id)

    def get_buffer(
        self, object_id: plasma.ObjectID, owner: Optional[str] = None
    ) -> memoryview:
        """
        Gets the plasma object from the local store if it's available,
        otherwise queries the plasmaflight owner for the object.
        """
        logger.debug("PlasmaFlightClient Get %s", object_id)
        if self.plasma_client.contains(object_id):
            # first check if the local store contains the object
            [buf] = self.plasma_client.get_buffers([object_id])
            return memoryview(buf)
        elif owner is not None:
            # fetch from the specified owner
            reader = self.get_flight(object_id, owner)
            table = reader.read_all()
            output = BytesIO(table.column(0)[0].as_py()).getbuffer()
            # cache buffer in local plasma
            self.put_raw_buffer(output, object_id)
            return output
        else:
            raise KeyError("ObjectID not found", object_id)

    def exists(self, object_id: plasma.ObjectID, owner: Optional[str] = None) -> bool:
        """
        Returns true if the remote plasmaflight server contains the plasma object.
        """
        # check cache
        if self.plasma_client.contains(object_id):
            return True
        # check remote
        if owner is not None:
            client = paf.FlightClient(
                f"{self._scheme}://{owner}", **self._connection_args
            )
            try:
                info = client.get_flight_info(
                    paf.FlightDescriptor.for_path(
                        object_id.binary().hex().encode("utf-8")
                    )
                )
                return True
            except paf.FlightError:
                return False
        return False
