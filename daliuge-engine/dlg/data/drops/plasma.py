#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia
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
import binascii
import os

import numpy as np
from pyarrow import plasma as plasma

from dlg.data.drops.data_base import DataDROP
from dlg.data.io import PlasmaIO, PlasmaFlightIO
from dlg.meta import dlg_string_param, dlg_bool_param


##
# @brief Plasma
# @details An object in a Apache Arrow Plasma in-memory object store
# @par EAGLE_START
# @param category Plasma
# @param tag daliuge
# @param data_volume Data volume/5/Float/ComponentParameter/readwrite//False/False/Estimated size of the data contained in this node
# @param group_end Group end/False/Boolean/ComponentParameter/readwrite//False/False/Is this node the end of a group?
# @param plasma_path Plasma Path//String/ComponentParameter/readwrite//False/False/Path to the local plasma store
# @param object_id Object Id//String/ComponentParameter/readwrite//False/False/PlasmaId of the object for all compute nodes
# @param use_staging Use Staging/False/Boolean/ComponentParameter/readwrite//False/False/Enables writing to a dynamically resizeable staging buffer
# @param dropclass dropclass/dlg.data.drops.plasma.PlasmaDROP/String/ComponentParameter/readwrite//False/False/Drop class
# @param dummy dummy//Object/InputPort/readwrite//False/False/Dummy input port
# @param dummy dummy//Object/OutputPort/readwrite//False/False/Dummy output port
# @par EAGLE_END
class PlasmaDROP(DataDROP):
    """
    A DROP that points to data stored in a Plasma Store
    """

    object_id: bytes = dlg_string_param("object_id", None)
    plasma_path: str = dlg_string_param("plasma_path", "/tmp/plasma")
    use_staging: bool = dlg_bool_param("use_staging", False)

    def initialize(self, **kwargs):
        super().initialize(**kwargs)
        self.plasma_path = os.path.expandvars(self.plasma_path)
        if self.object_id is None:
            self.object_id = (
                np.random.bytes(20)
                if len(self.uid) != 20
                else self.uid.encode("ascii")
            )
        elif isinstance(self.object_id, str):
            self.object_id = self.object_id.encode("ascii")

    def getIO(self):
        return PlasmaIO(
            plasma.ObjectID(self.object_id),
            self.plasma_path,
            expected_size=self._expectedSize,
            use_staging=self.use_staging,
        )

    @property
    def dataURL(self) -> str:
        return "plasma://%s" % (
            binascii.hexlify(self.object_id).decode("ascii")
        )


##
# @brief PlasmaFlight
# @details An Apache Arrow Flight server providing distributed access
# to a Plasma in-memory object store
# @par EAGLE_START
# @param category PlasmaFlight
# @param tag daliuge
# @param data_volume Data volume/5/Float/ComponentParameter/readwrite//False/False/Estimated size of the data contained in this node
# @param group_end Group end/False/Boolean/ComponentParameter/readwrite//False/False/Is this node the end of a group?
# @param plasma_path Plasma Path//String/ComponentParameter/readwrite//False/False/Path to the local plasma store
# @param object_id Object Id//String/ComponentParameter/readwrite//False/False/PlasmaId of the object for all compute nodes
# @param flight_path Flight Path//String/ComponentParameter/readwrite//False/False/IP and flight port of the drop owner
# @param dummy dummy//Object/InputPort/readwrite//False/False/Dummy input port
# @param dummy dummy//Object/OutputPort/readwrite//False/False/Dummy output port
# @par EAGLE_END
class PlasmaFlightDROP(DataDROP):
    """
    A DROP that points to data stored in a Plasma Store
    """

    object_id: bytes = dlg_string_param("object_id", None)
    plasma_path: str = dlg_string_param("plasma_path", "/tmp/plasma")
    flight_path: str = dlg_string_param("flight_path", None)
    use_staging: bool = dlg_bool_param("use_staging", False)

    def initialize(self, **kwargs):
        super().initialize(**kwargs)
        self.plasma_path = os.path.expandvars(self.plasma_path)
        if self.object_id is None:
            self.object_id = (
                np.random.bytes(20)
                if len(self.uid) != 20
                else self.uid.encode("ascii")
            )
        elif isinstance(self.object_id, str):
            self.object_id = self.object_id.encode("ascii")

    def getIO(self):
        return PlasmaFlightIO(
            plasma.ObjectID(self.object_id),
            self.plasma_path,
            flight_path=self.flight_path,
            expected_size=self._expectedSize,
            use_staging=self.use_staging,
        )

    @property
    def dataURL(self) -> str:
        return "plasmaflight://%s" % (
            binascii.hexlify(self.object_id).decode("ascii")
        )
