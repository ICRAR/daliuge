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
import logging
from abc import abstractmethod
from dlg.data.drops.data_base import DataDROP
from dlg.data.drops.container import ContainerDROP
from dlg.apps.app_base import BarrierAppDROP
from dlg.data.drops.store import DirectoryStore
from time import sleep

from dlg.data.io import DataIO

logger = logging.getLogger(__name__)

class ServiceDROP(BarrierAppDROP):
    def initialize(self, **kwargs):
        super().initialize(**kwargs)
        self._storeclass = None

    @abstractmethod
    def connectService(self):
        """

        """
        pass

    @abstractmethod
    def disconnectService(self):
        """

        """

    @abstractmethod
    def isPersist(self) -> bool:
        """

        """

    @abstractmethod
    def createStore(self)->str:
        return self._storeclass

##
# @brief DirectoryService
# @details A Service template drop
# @par EAGLE_START
# @param category Plasma
# @param categorytype Service
# @param tag daliuge
# @param directory  /String/ComponentParameter/NoPort/ReadWrite//False/False/
# @param dropclass
#   dlg.data.drops.service.DirectoryServiceDROP/String/ComponentParameter/NoPort/ReadWrite//False/False/Drop class
# @param storeclass dlg.data.drops.store.DirectoryStore/String
# /ComponentParameter/NoPort/ReadWrite//False/False/class
# @param storeargs
# @par EAGLE_END
class DirectoryServiceDROP(ServiceDROP):

    def initialize(self, **kwargs):
        super().initialize(**kwargs)
        # self._storeclass = kwargs['storeclass']
        # self._storeargs = kwargs['dirname']

    def connectService(self):
        pass

    def disconnectService(self):
        pass

    def isPersist(self) -> bool:
        return self._persist

    def createStore(self):
        try:
            dir = DirectoryStore("/tmp/")
            # self.setCompleted()
            return dir
        except Exception as err:
            logger.warning("%s occurred when constructing DirectoryStore", err)
    def run(self):
        sleep(15)

#
# ##
# # @brief Scatter
# # @details A Scatter template drop
# # @par EAGLE_START
# # @param category Scatter
# # @param categorytype Construct
# # @param tag template
# # @param num_of_copies 4/Integer/ConstructParameter/NoPort/ReadWrite//False/False/Specifies the number of replications of the content of the scatter construct
# # @par EAGLE_END
# class NGASServiceDrop(ServiceDrop):
#     pass
#
