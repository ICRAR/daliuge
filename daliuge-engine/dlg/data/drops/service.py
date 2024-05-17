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
import os.path
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
        self._store = None

    @abstractmethod
    def setup(self):
        """

        """
        pass

    @abstractmethod
    def tearDown(self):
        """

        """

    # @abstractmethod
    # def isPersist(self) -> bool:
    #     """
    #
    #     """
    #
    # @property
    # @abstractmethod
    # def store(self)->str:
    #     """
    #
    #     """


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
    """

    """

    def initialize(self, **kwargs):
        super().initialize(**kwargs)
        # self._storeclass = kwargs['storeclass']
        # self._storeargs = kwargs['dirname']

    def setup(self):
        print("Running the setup for the service drop...")
        sleep(5)
        self.setCompleted()
        # try:
        #     self._store =  DirectoryStore("/tmp/")

        # except Exception as err:
        #     logger.warning("%s occurred when constructing DirectoryStore", err
        #     pass

    def tearDown(self):
        pass

    def isPersist(self) -> bool:

        return self._persist

    @property
    def store(self):
        try:
            dir = DirectoryStore("/tmp/")
            # self.setCompleted()
            return dir
        except Exception as err:
            logger.warning("%s occurred when constructing DirectoryStore", err)
    def run(self):
        sleep(15)
