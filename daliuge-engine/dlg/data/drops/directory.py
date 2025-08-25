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
import logging
import os
import shutil

from dlg.data.drops.data_base import PathBasedDrop, DataDROP
from dlg.data import path_builder
# from dlg.data.drops.container import ContainerDROP
from dlg.exceptions import InvalidDropException, InvalidRelationshipException
from dlg.meta import dlg_bool_param
from dlg.data.io import DirectoryIO

logger = logging.getLogger(f"dlg.{__name__}")


# TODO: This needs some more work
##
# @brief Directory
# @details A DataDROP that represents a filesystem directory. 
# @par EAGLE_START
# @param category Data
# @param tag daliuge
# @param dropclass dlg.data.drops.directory.DirectoryDROP/String/ComponentParameter/NoPort
# /ReadWrite//False/False/Drop class
# @param base_name directorycontainer/String/ComponentParameter/NoPort/ReadOnly//False/False/Base name of application class
# @param data_volume 5/Float/ConstraintParameter/NoPort/ReadWrite//False/False/Estimated size of the data contained in this node
# @param group_end False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Is this node the end of a group?
# @param check_exists True/Boolean/ApplicationArgument/NoPort/ReadWrite//False/False/Perform a check to make sure the file path exists before proceeding with the application
# @param dirname /String/ApplicationArgument/NoPort/ReadWrite//False/False/"Directory name/path"
# @param overwrite_existing /Boolean/ApplicationArgument/NoPort/ReadWrite//False/False/"Overwrite existing directory if exists"
# @param block_skip False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/If set the drop will block a skipping chain until the last producer has finished and is not also skipped.
# @param io /Object/ApplicationArgument/OutputPort/ReadWrite//False/False/Input Output port
# @par EAGLE_END
class DirectoryDROP(PathBasedDrop, DataDROP):
    """
    A DataDROP that represents a filesystem directory. 

    This is used as a proxy for directories on the system, and does not automatically
    append an arbitrary filename to the directory if it does not exist. 
    """

    check_exists = dlg_bool_param("check_exists", True)

    def initialize(self, **kwargs):
        DataDROP.initialize(self, **kwargs)

        if "dirname" not in kwargs:
            raise InvalidDropException(
                self, 'DirectoryContainer needs a "dirname" parameter'
            )

        directory = os.path.expandvars(kwargs["dirname"])

        logger.debug("Checking existence of %s %s", directory, self.check_exists)
        if "check_exists" in kwargs and kwargs["check_exists"] is True:
            if not os.path.isdir(directory):
                raise InvalidDropException(self, f"{directory} is not a directory")

        self._path = self.get_dir(directory)


    def getIO(self): 
        """
        Return DirectoryIO object
        """
        return DirectoryIO(self._path)


    def delete(self):
        shutil.rmtree(self._path)

    def exists(self):
        return os.path.isdir(self._path)

    @property
    def dataURL(self) -> str:
        hostname = os.uname()[1]  # Dervied from FileDROP
        return "file://" + hostname + self._path