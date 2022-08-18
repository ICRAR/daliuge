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
import os
import shutil

from dlg.data.drops.file import FileDROP
from dlg.ddap_protocol import DROPRel, DROPLinkType
from dlg.drop import PathBasedDrop, ContainerDROP
from dlg.exceptions import InvalidDropException, InvalidRelationshipException
from dlg.meta import dlg_bool_param


class DirectoryContainer(PathBasedDrop, ContainerDROP):
    """
    A ContainerDROP that represents a filesystem directory. It only allows
    FileDROPs and DirectoryContainers to be added as children. Children
    can only be added if they are placed directly within the directory
    represented by this DirectoryContainer.
    """

    check_exists = dlg_bool_param("check_exists", True)

    def initialize(self, **kwargs):
        ContainerDROP.initialize(self, **kwargs)

        if "dirname" not in kwargs:
            raise InvalidDropException(
                self, 'DirectoryContainer needs a "dirname" parameter'
            )

        directory = kwargs["dirname"]

        if self.check_exists is True:
            if not os.path.isdir(directory):
                raise InvalidDropException(self, "%s is not a directory" % (directory))

        self._path = self.get_dir(directory)

    def addChild(self, child):
        if isinstance(child, (FileDROP, DirectoryContainer)):
            path = child.path
            if os.path.dirname(path) != self.path:
                raise InvalidRelationshipException(
                    DROPRel(child, DROPLinkType.CHILD, self),
                    "Child DROP is not under %s" % (self.path),
                )
            ContainerDROP.addChild(self, child)
        else:
            raise TypeError("Child DROP is not of type FileDROP or DirectoryContainer")

    def delete(self):
        shutil.rmtree(self._path)

    def exists(self):
        return os.path.isdir(self._path)
