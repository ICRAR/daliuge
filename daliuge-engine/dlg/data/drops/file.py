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
import errno
import os

from dlg.common.reproducibility.reproducibility import common_hash
from dlg.data import path_builder
from dlg.data.io import FileIO
from dlg.ddap_protocol import DROPStates
from .data_base import DataDROP, PathBasedDrop, logger, track_current_drop
from dlg.exceptions import InvalidDropException
from dlg.meta import dlg_bool_param
from dlg.utils import isabs
from typing import Union


##
# @brief File
# @details A standard file on a filesystem mounted to the deployment machine
# @par EAGLE_START
# @param category File
# @param tag daliuge
# @param filepath /String/ApplicationArgument/NoPort/ReadWrite//False/False/"File path for this file. In many cases this does not need to be specified. If it has a \/ at the end it will be treated as a directory name and the filename will be generated. If it does not have a \/, the last part will be treated as a filename. If filepath does not start with \/ (relative path) then the session directory will be prepended to make the path absolute.""
# @param check_filepath_exists False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Perform a check to make sure the file path exists before proceeding with the application
# @param dropclass dlg.data.drops.file.FileDROP/String/ComponentParameter/NoPort/ReadWrite//False/False/Drop class
# @param base_name file/String/ComponentParameter/NoPort/ReadOnly//False/False/Base name of application class
# @param streaming False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Specifies whether this data component streams input and output data
# @param persist True/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Specifies whether this data component contains data that should not be deleted after execution
# @param block_skip False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/If set the drop will block a skipping chain until the last producer has finished and is not also skipped.
# @param data_volume 5/Float/ConstraintParameter/NoPort/ReadWrite//False/False/Estimated size of the data contained in this node
# @param group_end False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Is this node the end of a group?
# @param io /Object/ApplicationArgument/InputOutput/ReadWrite//False/False/Input Output port
# @par EAGLE_END
class FileDROP(DataDROP, PathBasedDrop):
    """
    A DROP that points to data stored in a mounted filesystem.

    Users can fix both the path and the name of a FileDrop using the `filepath`
    parameter for each FileDrop. We distinguish four cases and their combinations.

    1) If not specified the filename will be generated.
    2) If it has a '/' at the end it will be treated as a directory name and the
       filename will the generated.
    3) If it does not end with a '/' and it is not an existing directory, it is
       treated as dirname plus filename.
    4) If filepath points to an existing directory, the filename will be generated

    In all cases above, if `filepath` does not start with '/’ (relative path)
    then the session directory will be pre-pended to make the path absolute.
    """

    delete_parent_directory = dlg_bool_param("delete_parent_directory", False)
    check_filepath_exists = dlg_bool_param("check_filepath_exists", False)
    # is_dir = dlg_bool_param("is_dir", False)

    # Make sure files are not deleted by default and certainly not if they are
    # marked to be persisted no matter what expireAfterUse said
    def __init__(self, *args, **kwargs):
        if "persist" not in kwargs:
            kwargs["persist"] = True
        if kwargs["persist"] and "lifespan" not in kwargs:
            kwargs["expireAfterUse"] = False
        self.is_dir = False
        self._updatedPorts = False
        super().__init__(*args, **kwargs)

    def sanitize_paths(self, filepath: str) -> Union[None, str]:
        """
        Expand ENV_VARS, but also deal with relative
        and absolute paths. filepath can be either just be
        a directory, a directory including a file name, only
        a directory (both relative and absolute), or just
        a file name.

        :param filepath: string, path and or directory

        :returns filepath
        """
        # replace any ENV_VARS on the names
        if not filepath:
            return None
        filepath = os.path.expandvars(filepath)
        if isabs(filepath):
            return filepath
        else:
            filepath = self.get_dir(filepath)
        return filepath


    def initialize(self, **kwargs):
        """
        FileDROP-specific initialization.
        """

        self._setupFilePaths()

    def _setupFilePaths(self):
        filepath = self.parameters.get("filepath", None)
        # TODO ADD SUFFIX/PREFIX
        dirname = None
        filename = None

        if filepath:  # if there is anything provided
            # TODO do f-string substitution if necessary
            if "/" not in filepath:  # just a name
                filename = filepath
                dirname = self.get_dir(".")
            # filepath = self.sanitize_paths(self.filepath)
            elif filepath.endswith("/"):  # just a directory name
                self.is_dir = True
                filename = None
                dirname = filepath
            else:
                filename = os.path.basename(filepath)
                dirname = os.path.dirname(filepath)
        if dirname is None:
            dirname = "."
        filename = os.path.expandvars(filename) if filename else None
        dirname = self.sanitize_paths(dirname) if dirname else None
        # We later check if the file exists, but only if the user has specified
        # an absolute dirname/filepath (otherwise it doesn't make sense, since
        # we create our own filenames/dirnames dynamically as necessary
        check = False
        if isabs(dirname):
            check = self.check_filepath_exists

        # Default filename to drop human readable format based on UID
        if filename is None:
            filename = path_builder.base_uid_filename(self.uid, self._humanKey)


        self.filename = filename
        self.dirname = self.get_dir(dirname)
        self._root = self.dirname
        self._path = (
            os.path.join(self.dirname, self.filename) if self.filename else self.dirname
        )
        logger.debug(
            "Set path of drop %s: %s check: %s %s",
            self._uid, self._path, check, os.path.isfile(self._path)
        )
        if check and not os.path.isfile(self._path):
            raise InvalidDropException(
                self, "File does not exist or is not a file: %s" % self._path
            )

        self._wio = None

    def getIO(self):

        # We need to update named_ports now we have runtime information
        # if not self._updatedPorts:
        if not self.parameters.get("filepath", None):
            self._map_input_ports_to_params()
            self._setupFilePaths()

        return FileIO(self._path)

    def delete(self):
        super().delete()
        if self.delete_parent_directory:
            try:
                os.rmdir(self._root)
            except OSError as e:
                # Silently ignore "Directory not empty" errors
                if e.errno != errno.ENOTEMPTY:
                    raise

    @track_current_drop
    def setCompleted(self):
        """
        Override this method in order to get the size of the drop set once it is completed.
        """
        # TODO: This implementation is almost a verbatim copy of the base class'
        # so we should look into merging them
        status = self.status
        if status == DROPStates.CANCELLED:
            return
        elif status == DROPStates.SKIPPED:
            self._fire("dropCompleted", status=status)
            return
        elif status not in [
            DROPStates.COMPLETED,
            DROPStates.INITIALIZED,
            DROPStates.WRITING,
        ]:
            raise Exception(
                "%r not in INITIALIZED or WRITING state (%s), cannot setComplete()"
                % (self, self.status)
            )

        self._closeWriters()

        if status != DROPStates.COMPLETED:
            logger.debug("Moving %r to COMPLETED", self)
            self.status = DROPStates.COMPLETED

        # here we set the size. It could happen that nothing is written into
        # this file, in which case we create an empty file so applications
        # downstream don't fail to read
        try:
            self._size = os.stat(self.path).st_size
        except FileNotFoundError:
            # we''ll try this again in case there is some other issue
            try:
                with open(self.path, "wb"):
                    pass
            except IOError:
                self.status = DROPStates.ERROR
                logger.error("Path not accessible: %s", self.path)
            self._size = 0
        # Signal our subscribers that the show is over
        self._fire("dropCompleted", status=DROPStates.COMPLETED)
        self.completedrop()

    @property
    def dataURL(self) -> str:
        hostname = os.uname()[1]  # TODO: change when necessary
        return "file://" + hostname + self._path

    # Override
    def generate_reproduce_data(self):
        from dlg.droputils import allDropContents

        try:
            data = allDropContents(self, self.size)
        except IOError:
            data = b""
        return {"data_hash": common_hash(data)}
