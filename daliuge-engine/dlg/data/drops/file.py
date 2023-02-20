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
import re

from dlg.common.reproducibility.reproducibility import common_hash
from dlg.ddap_protocol import DROPStates
from .data_base import DataDROP, PathBasedDrop, logger, track_current_drop
from dlg.exceptions import InvalidDropException
from dlg.data.io import FileIO
from dlg.meta import dlg_bool_param
from dlg.utils import isabs


##
# @brief File
# @details A standard file on a filesystem mounted to the deployment machine
# @par EAGLE_START
# @param category File
# @param tag daliuge
# @param data_volume Data volume/5/Float/ComponentParameter/readwrite//False/False/Estimated size of the data contained in this node
# @param group_end Group end/False/Boolean/ComponentParameter/readwrite//False/False/Is this node the end of a group?
# @param delete_parent_directory Delete parent directory/False/Boolean/ComponentParameter/readwrite//False/False/Also delete the parent directory of this file when deleting the file itself
# @param check_filepath_exists Check file path exists/False/Boolean/ComponentParameter/readwrite//False/False/Perform a check to make sure the file path exists before proceeding with the application
# @param filepath File Path//String/ComponentParameter/readwrite//False/False/Path to the file for this node
# @param dirname Directory name//String/ComponentParameter/readwrite//False/False/Path to the file for this node
# @param streaming Streaming/False/Boolean/ComponentParameter/readwrite//False/False/Specifies whether this data component streams input and output data
# @param persist Persist/True/Boolean/ComponentParameter/readwrite//False/False/Specifies whether this data component contains data that should not be deleted after execution
# @param dummy dummy//Object/InputPort/readwrite//False/False/Dummy input port
# @param dummy dummy//Object/OutputPort/readwrite//False/False/Dummy output port
# @par EAGLE_END
class FileDROP(DataDROP, PathBasedDrop):
    """
    A DROP that points to data stored in a mounted filesystem.

    Users can (but usually don't need to) specify both a `filepath` and a
    `dirname` parameter for each FileDrop. The combination of these two parameters
    will determine the final location of the file backed up by this drop on the
    underlying filesystem. When no ``filepath`` is provided, the drop's UID will be
    used as a filename. When a relative filepath is provided, it is relative to
    ``dirname``. When an absolute ``filepath`` is given, it is used as-is.
    When a relative ``dirname`` is provided, it is relative to the base directory
    of the currently running session (i.e., a directory with the session ID as a
    name, placed within the currently working directory of the Node Manager
    hosting that session). If ``dirname`` is absolute, it is used as-is.

    In some cases drops are created **outside** the context of a session, most
    notably during unit tests. In these cases the base directory is a fixed
    location under ``/tmp``.

    The following table summarizes the calculation of the final path used by
    the ``FileDrop`` class depending on its parameters:

    ============ ===================== ===================== ==========
         .                               filepath
    ------------ ------------------------------------------------------
    dirname      empty                 relative              absolute
    ============ ===================== ===================== ==========
    **empty**    /``$B``/``$u``        /``$B``/``$f``        /``$f``
    **relative** /``$B``/``$d``/``$u`` /``$B``/``$d``/``$f`` **ERROR**
    **absolute** /``$d``/``$u``        /``$d``/``$f``        **ERROR**
    ============ ===================== ===================== ==========

    In the table, ``$f`` is the value of ``filepath``, ``$d`` is the value of
    ``dirname``, ``$u`` is the drop's UID and ``$B`` is the base directory for
    this drop's session, namely ``/the/cwd/$session_id``.
    """

    # filepath = dlg_string_param("filepath", None)
    # dirname = dlg_string_param("dirname", None)
    delete_parent_directory = dlg_bool_param("delete_parent_directory", False)
    check_filepath_exists = dlg_bool_param("check_filepath_exists", False)

    # Make sure files are not deleted by default and certainly not if they are
    # marked to be persisted no matter what expireAfterUse said
    def __init__(self, *args, **kwargs):
        if "persist" not in kwargs:
            kwargs["persist"] = True
        if kwargs["persist"] and "lifespan" not in kwargs:
            kwargs["expireAfterUse"] = False
        super().__init__(*args, **kwargs)

    def sanitize_paths(self, filepath, dirname):

        # first replace any ENV_VARS on the names
        if filepath:
            filepath = os.path.expandvars(filepath)
        if dirname:
            dirname = os.path.expandvars(dirname)
        # No filepath has been given, there's nothing to sanitize
        if not filepath:
            return filepath, dirname

        # All is good, return unchanged
        filepath_b = os.path.basename(filepath)
        if filepath_b == filepath:
            return filepath, dirname

        # Extract the dirname from filepath and append it to dirname
        filepath_d = os.path.dirname(filepath)
        if not isabs(filepath_d) and dirname:
            filepath_d = os.path.join(dirname, filepath_d)
        return filepath_b, filepath_d

    non_fname_chars = re.compile(r":|%s" % os.sep)

    def initialize(self, **kwargs):
        """
        FileDROP-specific initialization.
        """
        # filepath, dirpath the two pieces of information we offer users to tweak
        # These are very intermingled but are not exactly the same, see below
        self.filepath = self.parameters.get("filepath", None)
        self.dirname = self.parameters.get("dirname", None)
        # Duh!
        if isabs(self.filepath) and self.dirname:
            raise InvalidDropException(
                self,
                "An absolute filepath does not allow a dirname to be specified",
            )

        # Sanitize filepath/dirname into proper directories-only and
        # filename-only components (e.g., dirname='lala' and filename='1/2'
        # results in dirname='lala/1' and filename='2'
        filepath, dirname = self.sanitize_paths(self.filepath, self.dirname)
        # We later check if the file exists, but only if the user has specified
        # an absolute dirname/filepath (otherwise it doesn't make sense, since
        # we create our own filenames/dirnames dynamically as necessary
        check = False
        if isabs(dirname) and filepath:
            check = self.check_filepath_exists

        # Default filepath to drop UID and dirname to per-session directory
        if not filepath:
            filepath = self.non_fname_chars.sub("_", self.uid)
        dirname = self.get_dir(dirname)

        self._root = dirname
        self._path = os.path.join(dirname, filepath)
        logger.debug(f"Set path of drop {self._uid}: {self._path}")
        if check and not os.path.isfile(self._path):
            raise InvalidDropException(
                self, "File does not exist or is not a file: %s" % self._path
            )

        self._wio = None

    def getIO(self):
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
            except:
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
        except Exception:
            data = b""
        return {"data_hash": common_hash(data)}
