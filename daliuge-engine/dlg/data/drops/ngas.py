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
from dlg.ddap_protocol import DROPStates
from dlg.data.drops.data_base import DataDROP, logger, track_current_drop
from dlg.data.io import NgasIO, NgasLiteIO
from dlg.meta import dlg_string_param, dlg_int_param


##
# @brief NGAS
# @details An archive on the Next Generation Archive System (NGAS).
# @par EAGLE_START
# @param category NGAS
# @param tag daliuge
# @param ngasSrv localhost/String/ComponentParameter/NoPort/ReadWrite//False/False/The URL of the NGAS Server
# @param ngasPort 7777/Integer/ComponentParameter/NoPort/ReadWrite//False/False/The port of the NGAS Server
# @param ngasFileId /String/ComponentParameter/NoPort/ReadWrite//False/False/File ID on NGAS (for retrieval only)
# @param ngasConnectTimeout 2/Integer/ComponentParameter/NoPort/ReadWrite//False/False/Timeout for connecting to the NGAS server
# @param ngasMime "text/ascii"/String/ComponentParameter/NoPort/ReadWrite//False/False/Mime-type to be used for archiving
# @param ngasTimeout 2/Integer/ComponentParameter/NoPort/ReadWrite//False/False/Timeout for receiving responses for NGAS
# @param block_skip False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/If set the drop will block a skipping chain until the last producer has finished and is not also skipped.
# @param io /Object/ApplicationArgument/InputOutput/ReadWrite//False/False/Input Output port
# @param dropclass dlg.data.drops.ngas.NgasDROP/String/ComponentParameter/NoPort/ReadWrite//False/False/Drop class
# @param base_name ngas/String/ComponentParameter/NoPort/ReadOnly//False/False/Base name of application class
# @param data_volume 5/Float/ConstraintParameter/NoPort/ReadWrite//False/False/Estimated size of the data contained in this node
# @param group_end False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Is this node the end of a group?
# @param streaming False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Specifies whether this data component streams input and output data
# @param persist True/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Specifies whether this data component contains data that should not be deleted after execution
# @par EAGLE_END
class NgasDROP(DataDROP):
    """
    A DROP that points to data stored in an NGAS server
    """

    ngasSrv = dlg_string_param("ngasSrv", "localhost")
    ngasPort = dlg_int_param("ngasPort", 7777)
    ngasFileId = dlg_string_param("ngasFileId", None)
    ngasTimeout = dlg_int_param("ngasTimeout", 2)
    ngasConnectTimeout = dlg_int_param("ngasConnectTimeout", 2)
    ngasMime = dlg_string_param("ngasMime", "application/octet-stream")
    len = dlg_int_param("len", -1)
    ngas_checksum = None

    def initialize(self, **kwargs):
        if self.len == -1:
            # TODO: For writing the len field should be set to the size of the input drop
            self.len = self._size
        if self.ngasFileId:
            self.fileId = self.ngasFileId
        else:
            self.fileId = self.uid

    def getIO(self):
        try:
            ngasIO = NgasIO(
                self.ngasSrv,
                self.fileId,
                self.ngasPort,
                self.ngasConnectTimeout,
                self.ngasTimeout,
                length=self.len,
                mimeType=self.ngasMime,
            )
        except ImportError:
            logger.warning("NgasIO not available, using NgasLiteIO instead")
            ngasIO = NgasLiteIO(
                self.ngasSrv,
                self.fileId,
                self.ngasPort,
                self.ngasConnectTimeout,
                self.ngasTimeout,
                length=self.len,
                mimeType=self.ngasMime,
            )
        return ngasIO

    @track_current_drop
    def setCompleted(self):
        """
        Override this method in order to get the size of the drop set once it is completed.
        """
        if not self._setCompletedStateCheck():
            return

        self._closeWriters()

        # here we set the size. It could happen that nothing is written into
        # this file, in which case we create an empty file so applications
        # downstream don't fail to read
        logger.debug("Trying to set size of NGASDrop")
        try:
            stat = self.getIO().fileStatus()
            logger.debug(
                "Setting size of NGASDrop %s to %s",
                self.fileId,
                stat["FileSize"],
            )
            self._size = int(stat["FileSize"])
            self.ngas_checksum = str(stat["Checksum"])
        except:
            # we''ll try this again in case there is some other issue
            # try:
            #     with open(self.path, 'wb'):
            #         pass
            # except:
            #     self.status = DROPStates.ERROR
            #     logger.error("Path not accessible: %s" % self.path)
            logger.debug("Setting size of NGASDrop to %s", 0)
            self._size = 0
            raise
        # Signal our subscribers that the show is over
        logger.debug("Moving %r to COMPLETED", self)
        self.status = DROPStates.COMPLETED
        self._fire("dropCompleted", status=DROPStates.COMPLETED)
        self.completedrop()

    @property
    def dataURL(self) -> str:
        return "ngas://%s:%s/%s" % (self.ngasSrv, self.ngasPort, self.fileId)

    # Override
    def generate_reproduce_data(self):
        if self.ngas_checksum is None or self.ngas_checksum == "":
            return {"fileid": self.ngasFileId, "size": self._size}
        return {"data_hash": self.ngas_checksum}
