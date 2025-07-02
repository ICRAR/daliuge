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
from abc import abstractmethod
import random
import os
import logging
from typing import Union

from dlg.drop import AbstractDROP, track_current_drop
from dlg.data.io import (
    DataIO,
    OpenMode,
    NullIO,
)
from dlg.ddap_protocol import (
    ChecksumTypes,
    DROPStates,
)
from dlg.utils import isabs, createDirIfMissing


checksum_disabled = "DLG_DISABLE_CHECKSUM" in os.environ
try:
    from crc32c import crc32c  # @UnusedImport

    _checksumType = ChecksumTypes.CRC_32C
except ImportError:
    from binascii import crc32 # pylint: disable=unused-import

    _checksumType = ChecksumTypes.CRC_32

logger = logging.getLogger(f"dlg.{__name__}")


##
# @brief Data
# @details A generic Data drop, whose functionality can be provided by an arbitrary class, as specified in the 'dropclass' component parameter. It is not useful without additional development.
# @par EAGLE_START
# @param category Data
# @param tag template
# @param dropclass dlg.data.drops.data_base.DataDROP/String/ComponentParameter/NoPort/ReadOnly//False/False/The python class that implements this data component
# @param base_name data_base/String/ComponentParameter/NoPort/ReadOnly//False/False/Base name of application class
# @param data_volume 5/Float/ConstraintParameter/NoPort/ReadWrite//False/False/Estimated size of the data contained in this node
# @param group_end False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Is this node the end of a group?
# @param streaming False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Specifies whether this data component streams input and output data
# @param persist False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Specifies whether this data component contains data that should not be deleted after execution
# @param io /Object/ApplicationArgument/InputOutput/ReadWrite//False/False/Input Output port
# @par EAGLE_END
class DataDROP(AbstractDROP):
    """
    A DataDROP is a DROP that stores data for writing with
    an AppDROP, or reading with one or more AppDROPs.

    DataDROPs have two different modes: "normal" and "streaming".
    Normal DataDROPs will wait until the COMPLETED state before being
    available as input to an AppDROP, while streaming AppDROPs may be
    read simutaneously with writing by chunking drop bytes together.

    This class contains two methods that need to be overrwritten:
    `getIO`, invoked by AppDROPs when reading or writing to a drop,
    and `dataURL`, a getter for a data URI uncluding protocol and address
    parsed by function `IOForURL`.
    """

    def incrRefCount(self):
        """
        Increments the reference count of this DROP by one atomically.
        """
        with self._refLock:
            self._refCount += 1

    def decrRefCount(self):
        """
        Decrements the reference count of this DROP by one atomically.
        """
        with self._refLock:
            self._refCount -= 1

    @track_current_drop
    def open(self, **kwargs):
        """
        Opens the DROP for reading, and returns a "DROP descriptor"
        that must be used when invoking the read() and close() methods.
        DROPs maintain a internal reference count based on the number
        of times they are opened for reading; because of that after a successful
        call to this method the corresponding close() method must eventually be
        invoked. Failing to do so will result in DROPs not expiring and
        getting deleted.
        """
        if self.status != DROPStates.COMPLETED:
            raise Exception(
                "%r is in state %s (!=COMPLETED), cannot be opened for reading"
                % (self, self.status)
            )

        io = self.getIO()
        logger.debug("Opening drop %s", self.oid)
        io.open(OpenMode.OPEN_READ, **kwargs)

        # Save the IO object in the dictionary and return its descriptor instead
        while True:
            descriptor = random.SystemRandom().randint(-(2**31), 2**31 - 1)
            if descriptor not in self._rios:
                break
        self._rios[descriptor] = io

        # This occurs only after a successful opening
        self.incrRefCount()
        self._fire("open")

        return descriptor

    @track_current_drop
    def close(self, descriptor, **kwargs):
        """
        Closes the given DROP descriptor, decreasing the DROP's
        internal reference count and releasing the underlying resources
        associated to the descriptor.
        """
        self._checkStateAndDescriptor(descriptor)

        # Decrement counter and then actually close
        self.decrRefCount()
        io = self._rios.pop(descriptor)
        io.close(**kwargs)

    def _closeWriters(self):
        """
        Close our writing IO instance.
        If written externally, self._wio will have remained None
        """
        if self._wio:
            try:
                self._wio.close()
            except IOError:
                pass  # this will make sure that a previous issue does not cause the graph to hang!
                # raise Exception("Problem closing file!")
            self._wio = None

    def read(self, descriptor, count=65536, **kwargs):
        """
        Reads `count` bytes from the given DROP `descriptor`.
        """
        self._checkStateAndDescriptor(descriptor)
        io = self._rios[descriptor]
        return io.read(count, **kwargs)

    def _checkStateAndDescriptor(self, descriptor):
        if self.status != DROPStates.COMPLETED:
            raise Exception(
                "%r is in state %s (!=COMPLETED), cannot be read" % (self, self.status)
            )
        if descriptor is None:
            raise ValueError("Illegal empty descriptor given")
        if descriptor not in self._rios:
            raise Exception(
                "Illegal descriptor %d given, remember to open() first" % (descriptor)
            )

    def isBeingRead(self):
        """
        Returns `True` if the DROP is currently being read; `False`
        otherwise
        """
        with self._refLock:
            return self._refCount > 0

    @track_current_drop
    def write(self, data: Union[bytes, memoryview]):
        """
        Writes the given `data` into this DROP. This method is only meant
        to be called while the DROP is in INITIALIZED or WRITING state;
        once the DROP is COMPLETE or beyond only reading is allowed.
        The underlying storage mechanism is responsible for implementing the
        final writing logic via the `self.writeMeta()` method.
        """

        if self.status not in [DROPStates.INITIALIZED, DROPStates.WRITING]:
            raise Exception("No more writing expected")

        if not isinstance(data, (bytes, memoryview, str)):
            raise Exception("Data type not of binary type: ", type(data).__name__)

        # We lazily initialize our writing IO instance because the data of this
        # DROP might not be written through this DROP
        if not self._wio:
            self._wio = self.getIO()
            try:
                self._wio.open(OpenMode.OPEN_WRITE)
            except IOError as e:
                self.status = DROPStates.ERROR
                raise IOError("Problem opening drop for write!") from e
        nbytes = self._wio.write(data)
        nbytes = 0 if nbytes is None else nbytes

        dataLen = len(data)
        if nbytes != dataLen:
            # TODO: Maybe this should be an actual error?
            logger.warning(
                "Not all data was correctly written by %s (%d/%d bytes written)",
                self,
                nbytes,
                dataLen,
            )

        # see __init__ for the initialization to None
        if self._size is None:
            self._size = 0
        self._size += nbytes

        # Trigger our streaming consumers
        if self._streamingConsumers:
            for streamingConsumer in self._streamingConsumers:
                streamingConsumer.dataWritten(self.uid, data)

        # Update our internal checksum
        if not checksum_disabled:
            self._updateChecksum(data)

        # If we know how much data we'll receive, keep track of it and
        # automatically switch to COMPLETED
        if self._expectedSize > 0:
            remaining = self._expectedSize - self._size
            if remaining > 0:
                self.status = DROPStates.WRITING
            else:
                if remaining < 0:
                    logger.warning(
                        "Received and wrote more bytes than expected: %d",
                        -remaining,
                    )
                logger.debug(
                    "Automatically moving %r to COMPLETED, all expected data arrived",
                    self,
                )
                self.setCompleted()
        else:
            self.status = DROPStates.WRITING

        return nbytes

    def _updateChecksum(self, chunk):
        # see __init__ for the initialization to None
        if self._checksum is None:
            self._checksum = 0
            self._checksumType = _checksumType
        if isinstance(chunk, str):
            chunk = bytes(chunk, encoding="utf8")
        self._checksum = crc32c(chunk, self._checksum)

    @property
    def checksum(self):
        """
        The checksum value for the data represented by this DROP. Its
        value is automatically calculated if the data was actually written
        through this DROP (using the `self.write()` method directly or
        indirectly). In the case that the data has been externally written, the
        checksum can be set externally after the DROP has been moved to
        COMPLETED or beyond.

        :see: `self.checksumType`
        """
        if self.status == DROPStates.COMPLETED and self._checksum is None:
            # Generate on the fly
            io = self.getIO()
            io.open(OpenMode.OPEN_READ)
            data = io.read(65536)
            if isinstance(data, str):
                data = bytes(data, encoding="utf8")
            while data is not None and len(data) > 0:
                self._updateChecksum(data)
                data = io.read(65536)
            io.close()
        return self._checksum

    @checksum.setter
    def checksum(self, value):
        if self._checksum is not None:
            raise Exception(
                "The checksum for DROP %s is already calculated, cannot overwrite with new value"
                % (self)
            )
        if self.status in [DROPStates.INITIALIZED, DROPStates.WRITING]:
            raise Exception(
                "DROP %s is still not fully written, cannot manually set a checksum yet"
                % (self)
            )
        self._checksum = value

    @property
    def checksumType(self):
        """
        The algorithm used to compute this DROP's data checksum. Its value
        if automatically set if the data was actually written through this
        DROP (using the `self.write()` method directly or indirectly). In
        the case that the data has been externally written, the checksum type
        can be set externally after the DROP has been moved to COMPLETED
        or beyond.

        :see: `self.checksum`
        """
        return self._checksumType

    @checksumType.setter
    def checksumType(self, value):
        if self._checksumType is not None:
            raise Exception(
                "The checksum type for DROP %s is already set, cannot overwrite with new value"
                % (self)
            )
        if self.status in [DROPStates.INITIALIZED, DROPStates.WRITING]:
            raise Exception(
                "DROP %s is still not fully written, cannot manually set a checksum type yet"
                % (self)
            )
        self._checksumType = value

    def _map_input_ports_to_params(self):
        """
        Map the input ports that are on the drop to the its parameters

        This method performs the following steps:

            - Iterate through each producer, finding the portname and value of the
            port associated with that producers

            - Iterate through the input port names of _this_ drop, and match the UID
            and name to the producer map, and then getting the value.

            - Finally, match the value of the named input drop with a DROP parameter (
            if it exists).

        It is expected that this method be used within the child DataDrop class that is
        inheriting this method; see the FileDrop class implemenetation for an example
        use case.
        """

        try:
            dropInputPorts = self.parameters["producers"]
        except KeyError:
            logging.debug("No producers available for drop: %s", self.uid)
            return

        producerPortValueMap = {}  # Map Producer UIDs to a portname
        finalDropPortMap = {}  # Final mapping of named port to value stored in producer

        for p in self.producers:
            producerUid = p.uid
            producerPortValueMap[producerUid] = {}
            params = p.parameters["outputs"]
            for param in params:
                try:
                    key = list(param.keys())[0]
                except AttributeError:
                    logging.debug("Producer %s does not have named ports", p.uid)
                    continue
                portName = param[key]
                portValue = ""
                if portName in p.parameters:
                    portValue = p.parameters[param[key]]
                producerPortValueMap[producerUid][portName] = portValue

        for port in dropInputPorts:
            try:
                port.items()
            except AttributeError:
                logging.debug("Producer %s does not have named ports", port.uid)
                continue
            for uid, input_port_name in port.items():
                try:
                    ouput_port_name = self.parameters["port_map"][input_port_name]
                    if ouput_port_name in producerPortValueMap[uid]:
                        finalDropPortMap[input_port_name] = producerPortValueMap[uid][
                            ouput_port_name]
                except KeyError:
                    logging.warning("%s not available.", input_port_name)

        for portname in finalDropPortMap:
            if portname in self.parameters:
                self.parameters[portname] = finalDropPortMap[portname]

    @abstractmethod
    def getIO(self) -> DataIO:
        """
        Returns an instance of one of the `dlg.io.DataIO` instances that
        handles the data contents of this DROP.
        """

    def delete(self):
        """
        Deletes the data represented by this DROP.
        """
        self.getIO().delete()

    def exists(self):
        """
        Returns `True` if the data represented by this DROP exists indeed
        in the underlying storage mechanism
        """
        return self.getIO().exists()

    @property
    @abstractmethod
    def dataURL(self) -> str:
        """
        A URL that points to the data referenced by this DROP. Different
        DROP implementations will use different URI schemes.
        """


class PathBasedDrop(object):
    """
    Base class for data drops that handle paths (i.e., file and directory drops)
    """

    _path: str = None

    def get_dir(self, dirname):
        """
        dirname will be based on the current working directory
        If we have a session, it goes into the path as well
        (most times we should have a session BTW, we should expect *not* to
        have one only during testing)

        :param dirname: str

        :returns dir
        """
        if isabs(dirname):
            return dirname

        parts = []
        if self._dlg_session_id:
            parts.append(".")
        else:
            parts.append("/tmp/daliuge_tfiles")
        if dirname:
            parts.append(dirname)

        the_dir = os.path.abspath(os.path.normpath(os.path.join(*parts)))
        logger.debug("Path used for drop: %s", the_dir)
        createDirIfMissing(the_dir)
        return the_dir

    @property
    def path(self) -> str:
        return self._path


##
# @brief NULL
# @details A Drop not storing any data (useful for just passing on events)
# @par EAGLE_START
# @param category Memory
# @param tag daliuge
# @param dropclass dlg.data.drops.data_base.NullDROP/String/ComponentParameter/NoPort/ReadWrite//False/False/Drop class
# @param base_name data_base/String/ComponentParameter/NoPort/ReadOnly//False/False/Base name of application class
# @param group_end False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Is this node the end of a group?
# @param data_volume 5/Float/ConstraintParameter/NoPort/ReadWrite//False/False/Estimated size of the data contained in this node
# @param persist True/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Specifies whether this data component contains data that should not be deleted after execution
# @param streaming False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Specifies whether this data component streams input and output data
# @param io /Object/ApplicationArgument/InputOutput/ReadWrite//False/False/Input Output port
# @par EAGLE_END
class NullDROP(DataDROP):
    """
    A DROP that doesn't store any data.
    """

    def getIO(self):
        return NullIO()

    @property
    def dataURL(self) -> str:
        return "null://"


class EndDROP(NullDROP):
    """
    A DROP that ends the session when reached
    """
