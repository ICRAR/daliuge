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
"""
Drops that interact with S3
"""
from asyncio.log import logger
from io import BytesIO
from typing import Tuple

from overrides import overrides

try:
    import boto3
    import botocore

except ImportError:
    logger.warning("BOTO bindings are not available")

from .data_base import DataDROP
from dlg.data.io import ErrorIO, OpenMode, DataIO
from dlg.meta import (
    dlg_component,
    dlg_batch_input,
    dlg_batch_output,
    dlg_streaming_input,
    dlg_string_param,
    dlg_list_param,
)

# from dlg.named_port_utils import identify_named_ports, check_ports_dict


##
# @brief S3
# @details An object available in a bucket on a S3 (Simple Storage Service) object storage platform
# @par EAGLE_START
# @param category S3
# @param tag daliuge
# @param data_volume 5/Float/ConstraintParameter/NoPort/ReadWrite//False/False/Estimated size of the data contained in this node
# @param group_end False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Is this node the end of a group?
# @param Bucket /String/ComponentParameter/NoPort/ReadWrite//False/False/The S3 Bucket
# @param Key /String/ComponentParameter/NoPort/ReadWrite//False/False/The S3 object key
# @param profile_name /String/ComponentParameter/NoPort/ReadWrite//False/False/The S3 profile name
# @param endpoint_url /String/ComponentParameter/NoPort/ReadWrite//False/False/The URL exposing the S3 REST API
# @param block_skip False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/If set the drop will block a skipping chain until the last producer has finished and is not also skipped.
# @param dropclass dlg.data.drops.s3_drop.S3DROP/String/ComponentParameter/NoPort/ReadWrite//False/False/Drop class
# @param base_name s3_drop/String/ComponentParameter/NoPort/ReadOnly//False/False/Base name of application class
# @param streaming False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Specifies whether this data component streams input and output data
# @param io /Object/ApplicationArgument/InputOutput/ReadWrite//False/False/Input Output port
# @par EAGLE_END
class S3DROP(DataDROP):
    """
    A DROP that points to data stored in S3
    """

    component_meta = dlg_component(
        "S3DROP",
        "S3 Data Drop",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

    Bucket = dlg_string_param("Bucket", None)
    Key = dlg_string_param("Key", None)
    storage_class = dlg_string_param("storage_class", "S3")
    tags = dlg_list_param("tags", None)
    # don't change the aws names
    aws_access_key_id = dlg_string_param("aws_access_key_id", None)
    aws_secret_access_key = dlg_string_param("aws_secret_access_key", None)
    profile_name = dlg_string_param("profile_name", None)
    endpoint_url = dlg_string_param("endpoint_url", None)

    def initialize(self, **kwargs):
        self.keyargs = {
            "Bucket": self.Bucket,
            "Key": self.Key,
            "storage_class": self.storage_class,
            "tags": self.tags,
            "aws_access_key_id": self.aws_access_key_id,
            "aws_secret_access_key": self.aws_secret_access_key,
            "profile_name": self.profile_name,
            "endpoint_url": self.endpoint_url,
        }
        logger.debug("S3 initializing: %s", self.keyargs)
        self.Key = self.uid if not self.Key else self.Key
        return super().initialize(**kwargs)

    @property
    def path(self) -> str:
        """
        Returns the path to the S3 object
        :return: the path
        """
        return "{}/{}".format(self.Bucket, self.Key)

    @property
    def dataURL(self) -> str:
        return "s3://{}/{}".format(self.Bucket, self.Key)

    @property
    def size(self) -> int:
        size = self.getIO()._size()
        logger.debug(("Size of object: %s", size))
        return size

    def getIO(self) -> DataIO:
        """
        Return
        :return:
        """
        logger.debug("S3DROP producers: %s", self._producers)
        # if check_ports_dict(self._producers):
        #     self.mapped_inputs = identify_named_ports(
        #         self._producers, {}, self.keyargs, mode="inputs"
        #     )
        logger.debug("Parameters found: %s", self.parameters)
        return S3IO(
            self.aws_access_key_id,
            self.aws_secret_access_key,
            self.profile_name,
            self.Bucket,
            self.Key,
            self.endpoint_url,
            self._expectedSize,
        )


class S3IO(DataIO):
    """
    IO class for the S3 Drop
    """

    _desc = None

    def __init__(
        self,
        aws_access_key_id=None,
        aws_secret_access_key=None,
        profile_name=None,
        Bucket=None,
        Key=None,
        endpoint_url=None,
        expectedSize=-1,
        **kwargs,
    ):
        super().__init__(**kwargs)

        logger.debug(
            (
                "key_id: %s; key: %s; profile: %s; bucket: %s; object_id: %s; %s",
                aws_access_key_id,
                aws_secret_access_key,
                profile_name,
                Bucket,
                Key,
                endpoint_url,
            )
        )
        self._s3 = None
        self._s3_access_key_id = aws_access_key_id
        self._s3_secret_access_key = aws_secret_access_key
        self._s3_endpoint_url = endpoint_url
        self._profile_name = profile_name
        self._bucket = Bucket
        self._key = Key
        self._s3_endpoint_url = endpoint_url
        self._s3 = self._get_s3_connection()
        self.url = f"{endpoint_url}/{Bucket}/{Key}"
        self._expectedSize = expectedSize
        self._buffer = b""
        if self._mode == 1:
            try:
                self._s3Stream = self._open()
            except botocore.exceptions.ClientError:
                if not self.exists():
                    logger.debug("Object does not exist yet. Creating!")
                    self._mode = 0

    def _get_s3_connection(self):
        s3 = None
        if self._s3 is None:
            if self._profile_name is not None or (
                self._s3_access_key_id is not None
                and self._s3_secret_access_key is not None
            ):
                logger.debug("Opening boto3 session")
                session = boto3.Session(profile_name=self._profile_name)
                s3 = session.client(
                    service_name="s3",
                    endpoint_url=self._s3_endpoint_url,
                )
            else:
                s3 = boto3.resource("s3")
        else:
            s3 = self._s3
        return s3

    def _open(self, **kwargs):
        logger.debug("Opening S3 object %s in mode %s", self._key, self._mode)
        if self._mode == OpenMode.OPEN_WRITE:
            exists = self._exists()
            if exists == (True, True):
                logger.error("Object exists already. Assuming part upload.")

            elif not exists[0]:
                # bucket does not exist, create first
                try:
                    self._s3.create_bucket(Bucket=self._bucket)
                except botocore.exceptions.ClientError as e:
                    raise e
            resp = self._s3.create_multipart_upload(
                Bucket=self._bucket,
                Key=self._key,
            )
            self._uploadId = resp["UploadId"]
            self._buffer = b""
            self._written = 0
            self._partNo = 1
            self._parts = {"Parts": []}
            return self._s3
        else:
            s3Object = self._s3.get_object(Bucket=self._bucket, Key=self._key)
            self._desc = s3Object["Body"]
        return s3Object["Body"]

    @overrides
    def _read(self, count=-1, **kwargs):
        # Read data from S3 and give it back to our reader
        if not self._desc:
            self._desc = self._open()
        if count != -1:
            return self._desc.read(count)
        else:
            return self._desc.read()

    def _writeBuffer2S3(self, write_buffer=b""):
        try:
            with BytesIO(write_buffer) as f:
                self._s3.upload_part(
                    Body=f,
                    Bucket=self._bucket,
                    Key=self._key,
                    UploadId=self._uploadId,
                    PartNumber=self._partNo,
                )
            logger.debug(
                "Wrote %d bytes part %d to S3: %s",
                len(write_buffer),
                self._partNo,
                self.url,
            )
            self._partNo += 1
            self._written += len(write_buffer)
        except botocore.exceptions.ClientError:
            logger.error("Writing to S3 failed")
            return -1

    @overrides
    def _write(self, data, **kwargs) -> int:
        """ """
        self._buffer += data
        PART_SIZE = 5 * 1024**2
        logger.debug("Length of S3 buffer: %d", len(self._buffer))
        if len(self._buffer) >= PART_SIZE:
            self._writeBuffer2S3(self._buffer[:PART_SIZE])
            self._buffer = self._buffer[PART_SIZE:]
        return len(data)  # we return the length of what we have received
        # to keep the client happy

    def _get_object_head(self) -> dict:
        return self._s3.head_object(Bucket=self._bucket, Key=self._key)

    @overrides
    def _size(self, **kwargs) -> int:
        if self.exists():
            object_head = self._get_object_head()
            logger.debug(("Size of object:%s", object_head["ContentLength"]))
            return object_head["ContentLength"]
        return -1

    @overrides
    def _close(self, **kwargs):
        if self._mode == OpenMode.OPEN_WRITE:
            if (
                len(self._buffer) > 0
            ):  # write, if there is still something in the buffer
                self._writeBuffer2S3(self._buffer)
            # complete multipart upload and cleanup
            res = self._s3.list_parts(
                Bucket=self._bucket, Key=self._key, UploadId=self._uploadId
            )
            parts = [
                {"ETag": p["ETag"], "PartNumber": p["PartNumber"]} for p in res["Parts"]
            ]
            # TODO: Check checksum!
            res = self._s3.complete_multipart_upload(
                Bucket=self._bucket,
                Key=self._key,
                UploadId=self._uploadId,
                MultipartUpload={"Parts": parts},
            )
            del self._buffer
            logger.info(
                "Wrote a total of %.1f MB to %s",
                self._written / (1024**2),
                self.url,
            )

        self._desc.close()
        del self._s3

    def _exists(self) -> Tuple[bool, bool]:
        """
        Need to have a way to get bucket and object existence
        seperately for writing.
        """
        s3 = self._s3

        try:
            # s3.meta.client.head_bucket(Bucket=self.bucket)
            logger.info("Checking existence of bucket: %s", self._bucket)
            s3.head_bucket(Bucket=self._bucket)
            logger.info("Bucket: %s exists", self._bucket)
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response["Error"]["Code"])
            if error_code == 404:
                logger.info("Bucket: %s does not exist", self._bucket)
                return False, False
            elif error_code == 403:
                logger.info("Access to bucket %s is forbidden", self._bucket)
                return False, False
            elif error_code > 300:
                logger.info(
                    "Error code %s when accessing bucket %s",
                    error_code,
                    self._bucket,
                )
        try:
            logger.info("Checking existence of object: %s", self._key)
            s3.head_object(Bucket=self._bucket, Key=self._key)
            logger.info("Object: %s exists", self._key)
            return True, True
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the object does not exist.
            error_code = int(e.response["Error"]["Code"])
            if error_code == 404:
                logger.info("Object: %s does not exist", self._key)
                return True, False
            else:
                raise RuntimeError("Error occured in Client: %s" % e.response) from e

    @overrides
    def exists(self) -> bool:
        bucket_exists, object_exists = self._exists()
        if bucket_exists and object_exists:
            return True
        else:
            return False

    @overrides
    def delete(self):
        if self.exists():
            self._s3.delete_object(Bucket=self._bucket, Key=self._key)
            return 0
        else:
            return ErrorIO()
