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
from http.client import HTTPConnection
from overrides import overrides
from typing import Tuple
from io import BytesIO


try:
    import boto3
    import botocore
    
except ImportError:
    logger.warning("BOTO bindings are not available")

from ...drop import DataDROP
from dlg.data.io import ErrorIO, OpenMode, DataIO
from ...meta import dlg_string_param, dlg_list_param

##
# @brief S3
# @details A 'bucket' object available on Amazon's Simple Storage Service (S3)
# @par EAGLE_START
# @param category S3
# @param tag daliuge
# @param data_volume Data volume/5/Float/ComponentParameter/readwrite//False/False/Estimated size of the data contained in this node
# @param group_end Group end/False/Boolean/ComponentParameter/readwrite//False/False/Is this node the end of a group?
# @param bucket Bucket//String/ComponentParameter/readwrite//False/False/The S3 Bucket
# @param object_name Object Name//String/ComponentParameter/readwrite//False/False/The S3 Object
# @param profile_name Profile Name//String/ComponentParameter/readwrite//False/False/The S3 Profile
# @param dummy dummy//Object/InputPort/readwrite//False/False/Dummy input port
# @param dummy dummy//Object/OutputPort/readwrite//False/False/Dummy output port
# @par EAGLE_END
class S3DROP(DataDROP):
    """
    A DROP that points to data stored in S3
    """

    Bucket = dlg_string_param("Bucket", None)
    Key = dlg_string_param("Key", None)
    storage_class = dlg_string_param("storage_class", "S3")
    tags = dlg_list_param("tags", None)
    # don't change the aws names
    aws_access_key_id = dlg_string_param("aws_access_key_id", None)
    aws_secret_access_key = dlg_string_param("aws_secret_access_key", None)
    profile_name = dlg_string_param("profile_name", None)
    endpoint_url = dlg_string_param("endpoint_url", None)

    def __init__(self, oid, uid, **kwargs):
        super().__init__(oid, uid, **kwargs)
        self.size

    @property
    def path(self):
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
        logger.debug("Size of object:{}", size)
        # if size > -1:
            # set drop to completed
            # S3 objects are immutable
            # self.status = 2
        return size

    def getIO(self) -> DataIO:
        """
        Return 
        :return:
        """
        logger.debug("Parameters found: {}", self.parameters)
        return S3IO(self.aws_access_key_id,
                    self.aws_secret_access_key,
                    self.profile_name,
                    self.Bucket,
                    self.Key,
                    self.endpoint_url)

class S3IO(DataIO):
    """
    IO class for the S3 Drop
    """
    _desc = None
    _mode = 1
    def __init__(self, 
        aws_access_key_id=None, 
        aws_secret_access_key=None,
        profile_name=None, Bucket=None, Key=None, endpoint_url=None, **kwargs):

        logger.debug("key_id: %s; key: %s; profile: %s; bucket: %s; object_id: %s; %s" %
            (aws_access_key_id, aws_secret_access_key, profile_name, Bucket, Key, endpoint_url))
        self._s3 = None
        self._s3_access_key_id = aws_access_key_id
        self._s3_secret_access_key = aws_secret_access_key
        self._s3_endpoint_url = endpoint_url
        self._profile_name = profile_name
        self._bucket = Bucket
        self._key = Key
        self._s3_endpoint_url = endpoint_url
        self._s3 = self._get_s3_connection()
        if self._mode == 1:
            try:
                self._s3Stream = self._open()
            except botocore.exceptions.ClientError as e:
                if not self.exists():
                    self._mode = 0

    def _get_s3_connection(self):
        if self._s3 is None:
            if (
                self._profile_name is not None
                or self._s3_access_key_id is not None
                or self._s3_secret_access_key is not None
            ):
                logger.debug("Opening boto3 session")
                session = boto3.Session(
                    profile_name=self._profile_name)
                s3 = session.client(
                    service_name="s3",
                    endpoint_url=self._s3_endpoint_url,
                )
                # s3 = session.resource("s3")
            else:
                s3 = boto3.resource("s3")
        return s3

    def _open(self, **kwargs):
        if self._mode == OpenMode.OPEN_WRITE:
            return self._s3
            # self._buf = b""
            # self._writtenDataSize = 0
        else:
            logger.debug("Opening S3 object %s", self._key)
            s3Object = self._s3.get_object(Bucket=self._bucket, Key=self._key)
            self._desc = s3Object['Body']
        return s3Object['Body']

    @overrides
    def _read(self, count=-1, **kwargs):
        # Read data from S3 and give it back to our reader
        if not self._desc: self._desc = self._open()
        if count != -1:
            return self._desc.read(count)
        else:
            return self._desc.read()

    @overrides
    def _write(self, data, **kwargs) -> int:
        """
        TODO: Need to implement streaming upload
        The current implementation will only upload a single block
        """
        PART_SIZE = 5*1024**2
        if 'size' in kwargs:
            logger.debug("Length of object to write: %d",kwargs['size'])
        self._mode = 0
        exists = self._exists()
        if 'buffer' in locals():
            buffer += data
        else:
            buffer=data
        if exists == (True, True):
            logger.error("Object exists already. Assuming part upload.")
            
        elif exists[0] == False:
            # bucket does not exist, create first
            try:
                self._s3.create_bucket(Bucket=self._bucket)
            except botocore.exceptions.ClientError as e:
                raise e
        if len(buffer) > PART_SIZE:
            try:
                with BytesIO(buffer[:PART_SIZE]) as f:
                    self._s3.upload_fileobj(f, self._bucket, self._key)
                    url = f"{self._s3_endpoint_url}/{self._bucket}/{self._key}"
                    logger.info("Wrote %d bytes to %s", len(PART_SIZE), url)
                    buffer = buffer[PART_SIZE:]
                    return len(PART_SIZE)
            except botocore.exceptions.ClientError as e:
                logger.error("Writing to S3 failed")
                return -1

        

    def _get_object_head(self) -> dict:
        return self._s3.head_object(
            Bucket=self._bucket, 
            Key=self._key)

    @overrides
    def _size(self, **kwargs) -> int:
        if self.exists():
            object_head = self._get_object_head()
            logger.debug("Size of object:{}", object_head['ContentLength'])
            return object_head['ContentLength']
        return -1

    @overrides
    def _close(self, **kwargs):
        self._desc.close()
        del(self._s3)

    def _exists(self) -> Tuple[bool, bool]:
        """
        Need to have a way to get bucket and object existence
        seperately for writing.
        """
        s3 = self._s3

        try:
            # s3.meta.client.head_bucket(Bucket=self.bucket)
            s3.head_bucket(Bucket=self._bucket)
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response["Error"]["Code"])
            if error_code == 404:
                return (False, False)
        try:
            s3.head_object(Bucket=self._bucket, Key=self._key)
            return (True, True)
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response["Error"]["Code"])
            if error_code == 404:
                return (True, False)
            else:
                raise ErrorIO()

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



