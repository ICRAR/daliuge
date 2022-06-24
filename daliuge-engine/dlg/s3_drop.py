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
Drops that interact with AWS S3
"""
import boto3
import botocore

from .drop import AbstractDROP
from .io import ErrorIO
from .meta import dlg_string_param, dlg_list_param


##
# @brief S3
# @details A 'bucket' object available on Amazon's Simple Storage Service (S3)
# @par EAGLE_START
# @param category S3
# @param tag daliuge
# @param[in] cparam/data_volume Data volume/5/Float/readwrite/False//False/
#     \~English Estimated size of the data contained in this node
# @param[in] cparam/group_end Group end/False/Boolean/readwrite/False//False/
#     \~English Is this node the end of a group?
# @param[in] cparam/bucket Bucket//String/readwrite/False//False/
#     \~English The S3 Bucket
# @param[in] cparam/object_name Object Name//String/readwrite/False//False/
#     \~English The S3 Object
# @param[in] cparam/profile_name Profile Name//String/readwrite/False//False/
#     \~English The S3 Profile
# @param[in] port/dummy dummy/Complex/Dummy input port
# @param[out] port/dummy dummy/Complex/Dummy output port
# @par EAGLE_END
class S3DROP(AbstractDROP):
    """
    A DROP that points to data stored in S3
    """

    bucket = dlg_string_param("bucket", None)
    key = dlg_string_param("key", None)
    storage_class = dlg_string_param("storage_class", None)
    tags = dlg_list_param("tags", None)
    aws_access_key_id = dlg_string_param("aws_access_key_id", None)
    aws_secret_access_key = dlg_string_param("aws_secret_access_key", None)
    profile_name = dlg_string_param("profile_name", None)

    def __init__(self, oid, uid, **kwargs):
        super().__init__(oid, uid, **kwargs)
        self._s3 = None

    @property
    def path(self):
        """
        Returns the path to the S3 object
        :return: the path
        """
        return "{}/{}".format(self.bucket, self.key)

    @property
    def dataURL(self) -> str:
        return "s3://{}/{}".format(self.bucket, self.key)

    def exists(self):
        s3 = self._get_s3_connection()
        try:
            s3.meta.client.head_bucket(Bucket=self.bucket)
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response["Error"]["Code"])
            if error_code == 404:
                return False

        try:
            s3.meta.client.head_object(Bucket=self.bucket, Key=self.key)
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response["Error"]["Code"])
            if error_code == 404:
                return False

        return True

    def size(self):
        if self.exists():
            s3 = self._get_s3_connection()
            object = s3.Object(self.bucket, self.key)
            return object.content_length

        return -1

    def getIO(self):
        """
        This type of DROP cannot be accessed directly
        :return:
        """
        return ErrorIO()

    def _get_s3_connection(self):
        if self._s3 is None:
            if (
                self.profile_name is not None
                or self.aws_access_key_id is not None
                or self.aws_secret_access_key is not None
            ):
                session = boto3.session.Session(
                    profile_name=self.profile_name,
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key,
                )
                self._s3 = session.resource("s3")
            else:
                self._s3 = boto3.resource("s3")
        return self._s3
