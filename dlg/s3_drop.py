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


class S3DROP(AbstractDROP):
    """
    A DROP that points to data stored in S3
    """
    def __init__(self, oid, uid, **kwargs):
        self._bucket = None
        self._key = None
        self._aws_access_key_id = None
        self._aws_secret_access_key = None
        self._profile_name = None
        self._s3 = None
        super(S3DROP, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        """

        :param kwargs: the dictionary of arguments
        """
        self._bucket = self._getArg(kwargs, 'bucket', None)
        self._key = self._getArg(kwargs, 'key', None)
        self._aws_access_key_id = self._getArg(kwargs, 'aws_access_key_id', None)
        self._aws_secret_access_key = self._getArg(kwargs, 'aws_secret_access_key', None)
        self._profile_name = self._getArg(kwargs, 'profile_name', None)

    @property
    def bucket(self):
        """
        Returns the bucket name
        :return: the bucket name
        """
        return self._bucket

    @property
    def key(self):
        """
        Return the S3 key
        :return: the S3 key
        """
        return self._key

    @property
    def path(self):
        """
        Returns the path to the S3 object
        :return: the path
        """
        return self._bucket + '/' + self._key

    @property
    def dataURL(self):
        return "s3://" + self._bucket + '/' + self._key

    def exists(self):
        s3 = self._get_s3_connection()
        try:
            s3.meta.client.head_bucket(Bucket=self._bucket)
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                return False

        try:
            s3.meta.client.head_object(Bucket=self._bucket, Key=self._key)
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                return False

        return True

    def size(self):
        if self.exists():
            s3 = self._get_s3_connection()
            object = s3.Object(self._bucket, self._key)
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
            if self._profile_name is not None or self._aws_access_key_id is not None or self._aws_secret_access_key is not None:
                session = boto3.session.Session(profile_name=self._profile_name, aws_access_key_id=self._aws_access_key_id, aws_secret_access_key=self._aws_secret_access_key)
                self._s3 = session.resource('s3')
            else:
                self._s3 = boto3.resource('s3')
        return self._s3
