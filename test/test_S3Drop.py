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
from unittest.case import skipIf
"""
Test the S3 Drop
"""
# If the profile is not present in the current user's account
# we simply skip the test

import unittest

import boto3
from dlg.s3_drop import S3DROP


PROFILE = 'aws-chiles02'
run_tests = True
try:
    boto3.session.Session(profile_name=PROFILE)
except:
    run_tests = False

class TestS3Drop(unittest.TestCase):

    @skipIf(not run_tests, "No profile found to run this test")
    def test_bucket_exists(self):
        drop = S3DROP('oid:A', 'uid:A', profile_name=PROFILE, bucket='DoesNotExist', key='Nonsense')
        self.assertEqual(drop.exists(), False)

        drop = S3DROP('oid:A', 'uid:A', profile_name=PROFILE, bucket='13b-266', key='Nonsense')
        self.assertEqual(drop.exists(), False)

        drop = S3DROP('oid:A', 'uid:A', profile_name=PROFILE, bucket='13b-266', key='13B-266.sb25386827.eb28551343.56619.33367407408_calibrated_deepfield.ms.tar')
        self.assertEqual(drop.exists(), True)

    @skipIf(not run_tests, "No profile found to run this test")
    def test_size(self):
        drop = S3DROP('oid:A', 'uid:A', profile_name=PROFILE, bucket='DoesNotExist', key='Nonsense')
        self.assertEqual(drop.size(), -1)

        drop = S3DROP('oid:A', 'uid:A', profile_name=PROFILE, bucket='13b-266', key='13B-266.sb25386827.eb28551343.56619.33367407408_calibrated_deepfield.ms.tar')
        self.assertEqual(drop.size(), 734067056640)