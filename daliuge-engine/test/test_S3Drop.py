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

run_tests = True
try:
    import boto3
    from dlg.s3_drop import S3DROP
except ImportError:
    run_tests = False

if run_tests:
    PROFILE = "aws-chiles02"
    try:
        boto3.session.Session(profile_name=PROFILE)
    except:
        run_tests = False


class TestS3Drop(unittest.TestCase):
    @skipIf(not run_tests, "No profile found to run this test")
    def test_bucket_exists(self):
        drop = S3DROP(
            "oid:A",
            "uid:A",
            profile_name=PROFILE,
            bucket="DoesNotExist",
            key="Nonsense",
        )
        self.assertEqual(drop.exists(), False)

        # drop = S3DROP(
        #     "oid:A", "uid:A", profile_name=PROFILE, bucket="ska-low-sim", key="Nonsense"
        # )
        # self.assertEqual(drop.exists(), False)

        drop = S3DROP('oid:A', 'uid:A', profile_name=PROFILE, bucket='13b-266', key='chan_avg_1/SPW_9/13B-266.sb25387671.eb28662252.56678.178276527775.spw_9.tar')
        self.assertEqual(drop.exists(), True)

    @skipIf(not run_tests, "No profile found to run this test")
    def test_size(self):
        drop = S3DROP(
            "oid:A",
            "uid:A",
            profile_name=PROFILE,
            bucket="DoesNotExist",
            key="Nonsense",
        )
        # self.assertEqual(drop.size(), -1)

        drop = S3DROP('oid:A', 'uid:A', profile_name=PROFILE, bucket='13b-266', key='chan_avg_1/SPW_9/13B-266.sb25387671.eb28662252.56678.178276527775.spw_9.tar')
        self.assertEqual(drop.size(), 250112000)
