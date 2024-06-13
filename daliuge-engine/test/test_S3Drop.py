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

# pylint: disable=possibly-used-before-assignment
"""
Test the S3 Drop
"""
# If the profile is not present in the current user's account
# we simply skip the test

import unittest

run_tests = True
try:
    import boto3
    from dlg.data.drops.s3_drop import S3DROP
except ImportError:
    run_tests = False

if run_tests:
    PROFILE = "acacia-awicenec"
    try:
        boto3.session.Session(profile_name=PROFILE)
        bucket = "rascil"
        key = "2022-09-27T17:30:02_-13_0/1"
        endpoint_url = "https://projects.pawsey.org.au"
    except:
        run_tests = False


class TestS3Drop(unittest.TestCase):
    @skipIf(not run_tests, "No profile found to run this test")
    def test_bucket_exists(self):
        drop = S3DROP(
            "oid:A", "uid:A", 
            profile_name=PROFILE, 
            Bucket="ska-low-sim", 
            Key="Nonsense",
            endpoint_url=endpoint_url,
        )
        self.assertEqual(drop.exists(), False)
        del(drop)

        drop = S3DROP(
            "oid:A",
            "uid:A",
            profile_name=PROFILE,
            Bucket=bucket,
            Key=key,
            endpoint_url=endpoint_url,
        )
        self.assertEqual(drop.exists(), True)

    @skipIf(not run_tests, "No profile found to run this test")
    def test_size(self):
        drop = S3DROP(
            "oid:A", "uid:A", 
            profile_name=PROFILE, 
            Bucket="ska-low-sim", 
            Key="Nonsense",
            endpoint_url=endpoint_url,
        )
        self.assertNotEqual(drop.size, 2785011)
        del(drop)

        drop = S3DROP('oid:A', 'uid:A', 
            profile_name=PROFILE, 
            Bucket=bucket, 
            Key=key,
            endpoint_url=endpoint_url,
            )
        self.assertEqual(drop.size, 2785011)
    
    @skipIf(not run_tests, "No profile found to run this test")
    def test_read(self):
        
        drop = S3DROP('oid:A', 'uid:A', 
            profile_name=PROFILE, 
            Bucket=bucket, 
            Key=key,
            endpoint_url=endpoint_url,
            )
        drop.status = 2
        desc = drop.open()
        self.assertEqual(len(drop.read(desc, count=-1)), 2785011)

    @skipIf(not run_tests, "No profile found to run this test")
    def test_write(self):
        """
        This is also testing delete.
        """
        drop = S3DROP('oid:A', 'uid:A', 
            profile_name=PROFILE, 
            Bucket=bucket, 
            Key="testData",
            endpoint_url=endpoint_url,
            )
        if drop.exists():
            drop.delete()
        testdata = 10000*b"a"
        written = drop.write(testdata)
        self.assertEqual(written, len(testdata))
        drop.delete()
