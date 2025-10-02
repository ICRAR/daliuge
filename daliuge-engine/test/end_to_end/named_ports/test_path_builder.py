#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2014
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
import os.path
import unittest
import datetime

from dlg.data.path_builder import (find_dlg_fstrings, filepath_from_string,
                                   base_uid_pathname, PathType)


class TestPathBuilders(unittest.TestCase):

    def setUp(self):

        self.filename = "test"


    def testBaseUIDGenerator(self):
        """
        Using the baseUID Generator with the humanreadable key
        """
        res = base_uid_pathname("", "")
        self.assertEqual(None, res)
        res = base_uid_pathname(123456, None)
        self.assertEqual("123456_", res)
        res = base_uid_pathname("123456_abcdef", "0-1")
        self.assertEqual("123456_0_1", res)


    def test_file_path_from_string(self):
        """
        Go through each of the variations we expect for Files we want to name
        :return:
        """
        uid = "123456"
        res = filepath_from_string("prefix_{uid}_{datetime}.dat", PathType.File, uid=uid)
        dstr = datetime.date.today().strftime("%Y-%m-%d")
        self.assertEqual(f"prefix_123456_{dstr}.dat", res)
        dt = "2025-08-17"
        res = filepath_from_string("prefix_{uid}_{datetime}.dat", PathType.File,
                                   uid=uid, datetime=dt)
        self.assertEqual("prefix_123456_2025-08-17.dat", res)
        res = filepath_from_string(None, PathType.File, uid=None)
        self.assertEqual(None, res)

    def test_auto_path_from_string(self):
        """
        Test that the 'auto' keyword works as expected by generating the base_uid_name.
        """
        uid = "123456"
        res = filepath_from_string("{auto}.dat", PathType.File, uid=uid, humanKey="0-1")
        self.assertEqual("123456_0-1.dat", res)
        res = filepath_from_string("{auto}.ms/", PathType.Directory, uid=uid, humanKey="0-1")
        self.assertEqual("123456_0-1.ms/", res)

    def test_directory_path_from_string(self):
        """
        Go through each of the variations we expect for Directories we want to name

        :return:
        """

        uid = "123456"
        # 'path' takes precedence over dirname if there is already a value.
        # This is based on InputApp setting the path for DirectoryDROP,
        # which takes precedence over the auto-generated directoryDROP path.
        res = filepath_from_string("$HOME/directory_name", PathType.Directory, uid=uid,
                                   dirname="/$HOME/session_dir/")
        self.assertEqual(os.path.expandvars("$HOME/directory_name"), res)

        # If 'path' is not absolute, we want to see it put undernead the
        # 'base_name' directory.

        res = filepath_from_string("directory_name", PathType.Directory, uid=uid,
                                   dirname='base_name/123456')
        self.assertEqual(os.path.expandvars("base_name/123456/directory_name"), res)

        # If 'path' is not set, we want to see it put underneath 'base_name' with the
        # auto-generated path.

        res = filepath_from_string(None, PathType.Directory, uid=uid,
                                   dirname='base_name/123456')
        self.assertEqual(os.path.expandvars("base_name/123456/"), res)

class TestHelperFunctions(unittest.TestCase):

    def test_find_dlg_strings_normal(self):
        fn_empty = ""
        self.assertEqual([], find_dlg_fstrings(fn_empty))
        fn = "prefix_{uid}_{datetime}.dat"  # ['uid', 'datetime']
        self.assertEqual(['uid', 'datetime'], find_dlg_fstrings(fn))
        fn = "prefix_{{uid}}.txt"
        self.assertEqual(["uid"], find_dlg_fstrings(fn))

    def test_find_dlg_strings_badinput(self):
        fn = 57
        self.assertListEqual([], find_dlg_fstrings(fn))

        fn = None
        self.assertListEqual([], find_dlg_fstrings(fn))

