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

import unittest
import datetime
from dlg.data.path_builder import (find_dlg_fstrings, filepath_from_string,
                                   base_uid_pathname)


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
        res = base_uid_pathname("123456_abcdef", "0/1")
        self.assertEqual("123456_0_1", res)



    def test_file_path_from_string(self):
        uid = "123456"
        res = filepath_from_string("prefix_{uid}_{datetime}.dat", None, uid=uid)
        dstr = datetime.date.today().strftime("%Y-%m-%d")
        self.assertEqual(f"prefix_123456_{dstr}.dat", res)
        dt = "2025-08-17"
        res = filepath_from_string("prefix_{uid}_{datetime}.dat", None,
                                   uid=uid, datetime=dt)
        self.assertEqual("prefix_123456_2025-08-17.dat", res)
        res = filepath_from_string(None, None, uid=None)
        self.assertEqual(None, res)


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

