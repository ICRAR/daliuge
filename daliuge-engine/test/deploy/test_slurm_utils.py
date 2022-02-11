#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2019
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

from dlg.deploy import deployment_utils


class TestSlurmUtils(unittest.TestCase):
    def assert_list_as_string(self, s, expected_list):
        slurm_list = deployment_utils.list_as_string(s)
        self.assertEqual(expected_list, slurm_list)

    def test_list_as_string(self):
        self.assert_list_as_string(
            "a008,b[072-073,076]", ["a008", "b072", "b073", "b076"]
        )
        self.assert_list_as_string(
            "pleiades[03-05]", ["pleiades03", "pleiades04", "pleiades05"]
        )
