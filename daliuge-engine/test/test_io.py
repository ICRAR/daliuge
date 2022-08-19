#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2015
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

from dlg.data.io import NullIO, OpenMode


class TestIO(unittest.TestCase):
    def test_invalidUseCases(self):
        io = NullIO()

        # Not opened yet
        self.assertRaises(ValueError, io.write, "")
        self.assertRaises(ValueError, io.read, "")

        # Opening in read-only mode
        io.open(OpenMode.OPEN_READ)
        self.assertRaises(ValueError, io.write, "")
        io.close()

        # Opening in write-only mode
        io.open(OpenMode.OPEN_WRITE)
        self.assertRaises(ValueError, io.read, 1)
        io.close()

        # It's OK to close it again
        io.close()
