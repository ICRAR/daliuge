#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2017
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

from dfms import droputils
from dfms.apps.dynlib import DynlibApp
from dfms.drop import InMemoryDROP


class DynlibAppTest(unittest.TestCase):

    def test_simple_loading(self):

        a = InMemoryDROP('a', 'a')
        b = DynlibApp('b', 'b', lib='libdynlib_example.so')
        c = InMemoryDROP('c', 'c')
        b.addInput(a)
        b.addOutput(c)

        # ~100 MBs of data should be copied over from a to c via b
        data = b'0' * 6 * 4096 * 4096
        with droputils.DROPWaiterCtx(self, c, 100):
            a.write(data)
            a.setCompleted()

        # Free up some memory
        a.delete()

        self.assertEqual(data, droputils.allDropContents(c), 'data is not the same')