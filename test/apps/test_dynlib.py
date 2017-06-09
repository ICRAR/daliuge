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
import functools
import unittest

import six

from dfms import droputils
from dfms.apps.dynlib import DynlibApp, DynlibStreamApp
from dfms.drop import InMemoryDROP, NullDROP
import os


class DynlibAppTest(unittest.TestCase):

    def test_simple_batch_copy(self):
        """
        A ----> B --> C
           \--> D --> E

        Both B and D use the same dynamically loaded library and work with their
        input A in batch mode; that is, only when A is completed B and D are
        launched and executed (via their run method).
        """

        a = InMemoryDROP('a', 'a')
        b = DynlibApp('b', 'b', lib='libdynlib_example.so')
        c = InMemoryDROP('c', 'c')
        d = DynlibApp('d', 'd', lib='libdynlib_example.so')
        e = InMemoryDROP('e', 'e')
        b.addInput(a)
        b.addOutput(c)
        d.addInput(a)
        d.addOutput(e)

        # ~100 MBs of random data should be copied over from a to c via b
        # We only get 1MB of random data and they replicate it; otherwise,
        # depending on your system, it might take a while to get that much
        # random data
        data = os.urandom(1024 * 1024) * 100
        with droputils.DROPWaiterCtx(self, (c, e), 10):
            a.write(data)
            a.setCompleted()

        self.assertEqual(data, droputils.allDropContents(c))
        self.assertEqual(data, droputils.allDropContents(e))

    def test_simple_stream_copy(self):
        """
        A ----> B ----> C
           |       \--> D
           |
           \--> E ----> F
                   \--> G

        Both B and E use the same dynamically loaded library and work with their
        input A in streaming mode to copy the inputs into their outputs;
        that is, as data is written into A, B and E copy it to C, D, F and G
        """
        a = NullDROP('a', 'a')
        b, e = (DynlibStreamApp(x, x, lib='libdynlib_example.so') for x in ('b', 'e'))
        c, d, f, g, h, i = (InMemoryDROP(x, x) for x in ('c', 'd', 'f', 'g', 'h', 'i'))

        b.addStreamingInput(a)
        b.addOutput(c)
        b.addOutput(d)
        e.addStreamingInput(a)
        e.addOutput(f)
        e.addOutput(g)

        # ~100 MBs of data should be copied over from a to c via b
        data = os.urandom(1024 * 1024) * 100
        reader = six.BytesIO(data)
        with droputils.DROPWaiterCtx(self, (c, d, f, g), 10):
            # Write the data in chunks so we actually exercise multiple calls
            # to the data_written library call
            for d in iter(functools.partial(reader.read, 1024*1024), b''):
                a.write(d)
            a.setCompleted()

        for drop in (c, d, f, g):
            self.assertEqual(len(data), len(droputils.allDropContents(drop)))
            self.assertEqual(data, droputils.allDropContents(c))