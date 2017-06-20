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
import os
import unittest

import six

from dfms import droputils
from dfms.apps.dynlib import DynlibApp, DynlibStreamApp
from dfms.drop import InMemoryDROP, NullDROP

_libname = 'dynlib_example'
_libfname = 'libdynlib_example.so'
_libpath = os.path.join(os.path.dirname(__file__), _libfname)
print_stats = 0

# Try to compile the library, if possible. If it's there already we're cool
def _try_library():

    if os.path.isfile(_libpath):
        return True

    prev_path = os.getcwd()
    os.chdir(os.path.dirname(__file__))
    try:
        import distutils.ccompiler

        from dfms import get_include_dir

        comp = distutils.ccompiler.new_compiler()
        distutils.sysconfig.customize_compiler(comp)

        comp.add_include_dir(get_include_dir())
        objs = comp.compile([_libname + '.c'])
        comp.link_shared_lib(objs, output_libname=_libname)

        return True
    except:
        return False
    finally:
        os.chdir(prev_path)

@unittest.skipUnless(_try_library(), "Dynamic library not available")
class DynlibAppTest(unittest.TestCase):

    def test_simple_batch_copy(self):
        self._test_simple_copy(False)

    def test_simple_stream_copy(self):
        self._test_simple_copy(True)

    def _test_simple_copy(self, streaming):
        """
        Checks that the following graph works, both in streaming and batch mode:

        A ----> B ----> C
           |       \--> D
           |
           \--> E ----> F
                   \--> G

        Both B and E use the same dynamically loaded library and work with their
        input A to copy the inputs into their outputs.
        """

        # Build the graph
        a = (NullDROP if streaming else InMemoryDROP)('a', 'a')
        b, e = ((DynlibStreamApp if streaming else DynlibApp)(x, x, lib=_libpath, print_stats=print_stats) for x in ('b', 'e'))
        c, d, f, g = (InMemoryDROP(x, x) for x in ('c', 'd', 'f', 'g'))
        for app, outputs in (b, (c, d)), (e, (f, g)):
            (app.addStreamingInput if streaming else app.addInput)(a)
            for o in outputs:
                app.addOutput(o)

        # ~100 MBs of data should be copied over from a to c and d via b, etc
        data = os.urandom(1024 * 1024) * 100
        reader = six.BytesIO(data)
        with droputils.DROPWaiterCtx(self, (c, d, f, g), 10):
            if streaming:
                # Write the data in chunks so we actually exercise multiple calls
                # to the data_written library call
                for datum in iter(functools.partial(reader.read, 1024*1024), b''):
                    a.write(datum)
            else:
                a.write(data)
            a.setCompleted()

        for drop in (c, d, f, g):
            drop_data = droputils.allDropContents(drop)
            self.assertEqual(len(data), len(drop_data), 'Data from %r is not what we wanted :(' % (drop,))
            self.assertEqual(data, drop_data)