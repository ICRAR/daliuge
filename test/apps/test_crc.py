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
"""
Test the CRCApp application
"""

import os
import unittest

import six

from dfms import droputils
from dfms.apps.crc import CRCApp, crc32
from dfms.apps.dynlib import DynlibApp
from dfms.drop import FileDROP, InMemoryDROP

from . import test_dynlib


class CRCAppTests(unittest.TestCase):

    @unittest.skipUnless(test_dynlib._try_library(), "Example dynamic library not available")
    def test_with_dynlib(self):
        """
        We test the following graph:

        A -----> B ----> C ---> D ---> E
           |        \--> F ---> G ---> H
           \------------------> I ---> J

        A and C are FileDrops; B is a DynlibApp; D, G and I are CRCApps;
        F, E, H and J are InMemoryDrops.

        The DynlibApp B copies A into C and F; therefore D, G and I should yield
        the same results, meaning that E, H and J should have the same contents.
        Similarly, A, C and F should have the same contents.

        This graph was experiencing some problems in a MacOS machine. Hopefully
        this test will shed some light on that issue and allow us to track it
        down and fix it.
        """

        # Build drops and wire them together
        a, c = (FileDROP(x, x) for x in ('a', 'c'))
        b = DynlibApp('b', 'b', lib=test_dynlib._libpath)
        d, g, i = (CRCApp(x, x) for x in ('d', 'g', 'i'))
        f, e, h, j = (InMemoryDROP(x, x) for x in ('f', 'e', 'h', 'j'))

        for data, app in (a, b), (c, d), (f, g), (a, i):
            app.addInput(data)
        for app, data in (b, c), (b, f), (d, e), (g, h), (i, j):
            app.addOutput(data)

        # The crc32 is the same used by the CRCApp, see the imports
        data = os.urandom(1024)
        crc = six.b(str(crc32(data)))

        # Execute the graph and check results
        with droputils.DROPWaiterCtx(self, (e, h, j), 5):
            a.write(data)
            a.setCompleted()

        # Data and CRCs are the expected ones
        for what, who in (data, (a, c, f)), (crc, (e, h, j)):
            for drop in who:
                self.assertEqual(what, droputils.allDropContents(drop))