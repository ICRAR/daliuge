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
import os
import unittest

from dlg import droputils
from dlg.apps.simple import SleepApp, CopyApp, SleepAndCopyApp
from dlg.ddap_protocol import DROPStates
from dlg.drop import NullDROP, InMemoryDROP


class TestSimpleApps(unittest.TestCase):

    def _test_graph_runs(self, drops, first, last):
        first = droputils.listify(first)
        with droputils.DROPWaiterCtx(self, last, 1):
            for f in first:
                f.setCompleted()

        for x in drops:
            self.assertEqual(DROPStates.COMPLETED, x.status)

    def test_sleepapp(self):

        # Nothing fancy, just run it and be done with it
        a = NullDROP('a', 'a')
        b = SleepApp('b', 'b')
        c = NullDROP('c', 'c')
        b.addInput(a)
        b.addOutput(c)

        self._test_graph_runs((a, b, c), a, c)

    def _test_copyapp_simple(self, app):

        # Again, not foo fancy, simple apps require simple tests
        a, c = (InMemoryDROP(x, x) for x in ('a', 'c'))
        b = app('b', 'b')
        b.addInput(a)
        b.addOutput(c)

        data = os.urandom(32)
        a.write(data)

        self._test_graph_runs((a, b, c), a, c)
        self.assertEqual(data, droputils.allDropContents(c))

    def _test_copyapp_order_preserved(self, app):

        # Inputs are copied in the order they are added
        a, b, d = (InMemoryDROP(x, x) for x in ('a', 'b', 'd'))
        c = app('c', 'c')
        for x in a, b:
            c.addInput(x)
        c.addOutput(d)

        data1 = os.urandom(32)
        data2 = os.urandom(32)
        a.write(data1)
        b.write(data2)

        self._test_graph_runs((a, b, c, d), (a, b), d)
        self.assertEqual(data1 + data2, droputils.allDropContents(d))

    def _test_copyapp(self, app):
        self._test_copyapp_simple(app)
        self._test_copyapp_order_preserved(app)

    def test_copyapp(self):
        self._test_copyapp(CopyApp)

    def test_sleepandcopyapp(self):
        self._test_copyapp(SleepAndCopyApp)