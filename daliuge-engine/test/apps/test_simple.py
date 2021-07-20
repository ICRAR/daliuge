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
import pickle
import unittest
from numpy import random, mean, array, concatenate


from dlg import droputils
from dlg.droputils import DROPWaiterCtx
from dlg.apps.simple import GenericScatterApp, SleepApp, CopyApp, SleepAndCopyApp
from dlg.apps.simple import RandomArrayApp, AverageArraysApp, HelloWorldApp
from dlg.ddap_protocol import DROPStates
from dlg.drop import NullDROP, InMemoryDROP, FileDROP, NgasDROP

class TestSimpleApps(unittest.TestCase):

    def _test_graph_runs(self, drops, first, last, timeout=1):
        first = droputils.listify(first)
        with droputils.DROPWaiterCtx(self, last, timeout):
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

        a = NullDROP('a', 'a')

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

    def test_randomarrayapp(self):
        i = NullDROP('i', 'i')
        c = RandomArrayApp('c', 'c')
        o = InMemoryDROP('o', 'o')
        c.addInput(i)
        c.addOutput(o)
        self._test_graph_runs((i, c, o), i, o)
        marray = c._getArray()
        data = pickle.loads(droputils.allDropContents(o))
        v = marray == data
        self.assertEqual(v.all(), True)

    def test_averagearraysapp(self):
        a = AverageArraysApp('a', 'a')
        i1, i2, o = (InMemoryDROP(x, x) for x in ('i1', 'i2', 'o'))
        c = AverageArraysApp('c', 'c')
        c.addInput(i1)
        c.addInput(i2)
        c.addOutput(o)
        for x in i1, i2:
            a.addInput(x)
        data1 = random.randint(0, 100, size=100)
        data2 = random.randint(0, 100, size=100)
        i1.write(pickle.dumps(data1))
        i2.write(pickle.dumps(data2))
        m = mean([array(data1), array(data2)], axis=0)
        self._test_graph_runs((i1, i2, c, o), (i1, i2), o)
        average = pickle.loads(droputils.allDropContents(o))
        v = (m == average)
        self.assertEqual(v.all(), True)

    def test_helloworldapp(self):
        h = HelloWorldApp('h', 'h')
        b = FileDROP('c', 'c')
        h.addOutput(b)
        b.addProducer(h)
        h.execute()
        self.assertEqual(h.greeting.encode('utf8'), droputils.allDropContents(b))

    def test_ngasio(self):
        nd_in = NgasDROP('HelloWorld.txt', 'HelloWorld.txt')
        nd_in.ngasSrv = 'ngas.ddns.net'
        b = CopyApp('b', 'b')
        nd_out = NgasDROP('HelloWorldOut.txt', 'HelloWorldOut.txt')
        nd_out.ngasSrv = 'ngas.ddns.net'
        i = InMemoryDROP('i', 'i')
        b.addInput(nd_in)
        b.addOutput(nd_out)
        nd_out.addProducer(b)
        i.addProducer(b)
        b.addOutput(i)
        self._test_graph_runs((nd_in,b,i,nd_out),nd_in, nd_out, timeout=4)
        self.assertEqual(b"Hello World", droputils.allDropContents(i))

    def test_genericScatter(self):
        data_in = random.randint(0, 100, size=100)
        b = InMemoryDROP('b', 'b')
        b.write(pickle.dumps(data_in))
        s = GenericScatterApp('s', 's')
        s.addInput(b)
        o1 = InMemoryDROP('o1', 'o1')
        o2 = InMemoryDROP('o2', 'o2')
        for x in o1, o2:
            s.addOutput(x)
        self._test_graph_runs((b, s, o1, o2), b, (o1, o2), timeout=4)

        data1 = pickle.loads(droputils.allDropContents(o1))
        data2 = pickle.loads(droputils.allDropContents(o2))
        data_out = concatenate([data1, data2])
        self.assertEqual(data_in.all(), data_out.all())



        
