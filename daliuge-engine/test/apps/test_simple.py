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
import logging
import os
import pickle
import sys
import time
import unittest
from multiprocessing.pool import ThreadPool
from numpy import random, mean, array, concatenate, random, testing
from psutil import cpu_count

from dlg import droputils
from dlg.apps.simple import (
    GenericScatterApp,
    GenericNpyScatterApp,
    SleepApp,
    CopyApp,
    SleepAndCopyApp,
    ListAppendThrashingApp
)
from dlg.apps.simple import RandomArrayApp, AverageArraysApp, HelloWorldApp
from dlg.ddap_protocol import DROPStates
from dlg.drop import NullDROP, InMemoryDROP, FileDROP, NgasDROP
if sys.version_info >= (3, 8):
    from dlg.manager.shared_memory_manager import DlgSharedMemoryManager

logger = logging.getLogger(__name__)


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
        a = NullDROP("a", "a")
        b = SleepApp("b", "b")
        c = NullDROP("c", "c")
        b.addInput(a)
        b.addOutput(c)

        a = NullDROP("a", "a")

    def _test_copyapp_simple(self, app):

        # Again, not foo fancy, simple apps require simple tests
        a, c = (InMemoryDROP(x, x) for x in ("a", "c"))
        b = app("b", "b")
        b.addInput(a)
        b.addOutput(c)

        data = os.urandom(32)
        a.write(data)

        self._test_graph_runs((a, b, c), a, c)
        self.assertEqual(data, droputils.allDropContents(c))

    def _test_copyapp_order_preserved(self, app):

        # Inputs are copied in the order they are added
        a, b, d = (InMemoryDROP(x, x) for x in ("a", "b", "d"))
        c = app("c", "c")
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
        i = NullDROP("i", "i")
        c = RandomArrayApp("c", "c")
        o = InMemoryDROP("o", "o")
        c.addInput(i)
        c.addOutput(o)
        self._test_graph_runs((i, c, o), i, o)
        marray = c._getArray()
        data = pickle.loads(droputils.allDropContents(o))
        v = marray == data
        self.assertEqual(v.all(), True)

    def test_averagearraysapp(self):
        a = AverageArraysApp("a", "a")
        i1, i2, o = (InMemoryDROP(x, x) for x in ("i1", "i2", "o"))
        c = AverageArraysApp("c", "c")
        c.addInput(i1)
        c.addInput(i2)
        c.addOutput(o)
        for x in i1, i2:
            a.addInput(x)
        data1 = random.randint(0, 100, size=100)
        data2 = random.randint(0, 100, size=100)
        i1.write(pickle.dumps(data1))
        i2.write(pickle.dumps(data2))
        big_mean = mean(mean([array(data1), array(data2)], axis=0))
        self._test_graph_runs((i1, i2, c, o), (i1, i2), o)
        average = pickle.loads(droputils.allDropContents(o))
        self.assertEqual(big_mean, average)

    def test_helloworldapp(self):
        h = HelloWorldApp("h", "h")
        b = FileDROP("c", "c")
        h.addOutput(b)
        b.addProducer(h)
        h.execute()
        self.assertEqual(h.greeting.encode("utf8"), droputils.allDropContents(b))

    def test_parallelHelloWorld(self):
        m0 = InMemoryDROP("m0", "m0")
        s = GenericScatterApp("s", "s", num_of_copies=4)
        greets = ["World", "Solar system", "Galaxy", "Universe"]
        m0.write(pickle.dumps(greets))
        s.addInput(m0)
        m = []
        h = []
        f = []
        for i in range(1, len(greets) + 1, 1):
            m.append(InMemoryDROP("m%d" % i, "m%d" % i))
            h.append(HelloWorldApp("h%d" % i, "h%d" % i))
            f.append(FileDROP("f%d" % i, "f%d" % i))
            s.addOutput(m[-1])
            h[-1].addInput(m[-1])
            h[-1].addOutput(f[-1])
        ad = [m0, s]
        ad.extend(m)
        ad.extend(h)
        ad.extend(f)
        self._test_graph_runs(ad, m0, f)
        for i in range(len(f)):
            self.assertEqual(
                ("Hello %s" % greets[i]).encode("utf8"), droputils.allDropContents(f[i])
            )

    def test_ngasio(self):
        nd_in = NgasDROP("HelloWorld.txt", "HelloWorld.txt")
        nd_in.ngasSrv = "ngas.ddns.net"
        b = CopyApp("b", "b")
        did = "HelloWorld-%f" % time.time()
        nd_out = NgasDROP(did, did, len=11)
        nd_out.ngasSrv = "ngas.ddns.net"
        nd_out.len = nd_in.size
        d = CopyApp("d", "d")
        i = InMemoryDROP("i", "i")
        # b.addInput(nd_in)
        # b.addOutput(nd_out)
        nd_in.addConsumer(b)
        nd_out.addProducer(b)
        d.addInput(nd_out)
        i.addProducer(d)
        # b.addOutput(i)
        self._test_graph_runs((nd_in, b, nd_out, i, d), nd_in, i, timeout=10)
        self.assertEqual(b"Hello World", droputils.allDropContents(i))

    def test_genericScatter(self):
        data_in = random.randint(0, 100, size=100)
        b = InMemoryDROP("b", "b")
        b.write(pickle.dumps(data_in))
        s = GenericScatterApp("s", "s", num_of_copies=2)
        s.addInput(b)
        o1 = InMemoryDROP("o1", "o1")
        o2 = InMemoryDROP("o2", "o2")
        for x in o1, o2:
            s.addOutput(x)
        self._test_graph_runs((b, s, o1, o2), b, (o1, o2), timeout=4)

        data1 = pickle.loads(droputils.allDropContents(o1))
        data2 = pickle.loads(droputils.allDropContents(o2))
        data_out = concatenate([data1, data2])
        self.assertEqual(data_in.all(), data_out.all())

    def test_genericNpyScatter(self):
        data_in = random.rand(100, 100)
        b = InMemoryDROP("b", "b")
        droputils.save_numpy(b, data_in)
        s = GenericNpyScatterApp("s", "s", num_of_copies=2)
        s.addInput(b)
        o1 = InMemoryDROP("o1", "o1")
        o2 = InMemoryDROP("o2", "o2")
        for x in o1, o2:
            s.addOutput(x)
        self._test_graph_runs((b, s, o1, o2), b, (o1, o2), timeout=4)

        data1 = droputils.load_numpy(o1)
        data2 = droputils.load_numpy(o2)
        data_out = concatenate([data1, data2])
        self.assertEqual(data_in.all(), data_out.all())

    def test_genericNpyScatter_multi(self):
        data1_in = random.rand(100, 100)
        data2_in = random.rand(100, 100)
        b = InMemoryDROP("b", "b")
        c = InMemoryDROP("c", "c")
        droputils.save_numpy(b, data1_in)
        droputils.save_numpy(c, data2_in)
        s = GenericNpyScatterApp("s", "s", num_of_copies=2, scatter_axes="[0,0]")
        s.addInput(b)
        s.addInput(c)
        o1 = InMemoryDROP("o1", "o1")
        o2 = InMemoryDROP("o2", "o2")
        o3 = InMemoryDROP("o3", "o3")
        o4 = InMemoryDROP("o4", "o4")
        for x in o1, o2, o3, o4:
            s.addOutput(x)
        self._test_graph_runs((b, s, o1, o2, o3, o4), (b, c), (o1, o2, o3, o4), timeout=4)

        data11 = droputils.load_numpy(o1)
        data12 = droputils.load_numpy(o2)
        data1_out = concatenate([data11, data12])
        self.assertEqual(data1_out.shape, data1_in.shape)
        testing.assert_array_equal(data1_out, data1_in)

        data21 = droputils.load_numpy(o3)
        data22 = droputils.load_numpy(o4)
        data2_out = concatenate([data21, data22])
        testing.assert_array_equal(data2_out, data2_in)

    def test_listappendthrashing(self, size=1000):
        a = InMemoryDROP('a', 'a')
        b = ListAppendThrashingApp('b', 'b', size=size)
        self.assertEqual(b.size, size)
        c = InMemoryDROP('c', 'c')
        b.addInput(a)
        b.addOutput(c)
        self._test_graph_runs((a, b, c), a, c, timeout=4)
        data_out = pickle.loads(droputils.allDropContents(c))
        self.assertEqual(b.marray, data_out)

    @unittest.skipIf(sys.version_info < (3, 8), "Multiprocessing not compatible with Python < 3.8")
    def test_multi_listappendthrashing(self, size=1000, parallel=True):
        max_threads = cpu_count(logical=False)
        drop_ids = [chr(97 + x) for x in range(max_threads)]
        threadpool = ThreadPool(processes=max_threads)
        memory_manager = DlgSharedMemoryManager()
        session_id = 1
        memory_manager.register_session(session_id)
        S = InMemoryDROP('S', 'S')
        X = AverageArraysApp('X', 'X')
        Z = InMemoryDROP('Z', 'Z')
        drops = [ListAppendThrashingApp(x, x, size=size) for x in drop_ids]
        mdrops = [InMemoryDROP(chr(65 + x), chr(65 + x)) for x in range(max_threads)]
        if parallel:
            # a bit of magic to get the app drops using the processes
            _ = [drop.__setattr__('_tp', threadpool) for drop in drops]
            _ = [drop.__setattr__('_tp', threadpool) for drop in mdrops]
            _ = [drop.__setattr__('_sessID', session_id) for drop in mdrops]
            _ = [memory_manager.register_drop(drop.uid, session_id) for drop in mdrops]
            X.__setattr__('_tp', threadpool)
            Z.__setattr__('_tp', threadpool)
            Z.__setattr__('_sessID', session_id)
            memory_manager.register_drop(Z.uid, session_id)

        _ = [d.addInput(S) for d in drops]
        _ = [d.addOutput(m) for d, m in zip(drops, mdrops)]
        _ = [X.addInput(m) for m in mdrops]
        X.addOutput(Z)
        print(f"Number of inputs/outputs: {len(X.inputs)}, {len(X.outputs)}")
        self._test_graph_runs([S, X, Z] + drops + mdrops, S, Z, timeout=200)
        # Need to run our 'copy' of the averaging APP
        num_array = []
        for drop in mdrops:
            buf = droputils.allDropContents(drop)
            num_array.extend(pickle.loads(buf))
        X.marray = num_array
        average = X.averageArray()
        # Load actual results
        graph_result = droputils.allDropContents(Z)
        graph_result = pickle.loads(graph_result)
        self.assertEqual(graph_result, average)
        # Must be called to unlink all shared memory
        memory_manager.shutdown_all()

    @unittest.skipIf(sys.version_info < (3, 8), "Multiprocessing not compatible with Python < 3.8")
    def test_speedup(self):
        """
        Run serial and parallel test and report speedup.
        NOTE: In order to get the stdout you need to run pyest with
        --capture=tee-sys
        """
        size = 2000
        print("Starting serial test..")
        st = time.time()
        self.test_multi_listappendthrashing(size=size, parallel=False)
        t1 = time.time() - st
        print("Starting parallel test..")
        st = time.time()
        self.test_multi_listappendthrashing(size=size, parallel=True)
        t2 = time.time() - st
        print(f"Speedup: {t1 / t2:.2f} from {cpu_count(logical=False)} cores")
        # TODO: This is unpredictable, but maybe we can do something meaningful.
        # self.assertAlmostEqual(t1/cpu_count(logical=False), t2, 1)
        # How about this? We only need to see some type of speedup
        if cpu_count(logical=False) > 1 and size > 500:
            self.assertGreater(t1 / t2, 1)
        else:
            # Ensure that multi-threading overhead doesn't ruin serial performance?
            self.assertAlmostEqual(t1, t2, delta=0.5)
