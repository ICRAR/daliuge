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
import logging
import tarfile
import binascii
import shutil

from dlg.drop import FileDROP, PlasmaDROP, InMemoryDROP
from dlg import droputils

cbf_unavailable = True
try:
    from cbf_sdp import msutils

    cbf_unavailable = False
except Exception as e:
    print(e)
    pass

casa_unavailable = True
try:
    import pyarrow.plasma as plasma
    from dlg.apps.plasma import MSPlasmaWriter, MSPlasmaReader
    from dlg.apps.plasma import MSStreamingPlasmaConsumer, MSStreamingPlasmaProducer
    from casacore import tables

    casa_unavailable = False
except Exception as e:
    print(e)
    pass

logging.basicConfig()


@unittest.skipIf(casa_unavailable, "python-casacore not available")
class CRCAppTests(unittest.TestCase):
    def compare_measurement_sets(self, in_file, out_file):
        asserter = type("asserter", (msutils.MSAsserter, unittest.TestCase), {})()
        asserter.assert_ms_equal(in_file, out_file)

    def compare_ms(self, in_file, out_file):
        a = []
        b = []
        with tables.table(out_file) as t1:
            for i in t1:
                a.append(i["DATA"])

        with tables.table(in_file) as t2:
            for i in t2:
                b.append(i["DATA"])

        for i, j in enumerate(a):
            comparison = j == b[i]
            self.assertEqual(comparison.all(), True)

    @unittest.skipIf(casa_unavailable, "sdp-cbf not available")
    def test_plasma_stream(self):
        in_file = "/tmp/test.ms"
        out_file = "/tmp/copy.ms"

        try:
            shutil.rmtree(in_file)
        except:
            pass

        try:
            shutil.rmtree(out_file)
        except:
            pass

        with tarfile.open("/daliuge/test/apps/data/test_ms.tar.gz", "r") as ref:
            ref.extractall("/tmp/")

        prod = MSStreamingPlasmaProducer("1", "1")
        cons = MSStreamingPlasmaConsumer("2", "2")
        drop = InMemoryDROP("3", "3")
        ms_in = FileDROP("4", "4", filepath=in_file)
        ms_out = FileDROP("5", "5", filepath=out_file)
        prod.addInput(ms_in)
        prod.addOutput(drop)
        drop.addStreamingConsumer(cons)
        cons.addOutput(ms_out)

        with droputils.DROPWaiterCtx(self, cons, 1000):
            prod.async_execute()

        # self.compare_ms(in_file, out_file)
        self.compare_measurement_sets(in_file, out_file)

    def test_plasma(self):
        in_file = "/tmp/test.ms"
        out_file = "/tmp/copy.ms"

        try:
            shutil.rmtree(in_file)
        except:
            pass

        try:
            shutil.rmtree(out_file)
        except:
            pass

        with tarfile.open("/daliuge/test/apps/data/test_ms.tar.gz", "r") as ref:
            ref.extractall("/tmp/")

        a = FileDROP("a", "a", filepath=in_file)
        b = MSPlasmaWriter("b", "b")
        c = PlasmaDROP("c", "c")
        d = MSPlasmaReader("d", "d")
        e = FileDROP("e", "e", filepath=out_file)

        b.addInput(a)
        b.addOutput(c)
        d.addInput(c)
        d.addOutput(e)

        # Check the MS DATA content is the same as original
        with droputils.DROPWaiterCtx(self, e, 5):
            a.setCompleted()

        # self.compare_ms(in_file, out_file)
        self.compare_measurement_sets(in_file, out_file)

        # check we can go from dataURL to plasma ID
        client = plasma.connect("/tmp/plasma")
        a = c.dataURL.split("//")[1]
        a = binascii.unhexlify(a)
        client.get(plasma.ObjectID(a))
