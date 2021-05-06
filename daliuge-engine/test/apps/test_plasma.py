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

from dlg.drop import FileDROP, PlasmaDROP
from dlg import droputils

casa_unavailable = True
try:
    import pyarrow.plasma as plasma
    from dlg.apps.plasma import MSPlasmaWriter, MSPlasmaReader
    from casacore import tables
    casa_unavailable = False
except:
    pass

logging.basicConfig()

@unittest.skipIf(casa_unavailable, "python-casacore not available")
class CRCAppTests(unittest.TestCase):

    def compare_ms(self, in_file, out_file):
        a = []
        b = []
        with tables.table(out_file) as t1:
            for i in t1:
                a.append(i['DATA'])

        with tables.table(in_file) as t2:
            for i in t2:
                b.append(i['DATA'])

        for i, j in enumerate(a):
            comparison = j == b[i]
            self.assertEqual(comparison.all(), True)

    def test_plasma(self):
        in_file = '/tmp/test.ms'
        out_file = '/tmp/copy.ms'

        with tarfile.open('./data/test_ms.tar.gz', 'r') as ref:
            ref.extractall('/tmp/')

        a = FileDROP('a', 'a', filepath=in_file)
        b = MSPlasmaWriter('b', 'b')
        c = PlasmaDROP('c', 'c')
        d = MSPlasmaReader('d', 'd')
        e = FileDROP('e', 'e', filepath=out_file)

        b.addInput(a)
        b.addOutput(c)
        d.addInput(c)
        d.addOutput(e)

        # Check the MS DATA content is the same as original
        with droputils.DROPWaiterCtx(self, e, 5):
            a.setCompleted()

        self.compare_ms(in_file, out_file)

        # check we can go from dataURL to plasma ID
        client = plasma.connect("/tmp/plasma")
        a = c.dataURL.split('//')[1].decode("hex")
        client.get(plasma.ObjectID(a))