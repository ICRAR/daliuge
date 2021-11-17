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
"""
Created on 22 Jun 2015

@author: rtobar
"""

import os
import shutil
import tempfile
import time
import unittest

from dlg.ddap_protocol import DROPStates, DROPPhases
from dlg.drop import FileDROP, DirectoryContainer, BarrierAppDROP
from dlg.droputils import DROPWaiterCtx
from dlg.lifecycle import dlm


class TestDataLifecycleManager(unittest.TestCase):
    def tearDown(self):
        shutil.rmtree("/tmp/daliuge_tfiles", True)
        shutil.rmtree("/tmp/sdp-hsm", True)

    def _writeAndClose(self, drop):
        drop.write(b" ")
        # all DROPs submitted to this method have expectedSize=1, so this
        # will trigger the change to COMPLETED

    def test_basicCreation(self):
        manager = dlm.DataLifecycleManager()
        manager.startup()
        manager.cleanup()

    def test_dropAddition(self):
        with dlm.DataLifecycleManager() as manager:
            drop = FileDROP("oid:A", "uid:A1", expectedSize=10)
            manager.addDrop(drop)

    def test_dropCompleteTriggersReplication(self):
        with dlm.DataLifecycleManager() as manager:
            drop = FileDROP("oid:A", "uid:A1", expectedSize=1)
            manager.addDrop(drop)
            self._writeAndClose(drop)

            # The call to close() should have turned it into a SOLID object
            # because the DLM replicated it
            self.assertEqual(DROPPhases.SOLID, drop.phase)
            self.assertEqual(2, len(manager.getDropUids(drop)))

            # Try the same with a non-precious data object, it shouldn't be replicated
            drop = FileDROP("oid:B", "uid:B1", expectedSize=1, precious=False)
            manager.addDrop(drop)
            self._writeAndClose(drop)
            self.assertEqual(DROPPhases.GAS, drop.phase)
            self.assertEqual(1, len(manager.getDropUids(drop)))

    def test_expiringNormalDrop(self):

        with dlm.DataLifecycleManager(checkPeriod=0.5) as manager:
            drop = FileDROP("oid:A", "uid:A1", expectedSize=1, lifespan=0.5)
            manager.addDrop(drop)

            # Writing moves the DROP to COMPLETE
            self._writeAndClose(drop)

            # Wait now, the DROP should be moved by the DLM to EXPIRED
            time.sleep(1)

            self.assertEqual(DROPStates.EXPIRED, drop.status)

    def test_lostDrop(self):
        with dlm.DataLifecycleManager(checkPeriod=0.5) as manager:
            drop = FileDROP(
                "oid:A", "uid:A1", expectedSize=1, lifespan=10, precious=False
            )
            manager.addDrop(drop)
            self._writeAndClose(drop)

            # "externally" remove the file, its contents now don't exist
            os.unlink(drop.path)

            # Let the DLM do its work
            time.sleep(1)

            # Check that the DROP is marked as LOST
            self.assertEqual(DROPPhases.LOST, drop.phase)

    def test_cleanupExpiredDrops(self):
        with dlm.DataLifecycleManager(checkPeriod=0.5, cleanupPeriod=2) as manager:
            drop = FileDROP(
                "oid:A", "uid:A1", expectedSize=1, lifespan=1, precious=False
            )
            manager.addDrop(drop)
            self._writeAndClose(drop)

            # Wait 2 seconds, the DROP is still COMPLETED
            time.sleep(0.5)
            self.assertEqual(DROPStates.COMPLETED, drop.status)
            self.assertTrue(drop.exists())

            # Wait 5 more second, now it should be expired but still there
            time.sleep(1)
            self.assertEqual(DROPStates.EXPIRED, drop.status)
            self.assertTrue(drop.exists())

            # Wait 2 more seconds, now it should have been deleted
            time.sleep(1)
            self.assertEqual(DROPStates.DELETED, drop.status)
            self.assertFalse(drop.exists())

    def test_expireAfterUse(self):
        """
        Simple test for the expireAfterUse flag. Two DROPs are created with
        different values, and after they are used we check whether their data
        is still there or not
        """
        with dlm.DataLifecycleManager(checkPeriod=0.5, cleanupPeriod=2) as manager:
            a = DirectoryContainer(
                "a",
                "a",
                precious=False,
                expireAfterUse=True,
                dirname=tempfile.mkdtemp(),
            )
            b_dirname = tempfile.mkdtemp()
            b = DirectoryContainer(
                "b", "b", precious=False, expireAfterUse=False, dirname=b_dirname
            )
            c = BarrierAppDROP("c", "c")
            d = BarrierAppDROP("d", "d")
            a.addConsumer(c)
            a.addConsumer(d)
            b.addConsumer(c)
            b.addConsumer(d)

            manager.addDrop(a)
            manager.addDrop(b)
            manager.addDrop(b)
            manager.addDrop(c)

            # Make sure all consumers are done
            with DROPWaiterCtx(self, [c, d], 1):
                a.setCompleted()
                b.setCompleted()

            # Both directories should be there, but after cleanup A's shouldn't
            # be there anymore
            self.assertTrue(a.exists())
            self.assertTrue(b.exists())
            time.sleep(2.5)
            self.assertFalse(a.exists())
            self.assertTrue(b.exists())
            b.delete()


if __name__ == "__main__":
    unittest.main()
