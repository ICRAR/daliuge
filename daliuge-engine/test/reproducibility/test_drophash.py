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
Tests the low-level functionality for drops to hash runtime data.
"""

import unittest

from dlg.common.reproducibility.constants import ReproducibilityFlags
from dlg.common.reproducibility.reproducibility import common_hash
from dlg.ddap_protocol import DROPStates
from dlg.drop import AbstractDROP
from merklelib import MerkleTree


class AbstractDROPHashTests(unittest.TestCase):
    """
    Tests the reprodata generation methods of the AbstractDROP class
    """

    def test_null_merkleroot(self):
        """
        Sanity check that the default MerkleRoot of an abstract drop is Null
        Consider it a cardinal sin to change this.
        """
        drop_a = AbstractDROP("a", "a")
        self.assertIsNone(drop_a.merkleroot)

    def test_generate_rerun_data(self):
        """
        Tests that completed Rerun data contains the completed flag.
        """
        drop_a = AbstractDROP("a", "a")
        drop_a.reproducibility_level = ReproducibilityFlags.RERUN
        drop_a.setCompleted()
        self.assertEqual(drop_a.generate_rerun_data(), {"status": DROPStates.COMPLETED})

    def test_commit_on_complete(self):
        """
        Tests that merkle_data is generated upon set_complete status and is correct (NOTHING, RERUN)
        """

        drop_a = AbstractDROP("a", "a")
        drop_b = AbstractDROP("b", "b")
        drop_a.reproducibility_level = ReproducibilityFlags.RERUN
        drop_b.reproducibility_level = ReproducibilityFlags.NOTHING
        self.assertIsNone(drop_a.merkleroot)
        self.assertIsNone(drop_b.merkleroot)

        # Test RERUN
        drop_a.setCompleted()
        test = MerkleTree({"status": DROPStates.COMPLETED}.items(), common_hash)
        # 689fcf0d74c42200bef177db545adc43c135dfb0d7dc85b166db3af1dcded235
        self.assertTrue(test.merkle_root == drop_a.merkleroot)

        # Test NOTHING
        drop_b.setCompleted()
        # None
        self.assertIsNone(drop_b.merkleroot)
        self.assertTrue(drop_b._committed)

    def test_set_reproducibility_level(self):
        """
        Tests functionality for changing a DROP's reproducibility flag
        If already committed. The drop should reset and re-commit all reproducibility data
        If not committed, the change can proceed simply.
        """
        drop_a = AbstractDROP("a", "a")
        drop_b = AbstractDROP("b", "b")
        drop_a.reproducibility_level = ReproducibilityFlags.NOTHING
        drop_b.reproducibility_level = ReproducibilityFlags.NOTHING

        drop_a.setCompleted()
        self.assertIsNone(drop_a.merkleroot)
        drop_a.reproducibility_level = ReproducibilityFlags.RERUN
        drop_a.commit()
        self.assertIsNotNone(drop_a.merkleroot)

        self.assertIsNone(drop_b.merkleroot)
        drop_b.reproducibility_level = ReproducibilityFlags.RERUN
        drop_b.setCompleted()
        self.assertIsNotNone(drop_b.merkleroot)

        with self.assertRaises(TypeError):
            drop_a.reproducibility_level = "REPEAT"

    def test_set_all(self):
        """
        Tests setting the reproducibility setting to ALL
        """
        drop_a = AbstractDROP("a", "a")
        self.assertIsNone(drop_a.merkleroot)
        drop_a.reproducibility_level = ReproducibilityFlags.ALL
        self.assertTrue(isinstance(drop_a.merkleroot, dict))

    def test_set_all_set_rerun(self):
        """
        Tests taking a drop that was set to ALL to be set to RERUN and have data structures
        reset correctly.
        """
        drop_a = AbstractDROP("a", "a")
        self.assertIsNone(drop_a.merkleroot)
        drop_a.reproducibility_level = ReproducibilityFlags.ALL
        drop_a.commit()
        self.assertTrue(isinstance(drop_a.merkleroot, dict))
        drop_a.reproducibility_level = ReproducibilityFlags.RERUN
        drop_a.commit()
        self.assertTrue(isinstance(drop_a.merkleroot, str))
