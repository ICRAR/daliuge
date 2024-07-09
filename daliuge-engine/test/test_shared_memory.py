#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2016
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
#    You should have received block_a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA
#
# pylint: disable=possibly-used-before-assignment
"""
Module tests the shared memory primitive.
"""
import pickle
import sys
import unittest

if sys.version_info >= (3, 8):
    from dlg.shared_memory import DlgSharedMemory, _MAXNAMELENGTH


@unittest.skipIf(sys.version_info < (3, 8), "Shared memory does not work < python 3.8")
class TestSharedMemory(unittest.TestCase):
    """
    Tests the shared memory primitive for atypical and typical uses.
    """

    def test_resize_grow(self):
        """
        The size should grow when requested
        """
        block_a = DlgSharedMemory("A")
        old_size = block_a.size
        block_a.resize(block_a.size * 2)
        self.assertEqual(block_a.size, old_size * 2)
        block_a.close()
        block_a.unlink()

    def test_resize_shrink(self):
        """
        Size should shrink when requested
        """
        block_a = DlgSharedMemory("A")
        old_size = block_a.size
        block_a.resize(old_size // 2)
        self.assertEqual(block_a.size, old_size // 2)
        block_a.close()
        block_a.unlink()

    def test_open_new(self):
        """
        When opening a new block by name, that name should be respected
        """
        block_a = DlgSharedMemory("A")
        self.assertEqual(block_a.name, "/A")
        block_a.close()
        block_a.unlink()

    def test_open_existing(self):
        """
        When opening an existing but still linked block, data should persist
        """
        block_a = DlgSharedMemory("A")
        data = pickle.dumps(3)
        block_a.buf[0: len(data)] = data
        block_a.resize(len(data))
        old_size = block_a.size
        block_a.close()
        block_b = DlgSharedMemory("A")
        self.assertEqual(block_b.size, old_size)
        self.assertEqual(block_b.name, "/A")
        self.assertEqual(block_b.buf[:], data)
        block_b.close()
        block_b.unlink()

    def test_open_random(self):
        """
        It should be possible to open a randomly named block (although this must be deliberate)
        """
        block_a = DlgSharedMemory(None)
        self.assertIsNotNone(block_a.name)
        block_a.close()
        block_a.unlink()

    def test_open_twice(self):
        """
        It should be possible to open a shared memory block simultaneously and therefore offer a
        view into the same memory buffer.
        This does bring to light that this primitive is not thread-safe by default.
        """
        block_a = DlgSharedMemory("A")
        block_b = DlgSharedMemory("A")
        self.assertEqual(block_a.buf, block_b.buf)
        block_a.close()
        block_b.close()
        block_a.unlink()

    def test_unlink_twice(self):
        """
        It should not be possible to unlink a block twice.
        """
        block_a = DlgSharedMemory("A")
        block_b = DlgSharedMemory("A")
        block_a.close()
        block_b.close()
        block_a.unlink()
        with self.assertWarns(RuntimeWarning):
            block_b.unlink()

    def test_long_name(self):
        """
        There is a maximum name size which should be respected
        """
        filename = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        block_a = DlgSharedMemory(filename)
        self.assertEqual(block_a.name, "/" + filename[: _MAXNAMELENGTH - 1])
        block_a.close()
        block_a.unlink()

    def test_start_slash(self):
        """
        If a user attempts to make a block with a name beginning with /, it should be ignored
        (rather than having // as a name)
        """
        filename = "/memory"
        block_a = DlgSharedMemory(filename)
        self.assertEqual(block_a.name, filename)
        block_a.close()
        block_a.unlink()
