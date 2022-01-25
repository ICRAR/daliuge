#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2014
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
Module tests shared memory manager.
"""

import sys
import unittest

if sys.version_info >= (3, 8):
    from dlg.manager.shared_memory_manager import DlgSharedMemoryManager


@unittest.skipIf(sys.version_info < (3, 8), "Shared memory does not work < python 3.8")
class TestSharedMemoryManager(unittest.TestCase):
    """
    Tests the DlgSharedMemory Manager in typical and atypical usecases
    """

    def test_register_session(self):
        """
        SMM should successfully register a session with no drops
        """
        manager = DlgSharedMemoryManager()
        manager.register_session('session1')
        self.assertTrue(len(manager.drop_names), 1)
        manager.shutdown_all()

    def test_register_drop(self):
        """
        SMM should register a drop with a pre-existing session
        """
        manager = DlgSharedMemoryManager()
        manager.register_session('session1')
        manager.register_drop('A', 'session1')
        self.assertTrue(len(manager.drop_names['session1']), 1)
        manager.shutdown_all()

    def test_register_drop_nonsession(self):
        """
        SMM should register the session for a drop if it did not already exist
        """
        manager = DlgSharedMemoryManager()
        manager.register_drop('A', 'session1')
        self.assertTrue((len(manager.drop_names['session1']), 1))
        manager.shutdown_all()

    def test_shutdown_session_existing(self):
        """
        SMM should successfully remove a session when requested
        """
        manager = DlgSharedMemoryManager()
        manager.register_session('session1')
        manager.register_session('session2')
        self.assertTrue(len(manager.drop_names.keys()), 2)
        manager.shutdown_session('session1')
        self.assertTrue(len(manager.drop_names.keys()), 1)
        manager.shutdown_all()

    def test_shutdown_session_nonexistent(self):
        """
        SMM should not error when attempting to shutdown a non-existent session
        """
        manager = DlgSharedMemoryManager()
        try:
            manager.shutdown_session('session1')
        except KeyError:
            self.fail('Manager errored when shutting down nonexistent session')

    def test_shutdown_all(self):
        """
        SMM should be able to remove all sessions and drop references when shutdown
        """
        manager = DlgSharedMemoryManager()
        manager.register_session('session1')
        manager.register_session('session2')
        self.assertEqual(len(manager.drop_names.keys()), 2)
        manager.shutdown_all()
        self.assertEqual(len(manager.drop_names.keys()), 0)

    def test_shutdown_all_nosessions(self):
        """
        SMM should successfully shutdown without error when no sessions or drops are present
        """
        manager = DlgSharedMemoryManager()
        try:
            manager.shutdown_all()
        except KeyError:
            self.fail('Manager errored when shutting down empty')
