#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2019
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
import tempfile
import unittest

from dlg.deploy import common
from dlg.manager import constants
from dlg.manager.session import SessionStates
from test.testutils import ManagerStarter


class TestDeployCommon(ManagerStarter, unittest.TestCase):

    port = constants.NODE_DEFAULT_REST_PORT

    def setUp(self):
        super(TestDeployCommon, self).setUp()
        self.nm_info = self.start_nm_in_thread()

    def tearDown(self):
        self.nm_info.stop()
        super(TestDeployCommon, self).tearDown()

    def _submit(self):
        pg = [{"oid":"A", "type":"plain", "storage": "memory"},
              {"oid":"B", "type":"app", "app": "dlg.apps.simple.SleepApp", "inputs": ["A"], "outputs":["C"]},
              {"oid":"C", "type":"plain", "storage": "memory"}]
        return common.submit(pg, '127.0.0.1', self.port)

    def test_submit(self):
        self._submit()

    def test_monitor(self):
        session_id = self._submit()
        status = common.monitor_sessions(session_id, port=self.port, poll_interval=0.1)
        self.assertEqual(SessionStates.FINISHED, status)

    def test_monitor_all(self):
        session_id1 = self._submit()
        session_id2 = self._submit()
        status = common.monitor_sessions(port=self.port, poll_interval=0.1)
        self.assertDictEqual({session_id1: SessionStates.FINISHED, session_id2: SessionStates.FINISHED}, status)

    def test_monitor_with_dumping(self):
        dump_path = tempfile.mktemp()
        session_id = self._submit()
        status = common.monitor_sessions(session_id=session_id, poll_interval=0.1,
                                        port=self.port, status_dump_path=dump_path)
        self.assertEqual(SessionStates.FINISHED, status)
        self.assertTrue(os.path.exists(dump_path))
        os.remove(dump_path)