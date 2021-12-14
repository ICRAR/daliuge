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


from dlg.testutils import ManagerStarter
from dlg.common import Categories


class CommonTestsBase(ManagerStarter):
    def _submit(self):
        pg = [
            {"oid": "A", "type": "plain", "storage": Categories.MEMORY},
            {
                "oid": "B",
                "type": "app",
                "app": "dlg.apps.simple.SleepApp",
                "inputs": ["A"],
                "outputs": ["C"],
            },
            {"oid": "C", "type": "plain", "storage": Categories.MEMORY},
        ]
        for drop in pg:
            drop["node"] = "127.0.0.1"
            drop["island"] = "127.0.0.1"
        return common.submit(pg, "127.0.0.1", self.port)

    def assert_sessions_finished(self, status, *session_ids):
        for session_id in session_ids:
            self.assert_session_finished(status[session_id])

    def test_submit(self):
        self._submit()

    def test_monitor(self):
        session_id = self._submit()
        status = common.monitor_sessions(session_id, port=self.port, poll_interval=0.1)
        self.assert_session_finished(status)

    def test_monitor_all(self):
        session_id1 = self._submit()
        session_id2 = self._submit()
        status = common.monitor_sessions(port=self.port, poll_interval=0.1)
        self.assert_sessions_finished(status, session_id1, session_id2)

    def test_monitor_with_dumping(self):
        dump_path = tempfile.mktemp()
        session_id = self._submit()
        status = common.monitor_sessions(
            session_id=session_id,
            poll_interval=0.1,
            port=self.port,
            status_dump_path=dump_path,
        )
        self.assert_session_finished(status)
        self.assertTrue(os.path.exists(dump_path))
        os.remove(dump_path)


class TestDeployCommonNM(CommonTestsBase, unittest.TestCase):

    port = constants.NODE_DEFAULT_REST_PORT

    def assert_session_finished(self, status):
        self.assertEqual(SessionStates.FINISHED, status)

    def setUp(self):
        super(TestDeployCommonNM, self).setUp()
        self.nm_info = self.start_nm_in_thread()

    def tearDown(self):
        self.nm_info.stop()
        super(TestDeployCommonNM, self).tearDown()


class TestDeployCommonDIM(TestDeployCommonNM, unittest.TestCase):

    port = constants.ISLAND_DEFAULT_REST_PORT

    def assert_session_finished(self, status):
        self.assertTrue(all([s == SessionStates.FINISHED for s in status]))

    def setUp(self):
        super(TestDeployCommonDIM, self).setUp()
        self.dim_info = self.start_dim_in_thread()

    def tearDown(self):
        self.dim_info.stop()
        super(TestDeployCommonDIM, self).tearDown()


class TestDeployCommonMM(TestDeployCommonDIM, unittest.TestCase):

    port = constants.MASTER_DEFAULT_REST_PORT

    def setUp(self):
        super(TestDeployCommonMM, self).setUp()
        self.mm_info = self.start_mm_in_thread()

    def tearDown(self):
        self.mm_info.stop()
        super(TestDeployCommonMM, self).tearDown()
