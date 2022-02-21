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
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA
#
import tempfile
import threading
import unittest

from dlg import exceptions
from dlg.manager import constants
from dlg.manager.client import NodeManagerClient, DataIslandManagerClient
from dlg.manager.node_manager import NodeManager
from dlg.manager.rest import NMRestServer, CompositeManagerRestServer
from dlg.restutils import RestClient
from dlg.manager.composite_manager import DataIslandManager
from dlg.exceptions import InvalidGraphException
from dlg.common import Categories


hostname = "localhost"


class TestRest(unittest.TestCase):
    def setUp(self):
        unittest.TestCase.setUp(self)
        self.dm = NodeManager(False)
        self._dm_server = NMRestServer(self.dm)
        self._dm_t = threading.Thread(
            target=self._dm_server.start,
            args=(hostname, constants.NODE_DEFAULT_REST_PORT),
        )
        self._dm_t.start()

        self.dim = DataIslandManager(dmHosts=[hostname])
        self._dim_server = CompositeManagerRestServer(self.dim)
        self._dim_t = threading.Thread(
            target=self._dim_server.start,
            args=(hostname, constants.ISLAND_DEFAULT_REST_PORT),
        )
        self._dim_t.start()

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        self._dm_server.stop()
        self._dm_t.join()
        self.dm.shutdown()
        self.assertFalse(self._dm_t.is_alive())

        self._dim_server.stop()
        self._dim_t.join()
        self.dim.shutdown()
        self.assertFalse(self._dim_t.is_alive())

    def test_index(self):
        # Just check that the HTML pages load properly
        with RestClient(hostname, constants.NODE_DEFAULT_REST_PORT, timeout=10) as c:
            c._GET("/")
            c._GET("/session")

    def test_errtype(self):

        sid = "lala"
        c = NodeManagerClient(hostname)
        c.createSession(sid)

        # already exists
        self.assertRaises(
            exceptions.SessionAlreadyExistsException, c.createSession, sid
        )

        # different session
        self.assertRaises(
            exceptions.NoSessionException, c.addGraphSpec, sid + "x", [{}]
        )

        # invalid dropspec, it has no oid/type (is completely empty actually)
        self.assertRaises(exceptions.InvalidGraphException, c.addGraphSpec, sid, [{}])

        # invalid dropspec, app doesn't exist
        self.assertRaises(
            exceptions.InvalidGraphException,
            c.addGraphSpec,
            sid,
            [{"oid": "a", "type": "app", "app": "doesnt.exist"}],
        )

        # invalid state, the graph status is only queried when the session is running
        self.assertRaises(exceptions.InvalidSessionState, c.getGraphStatus, sid)

        # valid dropspec, but the socket listener app doesn't allow inputs
        c.addGraphSpec(
            sid,
            [
                {"type": "socket", "oid": "a", "inputs": ["b"]},
                {"oid": "b", "type": "plain", "storage": Categories.MEMORY},
            ],
        )
        self.assertRaises(exceptions.InvalidRelationshipException, c.deploySession, sid)

        # And here we point to an unexisting file, making an invalid drop
        c.destroySession(sid)
        c.createSession(sid)
        fname = tempfile.mktemp()
        c.addGraphSpec(
            sid,
            [
                {
                    "type": "plain",
                    "storage": Categories.FILE,
                    "oid": "a",
                    "filepath": fname,
                    "check_filepath_exists": True,
                }
            ],
        )
        self.assertRaises(exceptions.InvalidDropException, c.deploySession, sid)

    def test_recursive(self):

        sid = "lala"
        c = DataIslandManagerClient(hostname)
        c.createSession(sid)

        # invalid dropspec, app doesn't exist
        # This is not checked at the DIM level but only at the NM level
        # The exception should still pass through though
        with self.assertRaises(exceptions.SubManagerException) as cm:
            c.addGraphSpec(
                sid,
                [{"oid": "a", "type": "app", "app": "doesnt.exist", "node": hostname}],
            )
        ex = cm.exception
        self.assertTrue(hostname in ex.args[0])
        self.assertTrue(isinstance(ex.args[0][hostname], InvalidGraphException))
