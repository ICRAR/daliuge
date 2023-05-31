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
from dlg.common.deployment_methods import DeploymentMethods
from dlg.exceptions import InvalidGraphException

from dlg.manager import constants
from dlg.manager.client import NodeManagerClient, DataIslandManagerClient
from dlg.manager.composite_manager import DataIslandManager
from dlg.manager.node_manager import NodeManager
from dlg.manager.rest import NMRestServer, CompositeManagerRestServer
from dlg.restutils import RestClient

default_repro = {
    "rmode": "1",
    "RERUN": {
        "lg_blockhash": "x",
        "pgt_blockhash": "y",
        "pg_blockhash": "z",
    },
}
default_graph_repro = {
    "rmode": "1",
    "meta_data": {"repro_protocol": 0.1, "hashing_alg": "_sha3.sha3_256"},
    "merkleroot": "a",
    "RERUN": {
        "signature": "b",
    },
}


def add_test_reprodata(graph: list):
    for drop in graph:
        drop["reprodata"] = default_repro.copy()
    graph.append(default_graph_repro.copy())
    return graph


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
        with RestClient(
            hostname, constants.NODE_DEFAULT_REST_PORT, timeout=10
        ) as c:
            c._GET("/")
            c._GET("/session")

    def test_errtype(self):
        sid = "lala"
        c = NodeManagerClient(hostname)
        c.createSession(sid)
        gempty = [{}]
        add_test_reprodata(gempty)
        # already exists
        self.assertRaises(
            exceptions.SessionAlreadyExistsException, c.createSession, sid
        )

        # different session
        self.assertRaises(
            exceptions.NoSessionException, c.addGraphSpec, sid + "x", [{}]
        )

        # invalid dropspec, it has no oid/type (is completely empty actually)
        self.assertRaises(
            exceptions.InvalidGraphException, c.addGraphSpec, sid, gempty
        )

        # invalid dropspec, app doesn't exist
        self.assertRaises(
            exceptions.InvalidGraphException,
            c.addGraphSpec,
            sid,
            [
                {
                    "oid": "a",
                    "categoryType": "Application",
                    "dropclass": "doesnt.exist",
                    "reprodata": default_repro.copy(),
                },
                default_graph_repro.copy(),
            ],
        )

        # invalid state, the graph status is only queried when the session is running
        self.assertRaises(
            exceptions.InvalidSessionState, c.getGraphStatus, sid
        )

        # valid dropspec, but the socket listener app doesn't allow inputs
        c.addGraphSpec(
            sid,
            [
                {
                    "categoryType": "socket",
                    "oid": "a",
                    "inputs": ["b"],
                    "reprodata": default_repro.copy(),
                },
                {
                    "oid": "b",
                    "categoryType": "Data",
                    "dropclass": "dlg.data.drops.memory.InMemoryDROP",
                    "reprodata": default_repro.copy(),
                },
                default_graph_repro.copy(),
            ],
        )
        self.assertRaises(
            exceptions.InvalidRelationshipException, c.deploySession, sid
        )

        # And here we point to an unexisting file, making an invalid drop
        c.destroySession(sid)
        c.createSession(sid)
        fname = tempfile.mktemp()
        c.addGraphSpec(
            sid,
            [
                {
                    "categoryType": "Data",
                    "dropclass": "dlg.data.drops.file.FileDROP",
                    "oid": "a",
                    "filepath": fname,
                    "check_filepath_exists": True,
                    "reprodata": default_repro.copy(),
                },
                default_graph_repro.copy(),
            ],
        )
        self.assertRaises(
            exceptions.InvalidDropException, c.deploySession, sid
        )

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
                [
                    {
                        "oid": "a",
                        "categoryType": "Application",
                        "dropclass": "doesnt.exist",
                        "node": hostname,
                        "reprodata": default_repro.copy(),
                    },
                    default_graph_repro.copy(),
                ],
            )
        ex = cm.exception
        self.assertTrue(hostname in ex.args[0])
        self.assertTrue(
            isinstance(ex.args[0][hostname], InvalidGraphException)
        )

    def test_reprodata_get(self):
        """
        Tests deploying an incredibly basic graph with and without reprodata
        Then querying the manager for that reprodata.
        """
        sid = "1234"
        c = NodeManagerClient(hostname)
        graph_spec = [
            {
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
                "oid": "a",
                "reprodata": default_repro.copy(),
            },
            default_graph_repro.copy(),
        ]
        # Test with reprodata
        c.createSession(sid)
        c.addGraphSpec(sid, graph_spec)
        c.deploySession(sid, completed_uids=["a"])
        response = c.session_repro_data(sid)
        self.assertIsNotNone(
            response["graph"]["a"]["reprodata"]["RERUN"]["rg_blockhash"]
        )
        self.assertIsNotNone(response["reprodata"])
        c.destroySession(sid)
        # Test without reprodata
        graph_spec = graph_spec[0:1]
        graph_spec[0].pop("reprodata")
        c.createSession(sid)
        c.addGraphSpec(sid, graph_spec)
        c.deploySession(sid, completed_uids=["a"])
        response = c.session_repro_data(sid)
        self.assertEqual(
            {
                "a": {
                    "oid": "a",
                    "dropclass": "dlg.data.drops.memory.InMemoryDROP",
                    "categoryType": "Data",
                }
            },
            response["graph"],
        )
        self.assertEqual({}, response["reprodata"])

    def test_reprostatus_get(self):
        # Test with reprodata
        sid = "1234"
        c = NodeManagerClient(hostname)
        graph_spec = [
            {
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
                "oid": "a",
                "reprodata": default_repro.copy(),
            },
            default_graph_repro.copy(),
        ]
        c.createSession(sid)
        c.addGraphSpec(sid, graph_spec)
        c.deploySession(sid, completed_uids=["a"])
        response = c.session_repro_status(sid)
        self.assertTrue(response)
        c.destroySession(sid)
        # Test without reprodata
        graph_spec = graph_spec[0:1]
        graph_spec[0].pop("reprodata")
        c.createSession(sid)
        c.addGraphSpec(sid, graph_spec)
        c.deploySession(sid, completed_uids=["a"])
        response = c.session_repro_status(sid)
        self.assertTrue(response)
        c.destroySession(sid)

    def test_submit_method(self):
        c = NodeManagerClient(hostname)
        response = c.get_submission_method()
        self.assertEqual({"methods": [DeploymentMethods.BROWSER, DeploymentMethods.SERVER]}, response)
