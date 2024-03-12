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
import http.client  #
import json
import threading
import sys
import time
import unittest

from dlg import utils, restutils
from dlg.manager import constants
from dlg.manager.client import MasterManagerClient
from dlg.manager.proc_daemon import DlgDaemon

_TIMEOUT = 5
IDENTITY = lambda x: x


def wait_until(
    update_condition, test_condition=IDENTITY, timeout=_TIMEOUT, interval=0.1
):
    timeout_time = time.time() + timeout
    while time.time() < timeout_time:
        output = update_condition()
        if test_condition(output):
            return output
        time.sleep(interval)
    return None


def _get_dims_from_client(client, **kwargs):
    return wait_until(lambda: client.dims(), **kwargs)


def _get_nodes_from_client(client, **kwargs):
    return wait_until(lambda: client.nodes(), **kwargs)


class TestDaemon(unittest.TestCase):
    def create_daemon(self, *args, **kwargs):
        self._daemon_t = None
        self._daemon = DlgDaemon(*args, **kwargs)

        if "noNM" not in kwargs or not kwargs["noNM"]:
            self.assertTrue(
                utils.portIsOpen(
                    "localhost", constants.NODE_DEFAULT_REST_PORT, _TIMEOUT
                ),
                "The NM did not start successfully",
            )
        if "master" in kwargs and kwargs["master"]:
            self.assertTrue(
                utils.portIsOpen(
                    "localhost", constants.MASTER_DEFAULT_REST_PORT, _TIMEOUT
                ),
                "The MM did not start successfully",
            )

        self._daemon_t = threading.Thread(
            target=lambda: self._daemon.start("localhost", 9000)
        )
        self._daemon_t.start()

        # Wait until the daemon's server has started
        # We can't simply check if the port is opened, because the server binds
        # before it is returned to us. In some tests we don't interact with it,
        # and therefore the shutdown of the daemon can occur before the server
        # is even returned to us. This would happen because portIsOpen will
        # succeed with a bound server, even if we haven't serve_forever()'d it
        # yet. In these situations shutting down the daemon will not shut down
        # the http server, and therefore the test will fail when checking that
        # the self._daemon_t is not alive anymore
        #
        # To actually avoid this we need to do some actual HTTP talk, which will
        # ensure the server is actually serving requests, and therefore already
        # in the daemon's hand
        # self.assertTrue(utils.portIsOpen('localhost', 9000, _TIMEOUT))
        try:
            restutils.RestClient("localhost", 9000, timeout=_TIMEOUT)._GET("/anything")
        except restutils.RestClientException:
            # We don't care about the result
            pass

    def tearDown(self):
        if self._daemon_t is not None:
            self._daemon.stop(_TIMEOUT)
            self._daemon_t.join(_TIMEOUT)
            self.assertFalse(
                self._daemon_t.is_alive(),
                "Daemon running thread should have finished by now",
            )
            self.assertTrue(
                utils.portIsClosed("localhost", 9000, _TIMEOUT),
                "DALiuGE Daemon REST interface should be off",
            )
        unittest.TestCase.tearDown(self)

    def test_nm_starts(self):
        # Simplest case...
        print(f"Python interpreter: {sys.executable}")
        self.create_daemon(master=False, noNM=False, disable_zeroconf=True)

    def test_mm_starts(self):
        # Start with the MM included
        self.create_daemon(master=True, noNM=False, disable_zeroconf=True)

    def test_nothing_starts(self):
        # Nothing should start now
        self.create_daemon(master=False, noNM=True, disable_zeroconf=True)
        self.assertTrue(
            utils.portIsClosed("localhost", constants.NODE_DEFAULT_REST_PORT, 0),
            "NM started but it should not have",
        )
        self.assertTrue(
            utils.portIsClosed("localhost", constants.MASTER_DEFAULT_REST_PORT, 0),
            "NM started but it should not have",
        )

    def test_zeroconf_discovery(self):

        self.create_daemon(master=True, noNM=False, disable_zeroconf=False)

        # Both managers started fine. If they zeroconf themselves correctly then
        # if we query the MM it should know about its nodes, which should have
        # one element
        mc = MasterManagerClient()
        nodes = _get_nodes_from_client(mc)
        self.assertIsNotNone(nodes)
        self.assertEqual(
            1,
            len(nodes),
            "MasterManager didn't find the NodeManager running on the same node",
        )

    def _test_zeroconf_dim_mm(self, disable_zeroconf=False):
        # Start an empty daemon, then a DIM and a Master on their own
        self.create_daemon(master=False, noNM=True, disable_zeroconf=disable_zeroconf)
        self._start("island", http.HTTPStatus.OK, {"nodes": []})
        self._start("master", http.HTTPStatus.OK)

        # Check that dim registers to MM when using zeroconf
        mc = MasterManagerClient()
        if not disable_zeroconf:

            def _test_dims(dims):
                return dims and dims["islands"]

            dims = _get_dims_from_client(mc, test_condition=_test_dims)
            self.assertIsNotNone(dims)
            return dims
        return mc.dims()

    def test_zeroconf_dim_mm(self):
        dims = self._test_zeroconf_dim_mm(disable_zeroconf=False)
        self.assertEqual(
            1,
            len(dims["islands"]),
            "MasterManager didn't find the DataIslandManager with zeroconf",
        )

    def test_without_zeroconf_dim_mm(self):
        dims = self._test_zeroconf_dim_mm(disable_zeroconf=True)
        self.assertEqual(
            0,
            len(dims["islands"]),
            "MasterManager found the DataIslandManager without zeroconf!?",
        )

    def _add_zeroconf_nm(self):
        self._start("node", http.HTTPStatus.OK)
        mc = MasterManagerClient()
        return _get_nodes_from_client(mc)

    def test_zeroconf_dim_nm_setup(self):
        """
        Sets up a mm with a node manager
        Sets up a DIM with zeroconf discovery
        Asserts that the mm attaches the nm to the discovered dim
        """
        self._test_zeroconf_dim_mm(disable_zeroconf=False)
        nodes = self._add_zeroconf_nm()
        self.assertIsNotNone(nodes)

    def test_without_zeroconf_dim_nm_setup(self):
        self._test_zeroconf_dim_mm(disable_zeroconf=True)
        nodes = self._add_zeroconf_nm()["nodes"]
        self.assertEqual(0, len(nodes))

    def test_zeroconf_nm_down(self):
        self._test_zeroconf_dim_mm(disable_zeroconf=False)
        nodes = self._add_zeroconf_nm()
        self.assertIsNotNone(nodes)
        self._stop("node", http.HTTPStatus.OK)
        mc = MasterManagerClient()

        def _test_nodes(nodes):
            return not nodes["nodes"]

        new_nodes = _get_nodes_from_client(mc, test_condition=_test_nodes)["nodes"]
        self.assertEqual(0, len(new_nodes))

    def test_start_dataisland_via_rest(self):

        self.create_daemon(master=True, noNM=False, disable_zeroconf=False)

        # Both managers started fine. If they zeroconf themselves correctly then
        # if we query the MM it should know about its nodes, which should have
        # one element
        mc = MasterManagerClient()
        nodes = _get_nodes_from_client(mc)
        self.assertIsNotNone(nodes)
        self.assertEqual(
            1,
            len(nodes),
            "MasterManager didn't find the NodeManager running on the same node",
        )

        # Check that the DataIsland starts with the given nodes
        self._start("island", http.HTTPStatus.OK, {"nodes": nodes})
        self.assertTrue(
            utils.portIsOpen("localhost", constants.ISLAND_DEFAULT_REST_PORT, _TIMEOUT),
            "The DIM did not start successfully",
        )

    def test_stop_dataisland_via_rest(self):

        # start master and island manager
        self.create_daemon(master=True, noNM=False, disable_zeroconf=False)
        mc = MasterManagerClient()
        nodes = _get_nodes_from_client(mc)
        self._start("island", http.HTTPStatus.OK, {"nodes": nodes})

        # Both managers started fine. If they zeroconf themselves correctly then
        # if we query the MM it should know about its nodes, which should have
        # one element
        mc = MasterManagerClient()
        nodes = _get_nodes_from_client(mc)
        self.assertIsNotNone(nodes)
        self.assertEqual(
            1,
            len(nodes),
            "MasterManager didn't find the NodeManager running on the same node",
        )

        # Check that the DataIsland stopped
        self._stop("island", http.HTTPStatus.OK, "")
        self.assertTrue(
            utils.portIsClosed(
                "localhost", constants.ISLAND_DEFAULT_REST_PORT, _TIMEOUT
            ),
            "The DIM did not stop successfully",
        )

    def test_stop_start_node_via_rest(self):

        # test both stop and start of NM via REST
        self.create_daemon(master=True, noNM=False, disable_zeroconf=False)

        # Both managers started fine. If they zeroconf themselves correctly then
        # if we query the MM it should know about its nodes, which should have
        # one element
        mc = MasterManagerClient()
        nodes = _get_nodes_from_client(mc)
        self.assertIsNotNone(nodes)
        self.assertEqual(
            1,
            len(nodes),
            "MasterManager didn't find the NodeManager running on the same node",
        )

        # Check that the NM stops
        self._stop("node", http.HTTPStatus.OK, "")
        self.assertTrue(
            utils.portIsClosed("localhost", constants.NODE_DEFAULT_REST_PORT, _TIMEOUT),
            "The node did not stop successfully",
        )

        # Check that the NM starts
        self._start("node", http.HTTPStatus.OK, {"pid": nodes})
        self.assertTrue(
            utils.portIsOpen("localhost", constants.NODE_DEFAULT_REST_PORT, _TIMEOUT),
            "The node did not start successfully",
        )

    def test_start_stop_master_via_rest(self):
        # test both stop and start of MASTER via REST
        self.create_daemon(master=False, noNM=False, disable_zeroconf=True)

        # Check that the MM starts
        self._start("master", http.HTTPStatus.OK)
        self.assertTrue(
            utils.portIsOpen("localhost", constants.MASTER_DEFAULT_REST_PORT, _TIMEOUT),
            "The MM did not start successfully",
        )

        # Check that the MM stops
        self._stop("master", http.HTTPStatus.OK, "")
        self.assertTrue(
            utils.portIsClosed(
                "localhost", constants.MASTER_DEFAULT_REST_PORT, _TIMEOUT
            ),
            "The MM did not stop successfully",
        )

    def test_get_dims(self):
        self.create_daemon(master=True, noNM=True, disable_zeroconf=False)
        # Check that the DataIsland starts with the given nodes
        mc = MasterManagerClient()
        dims = _get_dims_from_client(mc)
        self.assertIsNotNone(dims)
        self.assertEqual(
            0,
            len(dims["islands"]),
            "MasterManager didn't find the DataIslandManager running on the same node",
        )

    def _start(self, manager_name, expected_code, payload=None):
        conn = http.client.HTTPConnection("localhost", 9000)
        headers = {}
        if payload:
            payload = json.dumps(payload)
            headers["Content-Type"] = "application/json"
        conn.request(
            "POST",
            f"/managers/{manager_name}/start",
            body=payload,
            headers=headers,
        )
        response = conn.getresponse()
        self.assertEqual(expected_code, response.status, response.read())
        response.close()
        conn.close()

    def _stop(self, manager_name, expected_code, payload=None):
        conn = http.client.HTTPConnection("localhost", 9000)
        headers = {}
        if payload:
            payload = json.dumps(payload)
            headers["Content-Type"] = "application/json"
        conn.request(
            "POST",
            f"/managers/{manager_name}/stop",
            body=payload,
            headers=headers,
        )
        response = conn.getresponse()
        self.assertEqual(expected_code, response.status, response.read())
        response.close()
        conn.close()
