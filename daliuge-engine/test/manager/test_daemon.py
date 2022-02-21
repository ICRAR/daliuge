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
import time
import unittest

from dlg import utils, restutils
from dlg.manager import constants
from dlg.manager.client import MasterManagerClient
from dlg.manager.proc_daemon import DlgDaemon


_TIMEOUT = 10


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
            restutils.RestClient("localhost", 9000, timeout=10)._GET("/anything")
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
        self.create_daemon(master=False, noNM=False, disable_zeroconf=True)

    def test_mm_starts(self):
        # Start with the MM included
        self.create_daemon(master=True, noNM=False, disable_zeroconf=True)

    def test_nothing_starts(self):
        # Nothing should start now
        self.create_daemon(master=False, noNM=True, disable_zeroconf=True)
        self.assertFalse(
            utils.portIsOpen("localhost", constants.NODE_DEFAULT_REST_PORT, 0),
            "NM started but it should not have",
        )
        self.assertFalse(
            utils.portIsOpen("localhost", constants.MASTER_DEFAULT_REST_PORT, 0),
            "NM started but it should not have",
        )

    def test_zeroconf_discovery(self):

        self.create_daemon(master=True, noNM=False, disable_zeroconf=False)

        # Both managers started fine. If they zeroconf themselves correctly then
        # if we query the MM it should know about its nodes, which should have
        # one element
        nodes = self._get_nodes_from_master(_TIMEOUT)
        self.assertIsNotNone(nodes)
        self.assertEqual(
            1,
            len(nodes),
            "MasterManager didn't find the NodeManager running on the same node",
        )

    def test_start_dataisland_via_rest(self):

        self.create_daemon(master=True, noNM=False, disable_zeroconf=False)

        # Both managers started fine. If they zeroconf themselves correctly then
        # if we query the MM it should know about its nodes, which should have
        # one element
        nodes = self._get_nodes_from_master(_TIMEOUT)
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
        nodes = self._get_nodes_from_master(_TIMEOUT)
        self._start("island", http.HTTPStatus.OK, {"nodes": nodes})

        # Both managers started fine. If they zeroconf themselves correctly then
        # if we query the MM it should know about its nodes, which should have
        # one element
        nodes = self._get_nodes_from_master(_TIMEOUT)
        self.assertIsNotNone(nodes)
        self.assertEqual(
            1,
            len(nodes),
            "MasterManager didn't find the NodeManager running on the same node",
        )

        # Check that the DataIsland stopped
        self._stop("island", http.HTTPStatus.OK, "")
        self.assertFalse(
            utils.portIsOpen("localhost", constants.ISLAND_DEFAULT_REST_PORT, _TIMEOUT),
            "The DIM did not stop successfully",
        )

    def test_stop_start_node_via_rest(self):

        # test both stop and start of NM via REST
        self.create_daemon(master=True, noNM=False, disable_zeroconf=False)

        # Both managers started fine. If they zeroconf themselves correctly then
        # if we query the MM it should know about its nodes, which should have
        # one element
        nodes = self._get_nodes_from_master(_TIMEOUT)
        self.assertIsNotNone(nodes)
        self.assertEqual(
            1,
            len(nodes),
            "MasterManager didn't find the NodeManager running on the same node",
        )

        # Check that the NM stops
        self._stop("node", http.HTTPStatus.OK, "")
        self.assertFalse(
            utils.portIsOpen("localhost", constants.NODE_DEFAULT_REST_PORT, _TIMEOUT),
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
        self.assertFalse(
            utils.portIsOpen("localhost", constants.MASTER_DEFAULT_REST_PORT, _TIMEOUT),
            "The MM did not stop successfully",
        )

    def _start(self, manager_name, expected_code, payload=None):
        conn = http.client.HTTPConnection("localhost", 9000)
        headers = {}
        if payload:
            payload = json.dumps(payload)
            headers["Content-Type"] = "application/json"
        conn.request(
            "POST",
            "/managers/%s/start" % (manager_name,),
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
            "POST", "/managers/%s/stop" % (manager_name,), body=payload, headers=headers
        )
        response = conn.getresponse()
        self.assertEqual(expected_code, response.status, response.read())
        response.close()
        conn.close()

    def _get_nodes_from_master(self, timeout):
        mc = MasterManagerClient()
        timeout_time = time.time() + timeout
        while time.time() < timeout_time:
            nodes = mc.nodes()
            if nodes:
                return nodes
            time.sleep(0.1)
