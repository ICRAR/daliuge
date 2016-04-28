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
import httplib
import json
import threading
import time
import unittest

from dfms import utils
from dfms.manager import constants
from dfms.manager.client import MasterManagerClient
from dfms.manager.proc_daemon import DfmsDaemon


_TIMEOUT = 10

class TestDaemon(unittest.TestCase):

    def create_daemon(self, *args, **kwargs):
        self._daemon = DfmsDaemon(*args, **kwargs)

        if 'noNM' not in kwargs or not kwargs['noNM']:
            self.assertTrue(utils.portIsOpen('localhost', constants.NODE_DEFAULT_REST_PORT, _TIMEOUT), 'The NM did not start successfully')
        if 'master' in kwargs and kwargs['master']:
            self.assertTrue(utils.portIsOpen('localhost', constants.MASTER_DEFAULT_REST_PORT, _TIMEOUT), 'The MM did not start successfully')

        self._daemon_t = threading.Thread(target=lambda: self._daemon.start('localhost', 9000))
        self._daemon_t.start()

        # Wait until the daemon is fully available
        self.assertTrue(utils.portIsOpen('localhost', 9000, _TIMEOUT))

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        self._daemon.stop(_TIMEOUT)
        self._daemon_t.join(_TIMEOUT)
        self.assertFalse(self._daemon_t.is_alive(), "Daemon running thread should have finished by now")
        self.assertFalse(utils.portIsOpen('localhost', 9000, 0), 'DFMS Daemon REST interface should be off')

    def test_nm_starts(self):
        # Simplest case...
        self.create_daemon(master=False, noNM=False, disable_zeroconf=True)

    def test_mm_starts(self):
        # Start with the MM included
        self.create_daemon(master=True, noNM=False, disable_zeroconf=True)

    def test_nothing_starts(self):
        # Nothing should start now
        self.create_daemon(master=False, noNM=True, disable_zeroconf=True)
        self.assertFalse(utils.portIsOpen('localhost', constants.NODE_DEFAULT_REST_PORT, 0), 'NM started but it should not have')
        self.assertFalse(utils.portIsOpen('localhost', constants.MASTER_DEFAULT_REST_PORT, 0), 'NM started but it should not have')

    def test_start_master_via_rest(self):

        self.create_daemon(master=False, noNM=False, disable_zeroconf=True)

        # Check that the master starts
        self._start('master', httplib.OK)
        self.assertTrue(utils.portIsOpen('localhost', constants.MASTER_DEFAULT_REST_PORT, _TIMEOUT), 'The MM did not start successfully')

    def test_zeroconf_discovery(self):

        self.create_daemon(master=True, noNM=False, disable_zeroconf=False)

        # Both managers started fine. If they zeroconf themselves correctly then
        # if we query the MM it should know about its nodes, which should have
        # one element
        nodes = self._get_nodes_from_master(_TIMEOUT)
        self.assertIsNotNone(nodes)
        self.assertEquals(1, len(nodes), "MasterManager didn't find the NodeManager running on the same node")

    def test_start_dataisland_via_rest(self):

        self.create_daemon(master=True, noNM=False, disable_zeroconf=False)

        # Both managers started fine. If they zeroconf themselves correctly then
        # if we query the MM it should know about its nodes, which should have
        # one element
        nodes = self._get_nodes_from_master(_TIMEOUT)
        self.assertIsNotNone(nodes)
        self.assertEquals(1, len(nodes), "MasterManager didn't find the NodeManager running on the same node")

        # Check that the DataIsland starts with the given nodes
        self._start('dataisland', httplib.OK, {'nodes': nodes})
        self.assertTrue(utils.portIsOpen('localhost', constants.ISLAND_DEFAULT_REST_PORT, _TIMEOUT), 'The DIM did not start successfully')

    def _start(self, manager_name, expected_code, payload=None):
        conn = httplib.HTTPConnection('localhost', 9000)
        headers = {}
        if payload:
            payload = json.dumps(payload)
            headers['Content-Type'] = 'application/json'
        conn.request('POST', '/managers/%s' % (manager_name,), body=payload, headers=headers)
        response = conn.getresponse()
        self.assertEquals(expected_code, response.status, response.read())
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