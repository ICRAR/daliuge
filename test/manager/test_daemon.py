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
import threading
import time
import unittest

from dfms import utils
from dfms.manager import constants
from dfms.manager.daemon import DfmsDaemon


class TestDaemon(unittest.TestCase):

    def test_nm_starts(self):

        # At instantiation time the NM should already be running
        daemon = DfmsDaemon(master=False, noNM=False, disable_zeroconf=True)
        self.assertTrue(utils.portIsOpen('localhost', constants.NODE_DEFAULT_REST_PORT, 10), 'The NM did not start successfully')
        daemon.stop()

    def test_mm_starts(self):

        # At instantiation time the NM and MM should already be running
        daemon = DfmsDaemon(master=True, noNM=False, disable_zeroconf=True)
        self.assertTrue(utils.portIsOpen('localhost', constants.NODE_DEFAULT_REST_PORT, 10), 'The NM did not start successfully')
        self.assertTrue(utils.portIsOpen('localhost', constants.MASTER_DEFAULT_REST_PORT, 10), 'The MM did not start successfully')
        daemon.stop()

    def test_nothing_starts(self):
        daemon = DfmsDaemon(master=False, noNM=True, disable_zeroconf=True)
        self.assertFalse(utils.portIsOpen('localhost', constants.NODE_DEFAULT_REST_PORT, 1), 'NM started but it should not have')
        self.assertFalse(utils.portIsOpen('localhost', constants.MASTER_DEFAULT_REST_PORT, 1), 'NM started but it should not have')
        daemon.stop()

    def test_start_master_via_rest(self):

        daemon = DfmsDaemon(master=False, noNM=False, disable_zeroconf=True)
        self.assertTrue(utils.portIsOpen('localhost', constants.NODE_DEFAULT_REST_PORT, 10), 'The NM did not start successfully')
        t = threading.Thread(target=lambda: daemon.run('localhost', 9000))
        t.start()

        # Let the server actually start
        time.sleep(0.5)

        # Check that the master starts
        self._start('master', httplib.OK)
        self.assertTrue(utils.portIsOpen('localhost', constants.MASTER_DEFAULT_REST_PORT, 10), 'The MM did not start successfully')

        daemon.stop()
        t.join()

    def _start(self, manager_name, expected_code):
        conn = httplib.HTTPConnection('localhost', 9000)
        conn.request('POST', '/managers/%s' % (manager_name,))
        response = conn.getresponse()
        self.assertEquals(expected_code, response.status)
        response.close()
        conn.close()