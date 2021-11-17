#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2015
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
import unittest

from dlg import utils
from dlg import droputils
from dlg.apps.socket_listener import SocketListenerApp
from dlg.drop import InMemoryDROP
from dlg.ddap_protocol import DROPStates
from dlg.droputils import DROPWaiterCtx
from test.test_drop import SumupContainerChecksum
import os


try:
    from crc32c import crc32c  # @UnusedImport
except:
    from binascii import crc32  # @Reimport


class TestSocketListener(unittest.TestCase):
    def _test_socket_listener(self, **kwargs):
        """
        A simple test to check that SocketListenerApps are indeed working as
        expected; that is, they write the data they receive into their output,
        and finish when the connection is closed from the client side

        The data flow diagram looks like this:

        A --> B --> C --> D
        """

        host = "localhost"
        port = 9933
        data = os.urandom(1025)

        a = SocketListenerApp("oid:A", "uid:A", host=host, port=port, **kwargs)
        b = InMemoryDROP("oid:B", "uid:B")
        c = SumupContainerChecksum("oid:C", "uid:C")
        d = InMemoryDROP("oid:D", "uid:D")
        a.addOutput(b)
        b.addConsumer(c)
        c.addOutput(d)

        # Create the socket, write, and close the connection, allowing
        # A to move to COMPLETED
        with DROPWaiterCtx(self, d, 3):  # That's plenty of time
            a.async_execute()
            utils.write_to(host, port, data, 1)

        for drop in [a, b, c, d]:
            self.assertEqual(DROPStates.COMPLETED, drop.status)

        # Our expectations are fulfilled!
        bContents = droputils.allDropContents(b)
        dContents = int(droputils.allDropContents(d))
        self.assertEqual(data, bContents)
        self.assertEqual(crc32c(data, 0), dContents)

    def test_socket_listener(self):
        self._test_socket_listener()

    def test_socket_listener_integer_with_bufsize(self):
        for bufsize in (4096, "4096"):
            self._test_socket_listener(bufsize=bufsize)

    def test_invalid(self):

        # Shouldn't allow inputs
        a = SocketListenerApp("a", "a", port=1)
        a.addOutput(InMemoryDROP("c", "c"))
        self.assertRaises(Exception, a.addInput, InMemoryDROP("b", "b"))
        self.assertRaises(Exception, a.addStreamingInput, InMemoryDROP("b", "b"))

        # Shouldn't be able to open ports <= 1024
        a.execute()
        self.assertEqual(a.status, DROPStates.ERROR)
