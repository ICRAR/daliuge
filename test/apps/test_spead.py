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
import os
import threading
import time
import unittest

import spead2.send

from dfms import droputils
from dfms.apps.spead_receiver import SpeadReceiverApp
from dfms.drop import InMemoryDROP
from dfms.ddap_protocol import DROPStates
from dfms.droputils import DOWaiterCtx


class TestSpeadReceiverApp(unittest.TestCase):

    def test_speadApp(self):

        port = 1111
        itemId = 0x2000

        thread_pool = spead2.ThreadPool()
        self._stream = spead2.send.UdpStream(thread_pool, "localhost", port, spead2.send.StreamConfig(rate=1e7))

        a = SpeadReceiverApp('a','a',port=port, itemId=itemId)
        b = InMemoryDROP('b','b')
        a.addOutput(b)

        size = 1024
        threading.Thread(target=lambda: a.execute()).start()
        time.sleep(1)
        msg = os.urandom(size)
        with DOWaiterCtx(self, b, timeout=1):
            ig = spead2.send.ItemGroup(flavour=spead2.Flavour(4, 64, 48))
            item = ig.add_item(itemId, 'main_data', 'a char array', shape=(size,), format=[('c',8)])
            item.value = msg
            self._stream.send_heap(ig.get_heap())
            self._stream.send_heap(ig.get_end())

        for do in a,b:
            self.assertEquals(DROPStates.COMPLETED, do.status)

        self.assertEquals(size, b.size)
        self.assertEquals(msg, droputils.allDataObjectContents(b))