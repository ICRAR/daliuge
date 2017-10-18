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
"""
Module containing an (python) application that receives spead2 data
"""

import logging

try:
    import spead2.recv
except:
    pass

from ..drop import BarrierAppDROP


logger = logging.getLogger(__name__)

class SpeadReceiverApp(BarrierAppDROP):
    """
    A BarrierAppDROP that listens for data using the SPEAD protocol.

    This application opens a stream and adds a UDP reader on a specific host and
    port to listen for the data of item `itemId`. The stream is listened until
    it is closed. Each heap sent through the stream is checked for the item, and
    once found its data is written into each output of this application.

    Just like the SocketListenerApp, this application expects no input
    DROPs, and therefore raises an exception whenever one is added. On the
    output side, one or more outputs can be specified with the restriction that
    they are not ContainerDROPs so data can be written into them through
    the framework.
    """

    def initialize(self, **kwargs):
        super(SpeadReceiverApp, self).initialize(**kwargs)

        # Basic connectivity parameters
        self._host             = self._getArg(kwargs, 'host', 'localhost')
        self._port             = self._getArg(kwargs, 'port', 1111)
        self._itemId           = self._getArg(kwargs, 'itemId', 0x1000)

        # Performance tuning
        self._maxPacketSize    = self._getArg(kwargs, 'maxPacketSize', 9200)
        self._socketBufferSize = self._getArg(kwargs, 'socketBufferSize', 8388608)
        self._maxHeaps         = self._getArg(kwargs, 'maxHeaps', 4)

        # Memory and thread pool tuning
        self._mpLower          = self._getArg(kwargs, 'mpLower', 4096)
        self._mpUpper          = self._getArg(kwargs, 'mpUpper', 4096*8)
        self._mpMaxFree        = self._getArg(kwargs, 'mpMaxFree', 10)
        self._mpInitial        = self._getArg(kwargs, 'mpInitial', 1)
        self._tpThreads        = self._getArg(kwargs, 'tpThreads', 1)

    def run(self):

        # Create the stream with the given thread pool
        threadPool = spead2.ThreadPool(threads=self._tpThreads)
        stream = spead2.recv.Stream(thread_pool=threadPool, bug_compat=0, max_heaps=self._maxHeaps)
        stream.add_udp_reader(port=self._port,
                              max_size=self._maxPacketSize,
                              buffer_size=self._socketBufferSize,
                              bind_hostname=self._host)

        # Attach a memory pool to buffer the incoming data
        memoryPool = spead2.MemoryPool(lower=self._mpLower,
                                       upper=self._mpUpper,
                                       max_free=self._mpMaxFree,
                                       initial=self._mpInitial)
        stream.set_memory_pool(memoryPool)

        # Read heaps from the incoming stream until there are no more
        while True:
            try:
                self._processHeap(stream.get())
            except spead2.Stopped:
                logger.debug('Stream stopped, finishing listening')
                stream.stop()
                break

    def _processHeap(self, heap):
        # Get the data for the item we are interested in and write it to each
        # of our outputs
        for item in heap.get_items():
            if item.id == self._itemId:
                data = item.value
                for output in self.outputs:
                    output.write(data)
