#!/usr/bin/python
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

"""
DFMS Monitor runs outside the Pawsey firewall
--------------------------------------------------------------------------------
          Pawsey Magnus / Galaxy              |     Public         |
             Private Network                  |     Network        |
                                              |                    |
+---------+                +----------+       |      +--------+    |
|  DFMS   |                |  DFMS    |       |      | DFMS   |    |
| DropMgr | <== socket ==> |  Proxy   |<== socket ==>| Monitor|<- http <- Client
+---------+                +----------+       |      +--------+    |   (Browser)
                                              |                    |
                                           FIREWALL             GATEWAY
                                              |                    |
--------------------------------------------------------------------------------
"""

import socket, select
import time, os
import sys, logging
from optparse import OptionParser

BUFF_SIZE = 16384
outstanding_conn = 20
default_dfms_monitor_port = 30000
default_client_proxy_port = 30001
MAX_TIME_OUT = 3
MIN_TIME_OUT = 0.1
FORMAT = "%(asctime)-15s [%(levelname)5.5s] [%(threadName)15.15s] %(name)s#%(funcName)s:%(lineno)s %(message)s"

logger = logging.getLogger('deploy.pawsey.monitor')

class DFMSMonitor:
    input_list = []
    data = {}
    monitor_sock_queue = []
    select_time_out = MAX_TIME_OUT

    def __init__(self, host='0.0.0.0', monitor_port=default_dfms_monitor_port, client_port=default_client_proxy_port):
        """
        host:               listening host (string)
        monitor_port:       monitor port exposed to the dfms proxy  (int)
        client_port:        proxy port exposed to the client (e.g. Firefox) (int)
        """
        self.manager_listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.monitor_listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.setup_listening_socket(self.manager_listen_socket, host, monitor_port)
        self.setup_listening_socket(self.monitor_listen_socket, host, client_port)

    def setup_listening_socket(self, the_socket, host, port):
        the_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        the_socket.bind((host, port))
        the_socket.listen(outstanding_conn)

    def main_loop(self):
        self.input_list.append(self.manager_listen_socket)
        self.input_list.append(self.monitor_listen_socket)
        self.manager_socket = None
        self.monitor_socket = None
        self._temp_buff = None
        while 1:
            #logger.info("enter select")
            inputready, outputready, exceptready = select.select(self.input_list, [], [])
            #logger.info("exit select")
            for the_socket in inputready:
                if the_socket == self.manager_listen_socket or\
                   the_socket == self.monitor_listen_socket:
                    self.on_accept(the_socket)
                else:
                    # if (the_socket == self.manager_socket):
                    #     soc = "dfms_proxy"
                    # elif (the_socket == self.monitor_socket):
                    #     soc = "client"
                    #
                    # logger.info("data len = {0} from {1}".format(len(data), soc))
                    data = the_socket.recv(BUFF_SIZE)
                    self.data[the_socket] = data
                    if len(data) == 0:
                        #logger.debug("data == 0, close {0}".format(soc))
                        self.on_close(the_socket)
                    else:
                        self.on_recv(the_socket)
            for err_sock in exceptready:
                self.on_close(err_sock)

    def on_accept(self, the_socket):
        clientsock, clientaddr = the_socket.accept()
        if self.manager_listen_socket == the_socket:
            logger.info('receiving new socket from dfms_proxy')
            self.manager_socket = clientsock
            self.input_list.append(clientsock)
            if (self._temp_buff is not None):
                self.manager_socket.sendall(self._temp_buff)
                self._temp_buff = None
            if (self.monitor_socket is None and len(self.monitor_sock_queue) > 0):
                logger.debug("Processing outstanding client sockets")
                self.monitor_socket = self.monitor_sock_queue[0]
                self.monitor_sock_queue.remove(self.monitor_socket)
                self.input_list.append(self.monitor_socket)
                if (len(self.monitor_sock_queue) > 0):
                    self.select_time_out = MIN_TIME_OUT
                else:
                    self.select_time_out = MAX_TIME_OUT
        # else when we receive a connection from a localhost
        # AND we have a connection to the master maanger
        elif self.manager_socket is not None:
            if (self.monitor_socket is None):
                logger.info('receiving new socket from client')
                self.monitor_socket = clientsock
                self.input_list.append(clientsock)
            else:
                logger.info('receiving new socket from client, add to queue')
                self.monitor_sock_queue.append(clientsock)
                self.select_time_out = MIN_TIME_OUT
        # We don't have a connection to master manager yet so close the new connection
        # as we can't do anything with the data anyway!
        else:
            logger.debug('receiving socket from client, but')
            logger.warning("Can't establish connection with master manager server.")
            logger.info("Closing connection with client side {0}".format(clientaddr))
            self.on_close(clientsock)

    def on_close(self, the_socket):
        # if (the_socket == self.manager_socket):
        #     soc = "dfms_proxy"
        # elif (the_socket == self.monitor_socket):
        #     soc = "client"
        # logger.info("on_close called from {0}".format(soc))
        if (self.manager_socket == the_socket):
            #self.manager_socket.close()
            self.input_list.remove(self.manager_socket)
            self.manager_socket = None
        if (self.monitor_socket):
            self.monitor_socket.close()
            self.input_list.remove(self.monitor_socket)
            self.monitor_socket = None

        if (len(self.monitor_sock_queue) > 0):
            self.monitor_socket = self.monitor_sock_queue[0]
            self.monitor_sock_queue.remove(self.monitor_socket)
            # logger.debug("queue has {0} elements".format(len(self.monitor_sock_queue)))
            # if (len(self.monitor_sock_queue) > 0):
            #     self.select_time_out = MIN_TIME_OUT
            self.input_list.append(self.monitor_socket)
        else:
            self.select_time_out = MAX_TIME_OUT

        if self.data.has_key(the_socket):
            del self.data[the_socket]

    def on_recv(self, the_socket):
        data = self.data.get(the_socket, None)
        if (data is not None):
            if the_socket == self.manager_socket:
                if (self.monitor_socket):
                    self.monitor_socket.sendall(data)
            else:
                if (self.manager_socket):
                    self.manager_socket.sendall(data)
                else:
                    if (self._temp_buff is None):
                        self._temp_buff = data
                    else:
                        raise Exception("temp buffer overflow!")

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-H", "--host", action="store", type="string",
                    dest="host", help="The network interface the monitor is bind",
                    default='0.0.0.0')
    parser.add_option("-o", "--monitor_port", action="store", type="int",
                    dest="monitor_port", help = "The monitor port exposed to the dfms proxy",
                    default=default_dfms_monitor_port)
    parser.add_option("-c", "--client_port", action="store", type="int",
                    dest="client_port", help = "The proxy port exposed to the client",
                    default=default_client_proxy_port)
    parser.add_option("-l", "--log_dir", action="store", type="string",
                    dest="log_dir", help="log directory for dfms monitor server", default=os.path.realpath(__file__))
    (options, args) = parser.parse_args()
    logfile = "{0}/dfms_monitor.log".format(os.path.dirname(options.log_dir))
    logging.basicConfig(filename=logfile, level=logging.DEBUG, format=FORMAT)
    server = DFMSMonitor(options.host, options.monitor_port, options.client_port)
    try:
        server.main_loop()
    except KeyboardInterrupt:
        logger.warning("Ctrl C - Stopping DFMS Monitor server")
        sys.exit(1)
