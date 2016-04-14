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
DFMS Monitor runs outside the Pawsey firewall (e.g. somewhere in ICRAR)
"""

import socket
import select
import time
import sys

BUFF_SIZE = 16384
outstanding_conn = 20
default_dfms_monitor_port = 30000
default_client_proxy_port = 30001
MAX_TIME_OUT = 3
MIN_TIME_OUT = 0.1

class DFMSMonitor:
    input_list = []
    data = {}
    monitor_sock_queue = []
    select_time_out = MAX_TIME_OUT

    def __init__(self, host='0.0.0.0', dfms_port=default_dfms_monitor_port, client_port=default_client_proxy_port):
        """
        host:               listening host (string)
        dfms_port:          monitor port exposed to the dfms drop manager  (int)
        client_port:        proxy port exposed to the client (e.g. Firefox) (int)
        """
        self.manager_listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.monitor_listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.setup_listening_socket(self.manager_listen_socket, host, dfms_port)
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
        while 1:
            inputready, outputready, exceptready = select.select(self.input_list, [], [], self.select_time_out)
            if not (inputready or outputready or exceptready):
                if (self.monitor_socket is not None):
                    # modern browsers use HTTP1.1 persistent connection, so
                    # need to close the time-out connection and move onto the
                    # next connection in the queue
                    self.on_close(self.monitor_socket)
            else:
                for the_socket in inputready:
                    if the_socket == self.manager_listen_socket or\
                       the_socket == self.monitor_listen_socket:
                        self.on_accept(the_socket)
                    else:
                        data = the_socket.recv(BUFF_SIZE)
                        self.data[the_socket] = data
                        if len(data) == 0:
                            self.on_close(the_socket)
                        else:
                            self.on_recv(the_socket)
                for err_sock in exceptready:
                    self.on_close(err_sock)


    def on_accept(self, the_socket):
        clientsock, clientaddr = the_socket.accept()
        # When we receive a connection from master manager host
        if self.manager_listen_socket == the_socket:
            print 'receiving manager_socket'
            self.manager_socket = clientsock
            self.input_list.append(clientsock)
            if (self.monitor_socket is None and len(self.monitor_sock_queue) > 0):
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
                self.monitor_socket = clientsock
                self.input_list.append(clientsock)
            else:
                self.monitor_sock_queue.append(clientsock)
                self.select_time_out = MIN_TIME_OUT
        # We don't have a connection to master manager yet so close the new connection
        # as we can't do anything with the data anyway!
        else:
            print "Can't establish connection with master manager server.",
            print "Closing connection with client side", clientaddr
            self.on_close(clientsock)

    def on_close(self, the_socket):
        if (self.manager_socket == the_socket):
            self.manager_socket.close()
            self.input_list.remove(self.manager_socket)
            self.manager_socket = None
        if (self.monitor_socket):
            self.monitor_socket.close()
            self.input_list.remove(self.monitor_socket)
            self.monitor_socket = None

        if (len(self.monitor_sock_queue) > 0):
            self.monitor_socket = self.monitor_sock_queue[0]
            self.monitor_sock_queue.remove(self.monitor_socket)
            if (len(self.monitor_sock_queue) > 0):
                self.select_time_out = MIN_TIME_OUT
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

if __name__ == '__main__':
    server = DFMSMonitor()
    try:
        server.main_loop()
    except KeyboardInterrupt:
        print "Ctrl C - Stopping server"
        sys.exit(1)
