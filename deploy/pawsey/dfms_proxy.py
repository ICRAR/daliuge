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
DFMS Proxy runs inside the Pawsey firewall
"""

import socket
import select
import time
import sys

BUFF_SIZE = 16384
connection_retry_timeout = 15
delay = 0.0001
default_dfms_monitor_port = 30000
default_dfms_port = 8001

class DFMSProxy:
    def __init__(self, dfms_host, monitor_host, dfms_port=default_dfms_port, monitor_port=default_dfms_monitor_port):
        self._dfms_host = dfms_host
        self._dfms_port = dfms_port
        self._monitor_host = monitor_host
        self._monitor_port = monitor_port

    def send_to_manager_host(self, data):
        self.manager_socket.sendall(data)


    def send_to_monitor_host(self, data):
        self.monitor_socket.sendall(data)


    def connect_to_host(self, server, port):
        connected = False
        the_socket = None # keep the IDE happy!
        retry_count = 0
        while not connected:
            if retry_count >= 3:
                sys.exit(2)
            try:
                the_socket = socket.create_connection((server, port))
                connected = True
                print 'Connected to ' + server + ' on port ' + str(port)
            except Exception as e:
                print e
                # Sleep for a while before trying to connect again
                time.sleep(connection_retry_timeout)
                retry_count += 1
        return the_socket

    def connect_manager_host(self):
        self.manager_socket = self.connect_to_host(self._dfms_host, self._dfms_port)

    def connect_monitor_host(self):
        self.monitor_socket = self.connect_to_host(self._monitor_host, self._monitor_port)

    def loop(self):
        print 'connecting to dfms manager'
        self.connect_manager_host()
        print 'connecting to monitor host'
        self.connect_monitor_host()
        while 1:
            time.sleep(delay)
            inputready, outputready, exceptready = select.select(
                    [self.monitor_socket, self.manager_socket], [], [])
            for the_socker in inputready:
                data = the_socker.recv(BUFF_SIZE)
                if len(data) == 0:
                    # Reconnect to lost host
                    if the_socker == self.manager_socket:
                        self.connect_manager_host()
                    elif the_socker == self.monitor_socket:
                        self.connect_monitor_host()
                else:
                    if the_socker == self.manager_socket:
                        #print 'received from manager_socket ' + data
                        self.send_to_monitor_host(data)
                    elif the_socker == self.monitor_socket:
                        #print 'received from monitor_socket ' + data
                        self.send_to_manager_host(data)

if __name__ == '__main__':
    server = DFMSProxy('localhost', 'localhost')
    try:
        server.loop()
    except KeyboardInterrupt:
        print "Ctrl C - Stopping server"
        sys.exit(1)
