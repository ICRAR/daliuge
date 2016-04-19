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

import socket, select, time
import time, os, struct
import sys, logging
from optparse import OptionParser
from collections import defaultdict

BUFF_SIZE = 16384
outstanding_conn = 20
default_dfms_monitor_port = 30000
default_client_proxy_port = 30001
MAX_TIME_OUT = 3
MIN_TIME_OUT = 0.1
FORMAT = "%(asctime)-15s [%(levelname)5.5s] [%(threadName)15.15s] %(name)s#%(funcName)s:%(lineno)s %(message)s"

logger = logging.getLogger('deploy.pawsey.monitor')
delimit = '@#%!$'
dl = len(delimit)

def recvall(sock, count):
    buf = ''
    while count:
        # this will block
        newbuf = sock.recv(count)
        if not newbuf: return None
        buf += newbuf
        count -= len(newbuf)
    return buf

def send_to_dfms(sock, data):
    length = len(data)
    sock.sendall(struct.pack('!I', length))
    sock.sendall(data)

def recv_from_dfms(sock):
    lengthbuf = recvall(sock, 4)
    if (lengthbuf is None):
        return None
    length, = struct.unpack('!I', lengthbuf)
    return recvall(sock, length)

def make_tag(sock):
    return '{0}'.format(sock.__hash__())

class DFMSMonitor:
    input_list = []
    data = {}
    monitor_sock_queue = []
    select_time_out = MAX_TIME_OUT
    cl_sock_dict = dict()

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
        count = 0
        remove_dict = defaultdict(int)
        while 1:
            inputready, outputready, exceptready = select.select(self.input_list + self.cl_sock_dict.values(), [], [])
            to_be_removed = []
            #logger.debug("len of inputready = {0}".format(len(inputready)))
            for index, the_socket in enumerate(inputready):
                #logger.debug("index = {0}".format(index))
                if the_socket == self.manager_listen_socket or\
                   the_socket == self.monitor_listen_socket:
                    self.on_accept(the_socket)
                else:
                    if (the_socket == self.manager_socket):
                        data = recv_from_dfms(the_socket)
                        if (data is None):
                            logger.warning("Socket to dfms proxy is broken")
                            self.input_list.remove(the_socket)
                            if (self.manager_socket):
                                self.manager_socket.close()
                                self.manager_socket = None
                            continue
                        at = data.find(delimit)
                        if (at == -1):
                            logger.error('No tag id from dfms_proxy, discard the message')
                            continue
                        else:
                            tag = data[0:at]
                        client_sock = self.cl_sock_dict.get(tag, None)
                        to_send = data[at + dl:]
                        leng = len(to_send)
                        if (client_sock is None):
                            extra_err = ''
                            if (leng > 0):
                                extra_err = "But its data is not empty"
                            err_msg = 'Client socket for tag {0} is gone: {2}/{1}. {3}'.format(tag, index, count, extra_err)
                            logger.error(err_msg)
                        else:
                            if (leng == 0):
                                logger.debug("Length of data to be sent to client is zero: {1}/{0}".format(index, count))
                            client_sock.sendall(to_send)
                    else: # from one of the client sockets
                        data = the_socket.recv(BUFF_SIZE)
                        tag = make_tag(the_socket)
                        send_to_dfms(self.manager_socket, delimit.join([tag, data]))
                        if (len(data) == 0):
                            remove_dict[tag] += 1
                            logger.debug("Length of data to be sent to server is zero: {1}/{0}".format(index, count))
                            to_be_removed.append((the_socket, tag))

            for sock, tag in to_be_removed:
                if (remove_dict[tag] > 4):
                    sock.close()
                    del self.cl_sock_dict[tag]
                    #if (the_socket in self.input_list):
                    #self.input_list.remove(the_socket)
                    #else:
                    #logger.error("Tag {0} is gone from the inputlist".format(tag))
                    del remove_dict[tag]
                    logger.debug("Removed socket (tag {0}/{1}) from client socket dictionary\n".format(tag, count))
            count += 1

    def on_accept(self, the_socket):
        clientsock, clientaddr = the_socket.accept()
        if self.manager_listen_socket == the_socket:
            logger.info('receiving new socket from dfms_proxy')
            if (self.manager_socket is not None):
                raise Exception("already not none!")
            self.manager_socket = clientsock
            self.input_list.append(self.manager_socket)
        # else when we receive a connection from a localhost
        # AND we have a connection to the master maanger
        elif self.manager_socket is not None:
            logger.info('receiving new socket from client')
            tag = make_tag(clientsock)
            if (self.cl_sock_dict.has_key(tag)):
                raise Exception("duplicated tag {0}".format(tag))
            self.cl_sock_dict[str(tag)] = clientsock
            logger.debug("Added socket (tag {0}) to client socket dictionary".format(tag))
            #self.input_list.append(clientsock)
        else:
            logger.debug('receiving socket from client, but')
            logger.warning("Can't establish connection with master manager server.")
            logger.info("Closing connection with client side {0}".format(clientaddr))
            clientsock.close()

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
