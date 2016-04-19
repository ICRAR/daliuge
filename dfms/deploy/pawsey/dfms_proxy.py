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

import socket, os
import select, struct
import time
import sys, logging
from optparse import OptionParser
from collections import defaultdict

BUFF_SIZE = 16384
conn_retry_timeout = 5
conn_retry_count = 100
delay = 0.0001
default_dfms_monitor_port = 30000
default_dfms_port = 8001
FORMAT = "%(asctime)-15s [%(levelname)5.5s] [%(threadName)15.15s] %(name)s#%(funcName)s:%(lineno)s %(message)s"

logger = logging.getLogger('deploy.pawsey.proxy')
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

def send_to_monitor(sock, data):
    length = len(data)
    sock.sendall(struct.pack('!I', length))
    sock.sendall(data)

def recv_from_monitor(sock):
    lengthbuf = recvall(sock, 4)
    if (lengthbuf is None):
        return None
    length, = struct.unpack('!I', lengthbuf)
    return recvall(sock, length)

class DFMSProxy:
    def __init__(self, dfms_host, monitor_host, dfms_port=default_dfms_port, monitor_port=default_dfms_monitor_port):
        self._dfms_host = dfms_host
        self._dfms_port = dfms_port
        self._monitor_host = monitor_host
        self._monitor_port = monitor_port
        self._dfms_sock_dict = dict()
        self._dfms_sock_tag_dict = dict()

    def connect_to_host(self, server, port):
        connected = False
        the_socket = None # keep the IDE happy!
        retry_count = 0
        while not connected:
            if retry_count >= conn_retry_count:
                logger.error("Retry connecting to DFMS monitor exhausted, quit")
                sys.exit(2)
            try:
                the_socket = socket.create_connection((server, port))
                connected = True
                logger.info('Connected to ' + server + ' on port ' + str(port))
            except Exception as e:
                err = str(e)
                logger.error("Fail to connect to {0} on port {1} due to {2}".format(server, port, err))
                # Sleep for a while before trying to connect again
                time.sleep(conn_retry_timeout)
                retry_count += 1
        return the_socket

    def connect_monitor_host(self):
        self.monitor_socket = self.connect_to_host(self._monitor_host, self._monitor_port)

    def loop(self):
        self.connect_monitor_host()
        inputlist = [self.monitor_socket]
        remove_dict = defaultdict(int)
        just_re_connected = False
        while 1:
            if (just_re_connected):
                just_re_connected = False
            time.sleep(delay)
            inputready, outputready, exceptready = select.select(
                    inputlist, [], [])
            to_be_removed = []
            for the_socket in inputready:
                if (just_re_connected):
                    continue
                if (the_socket == self.monitor_socket):
                    data = recv_from_monitor(the_socket)
                    if (data is None):
                        logger.warning("Socket to dfms monitor is broken")
                        inputlist.remove(the_socket)
                        if (self.monitor_socket):
                            self.monitor_socket.close()
                            self.monitor_socket = None
                        logger.info("Try reconnecting to dfms monitor...")
                        self.connect_monitor_host()
                        just_re_connected = True
                        inputlist.append(self.monitor_socket)
                        continue
                    at = data.find(delimit)
                    if (at == -1):
                        logger.error('No tag id from dfms_monitor, discard the message')
                        continue
                    else:
                        tag = data[0:at]
                    dfms_sock = self._dfms_sock_dict.get(tag, None)
                    if (dfms_sock is None):
                        dfms_sock = self.connect_to_host(self._dfms_host, self._dfms_port)
                        self._dfms_sock_dict[tag] = dfms_sock
                        self._dfms_sock_tag_dict[dfms_sock] = tag
                        inputlist.append(dfms_sock)

                    to_send = data[at + dl:]
                    if (len(to_send) == 0):
                        logger.debug("Length of data to be sent to DFMS is zero")

                    dfms_sock.sendall(to_send)
                else:
                    # from one of the DFMS sockets
                    data = the_socket.recv(BUFF_SIZE)
                    tag = self._dfms_sock_tag_dict.get(the_socket, None)
                    if (tag is None):
                        raise Exception('Tag for dfms socket {0} is gone'.format(the_socket))
                    else:
                        send_to_monitor(self.monitor_socket, delimit.join([str(tag), data]))
                        if (len(data) == 0):
                            remove_dict[tag] += 1
                            logger.debug("Length of data sent to monitor is zero ")
                            to_be_removed.append((the_socket, tag))

            for sock, tag in to_be_removed:
                if (remove_dict[tag] > 2):
                    sock.close() #close itself, is this necessary?
                    del self._dfms_sock_dict[tag]
                    del self._dfms_sock_tag_dict[sock]
                    inputlist.remove(sock)
                    del remove_dict[tag]
                    logger.debug("Removed socket (tag {0}) from client socket dictionary\n".format(tag))

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-d", "--dfms_host", action="store", type="string",
                    dest="dfms_host", help="DFMS drop manager host IP (required)")
    parser.add_option("-m", "--monitor_host", action="store", type="string",
                    dest="monitor_host", help="Monitor host IP (required)")
    parser.add_option("-l", "--log_dir", action="store", type="string",
                    dest="log_dir", help="Log directory (optional)", default=os.path.realpath(__file__))
    parser.add_option("-f", "--dfms_port", action="store", type="int",
                    dest="dfms_port", help = "The port to bind dfms drop manager", default=default_dfms_port)
    parser.add_option("-o", "--monitor_port", action="store", type="int",
                    dest="monitor_port", help = "The port to bind dfms monitor", default=default_dfms_monitor_port)
    (options, args) = parser.parse_args()
    if (None == options.dfms_host or None == options.monitor_host):
        parser.print_help()
        sys.exit(1)
    logfile = "{0}/dfms_proxy.log".format(os.path.dirname(options.log_dir))
    logging.basicConfig(filename=logfile, level=logging.DEBUG, format=FORMAT)
    server = DFMSProxy(options.dfms_host, options.monitor_host, options.dfms_port, options.monitor_port)
    try:
        server.loop()
    except KeyboardInterrupt:
        logger.warning("Ctrl C - Stopping DFMS Proxy server")
        sys.exit(1)
