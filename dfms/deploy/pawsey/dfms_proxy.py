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

import select
import socket
import struct
import sys, logging
import time

import six

from dfms.utils import b2s as b2s

BUFF_SIZE = 16384
conn_retry_timeout = 5
conn_retry_count = 100
delay = 0.0001
default_dfms_monitor_port = 30000
default_dfms_port = 8001
FORMAT = "%(asctime)-15s [%(levelname)5.5s] [%(threadName)15.15s] %(name)s#%(funcName)s:%(lineno)s %(message)s"

logger = logging.getLogger(__name__)
delimit = b'@#%!$'
dl = len(delimit)

def recvall(sock, count):
    buf = b''
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
    def __init__(self, proxy_id, dfms_host, monitor_host, dfms_port=default_dfms_port, monitor_port=default_dfms_monitor_port):
        self._proxy_id = proxy_id if len(proxy_id) <= 80 else proxy_id[:80]
        self._dfms_host = dfms_host
        self._dfms_port = dfms_port
        self._monitor_host = monitor_host
        self._monitor_port = monitor_port
        self._dfms_sock_dict = dict()
        self._dfms_sock_tag_dict = dict()

    def connect_to_host(self, server, port):
        retry_count = 0
        while True:
            if retry_count >= conn_retry_count:
                logger.error("Retry connecting to DFMS monitor exhausted, quit")
                #sys.exit(2)
            try:
                the_socket = socket.create_connection((server, port))
                the_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                logger.info('Connected to %s on port %d' % (server, port))
                return the_socket
            except Exception:
                logger.exception("Failed to connect to %s:%d", server, port)
                # Sleep for a while before trying to connect again
                time.sleep(conn_retry_timeout)
                retry_count += 1

    def connect_monitor_host(self):
        # After connecting we identify ourselves using our ID with the monitor
        the_socket = self.connect_to_host(self._monitor_host, self._monitor_port)
        the_socket.sendall(b"%-80s" % six.b(self._proxy_id))
        logger.info('Identifying ourselves as %s with monitor', self._proxy_id)
        ok = int(recvall(the_socket, 1))
        if not ok:
            the_socket.shutdown(socket.SHUT_RDWR)
            the_socket.close()
            raise Exception("Monitor rejected us due to duplicated ID")
        logger.info('Identification successful!')
        self.monitor_socket = the_socket

    def close_dfms_socket(self, sock, tag):
        try:
            sock.close()
        except socket.error:
            pass
        del self._dfms_sock_dict[tag]
        del self._dfms_sock_tag_dict[sock]

    def loop(self):
        self.connect_monitor_host()
        inputlist = [self.monitor_socket]
        just_re_connected = False
        while 1:
            if (just_re_connected):
                just_re_connected = False
            inputready, _, _ = select.select(
                    inputlist + list(self._dfms_sock_dict.values()), [], [], delay)
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
                    logger.debug("Received {0} from Monitor".format(tag))
                    dfms_sock = self._dfms_sock_dict.get(tag, None)
                    to_send = data[at + dl:]
                    if (dfms_sock is None):
                        if (len(to_send) > 0):
                            dfms_sock = self.connect_to_host(self._dfms_host, self._dfms_port)
                            self._dfms_sock_dict[tag] = dfms_sock
                            self._dfms_sock_tag_dict[dfms_sock] = tag
                            send_to_dfms = True
                        else:
                            send_to_dfms = False
                    else:
                        send_to_dfms = True
                    if (send_to_dfms):
                        try:
                            dfms_sock.sendall(to_send)
                            logger.debug("Sent {0} to DFMS manager".format(tag))
                        except socket.error:
                            self.close_dfms_socket(dfms_sock, tag)
                else:
                    # from one of the DFMS sockets
                    data = the_socket.recv(BUFF_SIZE)
                    tag = self._dfms_sock_tag_dict.get(the_socket, None)
                    logger.debug("Received {0} from DFMS manager".format(b2s(tag)))
                    if (tag is None):
                        logger.error('Tag for dfms socket {0} is gone'.format(the_socket))
                    else:
                        send_to_monitor(self.monitor_socket, delimit.join([tag, data]))
                        logger.debug("Sent {0} to Monitor".format(b2s(tag)))
                        if (len(data) == 0):
                            self.close_dfms_socket(the_socket, tag)

def run(parser, args):
    '''
    Entry point for the dlg proxy command
    '''
    parser.add_option("-d", "--dfms_host", action="store", type="string",
                    dest="dfms_host", help="DFMS drop manager host IP (required)")
    parser.add_option("-m", "--monitor_host", action="store", type="string",
                    dest="monitor_host", help="Monitor host IP (required)")
    parser.add_option("-l", "--log_dir", action="store", type="string",
                    dest="log_dir", help="Log directory (optional)", default='.')
    parser.add_option("-f", "--dfms_port", action="store", type="int",
                    dest="dfms_port", help = "The port to bind dfms drop manager", default=default_dfms_port)
    parser.add_option("-o", "--monitor_port", action="store", type="int",
                    dest="monitor_port", help = "The port to bind dfms monitor", default=default_dfms_monitor_port)
    parser.add_option("-b", "--debug",
                  action="store_true", dest="debug", default=False,
                  help="Whether to log debug info")
    parser.add_option("-i", "--id", action="store", type="string",
                      dest="id", help="The ID of this proxy for on the monitor side (required)", default=None)
    (options, args) = parser.parse_args(args)
    if (None == options.dfms_host or None == options.monitor_host or None == options.id):
        parser.print_help()
        sys.exit(1)
    if (options.debug):
        ll = logging.DEBUG
    else:
        ll = logging.INFO
    logfile = "%s/dfms_proxy.log" % options.log_dir
    logging.basicConfig(filename=logfile, level=ll, format=FORMAT)
    server = DFMSProxy(options.id, options.dfms_host, options.monitor_host, options.dfms_port, options.monitor_port)
    try:
        server.loop()
    except KeyboardInterrupt:
        logger.warning("Ctrl C - Stopping DFMS Proxy server")
        sys.exit(1)