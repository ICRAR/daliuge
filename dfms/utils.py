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
Module containing miscellaneous utility classes and functions.
"""

import errno
import logging
import os
import socket
import threading
import time
import netifaces
from zeroconf import ServiceInfo, Zeroconf


logger = logging.getLogger(__name__)

def get_local_ip_addr():
    """
    Enumerate all interfaces and return bound IP addresses (exclude localhost)
    """
    PROTO = netifaces.AF_INET
    ifaces = netifaces.interfaces()
    if_addrs = [(netifaces.ifaddresses(iface), iface) for iface in ifaces]
    if_inet_addrs = [(tup[0][PROTO], tup[1]) for tup in if_addrs if PROTO in tup[0]]
    iface_addrs = [(s['addr'], tup[1]) for tup in if_inet_addrs for s in tup[0] \
                    if 'addr' in s and not s['addr'].startswith('127.')]
    return iface_addrs

def register_service(service_type_name, service_name, protocol, ipaddr, port):
    """
    ZeroConf: Register service type, protocol, ipaddr and port

    Returns ZeroConf object and ServiceInfo object
    """
    stn = '_{0}._{1}.local.'.format(service_type_name, protocol)
    sn = '{0} {1}'.format(service_name, stn)
    info = ServiceInfo(stn, sn, socket.inet_aton(ipaddr), port, 0, 0, '', '')
    zc = Zeroconf()
    zc.register_service(info)
    return (zc, info)

def deregister_service(zc, info):
    """
    ZeroConf: Deregister service
    """
    zc.unregister_service(info)
    zc.close()

def browse_service(service_type_name, protocol, callback):
    """
    ZeroConf: Browse for services based on service type and protocol

    callback signature: callback(zeroconf, service_type, name, state_change)
        zeroconf: ZeroConf object
        service_type: zeroconf service
        name: service name
        state_change: ServiceStateChange type (Added, Removed)

    Returns ZeroConf object
    """
    stn = '_{0}._{1}.local.'.format(service_type_name, protocol)
    zc = Zeroconf()
    browser = ServiceBrowser(zc, stn, handlers=[callback])
    return zc


class CountDownLatch(object):
    """
    An implementation that shadows Java's CountDownLatch, allowing one thread
    to wait for others to finish.

    Code taken from:
    http://stackoverflow.com/questions/10236947/does-python-have-a-similar-control-mechanism-to-javas-countdownlatch
    """

    def __init__(self, count):
        self.count = count
        self.lock = threading.Condition()

    def countDown(self):
        self.lock.acquire()
        self.count -= 1
        if self.count <= 0:
            self.lock.notifyAll()
        self.lock.release()

    def await(self):
        self.lock.acquire()
        while self.count > 0:
            self.lock.wait()
        self.lock.release()

def portIsOpen(host, port, timeout=0):
    """
    Checks if a given host/port is opened, with a given `connectTimeout`. The
    check is done by simply opening a connection and then closing it.
    """
    return writeToRemotePort(host, port, data=None, timeout=timeout)

def writeToRemotePort(host, port, data=None, timeout=0):
    """
    Writes the given data into the port specified by `host`:`port`. A maximum
    waiting time of `timeout` can be specified (in seconds), in which case this
    method will try to establish a connection with the remote port for at least
    that amount of time. A values of 0 (the default) means that no waiting is
    performed, and a value of `None` means that an infinite timeout is used.

    This method returns `True` if the connection was successfully established
    with the given timeout, and if the data was successfully sent; `False`
    otherwise.
    """

    start = time.time()
    while True:
        try:
            if timeout is not None and timeout != 0:
                thisTimeout = timeout - (time.time() - start)
                if thisTimeout <= 0:
                    return False
            else:
                thisTimeout = None
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(thisTimeout)
            s.connect((host, port))
            if data is not None:
                s.send(data)
            s.close()
            return True
        except socket.timeout:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('Timed out while trying to connect to %s:%d with timeout of %f [s]' % (host, port, thisTimeout))
            return False
        except socket.error as e:
            # If the connection becomes suddenly closed from the server-side.
            # We assume that it's not re-opening any time soon, so we simply
            # return False
            if e.errno == errno.ECONNRESET:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug("Connection closed by %s:%d, assuming it will stay closed" % (host, port))
                return False
            # If the port is closed we keep trying until enough time has gone by
            if e.errno == errno.ECONNREFUSED:
                if timeout is not None:
                    if time.time() - start > timeout:
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug('Refused connection to %s:%d for more than %f seconds' % (host, port, timeout))
                        return False
                time.sleep(0.1)
                continue
            # Any other error should be raised
            raise

def getDfmsDir():
    """
    Returns the root of the directory structure used by the DFMS framework at
    runtime.
    """
    if 'XDG_RUNTIME_DIR' in os.environ:
        return os.path.join(os.environ['XDG_RUNTIME_DIR'], "dfms")
    return os.path.join(os.path.expanduser("~"), ".dfms")

def getDfmsPidDir(createIfMissing=False):
    """
    Returns the location of the directory used by the DFMS framework to store
    its PIDs. If `createIfMissing` is True, the directory will be created if it
    currently doesn't exist
    """
    path = os.path.join(getDfmsDir(), 'pid')
    if createIfMissing:
        createDirIfMissing(path)
    return path

def getDfmsLogsDir(createIfMissing=False):
    """
    Returns the location of the directory used by the DFMS framework to store
    its logs. If `createIfMissing` is True, the directory will be created if it
    currently doesn't exist
    """
    path = os.path.join(getDfmsDir(), 'logs')
    if createIfMissing:
        createDirIfMissing(path)
    return path

def createDirIfMissing(path):
    """
    Creates the given directory if it doesn't exist
    """
    if os.path.exists(path):
        return
    os.makedirs(path)

def escapeQuotes(s, singleQuotes=True, doubleQuotes=True):
    """
    Escapes single and double quotes in a string. Useful to include commands
    in a shell invocation or similar.
    """
    if singleQuotes:
        s = s.replace("'","'\\''")
    if doubleQuotes:
        s = s.replace('"','\\"')
    return s

def isLocalhost(host):
    return host == 'localhost' or \
           host.startswith('127.0') or \
           host == socket.gethostname()
