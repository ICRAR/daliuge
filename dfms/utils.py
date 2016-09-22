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

import contextlib
import errno
import logging
import math
import os
import socket
import threading
import time


logger = logging.getLogger(__name__)

def get_local_ip_addr():
    """
    Enumerate all interfaces and return bound IP addresses (exclude localhost)
    """
    import netifaces
    PROTO = netifaces.AF_INET
    ifaces = netifaces.interfaces()
    if_addrs = [(netifaces.ifaddresses(iface), iface) for iface in ifaces]
    if_inet_addrs = [(tup[0][PROTO], tup[1]) for tup in if_addrs if PROTO in tup[0]]
    iface_addrs = [(s['addr'], tup[1]) for tup in if_inet_addrs for s in tup[0] \
                    if 'addr' in s and not s['addr'].startswith('127.')]
    return iface_addrs

def register_service(zc, service_type_name, service_name, ipaddr, port, protocol='tcp'):
    """
    ZeroConf: Register service type, protocol, ipaddr and port

    Returns ZeroConf object and ServiceInfo object
    """
    from zeroconf import ServiceInfo
    sn = service_name if len(service_name) <= 15 else service_name[:15]
    stn = '_{0}._{1}.local.'.format(service_type_name, protocol)
    sn = '{0}.{1}'.format(sn, stn)
    info = ServiceInfo(stn, sn, address=socket.inet_aton(ipaddr), port=port, properties={})
    zc.register_service(info)
    return info

def deregister_service(zc, info):
    """
    ZeroConf: Deregister service
    """
    zc.unregister_service(info)

def browse_service(zc, service_type_name, protocol, callback):
    """
    ZeroConf: Browse for services based on service type and protocol

    callback signature: callback(zeroconf, service_type, name, state_change)
        zeroconf: ZeroConf object
        service_type: zeroconf service
        name: service name
        state_change: ServiceStateChange type (Added, Removed)

    Returns ZeroConf object
    """
    from zeroconf import ServiceBrowser
    stn = '_{0}._{1}.local.'.format(service_type_name, protocol)
    browser = ServiceBrowser(zc, stn, handlers=[callback])
    return browser

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

def portIsClosed(host, port, timeout):
    """
    Checks if a given ``host``/``port`` is closed, with a given ``timeout``.
    """
    return check_port(host, port, timeout=timeout, checking_open=False)

def portIsOpen(host, port, timeout=0):
    """
    Checks if a given ``host``/``port`` is open, with a given ``timeout``.
    """
    return check_port(host, port, timeout=timeout, checking_open=True)

def connect_to(host, port, timeout=None):
    """
    Connects to ``host``:``port`` within the given timeout and return the
    connected socket. If no connection could be established ``None`` is
    returned.
    """
    return check_port(host, port, timeout=timeout, return_socket=True)

def write_to(host, port, data, timeout=None):
    """
    Connects to ``host``:``port`` within the given timeout and write the given
    piece of ``data`` into the connected socket.
    """
    sock = connect_to(host, port, timeout=timeout)
    with contextlib.closing(sock):
        sock.send(data)

def check_port(host, port, timeout=0, checking_open=True, return_socket=False):
    """
    Checks that the port specified by ``host``:``port`` is either open or
    closed (depending on the value of ``checking_open``) within a given
    ``timeout``.
    When checking for an open port, this method will keep trying to connect to
    it either until the given ``timeout`` has expired or until the socket is
    found open. When checking for a closed port this method will keep trying to
    connect to it until the connection is unsuccessful, or until the ``timeout``
    expires.
    Additionally, if some ``data`` is passed and the method is ``checking_open``
    then ``data`` will be written to the socket if it connects successfully.

    This method returns ``True`` if the port was found on the expected state
    within the time limit, and ``False`` otherwise.
    """

    if return_socket and not checking_open:
        raise ValueError("If return_socket is True then checking_open must be True")

    start = time.time()
    while True:
        try:

            # Calculate the timeout used during this cycle
            # If we're past the timeout we have failed already
            thisTimeout = None
            if timeout is not None and timeout != 0:
                thisTimeout = timeout - (time.time() - start)
                if thisTimeout <= 0:
                    return False

            # Create the socket and try to connect, sending data if required
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            if return_socket:
                ret = s
            else:
                ret = True

            # socket is closed always on error, conditionally otherwise
            try:
                s.settimeout(thisTimeout)
                s.connect((host, port))
                if not return_socket:
                    s.close()
            except socket.error:
                s.close()
                raise

            # Success if we were checking for an open port!
            if checking_open:
                return ret

            # Otherwise keep trying until we find the socket closed
            time.sleep(0.1)
            continue

        except socket.timeout:
            logger.debug('Timed out while trying to connect to %s:%d with timeout of %f [s]', host, port, thisTimeout)
            return not checking_open

        except socket.error as e:

            # If the connection becomes suddenly closed from the server-side.
            # We assume that it's not re-opening any time soon
            if e.errno == errno.ECONNRESET:
                logger.debug("Connection closed by %s:%d, assuming it will stay closed", host, port)
                if not return_socket:
                    return not checking_open
                raise

            # The port is closed
            elif e.errno == errno.ECONNREFUSED:

                if not checking_open:
                    return True

                # Keep trying because we're checking if it's open
                if timeout is not None:
                    if time.time() - start > timeout:
                        logger.debug('Refused connection to %s:%d for more than %f seconds', host, port, timeout)
                        if not return_socket:
                            return False
                        raise

                time.sleep(0.1)
                continue

            # Any other error should be raised
            raise

def getDfmsDir():
    """
    Returns the root of the directory structure used by the DFMS framework at
    runtime.
    """
    return os.path.join(os.path.expanduser("~"), ".dfms")

def getDfmsPidDir():
    """
    Returns the location of the directory used by the DFMS framework to store
    its PIDs. If `createIfMissing` is True, the directory will be created if it
    currently doesn't exist
    """
    return os.path.join(getDfmsDir(), 'pid')

def getDfmsLogsDir():
    """
    Returns the location of the directory used by the DFMS framework to store
    its logs. If `createIfMissing` is True, the directory will be created if it
    currently doesn't exist
    """
    return os.path.join(getDfmsDir(), 'logs')

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

def prepare_sql(sql, paramstyle, data=()):
    """
    Prepares the given SQL statement for proper execution depending on the
    parameter style supported by the database driver. For this the SQL statement
    must be written using the "{X}" or "{}" placeholders in place for each,
    parameter which is a style-agnostic parameter notation.

    This method returns a tuple containing the prepared SQL statement and the
    values to be bound into the query as required by the driver.
    """

    n = len(data)
    if not n:
        return (sql, ())

    # Depending on the different vendor, we need to write the parameters in
    # the SQL calls using different notations. This method will produce an
    # array containing all the parameter references in the SQL statement
    # (and not its values!)
    #
    # qmark     Question mark style, e.g. ...WHERE name=?
    # numeric   Numeric, positional style, e.g. ...WHERE name=:1
    # named     Named style, e.g. ...WHERE name=:name
    # format    ANSI C printf format codes, e.g. ...WHERE name=%s
    # pyformat  Python extended format codes, e.g. ...WHERE name=%(name)s

    logger.debug('Generating %d markers with paramstyle = %s', n, paramstyle)

    if   paramstyle == 'qmark':    markers = ['?'             for i in range(n)]
    elif paramstyle == 'numeric':  markers = [':%d'%(i)       for i in range(n)]
    elif paramstyle == 'named':    markers = [':n%d'%(i)      for i in range(n)]
    elif paramstyle == 'format':   markers = [':%s'           for i in range(n)]
    elif paramstyle == 'pyformat': markers = [':%%(n%d)s'%(i) for i in range(n)]
    else: raise Exception('Unknown paramstyle: %s' % (paramstyle))

    sql = sql.format(*markers)

    if paramstyle in ['format', 'pyformat']:
        data = {'n%d'%(i): d for i,d in enumerate(data)}

    return (sql, data)

def terminate_or_kill(proc, timeout):
    """
    Terminates a process and waits until it has completed its execution within
    the given timeout. If the process is still alive after the timeout it is
    killed.
    """

    pid = proc.pid
    logger.info('Terminating %d', pid)
    proc.terminate()

    waitLoops = 0
    max_loops = math.ceil(timeout/0.1)
    while proc.poll() is None and waitLoops < max_loops:
        time.sleep(0.1)
        waitLoops += 1

    kill9 = waitLoops == max_loops
    if kill9:
        logger.warning('Killing %s by brute force after waiting %.2f [s], BANG! :-(', pid, timeout)
        proc.kill()
    proc.wait()