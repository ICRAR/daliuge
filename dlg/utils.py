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
import functools
import json
import logging
import math
import os
import socket
import sys
import threading
import time
import types
import zlib

import netifaces
import six


logger = logging.getLogger(__name__)

if sys.version_info[0] > 2:
    def b2s(b, enc='utf8'):
        return b.decode(enc)
else:
    def b2s(b, enc='utf8'):
        return b
b2s.__doc__ = "Converts bytes into a string"

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

def get_all_ipv4_addresses():
    """Get a list of all IPv4 interfaces found in this computer"""
    proto = netifaces.AF_INET
    return [addr['addr']
        for iface in netifaces.interfaces()
        for iface_proto, addrs in netifaces.ifaddresses(iface).items() if proto == iface_proto
        for addr in addrs if 'addr' in addr
    ]

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

def zmq_safe(host_or_addr):
    """Converts `host_or_addr` to a format that is safe for ZMQ to use"""

    # The catch-all IP address, ZMQ needs a *
    if host_or_addr == '0.0.0.0':
        return '*'

    # Return otherwise always an IP address
    return socket.gethostbyname(host_or_addr)

def to_externally_contactable_host(host, prefer_local=False):
    """
    Turns `host`, which is an address used to bind a local service,
    into a host that can be used to externally contact that service.

    This should be used when there is no other way to find out how a client
    to that service is going to connect to it.
    """

    # A specific address was used for binding, use that
    # (regardless of the user preference), making sure we return an IP
    if host != '0.0.0.0':
        return socket.gethostbyname(host)

    # host was "all interfaces", select one based on preference
    # if only one interface is found we assume it's a loopback interface
    addresses = get_all_ipv4_addresses()
    if prefer_local or len(addresses) == 1:
        return '127.0.0.1'

    # Choose the first non-127.0.0.1 one
    for a in addresses:
        if not a.startswith('127.'):
            return a

    # All addresses were loopbacks! let's return the last one
    raise a

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

def getDlgDir():
    """
    Returns the root of the directory structure used by the DALiuGE framework at
    runtime.
    """
    if 'DLG_ROOT' in os.environ:
        return os.environ['DLG_ROOT']
    return os.path.join(os.path.expanduser("~"), ".dlg")

def getDlgPidDir():
    """
    Returns the location of the directory used by the DALiuGE framework to store
    its PIDs. If `createIfMissing` is True, the directory will be created if it
    currently doesn't exist
    """
    return os.path.join(getDlgDir(), 'pid')

def getDlgLogsDir():
    """
    Returns the location of the directory used by the DALiuGE framework to store
    its logs. If `createIfMissing` is True, the directory will be created if it
    currently doesn't exist
    """
    return os.path.join(getDlgDir(), 'logs')

def createDirIfMissing(path):
    """
    Creates the given directory if it doesn't exist
    """
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

def isabs(path):
    """Like os.path.isabs, but handles None"""
    return path and os.path.isabs(path)

def fname_to_pipname(fname):
    """
    Converts a graph filename (assuming it's a .json file) to its "pipeline"
    name (the basename without the extension).
    """
    fname = fname.split('/')[-1]
    if fname.endswith('.json'):
        fname = fname[:-5]
    return fname

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
    if proc.poll() is not None:
        return
    logger.info('Terminating %d', proc.pid)
    proc.terminate()
    wait_or_kill(proc, timeout)

def wait_or_kill(proc, timeout):
    waitLoops = 0
    max_loops = math.ceil(timeout/0.1)
    while proc.poll() is None and waitLoops < max_loops:
        time.sleep(0.1)
        waitLoops += 1

    kill9 = waitLoops == max_loops
    if kill9:
        logger.warning('Killing %s by brute force after waiting %.2f [s], BANG! :-(', proc.pid, timeout)
        proc.kill()
    proc.wait()

def object_tracking(name):
    """
    Returns a decorator that helps classes track which object is currently under
    execution. This is done via a thread local object, which can be accessed via
    the 'tlocal' attribute of the returned decorator.
    """

    current_object = threading.local()
    def track_current_drop(f):

        @functools.wraps(f)
        def _wrapper(*args, **kwargs):
            try:
                previous = getattr(current_object, name)
            except AttributeError:
                previous = None

            setattr(current_object, name, args[0])
            try:
                return f(*args, **kwargs)
            finally:
                setattr(current_object, name, previous)
        return _wrapper

    track_current_drop.tlocal = current_object
    return track_current_drop

class ZlibCompressedStream(object):
    """
    An object that takes a input of uncompressed stream and returns a compressed version of its
    contents when .read() is read.
    """

    def __init__(self, content):
        self.content = content
        self.compressor = zlib.compressobj()
        self.buf = []
        self.buflen = 0
        self.exhausted = False

    def readall(self):

        if not self.compressor:
            return b''

        content = self.content
        response = []
        compressor = self.compressor

        blocksize = 8192
        uncompressed = content.read(blocksize)
        while True:
            if not uncompressed:
                break
            compressed = compressor.compress(uncompressed)
            response.append(compressed)
            uncompressed = content.read(blocksize)

        response.append(compressor.flush())
        self.compressor = None
        return b''.join(response)

    def read(self, n=-1):

        if n <= 0:
            return self.readall()

        if self.buflen >= n:
            data = b''.join(self.buf)
            self.buf = [data[n:]]
            self.buflen -= n;
            return data[:n]

        # Dump contents of previous buffer
        response = []
        written = 0
        if self.buflen:
            written += self.buflen
            data = b''.join(self.buf)
            response.append(data)
            self.buf = []
            self.buflen = 0

        compressor = self.compressor
        if not compressor:
            return b''.join(response)

        while True:

            decompressed = self.content.read(n)
            if not decompressed:
                compressed = compressor.flush()
                compressor = self.compressor = None
            else:
                compressed = compressor.compress(decompressed)

            if compressed:
                size = len(compressed)

                # If we have more compressed bytes than we need we write only
                # those needed to get us to n.
                to_write = min(n - written, size)
                if to_write:
                    response.append(compressed[:to_write])
                    written += to_write

                # The rest of the unwritten compressed bytes go into our internal
                # buffer
                if written == n:
                    self.buf.append(compressed[to_write:])
                    self.buflen += size - to_write

            if written == n or not compressor:
                break

        return b''.join(response)

class ZlibUncompressedStream(object):
    """
    A class that reads gzip-compressed content and returns uncompressed content
    each time its read() method is called.
    """

    def __init__(self, content):
        self.content = content
        self.decompressor = zlib.decompressobj()
        self.buf = []
        self.buflen = 0

    def readall(self):

        if not self.decompressor:
            return b''

        content = self.content
        response = six.BytesIO()
        decompressor = self.decompressor

        blocksize = 8192
        compressed = content.read(blocksize)
        to_decompress = decompressor.unconsumed_tail + compressed

        while True:
            decompressed = decompressor.decompress(to_decompress)
            if not decompressed:
                break
            response.write(decompressed)
            to_decompress = decompressor.unconsumed_tail + content.read(blocksize)

        response.write(decompressor.flush())
        self.decompressor = None
        return response.getvalue()

    def read(self, n=-1):

        if n == -1:
            return self.readall()

        # The buffer has still enough data
        if self.buflen >= n:
            data = b''.join(self.buf)
            self.buf = [data[n:]]
            self.buflen -= n;
            return data[:n]

        response = []

        # Dump contents of previous buffer
        written = 0
        if self.buflen:
            data = b''.join(self.buf)
            written += self.buflen
            response.append(data)
            self.buf = []
            self.buflen = 0

        decompressor = self.decompressor
        if not decompressor:
            return b''.join(response)

        while True:

            # We hope that reading n compressed bytes will yield n uncompressed
            # bytes at least; we loop anyway until we read n uncompressed bytes
            compressed = self.content.read(n)
            to_decompress = decompressor.unconsumed_tail + compressed
            decompressed = decompressor.decompress(to_decompress)

            # we've exhausted both, there's nothing else to read/decompress
            if not compressed and not decompressed:
                decompressed = decompressor.flush()
                decompressor = self.decompressor = None

            if decompressed:
                size = len(decompressed)

                # If we have more decompressed bytes than we need we write only
                # those needed to get us to n.
                to_write = min(n - written, size)
                if to_write:
                    response.append(decompressed[:to_write])
                    written += to_write

                # The rest of the unwritten decompressed bytes go into our internal
                # buffer
                if written == n:
                    self.buf.append(decompressed[to_write:])
                    self.buflen += size - to_write

            if written == n or decompressor is None:
                break

        return b''.join(response)

class JSONStream(object):

    def __init__(self, objects):
        if isinstance(objects, (list, tuple, types.GeneratorType)):
            self.objects = enumerate(objects)
            self.isiter = True
        else:
            self.objects = objects
            self.isiter = False

        self.buf = []
        self.buflen = 0
        self.nreads = 0

    def read(self, n=-1):

        if n == -1:
            raise ValueError("n must be positive")

        if self.buflen >= n:
            self.buflen -= n;
            data = b''.join(self.buf)
            self.buf = [data[n:]]
            return data[:n]

        written = 0
        response = []

        # Dump contents of previous buffer
        if self.buflen:
            data = b''.join(self.buf)
            written += self.buflen
            response.append(data)
            self.buf = []
            self.buflen = 0

        if self.nreads and not self.isiter:
            return b''.join(response)
        self.nreads += 1

        while True:

            if self.isiter:
                try:
                    i,obj = next(self.objects)
                    json_out = b'[' if i == 0 else b','
                    json_out += json.dumps(obj).encode('latin1')
                except StopIteration:
                    json_out = b']'
                    self.isiter = False # not nice, but prevents more reads
            else:
                json_out = json.dumps(self.objects).encode('latin1')

            if json_out:
                size = len(json_out)

                # If we have more decompressed bytes than we need we write only
                # those needed to get us to n.
                to_write = min(n - written, size)
                if to_write:
                    response.append(json_out[0:to_write])
                    written += to_write

                # The rest of the unwritten decompressed bytes go into our internal
                # buffer
                if written == n:
                    self.buf.append(json_out[to_write:])
                    self.buflen += size - to_write

            if written == n:
                break

            if not self.isiter:
                break

        return b''.join(response)