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
import base64
import dill
import errno
import functools
import importlib
import io
import logging
import os
import signal
import socket
import threading
import time
from typing import Tuple
import zlib
import re
import grp
import pwd

import netifaces

from . import common

logger = logging.getLogger(f"dlg.{__name__}")


def timed_import(module_name):
    """Imports `module_name` and log how long it took to import it"""
    start = time.time()
    module = importlib.import_module(module_name)
    logger.info("Imported %s in %.3f seconds", module_name, time.time() - start)
    return module


def get_local_ip_addr():
    """
    Enumerate all interfaces and return bound IP addresses (exclude localhost)
    """
    PROTO = netifaces.AF_INET
    ifaces = netifaces.interfaces()
    if_addrs = [(netifaces.ifaddresses(iface), iface) for iface in ifaces]
    if_inet_addrs = [(tup[0][PROTO], tup[1]) for tup in if_addrs if PROTO in tup[0]]
    iface_addrs = [
        (s["addr"], tup[1])
        for tup in if_inet_addrs
        for s in tup[0]
        if "addr" in s and not s["addr"].startswith("127.")
    ]
    return iface_addrs


def get_all_ipv4_addresses():
    """Get a list of all IPv4 interfaces found in this computer"""
    proto = netifaces.AF_INET
    return [
        addr["addr"]
        for iface in netifaces.interfaces()
        for iface_proto, addrs in netifaces.ifaddresses(iface).items()
        if proto == iface_proto
        for addr in addrs
        if "addr" in addr
    ]


def register_service(zc, service_type_name, service_name, ipaddr, port, protocol="tcp"):
    """
    ZeroConf: Register service type, protocol, ipaddr and port

    Returns ZeroConf object and ServiceInfo object
    """
    import zeroconf

    sn = service_name if len(service_name) <= 15 else service_name[:15]
    stn = "_{0}._{1}.local.".format(service_type_name, protocol)
    sn = "{0}.{1}".format(sn, stn)

    # "addresses" deprecates "address" in 0.23+
    address = socket.inet_aton(ipaddr)
    kwargs = {}
    if tuple(map(int, zeroconf.__version__.split(".")))[:2] >= (0, 23):
        kwargs["addresses"] = [address]
    else:
        kwargs["address"] = address
    info = zeroconf.ServiceInfo(stn, sn, port=port, properties={}, **kwargs)
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

    stn = "_{0}._{1}.local.".format(service_type_name, protocol)
    browser = ServiceBrowser(zc, stn, handlers=[callback])
    return browser


def zmq_safe(host_or_addr):
    """Converts `host_or_addr` to a format that is safe for ZMQ to use"""

    # The catch-all IP address, ZMQ needs a *
    if host_or_addr == "0.0.0.0":
        return "*"
    host_or_addr = host_or_addr.split(":")[0]
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
    if host != "0.0.0.0":
        return socket.gethostbyname(host)

    # host was "all interfaces", select one based on preference
    # if only one interface is found we assume it's a loopback interface
    addresses = get_all_ipv4_addresses()
    if prefer_local or len(addresses) == 1:
        return "localhost"

    # Choose the first non-localhost one
    for a in addresses:
        if not a.startswith("127."):
            return a

    # All addresses were loopbacks! let's return the last one
    raise addresses[-1] if addresses else ''


def getDlgDir():
    """
    Returns the root of the directory structure used by the DALiuGE framework at
    runtime.
    """
    if "DLG_ROOT" in os.environ:
        path = os.environ["DLG_ROOT"]
    else:
        path = os.path.join(os.path.expanduser("~"), "dlg")
        os.environ["DLG_ROOT"] = path
    logger.debug("DLG_ROOT directory is %s", path)
    return path


def getDlgPidDir():
    """
    Returns the location of the directory used by the DALiuGE framework to store
    its PIDs. If `createIfMissing` is True, the directory will be created if it
    currently doesn't exist
    """
    return os.path.join(getDlgDir(), "pid")


def getDlgLogsDir():
    """
    Returns the location of the directory used by the DALiuGE framework to store
    its logs. If `createIfMissing` is True, the directory will be created if it
    currently doesn't exist
    """
    return os.path.join(getDlgDir(), "logs")


def getDlgWorkDir():
    """
    Returns the location of the directory used by the DALiuGE framework to store
    results. If `createIfMissing` is True, the directory will be created if it
    currently doesn't exist
    """
    return os.path.join(getDlgDir(), "workspace")


def getDlgPath():
    """
    Returns the location of the directory used by the DALiuGE framework to look
    for additional code. If `createIfMissing` is True, the directory will be created if it
    currently doesn't exist
    """
    return os.path.join(getDlgDir(), "code")


def getDlgVariable(key: str):
    """
    Queries environment for variables assumed to start with 'DLG_'.
    Special case for DLG_ROOT, since this is easily identifiable.
    """
    if key == "$DLG_ROOT":
        return getDlgDir()
    value = os.environ.get(key[1:])
    if value is None:
        return key
    return value


def createDirIfMissing(path):
    """
    Creates the given directory if it doesn't exist
    """
    try:
        os.makedirs(path)
        logger.debug("created path %s", path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def isabs(path):
    """Like os.path.isabs, but handles None"""
    return path and os.path.isabs(path)


def fname_to_pipname(fname):
    """
    Converts a graph filename (extension .json or .graph) to its "pipeline"
    name (the basename without the extension).
    """
    fname = os.path.split(fname)[-1]
    m = re.compile(r".json$|.graph$")
    res = m.search(fname)
    logger.info("regex result %s", res)

    if res:
        fname = fname[: res.start()]
    return fname


def escapeQuotes(s, singleQuotes=True, doubleQuotes=True):
    """
    Escapes single and double quotes in a string. Useful to include commands
    in a shell invocation or similar.
    """
    if singleQuotes:
        s = s.replace("'", "'\\''")
    if doubleQuotes:
        s = s.replace('"', '\\"')
    return s


def prepare_sql(sql, paramstyle, data=()) -> Tuple[str, dict]:
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
        return (sql, {})

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

    logger.debug("Generating %d markers with paramstyle = %s", n, paramstyle)

    if paramstyle == "qmark":
        markers = ["?" for i in range(n)]
    elif paramstyle == "numeric":
        markers = [":%d" % (i) for i in range(n)]
    elif paramstyle == "named":
        markers = [":n%d" % (i) for i in range(n)]
    elif paramstyle == "format":
        markers = [":%s" for i in range(n)]
    elif paramstyle == "pyformat":
        markers = [":%%(n%d)s" % (i) for i in range(n)]
    else:
        raise Exception("Unknown paramstyle: %s" % (paramstyle))

    sql = sql.format(*markers)

    if paramstyle in ["format", "pyformat"]:
        data = {"n%d" % (i): d for i, d in enumerate(data)}

    return (sql, data)


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
                if "self" in kwargs:
                    kwargs.pop("self")
                return f(*args, **kwargs)
            finally:
                setattr(current_object, name, previous)

        return _wrapper

    track_current_drop.tlocal = current_object
    return track_current_drop


def get_symbol(name):
    """Gets the global symbol ``name``, which is an "absolute path" to a python
    name in the form of ``pkg.subpkg.subpkg.module.name``"""
    parts = name.split(".")
    module = importlib.import_module(".".join(parts[:-1]))
    return getattr(module, parts[-1])


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
            return b""

        content = self.content
        response = io.BytesIO()
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
            data = b"".join(self.buf)
            self.buf = [data[n:]]
            self.buflen -= n
            return data[:n]

        response = []

        # Dump contents of previous buffer
        written = 0
        if self.buflen:
            data = b"".join(self.buf)
            written += self.buflen
            response.append(data)
            self.buf = []
            self.buflen = 0

        decompressor = self.decompressor
        if not decompressor:
            return b"".join(response)

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

        return b"".join(response)


class ExistingProcess(object):
    """A Popen-like class around an existing process"""

    def __init__(self, pid):
        """Creates a new ExistingProcess for the given pid"""
        self.pid = pid
        self.finished = False

    def poll(self):
        """Returns an exit status if the process finished, None if it exists"""
        try:
            os.kill(self.pid, signal.SIGCONT)
            return None
        except OSError as e:
            if e.errno == errno.ESRCH:
                self.finished = True
                return 0

    def terminate(self):
        """Send a TERM signal"""
        os.kill(self.pid, signal.SIGTERM)

    def kill(self):
        """Send a KILL signal"""
        os.kill(self.pid, signal.SIGKILL)

    def wait(self):
        """Wait until the process finishes"""
        if self.finished:
            return
        while self.poll() == None:
            time.sleep(0.1)


def prepareUser(DLG_ROOT=getDlgDir()):
    workdir = f"{DLG_ROOT}/workspace/settings"
    try:
        os.makedirs(workdir, exist_ok=True)
    except Exception as e:
        logger.debug("prepareUser has failed")
        raise e
    template_dir = os.path.dirname(__file__)
    # get current user info
    pw = pwd.getpwuid(os.getuid())
    gr = grp.getgrgid(pw.pw_gid)
    dgr = grp.getgrnam("docker")
    with open(os.path.join(workdir, "passwd"), "wt") as file:
        file.write(open(os.path.join(template_dir, "passwd.template"), "rt").read())
        file.write(
            f"{pw.pw_name}:x:{pw.pw_uid}:{pw.pw_gid}:{pw.pw_gecos}:{DLG_ROOT}:/bin/bash\n"
        )
        logger.debug("passwd file written %s", file.name)
    with open(os.path.join(workdir, "group"), "wt") as file:
        file.write(open(os.path.join(template_dir, "group.template"), "rt").read())
        file.write(f"{gr.gr_name}:x:{gr.gr_gid}:\n")
        file.write(f"docker:x:{dgr.gr_gid}\n")
        logger.debug("Group file written %s", file.name)

    return dgr.gr_gid


def serialize_data(d):
    # return dill.dumps(d)
    return b2s(base64.b64encode(dill.dumps(d)))


def deserialize_data(d):
    # return dill.loads(d)
    return dill.loads(base64.b64decode(d.encode("utf8")))


def truncateUidToKey(uid: str) -> str:
    """
    Given a UID of a Drop, generate a human-readable Uid that can be
    used for easier visualisation and file navigation.

    When submitting a graph through the UI, we add the "humanReadableKey" to
    the dropSpec. However, this key is not always available (i.e. in the instance we
    submit via the command line, or use tests). We need an alternative default
    that does not run the risk of being duplicated, which can lead to over-writes when
    generating files based on Drop UIDs.

    :params: uid, the Drop UID either generated or provided at runtime.

    Notes
    -----
    The heuristic for generating truncated UID is as follows:

    - Split on "_", which is used to separate the Date of the UID in standard Drops.
    - If there are no "_" in the UID, then we default to just using the UID.
    - If there is an "_" in the UID, we
        - Take the second element of the resulting split
        - If the length is less than the readableLengthlimit, we use the whole value
        - If the length is great than the readableLengthLimit, we use up to that value
        - if there are two "_" we also append everything including the second "_" to the result

    This assumes that the UID does not contain a "_" character.

    Examples
    --------
    >>> truncated("A") # "A"

    >>> truncated("2022-02-11T08:05:47_-1_0") # -1

    >>> truncated('2024-10-30T12:01:57_0140555b-8c23-4d6a-9e24-e16c15555e8c_0') # 0140_0
    """
    truncatedUid = uid
    readableLengthLimit = 4
    split = uid.split("_", 2)
    if len(split) > 1:
        second_el = str(split[1])
        if len(second_el) > readableLengthLimit:
            truncatedUid = split[1][:readableLengthLimit]
        else:
            truncatedUid = split[1]
        if len(split) == 3:
            truncatedUid = f"{truncatedUid}_{split[-1]}"  # add the rest of the original

    return truncatedUid


# Backwards compatibility
terminate_or_kill = common.osutils.terminate_or_kill
wait_or_kill = common.osutils.wait_or_kill
b2s = common.b2s

check_port = common.check_port
connect_to = common.connect_to
portIsClosed = common.portIsClosed
portIsOpen = common.portIsOpen
write_to = common.write_to

JSONStream = common.JSONStream
ZlibCompressedStream = common.ZlibCompressedStream
