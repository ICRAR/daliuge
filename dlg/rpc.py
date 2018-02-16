#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2017
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
RPC support for DALiuGE

This module contains all client and server RPC classes for all the different
technologies we support.
"""

import collections
import importlib
import logging
import os
import threading
import time

from six.moves import queue as Queue  # @UnresolvedImport

from . import utils
from .exceptions import DaliugeException


logger = logging.getLogger(__name__)

class RPCObject(object):
    """Base class for all RCP clients and server"""
    def start(self):
        self.rpc_running = True
    def shutdown(self):
        self.rpc_running = False

class RPCClientBase(RPCObject):
    """Base class for all RPC clients"""

    def get_drop_attribute(self, hostname, port, session_id, uid, name):

        logger.debug("Getting attribute %s for drop %s of session %s at %s:%d", name, uid, session_id, hostname, port)

        client, closer = self.get_rpc_client(hostname, port)

        # The remote method receives the same client used to inspect the remote
        # object, and it closes it when the method is not used anymore
        class remote_method(object):
            def __del__(self):
                closer()
            def __call__(self, *args):
                return client.call_drop(session_id, uid, name, *args)

        # Shortcut to avoid extra calls
        known_methods = ()
        #known_methods = ('open', 'read', 'write', 'close')
        closeit = False
        try:
            if name in known_methods or client.has_method(session_id, uid, name):
                return remote_method()
            closeit = True
            return client.get_drop_property(session_id, uid, name)
        finally:
            if closeit:
                closer()

class RPCServerBase(RPCObject):
    """Base class for all RPC server"""
    def __init__(self, host, port):
        self._rpc_host = host
        self._rpc_port = port


class ZeroRPCClient(RPCClientBase):
    """ZeroRPC client support"""

    request = collections.namedtuple('request', 'method args queue')

    def start(self):
        super(ZeroRPCClient, self).start()

        # One per remote host
        self._zrpcclient_acquisition_lock = threading.Lock()
        self._zrpcclients = {}
        self._zrpcclientthreads = []

    def shutdown(self):
        super(ZeroRPCClient, self).shutdown()
        for t in self._zrpcclientthreads:
            t.join(10)
            if t.is_alive():
                logger.warning("ZeroRPC client thread %s is still alive", t.name)

    def get_client_for_endpoint(self, host, port):

        endpoint = (host, port)

        with self._zrpcclient_acquisition_lock:
            if endpoint in self._zrpcclients:
                return self._zrpcclients[endpoint]

            # We start the new client on its own thread so it uses gevent, etc.
            # In this thread we create simply enqueue requests
            req_queue = Queue.Queue()
            tname_tpl, args = "zrpc(%s:%d)", (host, port)
            t = threading.Thread(target=self.run_zrpcclient, args=(host, port, req_queue),
                                 name=tname_tpl % args)
            t.start()

            class QueueingClient(object):
                def __make_call(self, method, *args):
                    res_queue = Queue.Queue()
                    req_queue.put(ZeroRPCClient.request(method, args, res_queue))
                    return res_queue.get()
                def call_drop(self, session_id, uid, name, *args):
                    return self.__make_call('call_drop', session_id, uid, name, *args)
                def get_drop_property(self, session_id, uid, name):
                    return self.__make_call('get_drop_property', session_id, uid, name)
                def has_method(self, session_id, uid, name):
                    return self.__make_call('has_method', session_id, uid, name)

            client = QueueingClient()
            self._zrpcclients[endpoint] = client
            self._zrpcclientthreads.append(t)
            return client

    def run_zrpcclient(self, host, port, req_queue):
        import gevent

        # Each client uses a different Context; otherwise they all share
        # the same Context.instance() which is global to the process,
        # and generates the same channel IDs, confusing the server
        import zerorpc
        ctx = zerorpc.Context()
        client = zerorpc.Client("tcp://%s:%d" % (host,port), context=ctx)

        forwarder = gevent.spawn(self.forward_requests, req_queue, client)
        gevent.joinall([forwarder])

        logger.info("Closing %s:%d ZeroRPC client", host, port)
        client.close()
        ctx.destroy()

    def forward_requests(self, req_queue, client):
        import gevent
        while self.rpc_running:
            try:
                req = req_queue.get_nowait()
                gevent.spawn(self.queue_request, client, req)
            except Queue.Empty:
                gevent.sleep(0.005)

    def queue_request(self, client, req):
        async_result = client.__call__(req.method, *req.args, async=True)
        async_result.rawlink(lambda x: req.queue.put(x.value))

    def get_rpc_client(self, hostname, port):
        client = self.get_client_for_endpoint(hostname, port)
        # No closing function since clients are long-lived
        return client, lambda: None

class ZeroRPCServer(RPCServerBase):
    """ZeroRPC server support"""

    def start(self):
        super(ZeroRPCServer, self).start()

        # Starts the single-threaded ZeroRPC server for RPC requests
        timeout = 30
        server_started = threading.Event()
        self._zrpcserverthread = threading.Thread(target=self.run_zrpcserver, name="ZeroRPC server", args=(self._rpc_host, self._rpc_port, server_started))
        self._zrpcserverthread.start()
        if not server_started.wait(timeout):
            raise Exception("ZeroRPC server didn't start within %d seconds" % (timeout,))

    def run_zrpcserver(self, host, port, server_started):

        # temporarily timing import statements to check FS times on HPC environs
        start = time.time()
        import gevent
        import zerorpc
        logger.info("Importing of gevent and zerorpc took %.3f seconds", time.time() - start)

        # Use a specific context; otherwise multiple servers on the same process
        # (only during tests) share the same Context.instance() which is global
        # to the process
        ctx = zerorpc.Context()
        self._zrpcserver = zerorpc.Server(self, context=ctx)
        # zmq needs an address, not a hostname
        endpoint = "tcp://%s:%d" % (utils.zmq_safe(host), port,)
        self._zrpcserver.bind(endpoint)
        logger.info("Listening for RPC requests via ZeroRPC on %s", endpoint)
        server_started.set()

        runner = gevent.spawn(self._zrpcserver.run)
        stopper = gevent.spawn(self.stop_zrpcserver)
        gevent.joinall([runner, stopper])
        ctx.destroy()

    def stop_zrpcserver(self):
        import gevent
        while self.rpc_running:
            gevent.sleep(0.01)
        logger.info("Closing ZeroRPC server on tcp://%s:%d", utils.zmq_safe(self._rpc_host), self._rpc_port)
        self._zrpcserver.close()

    def shutdown(self):
        super(ZeroRPCServer, self).shutdown()
        self._zrpcserverthread.join()


class RPyCClient(RPCClientBase): # pragma: no cover
    """RPyC client support"""

    def get_rpc_client(self, hostname, port):
        import rpyc
        client = rpyc.connect(hostname, port)
        return client.root, client.close

class RPyCServer(RPCServerBase): # pragma: no cover
    """RPyC server support"""

    def start(self):
        super(RPyCServer, self).start()

        import rpyc
        from rpyc.utils.server import ThreadedServer

        nm = self
        class NMService(rpyc.Service):
            def exposed_call_drop(self, session_id, uid, name, *args):
                return nm.call_drop(session_id, uid, name, *args)
            def exposed_get_drop_property(self, session_id, uid, name):
                return nm.get_drop_attribute(session_id, uid, name)
            def exposed_has_method(self, session_id, uid, name):
                return nm.has_method(session_id, uid, name)

        self._rpycserver = ThreadedServer(NMService, hostname=self._rpc_host, port=self._rpc_port) # ThreadPoolServer

        # Starts the single-threaded RPyC server for RPC requests
        self._rpycserverthread = threading.Thread(target=self._rpycserver.start, name="RPyC server")
        self._rpycserverthread.start()
        logger.info("Listening for RPC requests via RPyC on %s:%d", self._rpc_host, self._rpc_port)

    def shutdown(self):
        super(RPyCServer, self).shutdown()
        self._rpycserver.close()
        self._rpycserverthread.join()

class PyroRPCClient(RPCClientBase): # pragma: no cover
    """Pyro client support"""

    def get_rpc_client(self, hostname, port):
        import Pyro4
        uri = Pyro4.URI("PYRO:node_manager@%s:%d" % (hostname, port))
        proxy = Pyro4.Proxy(uri)
        return proxy, proxy._pyroRelease

class PyroRPCServer(RPCServerBase): # pragma: no cover
    """Base Pyro server support. Subclasses support different server types"""

    def start(self):

        import Pyro4

        super(PyroRPCServer, self).start()

        # Starts the single-threaded Pyro server for RPC requests
        logger.info("Listening for RPC requests via Pyro on %s:%d", self._rpc_host, self._rpc_port)
        self.setup_pyro()
        self._pyrodaemon = Pyro4.Daemon(self._rpc_host, self._rpc_port)
        self._pyrodaemon.register(self, "node_manager")
        self._pyroserverthread = threading.Thread(target=self._pyrodaemon.requestLoop, name="PyroRPC server")
        self._pyroserverthread.start()

    def shutdown(self):
        timeout = 5
        super(PyroRPCServer, self).shutdown()
        self._pyrodaemon.shutdown()
        self._pyroserverthread.join(timeout)
        host = 'localhost' if self._rpc_host == '0.0.0.0' else self._rpc_host
        if not utils.portIsClosed(host, self._rpc_port, timeout):
            logger.warning("Pyro RPC port %d is still open after %d seconds", timeout)

    def setup_pyro(self):
        """
        Sets up Pyro configuration items.

        Pyro >= 4.20 uses the 'serpent' serializer by default. In this serializer
        "most custom classes aren't dealt with automatically" [1], including our
        Event class. Thus, in order to support passing events via Pyro we need to
        either instruct Pyro how to serialize the Event class, or to use the
        'pickle' serializer.

        We used to choose to add explicit support for the Event class and keep
        using the 'serpent' serializer. Although more complex, it's in theory safer
        (see [1] again). Once we started supporting python 3 we found that the
        serpent serializer wasn't working correctly, most probably (but not totally
        proven) having troubles with the bytes/bytearray data types. This forced us
        to move back to the pickle serializer, which seems to perform better anyway.
        We leave the previous serpent-based configuration as a reference in case we
        want to revert to it.

        In Pyro >= 4.46 the REQUIRE_EXPOSE configuration flag was defaulted to True.
        Instead of embracing it (which would require us to change all our drop
        classes and decorate them with @expose) we change the flag back to False.

        [1] https://pythonhosted.org/Pyro4/clientcode.html#serialization
        """

        import Pyro4

        def setup_serpent():

            from .event import Event

            def __pyro4_class_to_dict(o):
                d = {'__class__' : o.__class__.__name__, '__module__': o.__class__.__module__}
                d.update(o.__dict__)
                return d

            def __pyro4_dict_to_class(classname, d):
                modname = d['__module__']
                module = importlib.import_module(modname)
                clazz = getattr(module, classname)
                o = clazz()
                for k in d:
                    if k in ['__class__', '__module__']: continue
                    setattr(o, k, d[k])
                return o

            Pyro4.util.SerializerBase.register_class_to_dict(Event, __pyro4_class_to_dict)
            Pyro4.util.SerializerBase.register_dict_to_class('Event', __pyro4_dict_to_class)

        def setup_pickle():
            Pyro4.config.SERIALIZER = 'pickle'
            Pyro4.config.SERIALIZERS_ACCEPTED = ['pickle']

        # We could also do one or the other depending on the major version of python
        #setup_serpent()
        setup_pickle()

        # In Pyro4 >= 4.46 the default for this option changed to True, which would
        # mean we need to decorate all our classes with Pyro-specific code.
        # We don't want that, and thus we restore the old "everything is exposed"
        # behavior.
        Pyro4.config.REQUIRE_EXPOSE = False

        # A final thing: we use a default timeout of 60 [s], which should be more
        # than enough
        Pyro4.config.COMMTIMEOUT = 60

class MultiplexPyroRPCServer(PyroRPCServer): # pragma: no cover
    """Pyro server support with mutliplex server type"""

    def setup_pyro(self):
        super(MultiplexPyroRPCServer, self).setup_pyro()
        import Pyro4
        Pyro4.config.SERVERTYPE = 'multiplex'

class ThreadedPyroRPCServer(PyroRPCServer): # pragma: no cover
    """Pyro server support with multithreaded server type"""

    def setup_pyro(self):
        super(ThreadedPyroRPCServer, self).setup_pyro()
        import Pyro4
        Pyro4.config.SERVERTYPE = 'thread'
        Pyro4.config.THREADPOOL_SIZE = 16
        if hasattr(Pyro4.config, 'THREADPOOL_ALLOW_QUEUE'):
            Pyro4.config.THREADPOOL_ALLOW_QUEUE = False

# Check which rpc backend should be exposed
rpc_lib = os.environ.get('DALIUGE_RPC', 'zerorpc')
if rpc_lib in ('pyro', 'pyro-multiplex'): # pragma: no cover
    RPCServer, RPCClient = MultiplexPyroRPCServer, PyroRPCClient
elif rpc_lib == 'pyro-threaded': # pragma: no cover
    RPCServer, RPCClient = ThreadedPyroRPCServer, PyroRPCClient
elif rpc_lib == 'zerorpc':
    RPCServer, RPCClient = ZeroRPCServer, ZeroRPCClient
elif rpc_lib == 'rpyc': # pragma: no cover
    RPCServer, RPCClient = RPyCServer, RPyCClient
else: # pragma: no cover
    raise DaliugeException("Unknown RPC lib %s, use one of pyro, pyro-multiplex, pyro-threaded, zerorpc, rpyc" % (rpc_lib,))

class DropProxy(object):
    """
    A proxy to a remote drop.

    It forwards attribute requests and procedure calls through the given RPC client.
    """

    def __init__(self, rpc_client, hostname, port, sessionId, uid):
        self.rpc_client = rpc_client
        self.hostname = hostname
        self.port = port
        self.session_id = sessionId
        self.uid = uid
        logger.debug("Created %r", self)

    def handleEvent(self, evt):
        pass

    def __getattr__(self, name):
        if name == 'uid':
            return self.uid
        elif name in ('inputs', 'streamingInputs', 'outputs', 'consumers', 'producers'):
            return []
        return self.rpc_client.get_drop_attribute(self.hostname, self.port, self.session_id, self.uid, name)

    def __repr__(self):
        return '<DropProxy %s, session %s @%s:%d>' % (self.uid, self.session_id, self.hostname, self.port)