#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2014
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
Module containing the NodeManager, which directly manages DROP instances, and
thus represents the bottom of the DROP management hierarchy.
"""

import abc
import collections
import importlib
import logging
import multiprocessing.pool
import os
import socket
import sys
import threading
import time

import six
from six.moves import queue as Queue  # @UnresolvedImport

from . import constants
from .drop_manager import DROPManager
from .session import Session
from .. import utils
from ..drop import AppDROP
from ..exceptions import NoSessionException, SessionAlreadyExistsException,\
    DaliugeException
from ..lifecycle.dlm import DataLifecycleManager


logger = logging.getLogger(__name__)

class NMDropEventListener(object):

    def __init__(self, nm, session_id):
        self._nm = nm
        self._session_id = session_id

    def handleEvent(self, event):
        event.session_id = self._session_id
        self._nm.publish_event(event)

class LogEvtListener(object):
    def handleEvent(self, event):
        if event.type == 'status':
            logger.debug('Drop uid=%s, oid=%s changed to state %s', event.uid, event.oid, event.status)
        elif event.type == 'execStatus':
            logger.debug('AppDrop uid=%s, oid=%s changed to execState %s', event.uid, event.oid, event.execStatus)

class NodeManagerBase(DROPManager):
    """
    Base class for a DROPManager that creates and holds references to DROPs.

    A NodeManagerBase is the ultimate responsible of handling DROPs. It does so not
    directly, but via Sessions, which represent and encapsulate separate,
    independent DROP graph executions. All DROPs created by the
    different Sessions are also given to a common DataLifecycleManager, which
    takes care of expiring them when needed and replicating them.

    Since a NodeManagerBase can handle more than one session, in principle only one
    NodeManagerBase is needed for each computing node, thus its name.
    """

    __metaclass__ = abc.ABCMeta

    def __init__(self,
                 useDLM=True,
                 dlgPath=None,
                 host=None,
                 error_listener=None,
                 enable_luigi=False,
                 events_port = constants.NODE_DEFAULT_EVENTS_PORT,
                 rpc_port = constants.NODE_DEFAULT_RPC_PORT,
                 max_threads = 0):

        self._dlm = DataLifecycleManager() if useDLM else None
        self._host = host or 'localhost'
        self._events_port = events_port
        self._rpc_port = rpc_port
        self._sessions = {}

        # dlgPath contains code added by the user with possible
        # DROP applications
        if dlgPath:
            dlgPath = os.path.expanduser(dlgPath)
            if os.path.isdir(dlgPath):
                logger.info("Adding %s to the system path", dlgPath)
                sys.path.append(dlgPath)

        # Error listener used by users to deal with errors coming from specific
        # Drops in whatever way they want
        if error_listener:
            if isinstance(error_listener, six.string_types):
                try:
                    parts   = error_listener.split('.')
                    module  = importlib.import_module('.'.join(parts[:-1]))
                except:
                    logger.exception('Creating the error listener')
                    raise
                error_listener = getattr(module, parts[-1])()
            if not hasattr(error_listener, 'on_error'):
                raise ValueError("error_listener doesn't contain an on_error method")
        self._error_listener = error_listener

        self._enable_luigi = enable_luigi

        # Start our thread pool
        if max_threads == 0:
            self._threadpool = None
        else:
            max_threads = max(min(max_threads, 200), 1)
            logger.info("Initializing thread pool with %d threads", max_threads)
            self._threadpool = multiprocessing.pool.ThreadPool(processes=max_threads)

        # Event handler that only logs status changes
        debugging = logger.isEnabledFor(logging.DEBUG)
        self._logging_event_listener = LogEvtListener() if debugging else None

        # Start the mix-ins
        self.start()

    @abc.abstractmethod
    def start(self):
        """
        Starts any background task required by this Node Manager
        """

    @abc.abstractmethod
    def shutdown(self):
        """
        Stops any pending background task run by this Node Manager
        """

    @abc.abstractmethod
    def subscribe(self, host, port):
        """
        Subscribes this Node Manager to events published in from ``host``:``port``
        """

    @abc.abstractmethod
    def publish_event(self, evt):
        """
        Publishes the event ``evt`` for other Node Managers to receive it
        """

    @abc.abstractmethod
    def get_rpc_client(self, hostname, port):
        """
        Creates an RPC client connected to the node manager running in
        ``host``:``port``, and its closing method, as a 2-tuple.
        """

    def deliver_event(self, evt):
        """
        Method called by subclasses when a new event has arrived through the
        subscription mechanism.
        """
        if not evt.session_id in self._sessions:
            logger.warning("No session %s found, event will be dropped" % (evt.session_id))
            return
        self._sessions[evt.session_id].deliver_event(evt)

    def _check_session_id(self, session_id):
        if session_id not in self._sessions:
            raise NoSessionException(session_id)

    def createSession(self, sessionId):
        if sessionId in self._sessions:
            raise SessionAlreadyExistsException(sessionId)
        self._sessions[sessionId] = Session(sessionId, self._host, self._error_listener, self._enable_luigi)
        logger.info('Created session %s', sessionId)

    def getSessionStatus(self, sessionId):
        self._check_session_id(sessionId)
        return self._sessions[sessionId].status

    def linkGraphParts(self, sessionId, lhOID, rhOID, linkType):
        self._check_session_id(sessionId)
        self._sessions[sessionId].linkGraphParts(lhOID, rhOID, linkType)

    def addGraphSpec(self, sessionId, graphSpec):
        self._check_session_id(sessionId)
        self._sessions[sessionId].addGraphSpec(graphSpec)

    def getGraphStatus(self, sessionId):
        self._check_session_id(sessionId)
        return self._sessions[sessionId].getGraphStatus()

    def getGraph(self, sessionId):
        self._check_session_id(sessionId)
        return self._sessions[sessionId].getGraph()

    def deploySession(self, sessionId, completedDrops=[]):
        self._check_session_id(sessionId)
        session = self._sessions[sessionId]

        def foreach(drop):
            if self._threadpool is not None:
                drop._tp = self._threadpool
            if self._dlm:
                self._dlm.addDrop(drop)

            # Remote event forwarding
            evt_listener = NMDropEventListener(self, sessionId)
            if isinstance(drop, AppDROP):
                drop.subscribe(evt_listener, 'producerFinished')
            else:
                drop.subscribe(evt_listener, 'dropCompleted')

            # Purely for logging purposes
            log_evt_listener = self._logging_event_listener
            if log_evt_listener:
                drop.subscribe(log_evt_listener, 'status')
                if isinstance(drop, AppDROP):
                    drop.subscribe(log_evt_listener, 'execStatus')

        session.deploy(completedDrops=completedDrops, foreach=foreach)

    def destroySession(self, sessionId):
        self._check_session_id(sessionId)
        session = self._sessions.pop(sessionId)
        session.destroy()

    def getSessionIds(self):
        return list(self._sessions.keys())

    def getGraphSize(self, sessionId):
        self._check_session_id(sessionId)
        session = self._sessions[sessionId]
        return len(session._graph)

    def trigger_drops(self, sessionId, uids):
        self._check_session_id(sessionId)
        t = threading.Thread(target=self._sessions[sessionId].trigger_drops,
                             name="Drop trigger",
                             args=(uids,))
        t.start()

    def add_node_subscriptions(self, sessionId, relationships):

        logger.debug("Received subscription information: %r", relationships)
        self._check_session_id(sessionId)
        self._sessions[sessionId].add_node_subscriptions(relationships, self)

        # Set up event channels subscriptions
        for nodesub in relationships:

            host = nodesub
            events_port = constants.NODE_DEFAULT_EVENTS_PORT
            if type(nodesub) is tuple:
                host, events_port, _ = nodesub

            # TODO: we also have to unsubscribe from them at some point
            self.subscribe(host, events_port)

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

    def has_method(self, sessionId, uid, mname):
        self._check_session_id(sessionId)
        return self._sessions[sessionId].has_method(uid, mname)

    def get_drop_property(self, sessionId, uuid, prop_name):
        self._check_session_id(sessionId)
        return self._sessions[sessionId].get_drop_property(uuid, prop_name)

    def call_drop(self, sessionId, uid, method, *args):
        self._check_session_id(sessionId)
        return self._sessions[sessionId].call_drop(uid, method, *args)

def zmq_safe(host_or_addr):

    # The catch-all IP address, ZMQ needs a *
    if host_or_addr == '0.0.0.0':
        return '*'

    # Return otherwise always an IP address
    return socket.gethostbyname(host_or_addr)

class BaseMixIn(object):
    def start(self):
        self._running = True
    def shutdown(self):
        self._running = False

class ZMQPubSubMixIn(BaseMixIn):

    subscription = collections.namedtuple('subscription', 'endpoint finished_evt')

    def start(self):

        # temporarily timing import statements to check FS times on HPC environs
        start = time.time()
        import zmq
        logger.info("Importing of zmq took %.3f seconds", time.time() - start)

        super(ZMQPubSubMixIn, self).start()
        self._pubevts = Queue.Queue()
        self._recvevts = Queue.Queue()
        self._subscriptions = Queue.Queue()

        # Setting up zeromq for event publishing/subscription
        # They share the same context, there's no need for two separate ones
        self._zmqctx = zmq.Context()

        # We create the sockets in their respective threads to avoid
        # multithreading issues with zmq, but still wait until they are created
        # via these events
        timeout = 30
        pubsock_created = threading.Event()
        subsock_created = threading.Event()

        self._zmqpubthread = threading.Thread(target = self._zmq_pub_thread, name="ZMQ evtpub", args=(pubsock_created,))
        self._zmqpubthread.start()
        if not pubsock_created.wait(timeout):
            raise Exception("Failed to create PUB ZMQ socket in %d seconds" % (timeout,))

        self._zmqsubthread = threading.Thread(target = self._zmq_sub_thread, name="ZMQ evtsub", args=(subsock_created,))
        self._zmqsubthread.start()
        if not subsock_created.wait(timeout):
            raise Exception("Failed to create PUB ZMQ socket in %d seconds" % (timeout,))

        self._zmqsubqthread = threading.Thread(target = self._zmq_sub_queue_thread, name="ZMQ evtsubq")
        self._zmqsubqthread.start()

    def shutdown(self):
        super(ZMQPubSubMixIn, self).shutdown()
        self._zmqsubqthread.join()
        self._zmqpubthread.join()
        self._zmqsubthread.join()
        self._zmqctx.destroy()
        logger.info("ZMQ context used for event pub/sub destroyed")

    def publish_event(self, evt):
        self._pubevts.put(evt)

    def subscribe(self, host, port):
        timeout = 5
        finished_evt = threading.Event()
        endpoint = "tcp://%s:%d" % (host, port)
        self._subscriptions.put(ZMQPubSubMixIn.subscription(endpoint, finished_evt))
        if not finished_evt.wait(timeout):
            raise DaliugeException("ZMQ subscription not achieved within %d seconds" % (timeout,))
        logger.info("Subscribed for events originating from %s", endpoint)

    def _zmq_pub_thread(self, sock_created):
        import zmq

        pub = self._zmqctx.socket(zmq.PUB)  # @UndefinedVariable
        pub.set_hwm(0) # Never drop messages that should be sent
        endpoint = "tcp://%s:%d" % (zmq_safe(self._host), self._events_port)
        pub.bind(endpoint)
        logger.info("Listening for events via ZeroMQ on %s", endpoint)
        sock_created.set()

        while self._running:

            try:
                obj = self._pubevts.get_nowait()
            except Queue.Empty:
                time.sleep(0.01)
                continue

            while self._running:
                try:
                    pub.send_pyobj(obj, flags = zmq.NOBLOCK)  # @UndefinedVariable
                    break
                except zmq.error.Again:
                    logger.debug("Got an 'Again' when publishing event")
                    time.sleep(0.01)
                    continue

    def _zmq_sub_queue_thread(self):
        while self._running:
            try:
                evt = self._recvevts.get_nowait()
                self.deliver_event(evt)
            except Queue.Empty:
                time.sleep(0.01)

    def _zmq_sub_thread(self, sock_created):
        import zmq

        sub = self._zmqctx.socket(zmq.SUB)  # @UndefinedVariable
        sub.setsockopt(zmq.SUBSCRIBE, six.b(''))  # @UndefinedVariable
        sock_created.set()

        while self._running:

            # A new subscription has been requested
            try:
                subscription = self._subscriptions.get_nowait()
                sub.connect(subscription.endpoint)
                subscription.finished_evt.set()
            except Queue.Empty:
                pass

            try:
                evt = sub.recv_pyobj(flags = zmq.NOBLOCK)  # @UndefinedVariable
                self._recvevts.put(evt)
            except zmq.error.Again:
                time.sleep(0.01)
            except Exception:
                # Figure out what to do here
                logger.exception("Something bad happened in %s:%d to ZMQ :'(", self._host, self._events_port)
                break

class ZeroRPCMixIn(BaseMixIn):

    request = collections.namedtuple('request', 'method args queue')
    response = collections.namedtuple('response', 'async_result queue')

    def start(self):
        super(ZeroRPCMixIn, self).start()

        # Starts the single-threaded ZeroRPC server for RPC requests
        timeout = 30
        server_started = threading.Event()
        self._zrpcserverthread = threading.Thread(target=self.run_zrpcserver, name="ZeroRPC server", args=(self._host, self._rpc_port, server_started))
        self._zrpcserverthread.start()
        if not server_started.wait(timeout):
            raise Exception("ZeroRPC server didn't start within %d seconds" % (timeout,))

        # One per remote host
        self._zrpcclient_acquisition_lock = threading.Lock()
        self._zrpcclients = {}
        self._zrpcclientthreads = []

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
        endpoint = "tcp://%s:%d" % (zmq_safe(host), port,)
        self._zrpcserver.bind(endpoint)
        logger.info("Listening for RPC requests via ZeroRPC on %s", endpoint)
        server_started.set()

        runner = gevent.spawn(self._zrpcserver.run)
        stopper = gevent.spawn(self.stop_zrpcserver)
        gevent.joinall([runner, stopper])
        ctx.destroy()

    def stop_zrpcserver(self):
        import gevent
        while self._running:
            gevent.sleep(0.01)
        logger.info("Closing ZeroRPC server on tcp://%s:%d", zmq_safe(self._host), self._rpc_port)
        self._zrpcserver.close()

    def shutdown(self):
        super(ZeroRPCMixIn, self).shutdown()
        for t in [self._zrpcserverthread] + self._zrpcclientthreads:
            t.join()

    def get_client_for_endpoint(self, host, port):

        endpoint = (host, port)

        with self._zrpcclient_acquisition_lock:
            if endpoint in self._zrpcclients:
                return self._zrpcclients[endpoint]

            # We start the new client on its own thread so it uses gevent, etc.
            # In this thread we create simply enqueue requests
            req_queue = Queue.Queue()
            tname_tpl, args = "zrpc(%s)", host
            if port != constants.NODE_DEFAULT_RPC_PORT:
                tname_tpl, args = "zrpc(%s:%d)", (host, port)
            t = threading.Thread(target=self.run_zrpcclient, args=(host, port, req_queue),
                                 name=tname_tpl % args)
            t.start()

            class QueueingClient(object):
                def __make_call(self, method, *args):
                    res_queue = Queue.Queue()
                    req_queue.put(ZeroRPCMixIn.request(method, args, res_queue))
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
        while self._running:
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

class RPyCMixIn(BaseMixIn): # pragma: no cover

    def start(self):
        super(RPyCMixIn, self).start()

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

        self._rpycserver = ThreadedServer(NMService, hostname=self._host, port=self._rpc_port) # ThreadPoolServer

        # Starts the single-threaded RPyC server for RPC requests
        self._rpycserverthread = threading.Thread(target=self._rpycserver.start, name="RPyC server")
        self._rpycserverthread.start()
        logger.info("Listening for RPC requests via RPyC on %s:%d", self._host, self._rpc_port)

    def shutdown(self):
        super(RPyCMixIn, self).shutdown()
        self._rpycserver.close()
        self._rpycserverthread.join()

    def get_rpc_client(self, hostname, port):
        import rpyc
        client = rpyc.connect(hostname, port)
        return client.root, client.close

class PyroRPCMixIn(BaseMixIn): # pragma: no cover

    def start(self):

        import Pyro4

        super(PyroRPCMixIn, self).start()

        # Starts the single-threaded Pyro server for RPC requests
        logger.info("Listening for RPC requests via Pyro on %s:%d", self._host, self._rpc_port)
        self.setup_pyro()
        self._pyrodaemon = Pyro4.Daemon(self._host, self._rpc_port)
        self._pyrodaemon.register(self, "node_manager")
        self._pyroserverthread = threading.Thread(target=self._pyrodaemon.requestLoop, name="PyroRPC server")
        self._pyroserverthread.start()

    def shutdown(self):
        timeout = 5
        super(PyroRPCMixIn, self).shutdown()
        self._pyrodaemon.shutdown()
        self._pyroserverthread.join(timeout)
        host = 'localhost' if self._host == '0.0.0.0' else self._host
        if not utils.portIsClosed(host, self._rpc_port, timeout):
            logger.warning("Pyro RPC port %d is still open after %d seconds", timeout)

    def get_rpc_client(self, hostname, port):
        import Pyro4
        uri = Pyro4.URI("PYRO:node_manager@%s:%d" % (hostname, port))
        proxy = Pyro4.Proxy(uri)
        return proxy, proxy._pyroRelease

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

            from ..event import Event

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

class MultiplexPyroRPCMixIn(PyroRPCMixIn): # pragma: no cover
    def setup_pyro(self):
        super(MultiplexPyroRPCMixIn, self).setup_pyro()
        import Pyro4
        Pyro4.config.SERVERTYPE = 'multiplex'

class ThreadedPyroRPCMixIn(PyroRPCMixIn): # pragma: no cover
    def setup_pyro(self):
        super(MultiplexPyroRPCMixIn, self).setup_pyro()
        import Pyro4
        Pyro4.config.SERVERTYPE = 'thread'
        Pyro4.config.THREADPOOL_SIZE = 16
        Pyro4.config.THREADPOOL_ALLOW_QUEUE = False

# So far we currently support ZMQ only
EventMixIn = ZMQPubSubMixIn

# Check which rpc backend should be used
rpc_lib = os.environ.get('DALIUGE_RPC', 'zerorpc')
if rpc_lib in ('pyro', 'pyro-multiplex'): # pragma: no cover
    RpcMixIn = MultiplexPyroRPCMixIn
elif rpc_lib == 'pyro-threaded': # pragma: no cover
    RpcMixIn = ThreadedPyroRPCMixIn
elif rpc_lib == 'zerorpc':
    RpcMixIn = ZeroRPCMixIn
elif rpc_lib == 'rpyc': # pragma: no cover
    RpcMixIn = RPyCMixIn
else: # pragma: no cover
    raise DaliugeException("Unknown RPC lib %s, use one of pyro, pyro-multiplex, pyro-threaded, zerorpc, rpyc" % (rpc_lib,))

class NodeManager(EventMixIn, RpcMixIn, NodeManagerBase): pass
