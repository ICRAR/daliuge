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
import sys
import threading
import time

import six
from six.moves import queue as Queue  # @UnresolvedImport

from . import constants
from .drop_manager import DROPManager
from .session import Session
from .. import rpc, utils
from ..ddap_protocol import DROPStates
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

class ErrorStatusListener(object):
    """An event listener that passes down the erroneous drop to an error handler"""

    def __init__(self, session, error_listener):
        self._session = session
        self._error_listener = error_listener

    def handleEvent(self, evt):
        if evt.status == DROPStates.ERROR:
            self._error_listener.on_error(self._session.drops[evt.uid])


def _load(obj, callable_attr):
    """
    Returns object (a python object or a string denoting a class within a
    python module only if it has the indicated attribute and it is callable.
    """
    if isinstance(obj, six.string_types):
        try:
            parts = obj.split('.')
            module = importlib.import_module('.'.join(parts[:-1]))
        except:
            logger.exception('Creating the error listener')
            raise
        obj = getattr(module, parts[-1])()
    if not hasattr(obj, callable_attr) or not callable(getattr(obj, callable_attr)):
        raise ValueError("%r doesn't contain an %s attribute that can be called" % (obj, callable_attr))
    return obj


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
                 error_listener=None,
                 event_listeners=[],
                 max_threads = 0):

        self._dlm = DataLifecycleManager() if useDLM else None
        self._sessions = {}

        # dlgPath contains code added by the user with possible
        # DROP applications
        if dlgPath:
            dlgPath = os.path.expanduser(dlgPath)
            if os.path.isdir(dlgPath):
                logger.info("Adding %s to the system path", dlgPath)
                sys.path.append(dlgPath)

        # Error listener used by users to deal with errors coming from specific
        # Drops in whatever way they want. This is a specific case of an event
        # listener, so we add it together with the rest of the user-supplied
        # event listeners
        self._error_listener = _load(error_listener, 'on_error') if error_listener else None
        self._event_listeners = [_load(l, 'handleEvent') for l in event_listeners]

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
        self._sessions[sessionId] = Session(sessionId, nm=self)
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

        # Add user-supplied listeners
        listeners = self._event_listeners[:]
        if self._error_listener:
            listeners.append(ErrorStatusListener(session, self._error_listener))

        session.deploy(completedDrops=completedDrops, event_listeners=listeners, foreach=foreach)

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
        self._sessions[sessionId].add_node_subscriptions(relationships)

        # Set up event channels subscriptions
        for nodesub in relationships:

            host = nodesub
            events_port = constants.NODE_DEFAULT_EVENTS_PORT
            if type(nodesub) is tuple:
                host, events_port, _ = nodesub

            # TODO: we also have to unsubscribe from them at some point
            self.subscribe(host, events_port)

    def has_method(self, sessionId, uid, mname):
        self._check_session_id(sessionId)
        return self._sessions[sessionId].has_method(uid, mname)

    def get_drop_property(self, sessionId, uuid, prop_name):
        self._check_session_id(sessionId)
        return self._sessions[sessionId].get_drop_property(uuid, prop_name)

    def call_drop(self, sessionId, uid, method, *args):
        self._check_session_id(sessionId)
        return self._sessions[sessionId].call_drop(uid, method, *args)


class ZMQPubSubMixIn(object):

    subscription = collections.namedtuple('subscription', 'endpoint finished_evt')

    def __init__(self, host, events_port):
        self._events_host = host
        self._events_port = events_port

    def start(self):

        # temporarily timing import statements to check FS times on HPC environs
        start = time.time()
        import zmq
        logger.info("Importing of zmq took %.3f seconds", time.time() - start)

        self._pubsub_running = True
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
        self._pubsub_running = False
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
        endpoint = "tcp://%s:%d" % (utils.zmq_safe(self._events_host), self._events_port)
        pub.bind(endpoint)
        logger.info("Listening for events via ZeroMQ on %s", endpoint)
        sock_created.set()

        while self._pubsub_running:

            try:
                obj = self._pubevts.get_nowait()
            except Queue.Empty:
                time.sleep(0.01)
                continue

            while self._pubsub_running:
                try:
                    pub.send_pyobj(obj, flags = zmq.NOBLOCK)  # @UndefinedVariable
                    break
                except zmq.error.Again:
                    logger.debug("Got an 'Again' when publishing event")
                    time.sleep(0.01)
                    continue

    def _zmq_sub_queue_thread(self):
        while self._pubsub_running:
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

        while self._pubsub_running:

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
                logger.exception("Something bad happened in %s:%d to ZMQ :'(", self._events_host, self._events_port)
                break


# So far we currently support ZMQ only for event publishing
EventMixIn = ZMQPubSubMixIn
# Load the corresponding RPC classes and finish the construciton of NodeManager
class RpcMixIn(rpc.RPCClient, rpc.RPCServer): pass

# Final NodeManager class
class NodeManager(EventMixIn, RpcMixIn, NodeManagerBase):

    def __init__(self, useDLM=True, dlgPath=None, error_listener=None, event_listeners=[], max_threads=0,
                 host=None, rpc_port=constants.NODE_DEFAULT_RPC_PORT,
                 events_port=constants.NODE_DEFAULT_EVENTS_PORT):
        host = host or '127.0.0.1'
        EventMixIn.__init__(self, host, events_port)
        RpcMixIn.__init__(self, host, rpc_port)
        NodeManagerBase.__init__(self, useDLM, dlgPath, error_listener, event_listeners, max_threads)