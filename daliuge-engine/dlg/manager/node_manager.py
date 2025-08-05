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

from __future__ import annotations
import abc
import collections
import copy
import logging
from typing import Optional

from psutil import cpu_count
import os
import queue
import signal
import sys
import threading
import time
import typing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, Future

from dlg import constants
from dlg.manager.drop_manager import DROPManager
from dlg.manager.session import Session

from dlg import rpc, utils
from dlg.ddap_protocol import DROPStates
from dlg.apps.app_base import AppDROP, DropRunner
from dlg.exceptions import (
    NoSessionException,
    SessionAlreadyExistsException,
    DaliugeException,
)
from ..lifecycle.dlm import DataLifecycleManager

logger = logging.getLogger(f"dlg.{__name__}")


class NMDropEventListener(object):
    def __init__(self, nm, session_id):
        self._nm = nm
        self._session_id = session_id

    def handleEvent(self, event):
        event.session_id = self._session_id
        self._nm.publish_event(event)


class LogEvtListener(object):
    def handleEvent(self, event):
        if event.type == "status":
            logger.debug(
                "Data Drop uid=%s, oid=%s changed to state %s",
                event.uid,
                event.oid,
                event.status,
            )
        elif event.type == "execStatus":
            logger.debug(
                "AppDrop uid=%s, oid=%s changed to execState %s",
                event.uid,
                event.oid,
                event.execStatus,
            )


class ErrorStatusListener(object):
    """An event listener that passes down the erroneous drop to an error handler"""

    def __init__(self, session, error_listener):
        self._session = session
        self._error_listener = error_listener

    def handleEvent(self, evt):
        if evt.type == "status" and evt.status == DROPStates.ERROR:
            self._error_listener.on_error(self._session.drops[evt.uid])


def _load(obj, callable_attr):
    """
    Returns object (a python object or a string denoting a class within a
    python module only if it has the indicated attribute and it is callable.
    """
    if isinstance(obj, str):
        obj = utils.get_symbol(obj)()
    if not hasattr(obj, callable_attr) or not callable(getattr(obj, callable_attr)):
        raise ValueError(
            "%r doesn't contain an %s attribute that can be called"
            % (obj, callable_attr)
        )
    return obj


class NodeManagerDropRunner(DropRunner):
    @abc.abstractmethod
    def start(self, rpc_endpoint):
        raise NotImplementedError

    @abc.abstractmethod
    def close(self):
        raise NotImplementedError


class NodeManagerThreadDropRunner(NodeManagerDropRunner):
    def __init__(self, max_workers: int):
        self._max_workers = max_workers
        self._thread_pool: Optional[ThreadPoolExecutor] = None

    def start(self, _rpc_endpoint):
        logger.info("Initializing thread pool with %d workers", self._max_workers)
        self._thread_pool = ThreadPoolExecutor(max_workers=self._max_workers)

    def run_drop(self, app_drop: AppDROP) -> Future:
        if logger.getEffectiveLevel() != logging.getLevelName(app_drop.log_level):
            logging.getLogger("dlg").setLevel(app_drop.log_level)
            logger.warning(
                "Set log-level for %s.%s to %s",
                app_drop.name,
                app_drop.humanKey,
                logging.getLevelName(logger.getEffectiveLevel()),
            )
        return self._thread_pool.submit(
            app_drop.run,
        )

    def close(self):
        self._thread_pool.shutdown(wait=True)


class NodeManagerProcessDropRunner(NodeManagerDropRunner):
    # Process isolated properties - should only be accessed in @classmethods
    # to ensure that they are global to a single process only
    _rpc_client: typing.Optional[rpc.RPCClient]
    _rpc_endpoint: typing.Tuple[str, int]

    def __init__(self, max_workers: int):
        self._max_workers = max_workers
        self._process_pool: typing.Optional[ProcessPoolExecutor] = None

    def start(self, rpc_endpoint):
        logger.info("Initializing process pool with %d workers", self._max_workers)

        self._process_pool = ProcessPoolExecutor(
            max_workers=self._max_workers,
            initializer=NodeManagerProcessDropRunner._setup_process,
            initargs=(rpc_endpoint,),
        )

    @classmethod
    def _setup_process(cls, rpc_endpoint):
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        cls._rpc_endpoint = rpc_endpoint
        cls._rpc_client = rpc.RPCClient()

        # We'll just let this be cleaned up when the process terminates
        # (instead of the normal `shutdown()`)
        cls._rpc_client.start()

    def run_drop(self, app_drop: AppDROP):
        inputs_proxy_info, outputs_proxy_info = (
            NodeManagerProcessDropRunner._get_proxy_infos(app_drop)
        )

        # MP Queues pickle on a background thread - we need to ensure that we don't
        # modify the input app_drop reference outside of the scope of this method
        # so we create a shallow copy that can be pickled at an indeterminate point in the future
        # See https://github.com/python/cpython/blob/v3.10.13/Lib/multiprocessing/queues.py#L95
        copied_drop = copy.copy(app_drop)
        copied_drop.inputs = collections.OrderedDict()
        copied_drop.outputs = collections.OrderedDict()

        return self._process_pool.submit(
            NodeManagerProcessDropRunner._run_app_drop,
            copied_drop,
            inputs_proxy_info,
            outputs_proxy_info,
        )

    @classmethod
    def _run_app_drop(cls, app_drop, inputs_proxy_info, outputs_proxy_info):
        cls._setup_drop_proxies(app_drop, inputs_proxy_info, outputs_proxy_info)
        return app_drop.run()

    @classmethod
    def _setup_drop_proxies(
        cls, app_drop: AppDROP, inputs_proxy_info, outputs_proxy_info
    ):
        app_drop.rpc_endpoint = cls._rpc_endpoint
        for input_proxy_info in inputs_proxy_info:
            app_drop.addInput(
                rpc.DropProxy(cls._rpc_client, input_proxy_info), back=False
            )
        for output_proxy_info in outputs_proxy_info:
            app_drop.addOutput(
                rpc.DropProxy(cls._rpc_client, output_proxy_info), back=False
            )

    @classmethod
    def _get_proxy_infos(cls, app_drop):
        inputs = [rpc.ProxyInfo.from_data_drop(i) for i in app_drop.inputs]
        outputs = [rpc.ProxyInfo.from_data_drop(o) for o in app_drop.outputs]
        return inputs, outputs

    def close(self):
        self._process_pool.shutdown(wait=True)
        logger.info("Process pool closed")


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

    def __init__(
        self,
        events_port,
        dlm_check_period=0,
        dlm_cleanup_period=0,
        dlm_enable_replication=False,
        dlgPath=None,
        error_listener=None,
        event_listeners: list = None,
        max_threads=0,
        use_processes=False,
        logdir=utils.getDlgLogsDir(),
        use_local_time=False
    ):
        self._events_port = events_port
        self._dlm = DataLifecycleManager(
            check_period=dlm_check_period,
            cleanup_period=dlm_cleanup_period,
            enable_drop_replication=dlm_enable_replication,
        )
        self.sessions = {}
        self.logdir = logdir
        self.use_local_time=use_local_time

        # dlgPath may contain code added by the user with possible
        # DROP applications
        if dlgPath:
            dlgPath = os.path.expanduser(dlgPath)
            if os.path.isdir(dlgPath):
                logger.info("Adding %s to the system path", dlgPath)
                sys.path.append(dlgPath)
                # we also add underlying site-packages dir to support
                # the --prefix installation of code
                pyVer = f"{sys.version_info.major}.{sys.version_info.minor}"
                extraPath = f"{dlgPath}/lib/python{pyVer}/site-packages"
                logger.info("Adding %s to the system path", extraPath)
                sys.path.append(extraPath)

        # Error listener used by users to deal with errors coming from specific
        # Drops in whatever way they want. This is a specific case of an event
        # listener, so we add it together with the rest of the user-supplied
        # event listeners
        self._error_listener = (
            _load(error_listener, "on_error") if error_listener else None
        )
        if not event_listeners:
            event_listeners = []
        self._event_listeners = [_load(l, "handleEvent") for l in event_listeners]

        if max_threads <= 0:
            max_threads = cpu_count(logical=False)

        self.drop_runner: NodeManagerDropRunner
        if use_processes:
            self.drop_runner = NodeManagerProcessDropRunner(max_threads)
        else:
            self.drop_runner = NodeManagerThreadDropRunner(max_threads)

        # Event handler that only logs status changes
        debugging = logger.isEnabledFor(logging.DEBUG)
        self._logging_event_listener = LogEvtListener() if debugging else None

    def start(self, rpc_endpoint):
        super().start()
        self.drop_runner.start(rpc_endpoint)
        self._dlm.startup()

    def shutdown(self):
        self._dlm.cleanup()
        self.drop_runner.close()
        super().shutdown()

    def deliver_event(self, evt):
        """
        Method called by subclasses when a new event has arrived through the
        subscription mechanism.
        """
        if not evt.session_id in self.sessions:
            logger.warning(
                "No session %s found, event (%s) will be dropped",
                evt.session_id,
                evt.type,
            )
            return
        self.sessions[evt.session_id].deliver_event(evt)

    def _check_session_id(self, session_id):
        if session_id not in self.sessions:
            raise NoSessionException(session_id)

    def createSession(self, sessionId):
        if sessionId in self.sessions:
            raise SessionAlreadyExistsException(sessionId)
        self.sessions[sessionId] = Session(sessionId, nm=self)
        logger.info("Created session %s", sessionId)

    def getSessionStatus(self, sessionId):
        try:
            self._check_session_id(sessionId)
        except NoSessionException:
            return None

        return self.sessions[sessionId].status

    def getSessionReproStatus(self, sessionId):
        return self.sessions[sessionId].reprostatus

    def getSessionDir(self, sessionId):
        return {"dir" : self.sessions[sessionId].sessionDir}

    def getGraphReproData(self, sessionId):
        return self.sessions[sessionId].graphreprodata

    def linkGraphParts(self, sessionId, lhOID, rhOID, linkType):
        self._check_session_id(sessionId)
        self.sessions[sessionId].linkGraphParts(lhOID, rhOID, linkType)

    def addGraphSpec(self, sessionId, graphSpec):
        self._check_session_id(sessionId)
        self.sessions[sessionId].addGraphSpec(graphSpec)

    def getGraphStatus(self, sessionId):
        self._check_session_id(sessionId)
        return self.sessions[sessionId].getGraphStatus()

    def getDropStatus(self, sessionId, dropId):
        return self.sessions[sessionId].getDropLogs(dropId)

    def getGraph(self, sessionId):
        self._check_session_id(sessionId)
        #  TODO: Ensure returns reproducibility data.
        return self.sessions[sessionId].getGraph()

    def getLogDir(self):
        return self.logdir

    def deploySession(self, sessionId, completedDrops: list[str] = None):
        self._check_session_id(sessionId)
        session = self.sessions[sessionId]
        if hasattr(self, "_memoryManager"):
            self._memoryManager.register_session(sessionId)

        def foreach(drop):
            drop.autofill_environment_variables()
            drop.drop_runner = self.drop_runner
            self._dlm.addDrop(drop)

            # Remote event forwarding
            evt_listener = NMDropEventListener(self, sessionId)
            if isinstance(drop, AppDROP):
                drop.subscribe(evt_listener, "producerFinished")
            else:
                drop.subscribe(evt_listener, "dropCompleted")

            # Purely for logging purposes
            log_evt_listener = self._logging_event_listener
            if log_evt_listener:
                drop.subscribe(log_evt_listener, "status")
                drop.subscribe(log_evt_listener, "reproducibility")
                if isinstance(drop, AppDROP):
                    drop.subscribe(log_evt_listener, "execStatus")

        # Add user-supplied listeners
        listeners = self._event_listeners[:]
        if self._error_listener:
            listeners.append(ErrorStatusListener(session, self._error_listener))

        session.deploy(
            completedDrops=completedDrops,
            event_listeners=listeners,
            foreach=foreach,
        )

    def cancelSession(self, sessionId):
        logger.info("Cancelling session: %s", sessionId)
        self._check_session_id(sessionId)
        self.sessions[sessionId].cancel()

    def destroySession(self, sessionId):
        logger.info("Destroying session: %s", sessionId)
        self._check_session_id(sessionId)
        session = self.sessions.pop(sessionId)
        if hasattr(self, "_memoryManager"):
            self._memoryManager.shutdown_session(sessionId)
        self._dlm.remove_drops(session.drops)
        session.destroy()

    def getSessionIds(self):
        return list(self.sessions.keys())

    def getGraphSize(self, sessionId):
        try:
            self._check_session_id(sessionId)
        except NoSessionException:
            return 0
        session = self.sessions[sessionId]
        return len(session.graph)

    def trigger_drops(self, sessionId, uids):
        self._check_session_id(sessionId)
        t = threading.Thread(
            target=self.sessions[sessionId].trigger_drops,
            name="Drop trigger",
            args=(uids,),
        )
        t.start()

    def add_node_subscriptions(self, sessionId, relationships):
        logger.debug("Received subscription information: %r", relationships)
        self._check_session_id(sessionId)
        self.sessions[sessionId].add_node_subscriptions(relationships)

        # Set up event channels subscriptions
        for nodesub in relationships:
            # node = Node(nodesub)
            # This needs to be changed
            events_port = nodesub.events_port  # constants.NODE_DEFAULT_EVENTS_PORT
            if type(nodesub) is tuple:
                host, events_port, _ = nodesub
            else:
                # TODO: we also have to unsubscribe from them at some point
                # if nodesub.find(":") > 0:
                #     host, _ = nodesub.split(":")
                # else:
                host = nodesub
            logger.debug("Sending subscription to %s", f"{host}:{events_port}")
            self.subscribe(host, events_port)

    def has_method(self, sessionId, uid, mname):
        self._check_session_id(sessionId)
        return self.sessions[sessionId].has_method(uid, mname)

    def get_drop_property(self, sessionId, uuid, prop_name):
        self._check_session_id(sessionId)
        return self.sessions[sessionId].get_drop_property(uuid, prop_name)

    def call_drop(self, sessionId, uid, method, *args):
        self._check_session_id(sessionId)
        return self.sessions[sessionId].call_drop(uid, method, *args)


class ZMQPubSubMixIn(object):
    """
    ZeroMQ-based event publisher and subscriber.

    Event publishing and event reception are done in their own separate
    threads, where the externally-facing ZeroMQ sockets are created and used.

    Events to be published are fed into the publishing thread via a safe-thread
    Queue object (self._events_out), enabling any local thread to publish events
    without having to worry about ZeroMQ thread-safeness.

    The event reception thread not only *receives* events, but also updates the
    subscription socket to connect to new peers. These updates are fed via a
    Queue object (self._subscriptions), enabling any local thread to indicate a
    new peer to subscribe to in a thread-safe manner.

    Note that we investigated not using Queue objects to communicate between
    threads, and use inproc:// ZeroMQ sockets instead. This works, but at a
    cost: all threads putting values into these sockets would need to check,
    each time they use a socket in any manner, if the Context object is still
    valid and hasn't been closed (or alternatively if self._pubsub_running is
    still True). Our experience with this alternative was not satisfactory, and
    therefore we went for a Queue-based thread communication model, making the
    handling of ZeroMQ resources simpler.
    """

    subscription = collections.namedtuple("subscription", "endpoint finished_evt")

    def __init__(self, host, events_port):
        self._events_host = host
        self._events_port = events_port

    def start(self):
        self._pubsub_running = True
        super(ZMQPubSubMixIn, self).start()
        self._events_in = queue.Queue()
        self._events_out = queue.Queue()
        self._subscriptions = queue.Queue()

        # Starts background threads, but wait until their sockets are created
        timeout = 30
        self._event_publisher = self._start_thread(
            self._publish_events, "Evt pub", timeout
        )
        self._event_receiver = self._start_thread(
            self._receive_events, "Evt recv", timeout
        )
        self._event_deliverer = self._start_thread(self._deliver_events, "Evt delivery")

    def _start_thread(self, target, name, timeout=None):
        evt = threading.Event() if timeout else None
        args = (evt,) if evt else ()
        t = threading.Thread(target=target, name=name, args=args)
        t.start()
        if evt and not evt.wait(timeout):
            raise Exception("Failed to start %s thread in %d seconds" % (name, timeout))
        return t

    def shutdown(self):
        super(ZMQPubSubMixIn, self).shutdown()
        self._pubsub_running = False
        self._event_deliverer.join()
        self._event_publisher.join()
        self._event_receiver.join()
        logger.info("ZeroMQ event publisher/subscriber finished")

    def publish_event(self, evt):
        self._events_out.put(evt)

    def subscribe(self, host, port):
        timeout = 5
        finished_evt = threading.Event()
        endpoint = "tcp://%s:%d" % (utils.zmq_safe(host.host), port)
        self._subscriptions.put(ZMQPubSubMixIn.subscription(endpoint, finished_evt))
        if not finished_evt.wait(timeout):
            raise DaliugeException(
                "ZMQ subscription not achieved within %d seconds" % (timeout,)
            )
        logger.info("Subscribed for events originating from %s", endpoint)

    def _publish_events(self, sock_created):
        import zmq

        pub = self._context.socket(zmq.PUB)  # @UndefinedVariable
        pub.set_hwm(0)  # Never drop messages that should be sent
        endpoint = "tcp://%s:%d" % (
            utils.zmq_safe(self._events_host),
            self._events_port,
        )
        pub.bind(endpoint)
        logger.info("Publishing events via ZeroMQ on %s", endpoint)
        sock_created.set()

        while self._pubsub_running:
            try:
                obj = self._events_out.get_nowait()
            except queue.Empty:
                time.sleep(0.01)
                continue

            while self._pubsub_running:
                try:
                    pub.send_pyobj(obj, flags=zmq.NOBLOCK)  # @UndefinedVariable
                    break
                except zmq.error.Again:
                    logger.debug("Got an 'Again' when publishing event")
                    time.sleep(0.01)
                    continue
        pub.close()

    def _deliver_events(self):
        while self._pubsub_running:
            try:
                evt = self._events_in.get_nowait()
                self.deliver_event(evt)
            except queue.Empty:
                time.sleep(0.01)

    def _receive_events(self, sock_created):
        import zmq
        from zmq.utils.monitor import recv_monitor_message

        sub = self._context.socket(zmq.SUB)  # @UndefinedVariable
        sub_endpoints = set()
        sub.setsockopt(zmq.SUBSCRIBE, b"")  # @UndefinedVariable
        sub_monitor = sub.get_monitor_socket()
        sock_created.set()

        pending_connections = {}
        while self._pubsub_running:
            # A new subscription has been requested
            try:
                subscription = self._subscriptions.get_nowait()
                if subscription.endpoint in sub_endpoints:
                    subscription.finished_evt.set()
                else:
                    sub.connect(subscription.endpoint)
                    pending_connections[subscription.endpoint] = (
                        subscription.finished_evt
                    )
            except queue.Empty:
                pass

            try:
                msg = recv_monitor_message(sub_monitor, flags=zmq.NOBLOCK)
                if msg["event"] != zmq.EVENT_CONNECTED:
                    continue
                endpoint = utils.b2s(msg["endpoint"])
                sub_endpoints.add(endpoint)
                finished_evt = pending_connections.pop(endpoint)
                finished_evt.set()
            except zmq.error.Again:
                pass

            try:
                evt = sub.recv_pyobj(flags=zmq.NOBLOCK)  # @UndefinedVariable
                self._events_in.put(evt)
            except zmq.error.Again:
                time.sleep(0.01)
            except zmq.error.ZMQError:
                # Figure out what to do here
                logger.exception(
                    "Something bad happened in %s:%d to ZMQ :'(",
                    self._events_host,
                    self._events_port,
                )
                break

        # Flush pending connection events to avoid callers hanging out forever
        for evt in pending_connections:
            evt.set()

        sub_monitor.close()
        sub.close()


# So far we currently support ZMQ only for event publishing
EventMixIn = ZMQPubSubMixIn


# Load the corresponding RPC classes and finish the construction of NodeManager
class RpcMixIn(rpc.RPCClient, rpc.RPCServer):
    pass


# Final NodeManager class
class NodeManager(NodeManagerBase, EventMixIn, RpcMixIn):
    def __init__( # pylint: disable=keyword-arg-before-vararg
        self,
        host=None,
        rpc_port=constants.NODE_DEFAULT_RPC_PORT,
        events_port=constants.NODE_DEFAULT_EVENTS_PORT,
        *args,
        **kwargs,
    ):
        host = host or "localhost"
        NodeManagerBase.__init__(self, events_port, *args, **kwargs)
        EventMixIn.__init__(self, host, events_port)
        RpcMixIn.__init__(self, host, rpc_port)
        self.start()

    def start(self): # pylint: disable=arguments-differ
        # We "just know" that our RpcMixIn will have a create_context static
        # method, which in reality means we are using the ZeroRPCServer class
        self._context = RpcMixIn.create_context()
        super().start(self.rpc_endpoint)

    def shutdown(self):
        super(NodeManager, self).shutdown()
        self._context.term()
