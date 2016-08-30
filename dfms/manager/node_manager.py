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
import inspect
import logging
import os
import socket
import sys
import threading
import time

import Pyro4
import gevent
import six
from six.moves import queue as Queue  # @UnresolvedImport
import zerorpc
import zmq

from dfms import utils
from dfms.ddap_protocol import DROPRel, DROPLinkType
from dfms.drop import AppDROP
from dfms.exceptions import NoSessionException, SessionAlreadyExistsException
from dfms.lifecycle.dlm import DataLifecycleManager
from dfms.manager import repository, constants
from dfms.manager.drop_manager import DROPManager
from dfms.manager.session import Session


logger = logging.getLogger(__name__)

def _functionAsTemplate(f):
    args, _, _, defaults = inspect.getargspec(f)

    # 'defaults' might be shorter than 'args' if some of the arguments
    # are not optional. In the general case anyway the optional
    # arguments go at the end of the method declaration, and therefore
    # a reverse iteration should yield the correct match between
    # arguments and their defaults
    defaults = list(defaults) if defaults else []
    defaults.reverse()
    argsList = []
    for i, arg in enumerate(reversed(args)):
        if i >= len(defaults):
            # mandatory argument
            argsList.append({'name':arg})
        else:
            # optional with default value
            argsList.append({'name':arg, 'default':defaults[i]})

    return {'name': inspect.getmodule(f).__name__ + "." + f.__name__, 'args': argsList}

class NMDropEventListener(object):

    def __init__(self, nm):
        self._nm = nm

    def handleEvent(self, event):
        self._nm.publish_event(event)

class NodeManager(DROPManager):
    """
    Base class for a DROPManager that creates and holds references to DROPs.

    A NodeManager is the ultimate responsible of handling DROPs. It does so not
    directly, but via Sessions, which represent and encapsulate separate,
    independent DROP graph executions. All DROPs created by the
    different Sessions are also given to a common DataLifecycleManager, which
    takes care of expiring them when needed and replicating them.

    Since a NodeManager can handle more than one session, in principle only one
    NodeManager is needed for each computing node, thus its name.
    """

    __metaclass__ = abc.ABCMeta

    def __init__(self,
                 useDLM=True,
                 dfmsPath=None,
                 host=None,
                 error_listener=None,
                 enable_luigi=False,
                 events_port = constants.NODE_DEFAULT_EVENTS_PORT,
                 rpc_port = constants.NODE_DEFAULT_RPC_PORT):

        self._event_listener = NMDropEventListener(self)
        self._dlm = DataLifecycleManager() if useDLM else None
        self._host = host or 'localhost'
        self._events_port = events_port
        self._rpc_port = rpc_port
        self._sessions = {}
        self._dropsubs = {}

        # dfmsPath contains code added by the user with possible
        # DROP applications
        if dfmsPath:
            dfmsPath = os.path.expanduser(dfmsPath)
            if os.path.isdir(dfmsPath):
                logger.info("Adding %s to the system path", dfmsPath)
                sys.path.append(dfmsPath)

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
    def get_drop_attribute(self, hostname, port, session_id, uid, name):
        """
        Requests the attribute ``name`` of the remote drop ``uid`` on session
        ``session_id`` living in host ``hostname``. The request is made on port
        ``port``.
        """

    def deliver_event(self, evt):
        """
        Method called by subclasses when a new event has arrived through the
        subscription mechanism.
        """

        if not evt.uid in self._dropsubs:
            logger.debug("No subscription found for drop %s", evt.uid)
            return

        for tgt in self._dropsubs[evt.uid]:
            for s in self._sessions.values():
                if tgt in s.drops:
                    drop = s.drops[tgt]
                    logger.debug("Passing event %r to %r", evt, drop)
                    drop.handleEvent(evt)
                    break

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
            uris[drop.uid] = drop.uri
            if self._dlm:
                self._dlm.addDrop(drop)
            if isinstance(drop, AppDROP):
                drop.subscribe(self._event_listener, 'producerFinished')
            else:
                drop.subscribe(self._event_listener, 'dropCompleted')

        uris = {}
        session.deploy(completedDrops=completedDrops, foreach=foreach)
        logger.debug('Registering new Drops with the DLM and collecting their URIs')

        return uris

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

        self._check_session_id(sessionId)

        evt_consumer = (DROPLinkType.CONSUMER, DROPLinkType.STREAMING_CONSUMER, DROPLinkType.OUTPUT)
        evt_producer = (DROPLinkType.INPUT,    DROPLinkType.STREAMING_INPUT,    DROPLinkType.PRODUCER)

        logger.debug("Received subscription information: %r", relationships)

        for nodesub, droprels in relationships.items():

            host = nodesub
            events_port = constants.NODE_DEFAULT_EVENTS_PORT
            rpc_port = constants.NODE_DEFAULT_RPC_PORT
            if type(nodesub) is tuple:
                host, events_port, rpc_port = nodesub

            # TODO: we also have to unsubscribe from them at some point
            self.subscribe(host, events_port)

            droprels = [DROPRel(*x) for x in droprels]

            # Which events should we react to?
            dropsubs = collections.defaultdict(list)
            for rel in droprels:

                # Look which side of the relationship is local
                for sid, s in self._sessions.items():

                    if sessionId != sid:
                        continue

                    local_uid = None
                    remote_uid = None
                    if rel.rhs in s._graph:
                        local_uid = rel.rhs
                        remote_uid = rel.lhs
                    elif rel.lhs in s._graph:
                        local_uid = rel.lhs
                        remote_uid = rel.rhs

                    if (rel.rel in evt_consumer and rel.lhs is local_uid) or \
                       (rel.rel in evt_producer and rel.rhs is local_uid):
                        dropsubs[remote_uid].append(local_uid)

                    break

            self._dropsubs.update(dropsubs)

            self._sessions[sessionId].add_relationships(host, rpc_port, droprels, self)

    def has_method(self, sessionId, uid, mname):
        self._check_session_id(sessionId)
        return self._sessions[sessionId].has_method(uid, mname)

    def get_drop_property(self, sessionId, uuid, prop_name):
        self._check_session_id(sessionId)
        return self._sessions[sessionId].get_drop_property(uuid, prop_name)

    def call_drop(self, sessionId, uid, method, *args):
        self._check_session_id(sessionId)
        return self._sessions[sessionId].call_drop(uid, method, *args)

    def getTemplates(self):

        # TODO: we currently have a hardcoded list of functions, but we should
        #       load these repositories in a different way (e.g., from a directory)

        templates = []
        for f in repository.complex_graph, repository.pip_cont_img_pg, repository.archiving_app:
            templates.append(_functionAsTemplate(f))
        return templates

    def materializeTemplate(self, tpl, sessionId, **tplParams):

        self._check_session_id(sessionId)

        # tpl currently has the form <full.mod.path.functionName>
        parts = tpl.split('.')
        module = importlib.import_module('.'.join(parts[:-1]))
        tplFunction = getattr(module, parts[-1])

        # invoke the template function with the given parameters
        # and add the new graph spec to the session
        graphSpec = tplFunction(**tplParams)
        self.addGraphSpec(sessionId, graphSpec)

        logger.info('Added graph from template %s to session %s with params: %s', tpl, sessionId, tplParams)

def zmq_safe(host_or_addr):
    if host_or_addr == '0.0.0.0':
        return '*'
    return socket.gethostbyaddr(host_or_addr)[2][0]

class BaseMixIn(object):
    def start(self):
        self._running = True
    def shutdown(self):
        self._running = False

class ZMQPubSubMixIn(BaseMixIn):

    def start(self):
        super(ZMQPubSubMixIn, self).start()
        self._zmq_sub_q = Queue.Queue()
        self._zmq_pub_q = Queue.Queue()

        # Setting up zeromq for event publishing/subscription
        self._zmq_running = True
        self._zmqcontextpub = zmq.Context()
        self._zmqsocketpub = self._zmqcontextpub.socket(zmq.PUB)  # @UndefinedVariable
        endpoint = "tcp://%s:%d" % (zmq_safe(self._host), self._events_port)
        self._zmqsocketpub.bind(endpoint)
        logger.info("Listening for events via ZeroMQ on %s", endpoint)

        self._zmqcontextsub = zmq.Context()
        self._zmqsocketsub = self._zmqcontextsub.socket(zmq.SUB)  # @UndefinedVariable
        self._zmqsocketsub.setsockopt(zmq.SUBSCRIBE, '')  # @UndefinedVariable

        self._zmqpubqthread = threading.Thread(target = self._zmq_pub_queue_thread, name="ZMQ evtpub")
        self._zmqpubqthread.start()

        self._zmqsubqthread = threading.Thread(target = self._zmq_sub_queue_thread, name="ZMQ evtsubq")
        self._zmqsubqthread.start()

        self._zmqsubthread = threading.Thread(target = self._zmq_sub_thread, name="ZMQ evtsub")
        self._zmqsubthread.start()

    def shutdown(self):
        super(ZMQPubSubMixIn, self).shutdown()
        self._zmqsubqthread.join()
        self._zmqpubqthread.join()
        self._zmqsubthread.join()
        self._zmqcontextpub.destroy()
        self._zmqcontextsub.destroy()

    def publish_event(self, evt):
        self._zmq_pub_q.put(evt)

    def subscribe(self, host, port):
        endpoint = "tcp://%s:%d" % (host, port)
        self._zmqsocketsub.connect(endpoint)
        logger.info("Subscribed for events originating from %s", endpoint)

    def _zmq_pub_queue_thread(self):
        while self._running:
            evt = None
            try:
                evt = self._zmq_pub_q.get_nowait()
            except Queue.Empty:
                time.sleep(0.01)
                continue

            while self._running:
                try:
                    self._zmqsocketpub.send_pyobj(evt, flags = zmq.NOBLOCK)  # @UndefinedVariable
                    break
                except zmq.error.Again:
                    logger.debug("Got an 'Again' when publishing event")
                    time.sleep(0.01)
                    continue

    def _zmq_sub_queue_thread(self):
        while self._running:
            evt = None
            try:
                evt = self._zmq_sub_q.get_nowait()
                self.deliver_event(evt)
            except Queue.Empty:
                time.sleep(0.01)
                continue

    def _zmq_sub_thread(self):
        while self._running:
            try:
                evt = self._zmqsocketsub.recv_pyobj(flags = zmq.NOBLOCK)  # @UndefinedVariable
                self._zmq_sub_q.put(evt)
            except zmq.error.Again:
                time.sleep(0.01)
            except Exception:
                import traceback
                traceback.print_exc()
                # Figure out what to do here
                break

class ZeroRPCMixIn(BaseMixIn):

    def start(self):
        super(ZeroRPCMixIn, self).start()

        # Starts the single-threaded ZeroRPC server for RPC requests
        self._zrpcserverthread = threading.Thread(target=self.run_zrpcserver, name="ZeroRPC server", args=(self._host, self._rpc_port,))
        self._zrpcserverthread.start()

    def run_zrpcserver(self, host, port):

        # zmq needs an address, not a hostname
        self._zrpcserver = zerorpc.Server(self)
        endpoint = "tcp://%s:%d" % (zmq_safe(host), port,)
        self._zrpcserver.bind(endpoint)
        logger.info("Listening for RPC requests via ZeroRPC on %s", endpoint)
        gr1 = gevent.spawn(self._zrpcserver.run)
        gr2 = gevent.spawn(self.stop_rpcserver)
        gevent.joinall([gr1, gr2])

    def stop_rpcserver(self):
        while self._running:
            gevent.sleep(0.2)
        self._zrpcserver.close()

    def shutdown(self):
        super(ZeroRPCMixIn, self).shutdown()
        self._zrpcserverthread.join()

    def get_drop_attribute(self, hostname, port, session_id, uid, name):

        # The remote method receives the same client used to inspect the remote
        # object
        class remote_method(object):
            def __del__(self):
                self.c.close()
            def __call__(self, *args):
                return self.c.call_drop(session_id, uid, name, *args)

        logger.debug("Getting attribute %s for drop %s of session %s at %s:%d", name, uid, session_id, hostname, port)

        c = zerorpc.Client("tcp://%s:%d" % (hostname,port))
        closeit = False
        try:
            if c.has_method(session_id, uid, name):
                method = remote_method()
                method.c = c
                return method
            closeit = True
            return c.get_drop_property(session_id, uid, name)
        finally:
            if closeit:
                c.close()


class PyroRPCMixIn(BaseMixIn):

    def start(self):
        super(PyroRPCMixIn, self).start()

        # Starts the single-threaded Pyro server for RPC requests
        logger.info("Listening for RPC requests via Pyro on %s:%d", self._host, self._rpc_port)
        Pyro4.config.SERVERTYPE = 'multiplex'
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

    def get_drop_attribute(self, hostname, port, session_id, uid, name):

        # The remote method receives the same client used to inspect the remote
        # object
        class remote_method(object):
            def __del__(self):
                self.nm._pyroRelease()
            def __call__(self, *args):
                return self.nm.call_drop(session_id, uid, name, *args)

        logger.debug("Getting attribute %s for drop %s of session %s at %s:%d", name, uid, session_id, hostname, port)

        uri = Pyro4.URI("PYRO:node_manager@%s:%d" % (hostname, port))
        nm = Pyro4.Proxy(uri)
        closeit = False
        try:
            if nm.has_method(session_id, uid, name):
                method = remote_method()
                method.nm = nm
                return method
            closeit = True
            return nm.get_drop_property(session_id, uid, name)
        finally:
            if closeit:
                nm._pyroRelease()

class ZMQNodeManager(ZMQPubSubMixIn,
                     PyroRPCMixIn,
                     NodeManager):
    pass