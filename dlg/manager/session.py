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
Module containing the logic of a session -- a given graph execution
"""

import collections
import inspect
import logging
import threading

from . import constants
from .. import droputils
from .. import graph_loader
from .. import rpc
from .. import utils
from ..ddap_protocol import DROPStates, DROPLinkType, DROPRel
from ..drop import AbstractDROP, AppDROP, InputFiredAppDROP, \
    LINKTYPE_1TON_APPEND_METHOD, LINKTYPE_1TON_BACK_APPEND_METHOD
from ..exceptions import InvalidSessionState, InvalidGraphException, \
    NoDropException, DaliugeException


logger = logging.getLogger(__name__)

class SessionStates:
    """
    An enumeration of the different states in which a Session can be found at
    any given point of time.
    """
    PRISTINE, BUILDING, DEPLOYING, RUNNING, FINISHED = range(5)

class LeavesCompletionListener(object):

    def __init__(self, leaves, session):
        self._session = session
        self._nexpected = len(leaves)
        self._completed = 0

    def handleEvent(self, evt):
        # TODO: be thread-safe
        self._completed += 1
        logger.debug("%d/%d leaf drops completed on session %s", self._completed, self._nexpected, self._session.sessionId)
        if self._completed == self._nexpected:
            self._session.finish()


track_current_session = utils.object_tracking('session')

class Session(object):
    """
    A DROP graph execution.

    A session is the runtime incarnation of a given DROP graph that gets
    executed in our framework. Thus, different executions of the same graph
    parts or specifications lead to different sessions.

    In order to be flexible enough, a session starts in a PRISTINE status. While
    in PRISTINE status "graph parts" can be added to it in separate calls, which
    can be connected later. The first time a "graph part" is added to the
    session its status is changed to BUILDING. Once all the parts have been
    submitted, users can finally deploy the session, which will move the session
    to the DEPLOYING status first, create the actual DROPs, and then move
    the session to the RUNNING status later. Once the execution of the
    graph has finished the session is moved to FINISHED.
    """

    def __init__(self, sessionId, nm=None):
        self._sessionId = sessionId
        self._graph = {} # key: oid, value: dropSpec dictionary
        self._drops = {} # key: oid, value: actual drop object
        self._statusLock = threading.Lock()
        self._roots = []
        self._proxyinfo = []
        self._worker = None
        self._status = SessionStates.PRISTINE
        self._error_status_listener = None
        self._nm = nm
        self._dropsubs = {}

    @property
    def sessionId(self):
        return self._sessionId

    @property
    def status(self):
        with self._statusLock:
            return self._status

    @status.setter
    def status(self, status):
        with self._statusLock:
            self._status = status

    @property
    def roots(self):
        return self._roots

    @property
    def drops(self):
        return self._drops

    @track_current_session
    def addGraphSpec(self, graphSpec):
        """
        Adds the graph specification given in `graphSpec` to the
        graph specification currently held by this session. A graphSpec is a
        list of dictionaries, each of which contains the information of one
        DROP. Each DROP specification is checked to see it contains
        all the necessary details to construct a proper DROP. If one
        DROP specification is found to be inconsistent the whole operation
        fill wail.

        Adding graph specs to the session is only allowed while the session is
        in the PRISTINE or BUILDING status; otherwise an exception will be
        raised.

        If the `graphSpec` being added contains DROPs that have already
        been added to the session an exception will be raised. DROPs are
        uniquely identified by their OID at this point.
        """

        status = self.status
        if status not in (SessionStates.PRISTINE, SessionStates.BUILDING):
            raise InvalidSessionState("Can't add graphs to this session since it isn't in the PRISTINE or BUILDING status: %d" % (status))

        self.status = SessionStates.BUILDING

        # This will check the consistency of each dropSpec
        graphSpecDict = graph_loader.loadDropSpecs(graphSpec)

        # Check for duplicates
        duplicates = set(graphSpecDict) & set(self._graph)
        if duplicates:
            raise InvalidGraphException('Trying to add drops with OIDs that already exist: %r' % (duplicates,))

        self._graph.update(graphSpecDict)

        logger.debug("Added a graph definition with %d DROPs", len(graphSpecDict))

    @track_current_session
    def linkGraphParts(self, lhOID, rhOID, linkType, force=False):
        """
        Links together two DROP specifications (i.e., those pointed by
        `lhOID` and `rhOID`) using `linkType`. The DROP specifications
        must both already be part of one of the graph specs contained in this
        session; otherwise an exception will be raised.
        """
        if self.status != SessionStates.BUILDING:
            raise InvalidSessionState("Can't link DROPs anymore since this session isn't in the BUILDING state")

        # Look for the two DROPs in all our graph parts and reporting
        # missing DROPs
        lhDropSpec = self._graph.get(lhOID, None)
        rhDropSpec = self._graph.get(rhOID, None)
        missingOids = []
        if lhDropSpec is None: missingOids.append(lhOID)
        if rhDropSpec is None: missingOids.append(rhOID)
        if missingOids:
            oids = 'OID' if len(missingOids) == 1 else 'OIDs'
            raise InvalidGraphException('No DROP found for %s %r' % (oids, missingOids))

        graph_loader.addLink(linkType, lhDropSpec, rhOID, force=force)

    @track_current_session
    def deploy(self, completedDrops=[], event_listeners=[], foreach=None):
        """
        Creates the DROPs represented by all the graph specs contained in
        this session, effectively deploying them.

        When this method has finished executing a Pyro Daemon will also be
        up and running, servicing requests to access to all the DROPs
        belonging to this session
        """

        # It could happen that this local session was created by a high-level
        # entity (like the DIM) but ended up receiving no Drops.
        # In those cases we still want to be able to "deploy" this session
        # to keep a consistent state across all NM sessions, even though
        # in reality this particular session is managing nothing
        status = self.status
        if (self._graph and status != SessionStates.BUILDING) or \
           (not self._graph and status != SessionStates.PRISTINE):
            raise InvalidSessionState("Can't deploy this session in its current status: %d" % (status))

        if not self._graph and completedDrops:
            raise InvalidGraphException("Drops are requested for immediate completion but none will be created")

        # Shortchut
        if not self._graph:
            self.finish()
            return

        self.status = SessionStates.DEPLOYING

        # Create the real DROPs from the graph specs
        logger.info("Creating DROPs for session %s", self._sessionId)

        self._roots = graph_loader.createGraphFromDropSpecList(self._graph.values(), session=self)
        logger.info("%d drops successfully created", len(self._graph))

        for drop,_ in droputils.breadFirstTraverse(self._roots):

            # Register them
            self._drops[drop.uid] = drop

            # Add a reference to the RPC server that exposes this drop,
            # which is our containing Node Manager.
            # This information is usually not necessary, but there are cases in
            # which we actually need it (like in the DynlibProcApp)
            drop._rpc_server = self._nm

            # Register them with the error handler
            for l in event_listeners:
                drop.subscribe(l, eventType='status')
        logger.info("Stored all drops, proceeding with further customization")

        # Add listeners that will move the session to FINISHED state
        leaves = droputils.getLeafNodes(self._roots)
        logger.info("Adding completion listener to leaf drops")
        listener = LeavesCompletionListener(leaves, self)
        for leaf in leaves:
            if isinstance(leaf, AppDROP):
                leaf.subscribe(listener, 'producerFinished')
            else:
                leaf.subscribe(listener, 'dropCompleted')
        logger.info("Listener added to leaf drops")

        # Foreach
        if foreach:
            logger.info("Invoking 'foreach' on each drop")
            for drop,_ in droputils.breadFirstTraverse(self._roots):
                foreach(drop)
            logger.info("'foreach' invoked for each drop")

        # Append proxies
        logger.info("Creating %d drop proxies: %r", len(self._proxyinfo), self._proxyinfo)
        for host, port, local_uid, relname, remote_uid in self._proxyinfo:
            proxy = rpc.DropProxy(self._nm, host, port, self._sessionId, remote_uid)
            logger.debug("Attaching proxy to local %r via %s(proxy, False)",
                         self._drops[local_uid], relname)
            method = getattr(self._drops[local_uid], relname)
            method(proxy, False)

        # We move to COMPLETED the DROPs that we were requested to
        # InputFiredAppDROP are here considered as having to be executed and
        # not directly moved to COMPLETED.
        #
        # This is done in a separate iteration at the very end because all drops
        # to make sure all event listeners are ready
        self.trigger_drops(completedDrops)

        self.status = SessionStates.RUNNING
        logger.info("Session %s is now RUNNING", self._sessionId)

    def _run(self, worker):
        worker.run()
        worker.stop()
        self.finish()

    def trigger_drops(self, uids):
        for drop,downStreamDrops in droputils.breadFirstTraverse(self._roots):
            downStreamDrops[:] = [dsDrop for dsDrop in downStreamDrops if isinstance(dsDrop, AbstractDROP)]
            if drop.uid in uids:
                if isinstance(drop, InputFiredAppDROP):
                    drop.async_execute()
                else:
                    drop.setCompleted()

    @track_current_session
    def deliver_event(self, evt):
        """
        Called when an event has been fired by a remote drop.
        The event is then delivered to the interested drops of this session.
        """
        if not evt.uid in self._dropsubs:
            logger.debug("No subscription found for drop %s", evt.uid)
            return
        for tgt in self._dropsubs[evt.uid]:
            drop = self._drops[tgt]
            logger.debug("Passing event %r to %r", evt, drop)
            drop.handleEvent(evt)

    def add_node_subscriptions(self, relationships):

        evt_consumer = (DROPLinkType.CONSUMER, DROPLinkType.STREAMING_CONSUMER, DROPLinkType.OUTPUT)
        evt_producer = (DROPLinkType.INPUT,    DROPLinkType.STREAMING_INPUT,    DROPLinkType.PRODUCER)

        for host, droprels in relationships.items():

            # Make sure we have DROPRel tuples
            droprels = [DROPRel(*x) for x in droprels]

            # Sanitize the host/rpc_port info if needed
            rpc_port = constants.NODE_DEFAULT_RPC_PORT
            if type(host) is tuple:
                host, _, rpc_port = host

            # Store which drops should receive events from which remote drops
            dropsubs = collections.defaultdict(set)
            for rel in droprels:

                # Which side of the relationship is local?
                local_uid = None
                remote_uid = None
                if rel.rhs in self._graph:
                    local_uid = rel.rhs
                    remote_uid = rel.lhs
                elif rel.lhs in self._graph:
                    local_uid = rel.lhs
                    remote_uid = rel.rhs

                # We are in the event receiver side
                if (rel.rel in evt_consumer and rel.lhs is local_uid) or \
                   (rel.rel in evt_producer and rel.rhs is local_uid):
                    dropsubs[remote_uid].add(local_uid)

            self._dropsubs.update(dropsubs)

            # Store the information needed to create the proxies later
            for rel in droprels:
                local_uid = rel.rhs
                mname = LINKTYPE_1TON_APPEND_METHOD[rel.rel]
                remote_uid = rel.lhs
                if local_uid not in self._graph:
                    local_uid = rel.lhs
                    remote_uid = rel.rhs
                    mname = LINKTYPE_1TON_BACK_APPEND_METHOD[rel.rel]

                self._proxyinfo.append((host, rpc_port, local_uid, mname, remote_uid))

    @track_current_session
    def finish(self):
        self.status = SessionStates.FINISHED
        logger.info("Session %s finished", self._sessionId)

    def getGraphStatus(self):
        if self.status not in (SessionStates.RUNNING, SessionStates.FINISHED):
            raise InvalidSessionState("The session is currently not running, cannot get graph status")

        # We shouldn't traverse the full graph because there might be nodes
        # attached to our DROPs that are actually part of other DM (and have been
        # wired together by the DIM after deploying each individual graph on
        # each of the DMs).
        # We recognize such nodes because they are actually not an instance of
        # AbstractDROP (they are DropProxy instances).
        statusDict = collections.defaultdict(dict)
        for drop, downStreamDrops in droputils.breadFirstTraverse(self._roots):
            downStreamDrops[:] = [dsDrop for dsDrop in downStreamDrops if isinstance(dsDrop, AbstractDROP)]
            if isinstance(drop, AppDROP):
                statusDict[drop.oid]['execStatus'] = drop.execStatus
            statusDict[drop.oid]['status'] = drop.status

        return statusDict

    def getGraph(self):
        return dict(self._graph)

    def destroy(self):
        pass

    __del__ = destroy

    def has_method(self, uid, mname):
        if uid not in self._drops:
            raise NoDropException(uid)
        try:
            drop = self._drops[uid]
            return inspect.ismethod(getattr(drop, mname))
        except AttributeError:
            return False

    def get_drop_property(self, uid, prop_name):
        if uid not in self._drops:
            raise NoDropException(uid)
        try:
            drop = self._drops[uid]
            return getattr(drop, prop_name)
        except AttributeError:
            raise DaliugeException("%r has no property called %s" % (drop, prop_name))

    def call_drop(self, uid, method, *args):
        if uid not in self._drops:
            raise NoDropException(uid)
        try:
            drop = self._drops[uid]
            m = getattr(drop, method)
        except AttributeError:
            raise DaliugeException("%r has no method called %s" % (drop, method))
        return m(*args)

    # Support for the 'with' keyword
    def __enter__(self):
        return self

    def __exit__(self, typ, value, traceback):
        self.destroy()
