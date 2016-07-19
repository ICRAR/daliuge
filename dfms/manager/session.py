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
import logging
import threading

import Pyro4
from luigi import scheduler, worker

from dfms import droputils, utils
from dfms import luigi_int, graph_loader
from dfms.ddap_protocol import DROPLinkType, DROPStates
from dfms.drop import AbstractDROP, AppDROP, InputFiredAppDROP
from dfms.exceptions import InvalidSessionState, InvalidGraphException


_LINKTYPE_TO_NREL = {
    DROPLinkType.CONSUMER: 'consumers',
    DROPLinkType.STREAMING_CONSUMER: 'streamingConsumers',
    DROPLinkType.CHILD: 'children',
    DROPLinkType.INPUT: 'inputs',
    DROPLinkType.STREAMING_INPUT: 'streamingInputs',
    DROPLinkType.OUTPUT: 'outputs',
    DROPLinkType.PRODUCER: 'producers'
}
_LINKTYPE_TO_REL = {
    DROPLinkType.PARENT: 'parent',
}

logger = logging.getLogger(__name__)

class SessionStates:
    """
    An enumeration of the different states in which a Session can be found at
    any given point of time.
    """
    PRISTINE, BUILDING, DEPLOYING, RUNNING, FINISHED = range(5)


class ErrorStatusListener(utils.noopctx):

    def __init__(self, session, event_listener):
        self._session = session
        self._event_listener = event_listener

    def handleEvent(self, evt):
        if evt.status == DROPStates.ERROR:
            self._event_listener.on_error(self._session.drops[evt.uid])

class LeavesCompletionListener(utils.noopctx):

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

    def __init__(self, sessionId, host=None, error_listener=None, enable_luigi=False):
        self._sessionId = sessionId
        self._graph = {} # key: oid, value: dropSpec dictionary
        self._drops = {} # key: oid, value: actual drop object
        self._statusLock = threading.Lock()
        self._roots = []
        self._daemon = None
        self._worker = None
        self._status = SessionStates.PRISTINE
        self._host = host
        self._error_status_listener = None
        self._enable_luigi = enable_luigi
        if error_listener:
            self._error_status_listener = ErrorStatusListener(self, error_listener)

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

        for oid in graphSpecDict:
            if oid in self._graph:
                raise InvalidGraphException('DROP with OID %s already exists, cannot add twice' % (oid))

        self._graph.update(graphSpecDict)

        logger.debug("Added a graph definition with %d DROPs", len(graphSpecDict))

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
        lhDropSpec = self.findByOidInParts(lhOID)
        rhDropSpec = self.findByOidInParts(rhOID)
        missingOids = []
        if lhDropSpec is None: missingOids.append(lhOID)
        if rhDropSpec is None: missingOids.append(rhOID)
        if missingOids:
            oids = 'OID' if len(missingOids) == 1 else 'OIDs'
            raise InvalidGraphException('No DROP found for %s %r' % (oids, missingOids))

        graph_loader.addLink(linkType, lhDropSpec, rhOID, force=force)

    def findByOidInParts(self, oid):
        if oid in self._graph:
            return self._graph[oid]
        return None

    def deploy(self, completedDrops=[]):
        """
        Creates the DROPs represented by all the graph specs contained in
        this session, effectively deploying them.

        When this method has finished executing a Pyro Daemon will also be
        up and running, servicing requests to access to all the DROPs
        belonging to this session
        """

        status = self.status
        if status != SessionStates.BUILDING:
            raise InvalidSessionState("Can't deploy this session in its current status: %d" % (status))

        self.status = SessionStates.DEPLOYING

        # Create the Pyro daemon that will serve the DROP proxies and start it
        logger.debug("Starting Pyro4 Daemon for session %s", self._sessionId)
        self._daemon = Pyro4.Daemon(host=self._host)
        self._daemonT = threading.Thread(target = lambda: self._daemon.requestLoop(), name="Session %s Pyro Daemon" % (self._sessionId))
        self._daemonT.daemon = True
        self._daemonT.start()

        # Create the real DROPs from the graph specs
        logger.debug("Creating DROPs for session %s", self._sessionId)

        self._roots = graph_loader.createGraphFromDropSpecList(self._graph.values())

        for drop,_ in droputils.breadFirstTraverse(self._roots):

            # Register them
            self._registerDrop(drop)

            # Register them with the error handler
            if self._error_status_listener:
                drop.subscribe(self._error_status_listener, eventType='status')

        # Start the luigi task that will make sure the graph is executed
        # If we're not using luigi we still
        if self._enable_luigi:
            logger.debug("Starting Luigi FinishGraphExecution task for session %s", self._sessionId)
            task = luigi_int.FinishGraphExecution(self._sessionId, self._roots)
            sch = scheduler.CentralPlannerScheduler()
            w = worker.Worker(scheduler=sch)
            w.add(task)
            workerT = threading.Thread(None, self._run, args=[w])
            workerT.daemon = True
            workerT.start()
        else:
            leaves = droputils.getLeafNodes(self._roots)
            logger.debug("Adding completion listener to leaf drops %r", leaves)
            listener = LeavesCompletionListener(leaves, self)
            for leaf in leaves:
                leaf.subscribe(listener, 'dropCompleted')
                leaf.subscribe(listener, 'producerFinished')

        # We move to COMPLETED the DROPs that we were requested to
        # InputFiredAppDROP are here considered as having to be executed and
        # not directly moved to COMPLETED.
        #
        # This is done in a separate iteration at the very end because all drops
        # to make sure all event listeners are ready
        for drop,_ in droputils.breadFirstTraverse(self._roots):
            if drop.uid in completedDrops:
                if isinstance(drop, InputFiredAppDROP):
                    drop.async_execute()
                else:
                    drop.setCompleted()

        self.status = SessionStates.RUNNING
        logger.info("Session %s is now RUNNING", self._sessionId)

    def _registerDrop(self, drop):
        uri = self._daemon.register(drop)
        drop.uri = uri.asString()
        logger.debug("Registered %r with Pyro. URI is %s", drop, uri)
        self._drops[drop.uid] = drop

    def _run(self, worker):
        worker.run()
        worker.stop()
        self.finish()

    def finish(self):
        self.status = SessionStates.FINISHED

    def getGraphStatus(self):
        if self.status not in (SessionStates.RUNNING, SessionStates.FINISHED):
            raise InvalidSessionState("The session is currently not running, cannot get graph status")

        # We shouldn't traverse the full graph because there might be nodes
        # attached to our DROPs that are actually part of other DM (and have been
        # wired together by the DIM after deploying each individual graph on
        # each of the DMs).
        # We recognize such nodes because they are actually not an instance of
        # AbstractDROP (they are Pyro4.Proxy instances).
        #
        # The same trick is used in luigi_int.RunDROPTask.requires
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
        """
        Destroys this session, shutting down the Pyro daemon if it exists.
        """
        if self._daemon:
            self._daemon.shutdown()
            self._daemon = None
            self._daemonT.join()

    __del__ = destroy

    # Support for the 'with' keyword
    def __enter__(self):
        return self
    def __exit__(self, typ, value, traceback):
        self.destroy()