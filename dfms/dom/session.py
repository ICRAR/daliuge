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
import logging
import threading

import Pyro4
from luigi import scheduler, worker

from dfms import luigi_int, graph_loader, doutils
from dfms.ddap_protocol import DOLinkType


_LINKTYPE_TO_NREL = {
    DOLinkType.CONSUMER: 'consumers',
    DOLinkType.STREAMING_CONSUMER: 'streamingConsumers',
    DOLinkType.CHILD: 'children',
    DOLinkType.INPUT: 'inputs',
    DOLinkType.STREAMING_INPUT: 'streamingInputs',
    DOLinkType.OUTPUT: 'outputs',
    DOLinkType.PRODUCER: 'producers'
}
_LINKTYPE_TO_REL = {
    DOLinkType.PARENT: 'parent',
}

logger = logging.getLogger(__name__)

class SessionStates:
    """
    An enumeration of the different states in which a Session can be found at
    any given point of time.
    """
    PRISTINE, DEPLOYING, RUNNING, FINISHED = xrange(4)

class Session(object):
    """
    A DataObject graph execution.

    A session is the runtime incarnation of a given DataObject graph that gets
    executed in our framework. Thus, different executions of the same graph
    template lead to different sessions.

    In order to be flexible enough, a session starts in a PRISTINE status. While
    in PRISTINE status "graph parts" can be added to it in separate calls, which
    can be connected later. Once all the parts have been submitted, users can
    finally deploySession the graph, which will move the session to the DEPLOYING
    status first, and to the RUNNING status later. Once the execution of the
    graph has finished the session is moved to FINISHED.
    """

    def __init__(self, sessionId):
        self._sessionId = sessionId
        self._graph = {} # key: oid, value: doSpec dictionary
        self._statusLock = threading.Lock()
        self._roots = []
        self._daemon = None
        self._worker = None
        self._status = SessionStates.PRISTINE

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

    def addGraphSpec(self, graphSpec):
        """
        Adds the graph specification given in `graphSpec` to the list of graph
        specifications currently held by this session. A graphSpec is a list of
        dictionaries, each of which contains the information of one DataObject.

        Adding graph specs to the session is only allowed while the session is
        in the PRISTINE status; otherwise an exception will be raised.

        If the `graphSpec` being added contains DataObjects that have already
        been added to the session an exception will be raised. DataObjects are
        uniquely identified by their OID at this point.
        """

        if self.status != SessionStates.PRISTINE:
            raise Exception("Can't add more graphs to this session since itn't PRISTINE anymore")

        if isinstance(graphSpec, basestring):
            graphSpecDict = graph_loader.loadDataObjectSpecsS(graphSpec)
        elif isinstance(graphSpec, dict):
            graphSpecDict = graphSpec
        elif isinstance(graphSpec, list):
            graphSpecDict = {}
            [graphSpecDict.__setitem__(spec['oid'],spec) for spec in graphSpec]
        elif hasattr(graphSpec, 'read'):
            graphSpecDict = graph_loader.loadDataObjectSpecs(graphSpec)
        else:
            raise TypeError('graphSpec should be either a string or a file-like object')

        for oid in graphSpecDict:
            if oid in self._graph:
                raise Exception('DataObject with OID %s already exists, cannot add twice' % (oid))

        self._graph.update(graphSpecDict)

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Added a graph definition with %d DataObjects" % (len(graphSpecDict)))

    def linkGraphParts(self, lhOID, rhOID, linkType, force=False):
        """
        Links together two DataObject specifications (i.e., those pointed by
        `lhOID` and `rhOID`) using `linkType`. The DataObject specifications
        must both already be part of one of the graph specs contained in this
        session; otherwise an exception will be raised.
        """
        if self.status != SessionStates.PRISTINE:
            raise Exception("Can't link DOs anymore since this session isn't PRISTINE anymore")

        # Look for the two DataObjects in all our graph parts and reporting
        # missing DOs
        lhDOSpec = self.findByOidInParts(lhOID)
        rhDOSpec = self.findByOidInParts(rhOID)
        missingOids = []
        if lhDOSpec is None: missingOids.append(lhOID)
        if rhDOSpec is None: missingOids.append(rhOID)
        if missingOids:
            oids = 'OID' if len(missingOids) == 1 else 'OIDs'
            raise Exception('No DataObject found for %s %s' % (oids, missingOids))

        # 1-N relationship
        if linkType in _LINKTYPE_TO_NREL:
            rel = _LINKTYPE_TO_NREL[linkType]
            if not rel in lhDOSpec:
                relList = []
                lhDOSpec[rel] = relList
            else:
                relList = lhDOSpec[rel]
            if rhOID not in relList:
                relList.append(rhOID)
            else:
                raise Exception("DataObject %s is already part of %s's %s" % (rhOID, lhOID, rel))
        # N-1 relationship, overwrite existing relationship only if `force` is specified
        elif linkType in _LINKTYPE_TO_REL:
            rel = _LINKTYPE_TO_REL[linkType]
            if rel and not force:
                raise Exception("DataObject %s already has a '%s', use 'force' to override" % (lhOID, rel))
            lhDOSpec[rel] = rhOID
        else:
            raise ValueError("Cannot handle link type %d" % (linkType))

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Successfully linked %s and %s via '%s'" % (lhOID, rhOID, rel))

    def findByOidInParts(self, oid):
        if oid in self._graph:
            return self._graph[oid]
        return None

    def deploy(self):
        """
        Creates the DataObjects represented by all the graph specs contained in
        this session, effectively deploying them.

        When this method has finished executing a Pyro Daemon will also be
        up and running, servicing requests to access to all the DataObjects
        belonging to this session
        """
        if self.status != SessionStates.PRISTINE:
            raise Exception("Can't deploy this session since its status is not PRISTINE")

        self.status = SessionStates.DEPLOYING

        # Create the Pyro daemon that will serve the DO proxies and start it
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Starting Pyro4 Daemon for session %s" % (self._sessionId))
        self._daemon = Pyro4.Daemon()
        self._daemonT = threading.Thread(target = lambda: self._daemon.requestLoop(), name="Session %s Pyro Daemon" % (self._sessionId))
        self._daemonT.daemon = True
        self._daemonT.start()

        # Create the real DataObjects from the graph specs
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Creating DataObjects for session %s" % (self._sessionId))

        self._roots = graph_loader.createGraphFromDOSpecList(self._graph.values())

        # Register them
        doutils.breadFirstTraverse(self._roots, self._registerDataObject)

        # Start the luigi task that will make sure the graph is executed
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Starting Luigi FinishGraphExecution task for session %s" % (self._sessionId))
        task = luigi_int.FinishGraphExecution(self._sessionId, self._roots)
        sch = scheduler.CentralPlannerScheduler()
        w = worker.Worker(scheduler=sch)
        w.add(task)
        workerT = threading.Thread(None, self._run, args=[w])
        workerT.daemon = True
        workerT.start()

        self.status = SessionStates.RUNNING
        if logger.isEnabledFor(logging.INFO):
            logger.info("Session %s is now RUNNING" % (self._sessionId))

    def _registerDataObject(self, dataObject):
        uri = self._daemon.register(dataObject)
        dataObject.uri = uri

    def _run(self, worker):
        worker.run()
        worker.stop()
        self.status = SessionStates.FINISHED

    def getGraphStatus(self):
        if self.status not in (SessionStates.RUNNING, SessionStates.FINISHED):
            raise Exception("The session is currently not running, cannot get graph status")
        statusDict = {}
        doutils.breadFirstTraverse(self._roots, lambda do: statusDict.__setitem__(do.oid, do.status))
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