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

from __future__ import annotations
import collections
import inspect
import json
import logging
import os
import threading
import time
import socket

from dlg.common.reproducibility.reproducibility import init_runtime_repro_data
from dlg.utils import createDirIfMissing

from dlg import droputils
from dlg import graph_loader
from dlg import rpc
from dlg import utils
from dlg.common.reproducibility.constants import ReproducibilityFlags, ALL_RMODES
from dlg.ddap_protocol import DROPLinkType, DROPRel, DROPStates
from dlg.drop import (
    AbstractDROP,
    LINKTYPE_1TON_APPEND_METHOD,
    LINKTYPE_1TON_BACK_APPEND_METHOD,
)
from ..apps.app_base import AppDROP, InputFiredAppDROP
from ..data.drops.data_base import EndDROP

from dlg.exceptions import (
    InvalidSessionState,
    InvalidGraphException,
    NoDropException,
    DaliugeException,
)


logger = logging.getLogger(f"dlg.{__name__}")


class SessionStates:
    """
    An enumeration of the different states in which a Session can be found at
    any given point of time.
    """

    PRISTINE, BUILDING, DEPLOYING, RUNNING, FINISHED, CANCELLED = range(6)


class LeavesCompletionListener(object):
    def __init__(self, leaves, session):
        self._session = session
        self._nexpected = len(leaves)
        self._completed = 0

    def handleEvent(self, evt):
        # TODO: be thread-safe
        self._completed += 1
        logger.debug(
            "%d/%d leaf drops completed on session %s during event %s",
            self._completed,
            self._nexpected,
            self._session.sessionId,
            evt
        )
        if self._completed == self._nexpected:
            self._session.finish()


class ReproFinishedListener(object):
    def __init__(self, graph, session):
        self._session = session
        self._nexpected = len(graph)
        self._completed = 0

    def handleEvent(self, evt):
        self._completed += 1
        self._session.append_reprodata(evt.oid, evt.reprodata)
        logger.debug(
            "%d/%d drops filed reproducibility",
            self._completed,
            self._nexpected,
        )
        if self._completed == self._nexpected:
            if not self._session.reprostatus:
                logger.debug("Building Reproducibility BlockDAG")
                new_reprodata = init_runtime_repro_data(
                    self._session.graph, self._session.graphreprodata
                ).get("reprodata", {})
                logger.debug(
                    "Reprodata for %s is %s",
                    self._session.sessionId,
                    json.dumps(new_reprodata),
                )
                self._session.graphreprodata = new_reprodata
                self._session.reprostatus = True
                self._session.write_reprodata()


class EndListener(object):
    """
    Listener for an EndDROP that will end the session when complete
    """

    def __init__(self, session):
        self._session = session

    def handleEvent(self, evt):
        logger.info("Handling event %s", evt)
        self._session.end()


track_current_session = utils.object_tracking("session")


def generateLogFileName(logdir, sessionId):
    hostname = socket.gethostname()
    ip = socket.gethostbyname(hostname)
    return f"{logdir}/dlg_{ip}_{sessionId}.log"


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

    def __init__(self, sessionId, nm: "NodeManager"=None):
        self._sessionId = sessionId
        self._graph = {}  # key: oid, value: dropSpec dictionary
        self._drops = {}  # key: oid, value: actual drop object
        self._statusLock = threading.Lock()
        self._roots = []
        self._proxyinfo = []
        self._worker = None
        self._status = SessionStates.PRISTINE
        self._error_status_listener = None
        self._nm = nm
        self._dropsubs = {}
        self._graphreprodata = None
        self._reprofinished = False

        # create the session directory and change CWD
        self._sessionDir = f"{utils.getDlgWorkDir()}/{sessionId}"
        logger.debug("Creating session directory: %s", self._sessionDir)
        createDirIfMissing(self._sessionDir)
        os.chdir(self._sessionDir)

        logger.debug("Updating ENV with SESSION_ID: %s", sessionId)
        os.environ.update({"DLG_SESSION_ID": sessionId})

        class SessionFilter(logging.Filter):
            def __init__(self, sessionId): #pylint: disable=super-init-not-called
                self.sessionId = sessionId

            def filter(self, record):
                return getattr(record, "session_id", None) == self.sessionId

        fmt = "%(asctime)-15s [%(levelname)5.5s] [%(threadName)15.15s] "
        fmt += "[%(drop_uid)10.10s] "
        fmt += "%(name)s#%(funcName)s:%(lineno)s %(message)s"
        fmt = logging.Formatter(fmt)
        if self._nm:
            fmt.converter = time.localtime if self._nm.use_local_time else time.gmtime
        else:
            fmt.converter = time.gmtime

        logfile = generateLogFileName(self._sessionDir, self.sessionId)
        try:
            self.file_handler = logging.FileHandler(logfile)
            self.file_handler.setFormatter(fmt)
            self.file_handler.addFilter(SessionFilter(self.sessionId))
            logging.root.addHandler(self.file_handler)
        except AttributeError as e:
            print(e)
        except FileNotFoundError as f:
            print(f)

    @property
    def sessionId(self):
        return self._sessionId

    @property
    def sessionDir(self):
        return self._sessionDir

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

    @property
    def reprostatus(self):
        return self._reprofinished

    @reprostatus.setter
    def reprostatus(self, status):
        with self._statusLock:  # TODO: Consider creating another lock
            self._reprofinished = status

    def write_reprodata(self):
        the_dir = self._sessionDir
        createDirIfMissing(the_dir)
        the_path = os.path.join(the_dir, "reprodata.out")
        with open(the_path, "w+", encoding="utf-8") as file:
            json.dump([self._graph, self._graphreprodata], file, indent=4)

    @track_current_session
    def addGraphSpec(self, graphSpec):
        """
        Adds the graph specification given in `graphSpec` to the
        graph specification currently held by this session. A graphSpec is a
        list of dictionaries, each of which contains the information of one
        DROP. Each DROP specification is checked to see it contains
        all the necessary details to construct a proper DROP. If one
        DROP specification is found to be inconsistent the whole operation
        will fail.

        This operation also 'slices off' a dictionary containing graph-wide
        reproducibility information. This is stored as a class variable for later use.

        Adding graph specs to the session is only allowed while the session is
        in the PRISTINE or BUILDING status; otherwise an exception will be
        raised.

        If the `graphSpec` being added contains DROPs that have already
        been added to the session an exception will be raised. DROPs are
        uniquely identified by their OID at this point.
        """

        status = self.status
        if status not in (SessionStates.PRISTINE, SessionStates.BUILDING):
            logger.exception(
                "Can't add graphs to this session since it isn't in the PRISTINE or "
                "BUILDING status: %d",
                status,
            )
            raise InvalidSessionState

        self.status = SessionStates.BUILDING

        # This will check the consistency of each dropSpec
        logger.debug("Trying to add graphSpec:")
        logger.debug("Number of drops: %s", len(graphSpec))

        # This is very excessive and should not be done in production
        # for x in graphSpec:
        #     logger.debug("%s: %s", x, x.keys())
        try:
            graphSpecDict, self._graphreprodata = graph_loader.loadDropSpecs(graphSpec)
            # Check for duplicates
            duplicates = set(graphSpecDict) & set(self._graph)
            if duplicates:
                logger.exception(
                    "Trying to add drops with OIDs that already exist: %r", duplicates
                )
                raise InvalidGraphException

            self._graph.update(graphSpecDict)

            logger.debug("Added a graph definition with %d DROPs", len(graphSpecDict))
        except InvalidGraphException as e:
            logger.exception(e)
            raise e

    @track_current_session
    def linkGraphParts(self, lhOID, rhOID, linkType, force=False):
        """
        Links together two DROP specifications (i.e., those pointed by
        `lhOID` and `rhOID`) using `linkType`. The DROP specifications
        must both already be part of one of the graph specs contained in this
        session; otherwise an exception will be raised.
        """
        if self.status != SessionStates.BUILDING:
            raise InvalidSessionState(
                "Can't link DROPs anymore since this session isn't in the BUILDING state"
            )

        # Look for the two DROPs in all our graph parts and reporting
        # missing DROPs
        lhDropSpec = self._graph.get(lhOID, None)
        rhDropSpec = self._graph.get(rhOID, None)
        missingOids = []
        if lhDropSpec is None:
            missingOids.append(lhOID)
        if rhDropSpec is None:
            missingOids.append(rhOID)
        if missingOids:
            oids = "OID" if len(missingOids) == 1 else "OIDs"
            raise InvalidGraphException("No DROP found for %s %r" % (oids, missingOids))

        graph_loader.addLink(linkType, lhDropSpec, rhOID, force=force)

    @track_current_session
    def deploy(
        self,
        completedDrops: list[str] = None,
        event_listeners: list = None,
        foreach=None,
    ):
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
        if not completedDrops:
            completedDrops = []
        status = self.status
        if (self._graph and status != SessionStates.BUILDING) or (
            not self._graph and status != SessionStates.PRISTINE
        ):
            logger.exception(
                "Can't deploy this session in its current status: %d", status
            )
            raise InvalidSessionState

        if not self._graph and completedDrops:
            logger.exception(
                "Drops are requested for immediate completion but none will be created"
            )
            raise InvalidGraphException

        # Shortchut
        if not self._graph:
            self.finish()
            return

        self.status = SessionStates.DEPLOYING

        # Create the real DROPs from the graph specs
        logger.info("Creating DROPs for session %s", self._sessionId)

        try:
            self._roots = graph_loader.createGraphFromDropSpecList(
                list(self._graph.values()), session=self
            )

        except KeyError as e:
            logger.exception(e)
            raise e
        except ModuleNotFoundError as e:
            logger.exception(e)
            raise e
        logger.info("%d drops successfully created", len(self._graph))

        #  Add listeners for reproducibility information
        repro_listener = ReproFinishedListener(self._graph, self)

        for drop, _ in droputils.breadFirstTraverse(self._roots):

            # Register them
            self._drops[drop.uid] = drop

            # Add a reference to the RPC server endpoint that exposes this drop,
            # which is our containing Node Manager.
            # This information is usually not necessary, but there are cases in
            # which we actually need it (like in the DynlibProcApp)
            if self._nm:
                drop.rpc_endpoint = self._nm.rpc_endpoint

            # Register them with the error handler
            if event_listeners:
                for l in event_listeners:
                    drop.subscribe(l)
            #  Register each drop for reproducibility listening
            drop.subscribe(repro_listener, "reproducibility")

        logger.info("Stored all drops, proceeding with further customization")

        # Add listeners that will move the session to FINISHED state
        leaves = droputils.getLeafNodes(self._roots)
        logger.info("Adding completion listener to leaf drops")

        # The leaves completion listener will trigger session completed
        # when all leaf nodes are completed
        leavesListener = LeavesCompletionListener(leaves, self)
        for leaf in leaves:
            if isinstance(leaf, AppDROP):
                leaf.subscribe(leavesListener, "producerFinished")
            else:
                leaf.subscribe(leavesListener, "dropCompleted")

        # The end listener will trigger session end when an end
        # node is completed (end nodes are always a leaf nodes)
        endListener = EndListener(self)
        for leaf in leaves:
            if isinstance(leaf, EndDROP):
                leaf.subscribe(endListener, "dropCompleted")
        logger.info("Listener added to leaf drops")

        # Foreach
        if foreach:
            logger.info("Invoking 'foreach' on each drop")
            for drop, _ in droputils.breadFirstTraverse(self._roots):
                foreach(drop)
            logger.info("'foreach' invoked for each drop")

        # Append proxies
        logger.info(
            "Creating %d drop proxies",
            len(self._proxyinfo),
        )
        for host, port, local_uid, relname, remote_uid in self._proxyinfo:
            proxy = rpc.DropProxy(
                self._nm, rpc.ProxyInfo(host, port, self._sessionId, remote_uid)
            )
            logger.debug(
                "Attaching proxy to local %r via %s(proxy, False)",
                self._drops[local_uid],
                relname,
            )
            method = getattr(self._drops[local_uid], relname)
            method(proxy, False)

        # We move to COMPLETED the DROPs that we were requested to
        # InputFiredAppDROP are here considered as having to be executed and
        # not directly moved to COMPLETED.
        #
        # This is done in a separate iteration at the very end because all drops
        # to make sure all event listeners are ready
        self.status = SessionStates.RUNNING
        self.trigger_drops(completedDrops)

        logger.info("Session %s is now RUNNING", self._sessionId)

    def _run(self, worker):
        worker.run()
        worker.stop()
        self.finish()

    def trigger_drops(self, uids):
        for drop, downStreamDrops in droputils.breadFirstTraverse(self._roots):
            downStreamDrops[:] = [
                dsDrop for dsDrop in downStreamDrops if isinstance(dsDrop, AbstractDROP)
            ]
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
        if evt.uid not in self._dropsubs:
            logger.debug("No subscription found for drop %s", evt.uid)
            return
        for tgt in self._dropsubs[evt.uid]:
            drop = self._drops[tgt]
            logger.debug("Passing event %r to %r", evt, drop)
            drop.handleEvent(evt)

    def add_node_subscriptions(self, relationships):
        # do we translate on the REST side? Probably best to do this actually.

        evt_consumer = (
            DROPLinkType.CONSUMER,
            DROPLinkType.STREAMING_CONSUMER,
            DROPLinkType.OUTPUT,
        )
        evt_producer = (
            DROPLinkType.INPUT,
            DROPLinkType.STREAMING_INPUT,
            DROPLinkType.PRODUCER,
        )

        for host, droprels in relationships.items():

            # Make sure we have DROPRel tuples
            droprels = [DROPRel(*x) for x in droprels]

            # Sanitize the host/rpc_port info if needed
            rpc_port = host.rpc_port

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
                if (rel.rel in evt_consumer and rel.lhs is local_uid) or (
                    rel.rel in evt_producer and rel.rhs is local_uid
                ):
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

    def append_reprodata(self, oid, reprodata):
        if oid in self._graph:
            if self._graph[oid].get("reprodata") is None:
                return
            if self._graph[oid]["reprodata"]["rmode"] == str(
                ReproducibilityFlags.ALL.value
            ):
                drop_reprodata = reprodata.get("data", {})
                drop_hashes = reprodata.get("merkleroot", {})
                for rmode in ALL_RMODES:
                    self._graph[oid]["reprodata"][rmode.name]["rg_data"] = (
                        drop_reprodata[rmode.name]
                    )
                    self._graph[oid]["reprodata"][rmode.name]["rg_data"][
                        "merkleroot"
                    ] = drop_hashes.get(rmode.name, b"")

            else:
                self._graph[oid]["reprodata"]["rg_data"] = reprodata.get("data", {})
                self._graph[oid]["reprodata"]["rg_data"]["merkleroot"] = reprodata.get(
                    "merkleroot", b""
                )

    @track_current_session
    def finish(self):
        self.status = SessionStates.FINISHED
        logger.info("Session %s finished", self._sessionId)
        for drop, downStreamDrops in droputils.breadFirstTraverse(self._roots):
            downStreamDrops[:] = [
                dsDrop for dsDrop in downStreamDrops if isinstance(dsDrop, AbstractDROP)
            ]
            if drop.status in (DROPStates.INITIALIZED, DROPStates.WRITING):
                drop.setCompleted()

    @track_current_session
    def end(self):
        self.status = SessionStates.FINISHED
        logger.info("Session %s ended", self._sessionId)
        for drop, downStreamDrops in droputils.breadFirstTraverse(self._roots):
            downStreamDrops[:] = [
                dsDrop for dsDrop in downStreamDrops if isinstance(dsDrop, AbstractDROP)
            ]
            if drop.status in (DROPStates.INITIALIZED, DROPStates.WRITING):
                drop.skip()

    def getGraphStatus(self):
        if self.status not in (
            SessionStates.RUNNING,
            SessionStates.FINISHED,
            SessionStates.CANCELLED,
        ):
            raise InvalidSessionState(
                "The session is currently not running, cannot get graph status"
            )

        # We shouldn't traverse the full graph because there might be nodes
        # attached to our DROPs that are actually part of other DM (and have been
        # wired together by the DIM after deploying each individual graph on
        # each of the DMs).
        # We recognize such nodes because they are actually not an instance of
        # AbstractDROP (they are DropProxy instances).
        statusDict = collections.defaultdict(dict)
        for drop, downStreamDrops in droputils.breadFirstTraverse(self._roots):
            downStreamDrops[:] = [
                dsDrop for dsDrop in downStreamDrops if isinstance(dsDrop, AbstractDROP)
            ]
            if isinstance(drop, AppDROP):
                statusDict[drop.oid]["execStatus"] = drop.execStatus
            statusDict[drop.oid]["status"] = drop.status

        return statusDict

    def getDropLogs(self, drop_oid: str):
        """
        Retrieve the logs stored in the given DROP
        :param drop_oid: drop_oid

        :return:
        """
        return {"session": self.sessionId,
                "status": self.status,
                "oid": drop_oid,
                "logs": self._drops[drop_oid].getLogs()}
                # "stderr": self._drops[drop_oid].getStdError(),
                # "stdout": self._drops[drop_oid].getStdOut()}


    @track_current_session
    def cancel(self):
        status = self.status
        if status != SessionStates.RUNNING:
            raise InvalidSessionState(
                "Can't cancel this session in its current status: %d" % (status)
            )
        for drop, downStreamDrops in droputils.breadFirstTraverse(self._roots):
            downStreamDrops[:] = [
                dsDrop for dsDrop in downStreamDrops if isinstance(dsDrop, AbstractDROP)
            ]
            if drop.status not in (
                DROPStates.ERROR,
                DROPStates.COMPLETED,
                DROPStates.CANCELLED,
            ):
                drop.cancel()
        self.status = SessionStates.CANCELLED
        logger.info("Session %s cancelled", self._sessionId)

    def getGraph(self):
        return dict(self._graph)

    @property
    def graph(self):
        return self._graph

    @property
    def graphreprodata(self):
        return self._graphreprodata

    @graphreprodata.setter
    def graphreprodata(self, data):
        self._graphreprodata = data

    def destroy(self):
        try:
            self.file_handler.close()
            logging.root.removeHandler(self.file_handler)
        except AttributeError as e:
            print(e)

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
        except AttributeError as e:
            raise DaliugeException(
                "%r has no property called %s" % (drop, prop_name)) from e

    def call_drop(self, uid, method, *args):
        if uid not in self._drops:
            raise NoDropException(uid)
        try:
            drop = self._drops[uid]
            m = getattr(drop, method)
        except AttributeError as e:
            raise DaliugeException(
                "%r has no method called %s" % (drop, method)) from e
        return m(*args)

    # Support for the 'with' keyword
    def __enter__(self):
        return self

    def __exit__(self, typ, value, traceback):
        self.destroy()
