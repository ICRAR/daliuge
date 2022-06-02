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
Module containing the REST layer that exposes the methods of the different
Data Managers (DROPManager and DataIslandManager) to the outside world.
"""

import cgi
import functools
import io
import json
import logging
import os
import tarfile
import threading

import bottle
import pkg_resources

from bottle import static_file

from . import constants
from .client import NodeManagerClient, DataIslandManagerClient
from .. import utils
from ..exceptions import (
    InvalidGraphException,
    InvalidSessionState,
    DaliugeException,
    NoSessionException,
    SessionAlreadyExistsException,
    InvalidDropException,
    InvalidRelationshipException,
    SubManagerException,
)
from ..restserver import RestServer
from ..restutils import RestClient, RestClientException
from .session import generateLogFileName

logger = logging.getLogger(__name__)


def file_as_string(fname, enc="utf8"):
    b = pkg_resources.resource_string(__name__, fname)  # @UndefinedVariable
    return utils.b2s(b, enc)


def daliuge_aware(func):
    @functools.wraps(func)
    def fwrapper(*args, **kwargs):
        try:
            res = func(*args, **kwargs)

            if isinstance(res, (bytes, bottle.HTTPResponse)):
                return res

            if res is not None:
                bottle.response.content_type = "application/json"
                # set CORS headers
                bottle.response.headers[
                    "Access-Control-Allow-Origin"
                ] = "http://localhost:8084"
                bottle.response.headers["Access-Control-Allow-Credentials"] = "true"
                bottle.response.headers[
                    "Access-Control-Allow-Methods"
                ] = "GET, POST, PUT, OPTIONS"
                bottle.response.headers[
                    "Access-Control-Allow-Headers"
                ] = "Origin, Accept, Content-Type, Content-Encoding, X-Requested-With, X-CSRF-Token"
            return json.dumps(res)
        except Exception as e:
            logger.exception("Error while fulfilling request")

            status, eargs = 500, ()
            if isinstance(e, NotImplementedError):
                status, eargs = 501, e.args
            elif isinstance(e, NoSessionException):
                status, eargs = 404, (e._session_id,)
            elif isinstance(e, SessionAlreadyExistsException):
                status, eargs = 409, (e._session_id,)
            elif isinstance(e, InvalidDropException):
                status, eargs = 409, ((e.oid, e.uid), e.reason)
            elif isinstance(e, InvalidRelationshipException):
                status, eargs = 409, (e.rel, e.reason)
            elif isinstance(e, InvalidGraphException):
                status, eargs = 400, e.args
            elif isinstance(e, InvalidSessionState):
                status, eargs = 400, e.args
            elif isinstance(e, RestClientException):
                status, eargs = 556, e.args
            elif isinstance(e, SubManagerException):
                status = 555
                eargs = {}
                # args[1] is a dictionary of host:exception
                for host, subex in e.args[1].items():
                    eargs[host] = {"type": subex.__class__.__name__, "args": subex.args}
            elif isinstance(e, DaliugeException):
                status, eargs = 555, e.args
            else:
                raise

            error = {"type": e.__class__.__name__, "args": eargs}
            bottle.response.status = status
            return json.dumps(error)

    return fwrapper


class ManagerRestServer(RestServer):
    """
    An object that wraps a DataManager and exposes its methods via a REST
    interface. The server is started via the `start` method in a separate thread
    and runs until the process is shut down.

    This REST server currently also serves HTML pages in some of its methods
    (i.e. those not under /api).
    """

    def __init__(self, dm, maxreqsize=10):

        super(ManagerRestServer, self).__init__()

        # Increase maximum file sizes
        bottle.BaseRequest.MEMFILE_MAX = maxreqsize * 1024 * 1024

        self.dm = dm

        # Mappings
        app = self.app
        app.post("/api/stop", callback=self.stop_manager)
        app.post("/api/sessions", callback=self.createSession)
        app.get("/api/sessions", callback=self.getSessions)
        app.get("/api/sessions/<sessionId>", callback=self.getSessionInformation)
        app.delete("/api/sessions/<sessionId>", callback=self.destroySession)
        app.get("/api/sessions/<sessionId>/logs", callback=self.getLogFile)
        app.get("/api/sessions/<sessionId>/status", callback=self.getSessionStatus)
        app.post("/api/sessions/<sessionId>/deploy", callback=self.deploySession)
        app.post("/api/sessions/<sessionId>/cancel", callback=self.cancelSession)
        app.get("/api/sessions/<sessionId>/graph", callback=self.getGraph)
        app.get("/api/sessions/<sessionId>/graph/size", callback=self.getGraphSize)
        app.get("/api/sessions/<sessionId>/graph/status", callback=self.getGraphStatus)
        app.post("/api/sessions/<sessionId>/graph/append", callback=self.addGraphParts)
        app.get(
            "/api/sessions/<sessionId>/repro/data", callback=self.getSessionReproData
        )
        app.get(
            "/api/sessions/<sessionId>/repro/status",
            callback=self.getSessionReproStatus,
        )

        app.route("/api/sessions", method="OPTIONS", callback=self.acceptPreflight)
        app.route(
            "/api/sessions/<sessionId>/graph/append",
            method="OPTIONS",
            callback=self.acceptPreflight2,
        )

        # The non-REST mappings that serve HTML-related content
        app.route("/static/<filepath:path>", callback=self.server_static)
        app.get("/session", callback=self.visualizeSession)

        # sub-class specifics
        self.initializeSpecifics(app)

    def initializeSpecifics(self, app):
        """
        Methods through which subclasses can initialize other mappings on top of
        the default ones and perform other DataManager-specific actions.
        The default implementation does nothing.
        """

    def _stop_manager(self):
        self.dm.shutdown()
        self.stop()
        logger.info(
            "Thanks for using our %s, come back again :-)",
            self.dm.__class__.__name__
        )

    @daliuge_aware
    def stop_manager(self):
        threading.Thread(target=self._stop_manager).start()

    @daliuge_aware
    def createSession(self):
        newSession = bottle.request.json
        sessionId = newSession["sessionId"]
        self.dm.createSession(sessionId)
        return {"sessionId": sessionId}

    @daliuge_aware
    def acceptPreflight(self):
        return {}

    @daliuge_aware
    def acceptPreflight2(self, sessionId):
        return {}

    def sessions(self):
        sessions = []
        for sessionId in self.dm.getSessionIds():
            sessions.append(
                {
                    "sessionId": sessionId,
                    "status": self.dm.getSessionStatus(sessionId),
                    "size": self.dm.getGraphSize(sessionId),
                }
            )
        return sessions

    @daliuge_aware
    def getSessions(self):
        return self.sessions()

    @daliuge_aware
    def getSessionInformation(self, sessionId):
        status = self.dm.getSessionStatus(sessionId)
        try:
            graphDict = self.dm.getGraph(sessionId)
        except:  # Pristine state sessions don't have a graph, yet.
            graphDict = {}
            status = 0
        return {"status": status, "graph": graphDict}

    @daliuge_aware
    def getSessionReproStatus(self, sessionId):
        return self.dm.getSessionReproStatus(sessionId)

    @daliuge_aware
    def getSessionsReproStatus(self):
        sessions = []
        for sessionId in self.dm.getSessionIds():
            sessions.append(
                {
                    "sessionId": sessionId,
                    "status": self.dm.getSessionStatus(sessionId),
                    "size": self.dm.getGraphSize(sessionId),
                    "repro": self.dm.getSessionReproStatus(sessionId),
                }
            )
        return sessions

    @daliuge_aware
    def getSessionReproData(self, sessionId):
        #  For now, we only have information on a per-graph basis.
        graphDict = self.dm.getGraph(sessionId)
        reprodata = self.dm.getGraphReproData(sessionId)
        return {"graph": graphDict, "reprodata": reprodata}

    @daliuge_aware
    def destroySession(self, sessionId):
        self.dm.destroySession(sessionId)

    @daliuge_aware
    def getSessionStatus(self, sessionId):
        return self.dm.getSessionStatus(sessionId)

    @daliuge_aware
    def deploySession(self, sessionId):
        completedDrops = []
        if "completed" in bottle.request.forms:
            completedDrops = bottle.request.forms["completed"].split(",")
        self.dm.deploySession(sessionId, completedDrops=completedDrops)
        return {}

    @daliuge_aware
    def cancelSession(self, sessionId):
        self.dm.cancelSession(sessionId)

    @daliuge_aware
    def getGraph(self, sessionId):
        return self.dm.getGraph(sessionId)

    @daliuge_aware
    def getGraphSize(self, sessionId):
        return self.dm.getGraphSize(sessionId)

    @daliuge_aware
    def getGraphStatus(self, sessionId):
        return self.dm.getGraphStatus(sessionId)

    # TODO: addGraphParts v/s addGraphSpec
    @daliuge_aware
    def addGraphParts(self, sessionId):
        # WARNING: TODO: Somehow, the content_type can be overwritten to 'text/plain'
        logger.debug(bottle.request.content_type)
        if (
            "application/json" not in bottle.request.content_type
            and "text/plain" not in bottle.request.content_type
        ):
            bottle.response.status = 415
            return

        # We also accept gzipped content
        hdrs = bottle.request.headers
        if hdrs.get("Content-Encoding", None) == "gzip":
            json_content = utils.ZlibUncompressedStream(bottle.request.body)
        else:
            json_content = bottle.request.body

        graph_parts = bottle.json_loads(json_content.read())

        self.dm.addGraphSpec(sessionId, graph_parts)
        return {"graph_parts": graph_parts}

    # ===========================================================================
    # non-REST methods
    # ===========================================================================
    def server_static(self, filepath):
        staticRoot = pkg_resources.resource_filename(
            __name__, "/web/static"
        )  # @UndefinedVariable
        return bottle.static_file(filepath, root=staticRoot)

    def visualizeSession(self):
        params = bottle.request.params
        sessionId = params["sessionId"] if "sessionId" in params else ""
        selectedNode = params["node"] if "node" in params else ""
        viewMode = params["view"] if "view" in params else ""
        tpl = file_as_string("web/session.html")
        urlparts = bottle.request.urlparts
        serverUrl = urlparts.scheme + "://" + urlparts.netloc
        return bottle.template(
            tpl,
            sessionId=sessionId,
            selectedNode=selectedNode,
            viewMode=viewMode,
            serverUrl=serverUrl,
            dmType=self.dm.__class__.__name__,
        )


class NMRestServer(ManagerRestServer):
    """
    A REST server for NodeManagers. It includes mappings for NM-specific
    methods and the mapping for the main visualization HTML pages.
    """

    def initializeSpecifics(self, app):
        app.get("/api", callback=self.getNMStatus)
        app.post("/api/sessions/<sessionId>/graph/link", callback=self.linkGraphParts)
        app.post(
            "/api/sessions/<sessionId>/subscriptions",
            callback=self.add_node_subscriptions,
        )
        app.post("/api/sessions/<sessionId>/trigger", callback=self.trigger_drops)
        # The non-REST mappings that serve HTML-related content
        app.get("/", callback=self.visualizeDM)
        app.get("/api/shutdown", callback=self.shutdown_node_manager)

    @daliuge_aware
    def shutdown_node_manager(self):
        logger.debug("Shutting down node manager")
        self.dm.shutdown()

    @daliuge_aware
    def getNMStatus(self):
        # we currently return the sessionIds, more things might be added in the
        # future
        logger.debug("NM REST call: status")
        return {"sessions": self.sessions()}

    @daliuge_aware
    def getLogFile(self, sessionId):
        logger.debug("NM REST call: logfile")
        logdir = self.dm.getLogDir()
        logfile = generateLogFileName(logdir, sessionId)
        if not os.path.isfile(logfile):
            raise NoSessionException(sessionId, "Log file not found.")
        return static_file(
            os.path.basename(logfile), root=logdir, download=os.path.basename(logfile)
        )

    @daliuge_aware
    def linkGraphParts(self, sessionId):
        logger.debug("NM REST call: graph/link")
        params = bottle.request.params
        lhOID = params["lhOID"]
        rhOID = params["rhOID"]
        linkType = int(params["linkType"])
        self.dm.linkGraphParts(sessionId, lhOID, rhOID, linkType)

    @daliuge_aware
    def add_node_subscriptions(self, sessionId):
        logger.debug("NM REST call: add_subscriptions %s", bottle.request.json)
        if bottle.request.content_type != "application/json":
            bottle.response.status = 415
            return
        self.dm.add_node_subscriptions(sessionId, bottle.request.json)

    @daliuge_aware
    def trigger_drops(self, sessionId):
        if bottle.request.content_type != "application/json":
            bottle.response.status = 415
            return
        self.dm.trigger_drops(sessionId, bottle.request.json)

    # ===========================================================================
    # non-REST methods
    # ===========================================================================
    def visualizeDM(self):
        tpl = file_as_string("web/dm.html")
        urlparts = bottle.request.urlparts
        serverUrl = urlparts.scheme + "://" + urlparts.netloc
        return bottle.template(
            tpl, serverUrl=serverUrl, dmType=self.dm.__class__.__name__, reset="false"
        )


class CompositeManagerRestServer(ManagerRestServer):
    """
    A REST server for DataIslandManagers. It includes mappings for DIM-specific
    methods.
    """

    def initializeSpecifics(self, app):
        app.get("/api", callback=self.getCMStatus)
        app.get("/api/nodes", callback=self.getCMNodes)
        app.post("/api/nodes/<node>", callback=self.addCMNode)
        app.delete("/api/nodes/<node>", callback=self.removeCMNode)

        # Query forwarding to sub-nodes
        app.get("/api/nodes/<node>/sessions", callback=self.getNodeSessions)
        app.get(
            "/api/nodes/<node>/sessions/<sessionId>",
            callback=self.getNodeSessionInformation,
        )
        app.get(
            "/api/nodes/<node>/sessions/<sessionId>/status",
            callback=self.getNodeSessionStatus,
        )
        app.get(
            "/api/nodes/<node>/sessions/<sessionId>/graph", callback=self.getNodeGraph
        )
        app.get(
            "/api/nodes/<node>/sessions/<sessionId>/graph/status",
            callback=self.getNodeGraphStatus,
        )

        # The non-REST mappings that serve HTML-related content
        app.get("/", callback=self.visualizeDIM)

    @daliuge_aware
    def getCMStatus(self):
        return {"hosts": self.dm.dmHosts, "sessionIds": self.dm.getSessionIds()}

    @daliuge_aware
    def getCMNodes(self):
        return self.dm.nodes

    def getAllCMNodes(self):
        return self.dm.nodes

    @daliuge_aware
    def addCMNode(self, node):
        logger.debug("Adding node %s", node)
        self.dm.add_node(node)

    @daliuge_aware
    def removeCMNode(self, node):
        logger.debug("Removing node %s", node)
        self.dm.remove_node(node)

    @daliuge_aware
    def getNodeSessions(self, node):
        if node not in self.dm.nodes:
            raise Exception(f"{node} not in current list of nodes")
        with NodeManagerClient(host=node) as dm:
            return dm.sessions()

    def _tarfile_write(self, tar, headers, stream):
        file_header = headers.getheader("Content-Disposition")
        length = headers.getheader("Content-Length")
        _, params = cgi.parse_header(file_header)
        filename = params["filename"]
        info = tarfile.TarInfo(filename)
        info.size = int(length)

        content = []
        while True:
            buffer = stream.read()
            if not buffer:
                break
            content.append(buffer)

        tar.addfile(info, io.BytesIO(initial_bytes="".join(content).encode()))

    @daliuge_aware
    def getLogFile(self, sessionId):
        fh = io.BytesIO()
        with tarfile.open(fileobj=fh, mode="w:gz") as tar:
            for node in self.getAllCMNodes():
                with NodeManagerClient(host=node) as dm:
                    try:
                        stream, resp = dm.get_log_file(sessionId)
                        self._tarfile_write(tar, resp, stream)
                    except NoSessionException:
                        pass

        data = fh.getvalue()
        size = len(data)
        bottle.response.set_header("Content-type", "application/x-tar")
        bottle.response["Content-Disposition"] = (
            f"attachment; " f"filename=dlg_{sessionId}.tar"
        )
        bottle.response["Content-Length"] = size
        return data

    @daliuge_aware
    def getNodeSessionInformation(self, node, sessionId):
        if node not in self.dm.nodes:
            raise Exception(f"{node} not in current list of nodes")
        with NodeManagerClient(host=node) as dm:
            return dm.session(sessionId)

    @daliuge_aware
    def getNodeSessionStatus(self, node, sessionId):
        if node not in self.dm.nodes:
            raise Exception(f"{node} not in current list of nodes")
        with NodeManagerClient(host=node) as dm:
            return dm.session_status(sessionId)

    @daliuge_aware
    def getNodeGraph(self, node, sessionId):
        if node not in self.dm.nodes:
            raise Exception(f"{node} not in current list of nodes")
        with NodeManagerClient(host=node) as dm:
            return dm.graph(sessionId)

    @daliuge_aware
    def getNodeGraphStatus(self, node, sessionId):
        if node not in self.dm.nodes:
            raise Exception(f"{node} not in current list of nodes")
        with NodeManagerClient(host=node) as dm:
            return dm.graph_status(sessionId)

    # ===========================================================================
    # non-REST methods
    # ===========================================================================
    def visualizeDIM(self):
        tpl = file_as_string("web/dim.html")
        urlparts = bottle.request.urlparts
        selectedNode = (
            bottle.request.params["node"] if "node" in bottle.request.params else ""
        )
        serverUrl = urlparts.scheme + "://" + urlparts.netloc
        return bottle.template(
            tpl,
            dmType=self.dm.__class__.__name__,
            dmPort=self.dm.dmPort,
            serverUrl=serverUrl,
            dmHosts=json.dumps(self.dm.dmHosts),
            nodes=json.dumps(self.dm.nodes),
            selectedNode=selectedNode,
        )


class MasterManagerRestServer(CompositeManagerRestServer):
    def initializeSpecifics(self, app):
        CompositeManagerRestServer.initializeSpecifics(self, app)
        # DIM manamagement
        app.post("/api/islands/<dim>", callback=self.addDIM)
        app.delete("/api/islands/<dim>", callback=self.removeDIM)
        # Query forwarding to daemons
        app.post("/api/managers/<host>/dataisland", callback=self.createDataIsland)
        app.post("/api/managers/<host>/node/start", callback=self.startNM)
        app.post("/api/managers/<host>/node/stop", callback=self.stopNM)
        app.post("/api/managers/<host>/nodes/<node>", callback=self.addNM)
        app.delete("/api/managers/<host>/nodes/<node>", callback=self.removeNM)
        # Querying about managers
        app.get("/api/islands", callback=self.getDIMs)
        app.get("/api/nodes", callback=self.getNMs)
        app.get("/api/managers/<host>/node", callback=self.getNMInfo)
        app.get("/api/managers/<host>/dataisland", callback=self.getDIMInfo)
        app.get("/api/managers/<host>/master", callback=self.getMMInfo)

    @daliuge_aware
    def createDataIsland(self, host):
        with RestClient(
            host=host, port=constants.DAEMON_DEFAULT_REST_PORT, timeout=10
        ) as c:
            c._post_json("/managers/dataisland", bottle.request.body.read())
        self.dm.addDmHost(host)

    @daliuge_aware
    def getDIMs(self):
        return {"islands": self.dm.dmHosts}

    @daliuge_aware
    def addDIM(self, dim):
        logger.debug("Adding DIM %s", dim)
        self.dm.addDmHost(dim)

    @daliuge_aware
    def removeDIM(self, dim):
        logger.debug("Removing dim %s", dim)
        self.dm.removeDmHost(dim)

    @daliuge_aware
    def getNMs(self):
        return {"nodes": self.dm.nodes}

    @daliuge_aware
    def startNM(self, host):
        port = constants.DAEMON_DEFAULT_REST_PORT
        logger.debug("Sending NM start request to %s:%s", host, port)
        with RestClient(host=host, port=port, timeout=10) as c:
            return json.loads(c._POST("/managers/node/start").read())

    @daliuge_aware
    def stopNM(self, host):
        port = constants.DAEMON_DEFAULT_REST_PORT
        logger.debug("Sending NM stop request to %s:%s", host, port)
        with RestClient(host=host, port=port, timeout=10) as c:
            return json.loads(c._POST("/managers/node/stop").read())

    @daliuge_aware
    def addNM(self, host, node):
        port = constants.ISLAND_DEFAULT_REST_PORT
        logger.debug("Adding NM %s to DIM %s", node, host)
        with RestClient(host=host, port=port, timeout=10, url_prefix="/api") as c:
            return json.loads(
                c._POST(
                    f"/nodes/{node}",
                ).read()
            )

    @daliuge_aware
    def removeNM(self, host, node):
        port = constants.ISLAND_DEFAULT_REST_PORT
        logger.debug("Removing NM %s from DIM %s", node, host)
        with RestClient(host=host, port=port, timeout=10, url_prefix="/api") as c:
            return json.loads(c._DELETE(f"/nodes/{node}").read())

    @daliuge_aware
    def getNMInfo(self, host):
        port = constants.DAEMON_DEFAULT_REST_PORT
        logger.debug("Sending request %s:%s/managers/node", host, port)
        with RestClient(host=host, port=port, timeout=10) as c:
            return json.loads(c._GET("/managers/node").read())

    @daliuge_aware
    def getDIMInfo(self, host):
        with RestClient(
            host=host, port=constants.DAEMON_DEFAULT_REST_PORT, timeout=10
        ) as c:
            return json.loads(c._GET("/managers/dataisland").read())

    @daliuge_aware
    def getMMInfo(self, host):
        with RestClient(
            host=host, port=constants.DAEMON_DEFAULT_REST_PORT, timeout=10
        ) as c:
            return json.loads(c._GET("/managers/master").read())

    def getAllCMNodes(self):
        nodes = []
        for node in self.dm.dmHosts:
            with DataIslandManagerClient(host=node) as dm:
                nodes += dm.nodes()
        return nodes
