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
# pylint: disable=protected-access

from email.message import Message
import functools
import io
import json
import logging
import os
import re
import tarfile
import threading

import bottle

from bottle import static_file
from pathlib import Path

from dlg import constants
from dlg.manager.client import NodeManagerClient, DataIslandManagerClient
from dlg import utils
from dlg.exceptions import (
    InvalidGraphException,
    InvalidSessionState,
    DaliugeException,
    NoSessionException,
    SessionAlreadyExistsException,
    InvalidDropException,
    InvalidRelationshipException,
    SubManagerException,
)
from dlg.restserver import RestServer
from dlg.restutils import RestClient, RestClientException
from dlg.manager.session import generateLogFileName
from dlg.common.deployment_methods import DeploymentMethods
from dlg.manager.manager_data import Node

logger = logging.getLogger(f"dlg.{__name__}")

def file_as_string(fname, enc="utf8"):
    res = Path(__file__).parent / fname
    return utils.b2s(res.read_bytes(), enc)


def daliuge_aware(func):
    @functools.wraps(func)
    def fwrapper(*args, **kwargs):
        try:
            res = func(*args, **kwargs)

            if isinstance(res, (bytes, bottle.HTTPResponse)):
                return res

            if res is not None:
                # set CORS headers
                origin = bottle.request.headers.raw("Origin")
                # logger.debug("CORS request comming from: %s", origin)
                # logger.debug("Request method: %s", bottle.request.method)
                if origin is None or re.match(
                    r"(http://dlg-trans.local:80[0-9][0-9]|https://dlg-trans.icrar.org)",
                    origin,
                ):
                    pass
                elif re.match(r"http://((localhost)|(127.0.0.1)):80[0-9][0-9]", origin):
                    origin = "http://localhost:8084"

                bottle.response.headers["Access-Control-Allow-Origin"] = origin
                bottle.response.headers["Access-Control-Allow-Credentials"] = "true"
                bottle.response.headers["Access-Control-Allow-Methods"] = (
                    "GET, POST, PUT, OPTIONS, HEAD"
                )
                bottle.response.headers["Access-Control-Allow-Headers"] = (
                    "Origin, Accept, Content-Type, Content-Encoding, X-Requested-With, X-CSRF-Token"
                )
                # logger.debug("CORS headers set to allow from: %s", origin)
            bottle.response.content_type = "application/json"
            # logger.debug("REST function called: %s", func.__name__)
            jres = (
                json.dumps(res)
                if res is not None
                else json.dumps({"Status": "Success"})
            )
            # logger.debug("Bottle sending back result: %s", jres[: min(len(jres), 80)])
            return jres
        except Exception as e: # pylint: disable=broad-exception-caught
            logger.exception("Error while fulfilling request for func %s ", func)

            status, eargs = 500, ()
            if isinstance(e, NotImplementedError):
                status, eargs = 501, e.args
            elif isinstance(e, NoSessionException):
                status, eargs = 404, (e.session_id,)
            elif isinstance(e, SessionAlreadyExistsException):
                status, eargs = 409, (e.session_id,)
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
                    logger.debug(">>>> Error class name: %s", subex.__class__.__name__)
                    eargs[host] = {
                        "type": subex.__class__.__name__,
                        # "args": subex.args,
                        "args": "dummy",
                    }
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
        app.get("/api/submission_method", callback=self.submit_methods)
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
        app.get(
            "/api/sessions/<sessionId>/graph/status",
            callback=self.getGraphStatus,
        )
        app.post(
            "/api/sessions/<sessionId>/graph/append",
            callback=self.addGraphSpec,
        )
        app.get(
            "/api/sessions/<sessionId>/repro/data",
            callback=self.getSessionReproData,
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
        app.route("/api/sessions/<sessionId>/dir", callback=self._getSessionDir)
        app.route("/api/sessions/<sessionId>/graph/drop/<dropId>",
                  callback=self._getDropStatus)
        app.route("/sessions/<sessionId>/graph/drop/<dropId>",
                callback=self.getDropStatus)

        # sub-class specifics
        self.initializeSpecifics(app)

    def initializeSpecifics(self, app):
        """
        Methods through which subclasses can initialize other mappings on top of
        the default ones and perform other DataManager-specific actions.
        The default implementation does nothing.
        """

    @daliuge_aware
    def submit_methods(self):
        return {"methods": [DeploymentMethods.BROWSER, DeploymentMethods.SERVER]}

    def _stop_manager(self):
        self.dm.shutdown()
        self.stop()
        logger.info(
            "Thanks for using our %s, come back again :-)",
            self.dm.__class__.__name__,
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
        logger.info("Preflight2 for %s", sessionId)
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
            directory = self.dm.getSessionDir(sessionId)
        except KeyError:  # Pristine state sessions don't have a graph, yet.
            graphDict = {}
            status = 0
            directory = ""
        return {"status": status, "graph": graphDict, "dir":directory}

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
        return self.dm.deploySession(sessionId, completedDrops=completedDrops)
        # return {"Status": "Success"}

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

    @daliuge_aware
    def addGraphSpec(self, sessionId):
        # WARNING: TODO: Somehow, the content_type can be overwritten to 'text/plain'
        logger.debug("Graph content type: %s", bottle.request.content_type)
        if (
            "application/json" not in bottle.request.content_type
            and "text/plain" not in bottle.request.content_type
        ):
            bottle.response.status = 415
            return

        # We also accept gzipped content
        hdrs = bottle.request.headers
        logger.debug("Graph hdr: %s", {k: v for k, v in hdrs.items()})
        if hdrs.get("Content-Encoding", None) == "gzip":
            json_content = utils.ZlibUncompressedStream(bottle.request.body)
        else:
            json_content = bottle.request.body

        graph_parts = bottle.json_loads(json_content.read())

        # Do something about host Nodes in graph_parts?
        return self.dm.addGraphSpec(sessionId, graph_parts)
        # return {"graph_parts": graph_parts}

    # ===========================================================================
    # non-REST methods
    # ===========================================================================
    def server_static(self, filepath):
        staticRoot = Path(__file__).parent / "web/static"
        return bottle.static_file(filepath, root=staticRoot)

    def _getSessionDir(self, sessionId):
        return self.dm.getSessionDir(sessionId)

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
            sessionDir=sessionId
        )

    def _getDropStatus(self, sessionId, dropId):
        return self.dm.getDropStatus(sessionId, dropId)

    def getDropStatus(self, sessionId, dropId):
        params = bottle.request.params
        logger.warning("PARAMS: %s", params)

        urlparts = bottle.request.urlparts
        serverUrl = urlparts.scheme + "://" + urlparts.netloc

        data = self._getDropStatus(sessionId, dropId)
        if data["logs"]:
            columns = [col for col in data["logs"][-1].keys()]
            filter_column = "Level"
            filter_column_index = columns.index(filter_column)
        else:
            columns = []
            filter_column_index=0

        tpl = file_as_string("web/drop_log.html")
        return bottle.template(
            tpl,
            data=data,
            columns=columns,
            filter_index=filter_column_index,
            sessionId=sessionId,
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
        app.post(
            "/api/sessions/<sessionId>/graph/link",
            callback=self.linkGraphParts,
        )
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
            os.path.basename(logfile),
            root=logdir,
            download=os.path.basename(logfile),
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
        subscriptions = self._parse_subscriptions(bottle.request.json)
        self.dm.add_node_subscriptions(sessionId, subscriptions)

    def _parse_subscriptions(self, json_request):
        return {Node(host): droprels for host, droprels in json_request.items()}

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
            tpl,
            serverUrl=serverUrl,
            dmType=self.dm.__class__.__name__,
            reset="false",
        )


class CompositeManagerRestServer(ManagerRestServer):
    """
    A REST server for DataIslandManagers. It includes mappings for DIM-specific
    methods.
    """

    def initializeSpecifics(self, app):
        app.get("/api", callback=self.getCMStatus)
        app.get("/api/past_sessions", callback=self.getPastSessions)
        app.get("/api/nodes", callback=self.getCMNodes)
        app.post("/api/node/<node>", callback=self.addCMNode)
        app.delete("/api/node/<node>", callback=self.removeCMNode)

        # Query forwarding to sub-nodes
        app.get("/api/node/<node>/sessions", callback=self.getNodeSessions)
        app.get(
            "/api/node/<node>/sessions/<sessionId>",
            callback=self.getNodeSessionInformation,
        )
        app.get(
            "/api/node/<node>/sessions/<sessionId>/status",
            callback=self.getNodeSessionStatus,
        )
        app.get(
            "/api/node/<node>/sessions/<sessionId>/graph",
            callback=self.getNodeGraph,
        )
        app.get(
            "/api/node/<node>/sessions/<sessionId>/graph/status",
            callback=self.getNodeGraphStatus,
        )

        # The non-REST mappings that serve HTML-related content
        app.get("/", callback=self.visualizeDIM)

    @daliuge_aware
    def getCMStatus(self):
        """
        REST (GET): /api/

        Return JSON-compatible list of Composite Manager nodes and sessions
        """
        return {
            "hosts": [str(n) for n in self.dm.dmHosts],
            "sessionIds": self.dm.getSessionIds(),
        }

    @daliuge_aware
    def getCMNodes(self):
        """
        REST (GET): /api/nodes

        Return JSON-compatible list of Composite Manager nodes
        """
        return [str(n) for n in self.dm.nodes]

    def _getAllCMNodes(self):
        return self.dm.nodes

    @daliuge_aware
    def addCMNode(self, node):
        """
        REST (POST): "/api/node/<node>"

        Add the posted node to the Composite Manager

        Converts from JSON to our ser
        """
        logger.debug("Adding node %s", node)
        self.dm.add_node(Node(node))

    @daliuge_aware
    def removeCMNode(self, node):
        """
        REST (DELETE): "/api/node/<node>"

        Add the posted node to the Composite Manager

        """
        logger.debug("Removing node %s", node)
        self.dm.remove_node(Node(node))

    @daliuge_aware
    def getNodeSessions(self, node):
        """
        REST (GET): "/api/node/<node>/sessions"

        Retrieve sessions for given node
        """
        host_node = Node(node)
        if host_node not in self.dm.nodes:
            raise RuntimeError(f"{host_node} not in current list of nodes")
        with NodeManagerClient(host=host_node.host, port=host_node.port) as dm:
            return dm.sessions()

    @daliuge_aware
    def getPastSessions(self):
        """
        REST (GET): /api/past_sessions

        Return JSON-compatible list of Composite Manager nodes
        """
        return self.pastSessions()

    def pastSessions(self):
        """
        Retrieve sessions from this DropManager and place it in JSON-format, for
        serialisation across the wire.
        """

        return [
            {"sessionId": pastSession} for pastSession in self.dm.getPastSessionIds()
        ]

    def _tarfile_write(self, tar, headers, stream):
        file_header = headers.getheader("Content-Disposition")
        length = headers.getheader("Content-Length")
        m = Message()
        m.add_header("content-disposition", file_header)
        filename = m.get_params("filename")
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
            for node in self._getAllCMNodes():
                with NodeManagerClient(host=node.host, port=node.port) as dm:
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
    def getNodeSessionInformation(self, node_str, sessionId):
        try:
            node = self.dm.get_node_from_json(node_str)
            with NodeManagerClient(host=node.host, port=node.port) as dm:
                return dm.session(sessionId)
        except ValueError as e:
            raise ValueError(f"{node_str} not in current list of nodes") from e

    @daliuge_aware
    def getNodeSessionStatus(self, node_str, sessionId):
        try:
            node = self.dm.get_node_from_json(node_str)
            with NodeManagerClient(host=node.host, port=node.port) as dm:
                return dm.session_status(sessionId)
        except ValueError as e:
            raise ValueError(f"{node_str} not in current list of nodes") from e

    @daliuge_aware
    def getNodeGraph(self, node_str, sessionId):
        try:
            node = self.dm.get_node_from_json(node_str)
            with NodeManagerClient(host=node.host, port=node.port) as dm:
                return dm.graph(sessionId)
        except ValueError as e:
            raise ValueError(f"{node_str} not in current list of nodes") from e

    @daliuge_aware
    def getNodeGraphStatus(self, node_str, sessionId):
        try:
            node = self.dm.get_node_from_json(node_str)
            with NodeManagerClient(host=node.host, port=node.port) as dm:
                return dm.graph_status(sessionId)
        except ValueError as e:
            raise ValueError(f"{node_str} not in current list of nodes") from e

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
            dmHosts=json.dumps([str(n) for n in self.dm.dmHosts]),
            nodes=json.dumps([str(n) for n in self.dm.nodes]),
            selectedNode=selectedNode,
        )


class MasterManagerRestServer(CompositeManagerRestServer):
    def initializeSpecifics(self, app):
        CompositeManagerRestServer.initializeSpecifics(self, app)
        # DIM manamagement
        app.post("/api/island/<dim>", callback=self.addDIM)
        app.delete("/api/island/<dim>", callback=self.removeDIM)
        # Query forwarding to daemons
        app.post("/api/managers/<host>/island", callback=self.createDataIsland)
        app.post("/api/managers/<host>/node/start", callback=self.startNM)
        app.post("/api/managers/<host>/node/stop", callback=self.stopNM)
        # manage node manager assignment
        app.post("/api/managers/<host>/node/<node>", callback=self.addNM)
        app.delete("/api/managers/<host>/node/<node>", callback=self.removeNM)
        # Querying about managers
        app.get("/api/islands", callback=self.getDIMs)
        app.get("/api/nodes", callback=self.getNMs)
        app.get("/api/managers/<host>/node", callback=self.getNMInfo)
        app.get("/api/managers/<host>/island", callback=self.getDIMInfo)
        app.get("/api/managers/<host>/master", callback=self.getMMInfo)

    @daliuge_aware
    def createDataIsland(self, host):
        with RestClient(
            host=host, port=constants.DAEMON_DEFAULT_REST_PORT, timeout=10
        ) as c:
            c._post_json("/managers/island/start", bottle.request.body.read())
        self.dm.addDmHost(host)
        return {"islands": self.dm.dmHosts}

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
        return {"nodes": [str(n) for n in self.dm.nodes]}

    @daliuge_aware
    def startNM(self, host):
        port = constants.DAEMON_DEFAULT_REST_PORT
        logger.debug("Sending NM start request to %s:%s", host, port)
        with RestClient(host=host, port=port, timeout=10) as c:
            return json.loads(c.POST("/managers/node/start").read())

    @daliuge_aware
    def stopNM(self, host):
        port = constants.DAEMON_DEFAULT_REST_PORT
        logger.debug("Sending NM stop request to %s:%s", host, port)
        with RestClient(host=host, port=port, timeout=10) as c:
            return json.loads(c.POST("/managers/node/stop").read())

    @daliuge_aware
    def addNM(self, host, node):
        port = constants.ISLAND_DEFAULT_REST_PORT
        logger.debug("Adding NM %s to DIM %s", node, host)
        with RestClient(host=host, port=port, timeout=10, url_prefix="/api") as c:
            return json.loads(
                c.POST(
                    f"/node/{node}",
                ).read()
            )

    @daliuge_aware
    def removeNM(self, host, node):
        port = constants.ISLAND_DEFAULT_REST_PORT
        logger.debug("Removing NM %s from DIM %s", node, host)
        with RestClient(host=host, port=port, timeout=10, url_prefix="/api") as c:
            return json.loads(c._DELETE(f"/node/{node}").read())

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
            return json.loads(c._GET("/managers/island").read())

    @daliuge_aware
    def getMMInfo(self, host):
        with RestClient(
            host=host, port=constants.DAEMON_DEFAULT_REST_PORT, timeout=10
        ) as c:
            return json.loads(c._GET("/managers/master").read())

    def _getAllCMNodes(self):
        nodes = []
        for host in self.dm.dmHosts:
            h = Node(host)
            with DataIslandManagerClient(host=h.host, port=h.port) as dm:
                nodes += dm.nodes()
        return [str(n) for n in nodes]
