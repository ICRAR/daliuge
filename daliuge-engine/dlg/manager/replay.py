#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2016
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
from __future__ import annotations
import json
import logging

import bottle

from pathlib import Path
from typing import List, Optional

from dlg.manager.drop_manager import DROPManager
from dlg.manager.rest import ManagerRestServer
from dlg.manager.session import SessionStates

from dlg import utils
from dlg.exceptions import NoSessionException, InvalidSessionState

logger = logging.getLogger(f"dlg.{__name__}")

build_step = 3
deploy_step = 6
run_step = 7


class ReplayManager(DROPManager):
    def __init__(self, graph_file, status_file):

        with open(graph_file) as gf:
            contents = json.load(gf)
            session_id = contents["ssid"]
            self._graph = contents["g"]

        self._session_id = session_id
        self._status_filename = status_file
        self._status_file = None

        self.reset()

    def reset(self):
        if self._status_file and self._status_file.closed:
            self._status_file.close()
        self._status_file = open(self._status_filename)
        self._last_graph_status = None
        self._session_status_reqno = 0
        self._status = SessionStates.PRISTINE

    def __del__(self):
        if self._status_file and self._status_file.closed:
            self._status_file.close()

    # Only queries are supported by the replay manager
    def createSession(self, sessionId):
        raise NotImplementedError()

    def addGraphSpec(self, sessionId, graphSpec):
        raise NotImplementedError()

    def deploySession(self, sessionId, completedDrops: Optional[List[str]] = None):
        raise NotImplementedError()

    def destroySession(self, sessionId):
        raise NotImplementedError()

    def check_session_id(self, session_id):
        if session_id != self._session_id:
            raise NoSessionException(session_id)

    def getSessionStatus(self, sessionId):

        self.check_session_id(sessionId)

        # Move through the different steps as we are requested
        # our status
        self._session_status_reqno += 1
        logger.info("Session status request #%d", self._session_status_reqno)

        if build_step <= self._session_status_reqno < deploy_step:
            self._status = SessionStates.BUILDING
        elif deploy_step <= self._session_status_reqno < run_step:
            self._status = SessionStates.DEPLOYING
        elif run_step <= self._session_status_reqno:
            self._status = SessionStates.RUNNING

        return self._status

    def getGraphStatus(self, sessionId):

        self.check_session_id(sessionId)
        if self._session_status_reqno < run_step:
            raise InvalidSessionState(
                "Requesting status of graph that is not running yet"
            )

        while True:
            l = self._status_file.readline()
            if not l:
                self._status_file.close()
                self._status = SessionStates.FINISHED
                return self._last_graph_status

            content = json.loads(l)

            this_session_id = content["ssid"]
            if this_session_id != sessionId:
                continue

            graph_status = content["gs"]
            self._last_graph_status = graph_status

            logger.info("Serving graph status")
            return graph_status

    def getGraph(self, sessionId):
        self.check_session_id(sessionId)
        logger.info("Serving graph")
        return self._graph

    def getGraphSize(self, sessionId):
        self.check_session_id(sessionId)
        logger.info("Serving graph size")
        return len(self._graph)

    def getSessionIds(self):
        return [self._session_id]

def file_as_string(fname, enc="utf8"):
    res = Path(__file__).parent / fname
    return utils.b2s(res.read_bytes(), enc)

class ReplayManagerServer(ManagerRestServer):
    def initializeSpecifics(self, app):
        super(ReplayManagerServer, self).initializeSpecifics(app)
        app.post("/api/reset", callback=self.dm.reset)
        app.get("/", callback=self.visualizeDM)

    def visualizeDM(self):
        tpl = file_as_string("web/dm.html")
        urlparts = bottle.request.urlparts
        serverUrl = urlparts.scheme + "://" + urlparts.netloc
        return bottle.template(
            tpl, serverUrl=serverUrl, dmType=self.dm.__class__.__name__, reset="true"
        )
