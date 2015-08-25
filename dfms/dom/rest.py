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
Module containing the REST layer that exposes the DataObjectManager methods to
the outside world
"""

import json
import threading

from bottle import Bottle, template, static_file, request, run, response
import pkg_resources


class RestServer(object):
    """
    An object that wraps a DataObjectManager and exposes its methods via a REST
    interface. The server is started via the `start` method in a separate thread
    and runs until the process is shut down.

    This REST server currently also serves HTML pages in one of its methods
    (i.e., /<sessionId>/show). That probably could live somehere
    """

    def __init__(self, dom):
        super(RestServer, self).__init__()
        app = Bottle()

        # Everything is currently centered on the session
        app.put(   '/<sessionId>',              callback=self.createSession)
        app.delete('/<sessionId>',              callback=self.destroySession)
        app.get(   '/<sessionId>',              callback=self.getSessionInformation)
        app.get(   '/<sessionId>/status',       callback=self.getSessionStatus)
        app.post(  '/<sessionId>/deploy',       callback=self.deploySession)
        app.post(  '/<sessionId>/quick_deploy', callback=self.quickDeploy)
        app.get(   '/<sessionId>/graph',        callback=self.getGraph)
        app.get(   '/<sessionId>/graph/status', callback=self.getGraphStatus)
        app.put(   '/<sessionId>/graph/parts',  callback=self.addGraphParts)

        # The bad boys that serve HTML-related content
        app.route('/static/<filepath:path>', callback=self.server_static)
        app.get(   '/<sessionId>/show', callback=self.showSession)

        self.app = app
        self.dom = dom

    def start(self, host, port):
        if host is None:
            host = '0.0.0.0'
        if port is None:
            port = 8080

        # It seems it's not trivial to stop a running bottle server, so we simply
        # start it but never end it. It will successfully end anyway when we finish
        # our process
        t = threading.Thread(None, lambda: run(self.app, server='tornado', host=host, port=port, quiet=True))
        t.daemon = 1
        t.start()

    def createSession(self, sessionId):
        self.dom.createSession(sessionId)

    def destroySession(self, sessionId):
        self.dom.destroySession(sessionId)

    def getSessionStatus(self, sessionId):
        response.content_type = 'application/json'
        return json.dumps(self.dom.getSessionStatus(sessionId))

    def getSessionInformation(self, sessionId):
        graphDict = self.dom.getGraph(sessionId)
        status = self.dom.getSessionStatus(sessionId)
        response.content_type = 'application/json'
        return json.dumps({'sessionStatus': status, 'graph': graphDict})

    def deploySession(self, sessionId):
        self.dom.deploySession(sessionId)

    def quickDeploy(self, sessionId):
        graphJson = request._get_body_string()
        uris = self.dom.quickDeploy(sessionId, graphJson)
        response.content_type = 'application/json'
        return [str(uri) for uri in uris]

    def getGraph(self, sessionId):
        graphDict = self.dom.getGraph(sessionId)
        response.content_type = 'application/json'
        return json.dumps(graphDict)

    def getGraphStatus(self, sessionId):
        graphStatusDict = self.dom.getGraphStatus(sessionId)
        response.content_type = 'application/json'
        return json.dumps(graphStatusDict)

    # TODO: addGraphParts v/s addGraphSpec
    def addGraphParts(self, sessionId):
        graphJson = request._get_body_string()
        self.dom.addGraphSpec(sessionId, graphJson)

    def server_static(self, filepath):
        staticRoot = pkg_resources.resource_filename(__name__, '/web/static')  # @UndefinedVariable
        return static_file(filepath, root=staticRoot)

    def showSession(self, sessionId):
        tpl = pkg_resources.resource_string(__name__, 'web/graph_display.html')  # @UndefinedVariable
        urlparts = request.urlparts
        serverUrl = urlparts.scheme + '://' + urlparts.netloc
        return template(tpl, sessionId=sessionId, serverUrl=serverUrl)
