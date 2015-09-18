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

        app.get(   '/api',                                   callback=self.getDOMStatus)
        app.post(  '/api/sessions/',                         callback=self.createSession)
        app.delete('/api/sessions/<sessionId>',              callback=self.destroySession)
        app.get(   '/api/sessions/<sessionId>',              callback=self.getSessionInformation)
        app.get(   '/api/sessions/<sessionId>/status',       callback=self.getSessionStatus)
        app.post(  '/api/sessions/<sessionId>/deploy',       callback=self.deploySession)
        app.get(   '/api/sessions/<sessionId>/graph',        callback=self.getGraph)
        app.get(   '/api/sessions/<sessionId>/graph/status', callback=self.getGraphStatus)
        app.post(  '/api/sessions/<sessionId>/graph/parts',  callback=self.addGraphParts)
        app.post(  '/api/sessions/<sessionId>/graph/link',   callback=self.linkGraphParts)
        app.post(  '/api/templates/<tpl>/materialize',       callback=self.materializeTemplate)

        # The bad boys that serve HTML-related content
        app.route('/static/<filepath:path>', callback=self.server_static)
        app.get(  '/session', callback=self.visualizeSession)
        app.get(  '/', callback=self.visualizeDOM)

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

    def getDOMStatus(self):
        # we currently return the sessionIds, more things might be added in the
        # future
        sessions = []
        for sessionId in self.dom.getSessionIds():
            sessions.append({'sessionId': sessionId, 'status': self.dom.getSessionStatus(sessionId)})
        return json.dumps({'sessions': sessions, 'templates': self.dom.getTemplates()})

    def createSession(self):
        newSession = request.json
        sessionId = newSession['sessionId']
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
        self.dom.addGraphSpec(sessionId, request.body)

    def linkGraphParts(self, sessionId):
        params = request.params
        lhOID = params['lhOID']
        rhOID = params['rhOID']
        linkType = int(params['linkType'])
        self.dom.linkGraphParts(sessionId, lhOID, rhOID, linkType)

    def materializeTemplate(self, tpl):
        tplParams = dict(request.params)
        sessionId = tplParams.pop('sessionId')
        self.dom.materializeTemplate(tpl, sessionId, **tplParams)

    #===========================================================================
    # HTML-related methods
    #===========================================================================
    def server_static(self, filepath):
        staticRoot = pkg_resources.resource_filename(__name__, '/web/static')  # @UndefinedVariable
        return static_file(filepath, root=staticRoot)

    def visualizeSession(self):
        sessionId = request.params['sessionId']
        tpl = pkg_resources.resource_string(__name__, 'web/session.html')  # @UndefinedVariable
        urlparts = request.urlparts
        serverUrl = urlparts.scheme + '://' + urlparts.netloc
        return template(tpl, sessionId=sessionId, serverUrl=serverUrl)

    def visualizeDOM(self):
        tpl = pkg_resources.resource_string(__name__, 'web/index.html')  # @UndefinedVariable
        urlparts = request.urlparts
        serverUrl = urlparts.scheme + '://' + urlparts.netloc
        return template(tpl, domId=self.dom.domId, serverUrl=serverUrl)