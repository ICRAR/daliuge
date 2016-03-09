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

import json
import threading

from bottle import Bottle, template, static_file, request, response
import bottle
import pkg_resources
import tornado.httpserver
import tornado.wsgi


class RestServer(object):
    """
    An object that wraps a DataManager and exposes its methods via a REST
    interface. The server is started via the `start` method in a separate thread
    and runs until the process is shut down.

    This REST server currently also serves HTML pages in some of its methods
    (i.e. those not under /api).
    """

    def __init__(self, dm):

        # Increase maximum file sizes
        bottle.BaseRequest.MEMFILE_MAX = 1024 * 1024 * 10

        self._server = None
        self.app = app = Bottle()

        # Error are returned as strings in a dictionary
        def jsonify_error(e):
            bottle.response.content_type = 'application/json'
            return json.dumps({'err_str': e.body})
        app.error_handler[500] = jsonify_error

        self.dm = dm

        # Mappings
        app.post(  '/api/sessions',                          callback=self.createSession)
        app.get(   '/api/sessions',                          callback=self.getSessions)
        app.get(   '/api/sessions/<sessionId>',              callback=self.getSessionInformation)
        app.delete('/api/sessions/<sessionId>',              callback=self.destroySession)
        app.get(   '/api/sessions/<sessionId>/status',       callback=self.getSessionStatus)
        app.post(  '/api/sessions/<sessionId>/deploy',       callback=self.deploySession)
        app.get(   '/api/sessions/<sessionId>/graph',        callback=self.getGraph)
        app.get(   '/api/sessions/<sessionId>/graph/status', callback=self.getGraphStatus)
        app.post(  '/api/sessions/<sessionId>/graph/append', callback=self.addGraphParts)

        # The non-REST mappings that serve HTML-related content
        app.route('/static/<filepath:path>', callback=self.server_static)
        app.get(  '/session', callback=self.visualizeSession)

        # sub-class specifics
        self.initializeSpecifics(app)

    def initializeSpecifics(self, app):
        """
        Methods through which subclasses can initialize other mappings on top of
        the default ones and perform other DataManager-specific actions.
        The default implementation does nothing.
        """

    def start(self, host, port):
        if host is None:
            host = '0.0.0.0'
        if port is None:
            port = 8080

        # It seems it's not trivial to stop a running bottle server, so we simply
        # start it but never end it. It will successfully end anyway when we finish
        # our process
        self._ioloop = tornado.ioloop.IOLoop()
        self._ioloop.make_current()
        self._server = tornado.httpserver.HTTPServer(tornado.wsgi.WSGIContainer(self.app), io_loop=self._ioloop)
        self._server.listen(port=port,address=host)
        self._ioloop.start()


    def stop(self, timeout=None):
        if self._ioloop:

            self.app.close()

            # Submit a callback to the IOLoop to stop itself and wait until it's
            # done with it
            ioloop_stopped = threading.Event()
            def stop_ioloop():
                self._ioloop.stop()
                ioloop_stopped.set()

            self._ioloop.add_callback(stop_ioloop)
            if not ioloop_stopped.wait(timeout):
                print "Timed out while waiting for the server to stop"
            self._server.stop()



    def createSession(self):
        newSession = request.json
        sessionId = newSession['sessionId']
        self.dm.createSession(sessionId)

    def sessions(self):
        sessions = []
        for sessionId in self.dm.getSessionIds():
            sessions.append({'sessionId':sessionId, 'status':self.dm.getSessionStatus(sessionId)})
        return sessions

    def getSessions(self):
        response.content_type = 'application/json'
        return json.dumps(self.sessions())

    def getSessionInformation(self, sessionId):
        graphDict = self.dm.getGraph(sessionId)
        status = self.dm.getSessionStatus(sessionId)
        response.content_type = 'application/json'
        return json.dumps({'status': status, 'graph': graphDict})

    def destroySession(self, sessionId):
        self.dm.destroySession(sessionId)

    def getSessionStatus(self, sessionId):
        response.content_type = 'application/json'
        return json.dumps(self.dm.getSessionStatus(sessionId))

    def deploySession(self, sessionId):
        completedDrops = []
        if 'completed' in request.forms:
            completedDrops = request.forms['completed'].split(',')
        response.content_type = 'application/json'
        return json.dumps(self.dm.deploySession(sessionId,completedDrops=completedDrops))

    def getGraph(self, sessionId):
        graphDict = self.dm.getGraph(sessionId)
        response.content_type = 'application/json'
        return json.dumps(graphDict)

    def getGraphStatus(self, sessionId):
        graphStatusDict = self.dm.getGraphStatus(sessionId)
        response.content_type = 'application/json'
        return json.dumps(graphStatusDict)

    # TODO: addGraphParts v/s addGraphSpec
    def addGraphParts(self, sessionId):
        self.dm.addGraphSpec(sessionId, request.json)

    #===========================================================================
    # non-REST methods
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

class NMRestServer(RestServer):
    """
    A REST server for NodeManagers. It includes mappings for NM-specific
    methods and the mapping for the main visualization HTML pages.
    """

    def initializeSpecifics(self, app):
        app.get(   '/api',                                   callback=self.getNMStatus)
        app.post(  '/api/sessions/<sessionId>/graph/link',   callback=self.linkGraphParts)
        app.post(  '/api/templates/<tpl>/materialize',       callback=self.materializeTemplate)

        # The non-REST mappings that serve HTML-related content
        app.get(  '/', callback=self.visualizeDM)

    def getNMStatus(self):
        # we currently return the sessionIds, more things might be added in the
        # future
        response.content_type = 'application/json'
        return json.dumps({'sessions': self.sessions(), 'templates': self.dm.getTemplates()})

    def linkGraphParts(self, sessionId):
        params = request.params
        lhOID = params['lhOID']
        rhOID = params['rhOID']
        linkType = int(params['linkType'])
        self.dm.linkGraphParts(sessionId, lhOID, rhOID, linkType)

    def materializeTemplate(self, tpl):
        tplParams = dict(request.params)
        sessionId = tplParams.pop('sessionId')
        self.dm.materializeTemplate(tpl, sessionId, **tplParams)

    #===========================================================================
    # non-REST methods
    #===========================================================================
    def visualizeDM(self):
        tpl = pkg_resources.resource_string(__name__, 'web/dm.html')  # @UndefinedVariable
        urlparts = request.urlparts
        serverUrl = urlparts.scheme + '://' + urlparts.netloc
        return template(tpl, dmId=self.dm.id, serverUrl=serverUrl)

class CompositeManagerRestServer(RestServer):
    """
    A REST server for DataIslandManagers. It includes mappings for DIM-specific
    methods.
    """

    def initializeSpecifics(self, app):
        app.get(   '/api',                                   callback=self.getCMStatus)
        app.get(   '/api/nodes',                             callback=self.getCMNodes)
        app.post(  '/api/nodes/<node>',                      callback=self.addCMNode)
        app.delete('/api/nodes/<node>',                      callback=self.removeCMNode)

        # The non-REST mappings that serve HTML-related content
        app.get(  '/', callback=self.visualizeDIM)

    def getCMStatus(self):
        response.content_type = 'application/json'
        return json.dumps({'hosts': self.dm.dmHosts, 'sessionIds': self.dm.getSessionIds()})

    def getCMNodes(self):
        response.content_type = 'application/json'
        return json.dumps(self.dm.nodes)

    def addCMNode(self, node):
        self.dm.add_node(node)

    def removeCMNode(self, node):
        self.dm.remove_node(node)

    #===========================================================================
    # non-REST methods
    #===========================================================================
    def visualizeDIM(self):
        tpl = pkg_resources.resource_string(__name__, 'web/dim.html')  # @UndefinedVariable
        urlparts = request.urlparts
        serverUrl = urlparts.scheme + '://' + urlparts.netloc
        return template(tpl,
                        dmId=self.dm.id,
                        dmType=self.dm.__class__.__name__,
                        dmPort=self.dm.dmPort,
                        serverUrl=serverUrl,
                        hosts=json.dumps(self.dm.dmHosts))