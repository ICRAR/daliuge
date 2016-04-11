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
import logging

import bottle
import pkg_resources

from dfms.manager import constants
from dfms.manager.client import NodeManagerClient
from dfms.restutils import RestServer, RestClient

LOG = logging.getLogger(__name__)


class ManagerRestServer(RestServer):
    """
    An object that wraps a DataManager and exposes its methods via a REST
    interface. The server is started via the `start` method in a separate thread
    and runs until the process is shut down.

    This REST server currently also serves HTML pages in some of its methods
    (i.e. those not under /api).
    """

    def __init__(self, dm):

        super(ManagerRestServer, self).__init__()

        # Increase maximum file sizes
        bottle.BaseRequest.MEMFILE_MAX = 1024 * 1024 * 10

        self.dm = dm

        # Mappings
        app = self.app
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

    def createSession(self):
        newSession = bottle.request.json
        sessionId = newSession['sessionId']
        LOG.debug('createSession: {0}'.format(sessionId))
        self.dm.createSession(sessionId)

    def sessions(self):
        sessions = []
        for sessionId in self.dm.getSessionIds():
            sessions.append({'sessionId':sessionId, 'status':self.dm.getSessionStatus(sessionId)})
        return sessions

    def getSessions(self):
        LOG.debug('getSessions')
        bottle.response.content_type = 'application/json'
        return json.dumps(self.sessions())

    def getSessionInformation(self, sessionId):
        LOG.debug('getSessionInformation: {0}'.format(sessionId))
        graphDict = self.dm.getGraph(sessionId)
        status = self.dm.getSessionStatus(sessionId)
        bottle.response.content_type = 'application/json'
        return json.dumps({'status': status, 'graph': graphDict})

    def destroySession(self, sessionId):
        LOG.debug('destroySession: {0}'.format(sessionId))
        self.dm.destroySession(sessionId)

    def getSessionStatus(self, sessionId):
        LOG.debug('getSessionStatus: {0}'.format(sessionId))
        bottle.response.content_type = 'application/json'
        return json.dumps(self.dm.getSessionStatus(sessionId))

    def deploySession(self, sessionId):
        LOG.debug('deploySession: {0}'.format(sessionId))
        completedDrops = []
        if 'completed' in bottle.request.forms:
            completedDrops = bottle.request.forms['completed'].split(',')
        bottle.response.content_type = 'application/json'
        return json.dumps(self.dm.deploySession(sessionId,completedDrops=completedDrops))

    def getGraph(self, sessionId):
        LOG.debug('getGraph: {0}'.format(sessionId))
        graphDict = self.dm.getGraph(sessionId)
        bottle.response.content_type = 'application/json'
        return json.dumps(graphDict)

    def getGraphStatus(self, sessionId):
        LOG.debug('getGraphStatus: {0}'.format(sessionId))
        graphStatusDict = self.dm.getGraphStatus(sessionId)
        bottle.response.content_type = 'application/json'
        return json.dumps(graphStatusDict)

    # TODO: addGraphParts v/s addGraphSpec
    def addGraphParts(self, sessionId):
        LOG.debug('addGraphParts: {0}'.format(sessionId))
        self.dm.addGraphSpec(sessionId, bottle.request.json)

    #===========================================================================
    # non-REST methods
    #===========================================================================
    def server_static(self, filepath):
        staticRoot = pkg_resources.resource_filename(__name__, '/web/static')  # @UndefinedVariable
        return bottle.static_file(filepath, root=staticRoot)

    def visualizeSession(self):
        sessionId = bottle.request.params['sessionId']
        selectedNode = bottle.request.params['node'] if 'node' in bottle.request.params else ''
        tpl = pkg_resources.resource_string(__name__, 'web/session.html')  # @UndefinedVariable
        urlparts = bottle.request.urlparts
        serverUrl = urlparts.scheme + '://' + urlparts.netloc
        return bottle.template(tpl, sessionId=sessionId, selectedNode=selectedNode, serverUrl=serverUrl)

class NMRestServer(ManagerRestServer):
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
        bottle.response.content_type = 'application/json'
        return json.dumps({'sessions': self.sessions(), 'templates': self.dm.getTemplates()})

    def linkGraphParts(self, sessionId):
        params = bottle.request.params
        lhOID = params['lhOID']
        rhOID = params['rhOID']
        linkType = int(params['linkType'])
        self.dm.linkGraphParts(sessionId, lhOID, rhOID, linkType)

    def materializeTemplate(self, tpl):
        tplParams = dict(bottle.request.params)
        sessionId = tplParams.pop('sessionId')
        self.dm.materializeTemplate(tpl, sessionId, **tplParams)

    #===========================================================================
    # non-REST methods
    #===========================================================================
    def visualizeDM(self):
        tpl = pkg_resources.resource_string(__name__, 'web/dm.html')  # @UndefinedVariable
        urlparts = bottle.request.urlparts
        serverUrl = urlparts.scheme + '://' + urlparts.netloc
        return bottle.template(tpl, serverUrl=serverUrl)

class CompositeManagerRestServer(ManagerRestServer):
    """
    A REST server for DataIslandManagers. It includes mappings for DIM-specific
    methods.
    """

    def initializeSpecifics(self, app):
        app.get(   '/api',                                   callback=self.getCMStatus)
        app.get(   '/api/nodes',                             callback=self.getCMNodes)
        app.post(  '/api/nodes/<node>',                      callback=self.addCMNode)
        app.delete('/api/nodes/<node>',                      callback=self.removeCMNode)

        # Query forwarding to sub-nodes
        app.get(   '/api/nodes/<node>/sessions',                          callback=self.getNodeSessions)
        app.get(   '/api/nodes/<node>/sessions/<sessionId>',              callback=self.getNodeSessionInformation)
        app.get(   '/api/nodes/<node>/sessions/<sessionId>/status',       callback=self.getNodeSessionStatus)
        app.get(   '/api/nodes/<node>/sessions/<sessionId>/graph',        callback=self.getNodeGraph)
        app.get(   '/api/nodes/<node>/sessions/<sessionId>/graph/status', callback=self.getNodeGraphStatus)

        # The non-REST mappings that serve HTML-related content
        app.get(  '/', callback=self.visualizeDIM)

    def getCMStatus(self):
        bottle.response.content_type = 'application/json'
        return json.dumps({'hosts': self.dm.dmHosts, 'sessionIds': self.dm.getSessionIds()})

    def getCMNodes(self):
        bottle.response.content_type = 'application/json'
        return json.dumps(self.dm.nodes)

    def addCMNode(self, node):
        self.dm.add_node(node)

    def removeCMNode(self, node):
        self.dm.remove_node(node)

    def getNodeSessions(self, node):
        if node not in self.dm.nodes:
            raise Exception("%s not in current list of nodes" % (node,))
        bottle.response.content_type = 'application/json'
        with NodeManagerClient(host=node) as dm:
            return json.dumps(dm.sessions())

    def getNodeSessionInformation(self, node, sessionId):
        if node not in self.dm.nodes:
            raise Exception("%s not in current list of nodes" % (node,))
        bottle.response.content_type = 'application/json'
        with NodeManagerClient(host=node) as dm:
            return json.dumps(dm.session(sessionId))

    def getNodeSessionStatus(self, node, sessionId):
        if node not in self.dm.nodes:
            raise Exception("%s not in current list of nodes" % (node,))
        bottle.response.content_type = 'application/json'
        with NodeManagerClient(host=node) as dm:
            return json.dumps(dm.session_status(sessionId))

    def getNodeGraph(self, node, sessionId):
        if node not in self.dm.nodes:
            raise Exception("%s not in current list of nodes" % (node,))
        bottle.response.content_type = 'application/json'
        with NodeManagerClient(host=node) as dm:
            return json.dumps(dm.graph(sessionId))

    def getNodeGraphStatus(self, node, sessionId):
        if node not in self.dm.nodes:
            raise Exception("%s not in current list of nodes" % (node,))
        bottle.response.content_type = 'application/json'
        with NodeManagerClient(host=node) as dm:
            return json.dumps(dm.graph_status(sessionId))

    #===========================================================================
    # non-REST methods
    #===========================================================================
    def visualizeDIM(self):
        tpl = pkg_resources.resource_string(__name__, 'web/dim.html')  # @UndefinedVariable
        urlparts = bottle.request.urlparts
        selectedNode = bottle.request.params['node'] if 'node' in bottle.request.params else ''
        serverUrl = urlparts.scheme + '://' + urlparts.netloc
        return bottle.template(tpl,
                        dmType=self.dm.__class__.__name__,
                        dmPort=self.dm.dmPort,
                        serverUrl=serverUrl,
                        dmHosts=json.dumps(self.dm.dmHosts),
                        nodes=json.dumps(self.dm.nodes),
                        selectedNode=selectedNode)

class MasterManagerRestServer(CompositeManagerRestServer):

    def initializeSpecifics(self, app):
        CompositeManagerRestServer.initializeSpecifics(self, app)

        # Query forwarding to daemons
        app.post(  '/api/managers/<host>/dataisland', callback=self.createDataIsland)

    def createDataIsland(self, host):
        with RestClient(host=host, port=constants.DAEMON_DEFAULT_REST_PORT, timeout=10) as c:
            c._post_json('/managers/dataisland', bottle.request.body.read())
        self.dm.addDmHost(host)
