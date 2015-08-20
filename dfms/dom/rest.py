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

from bottle import Bottle, template, static_file, request, run
import pkg_resources

from dfms import doutils
from dfms.data_object import AppDataObject, ContainerDataObject, SocketListener
from dfms.dom.session import SessionStates


sessionStatusStrings = {
    SessionStates.PRISTINE: 'Pristine',
    SessionStates.DEPLOYING: 'Deploying',
    SessionStates.RUNNING: 'Running',
    SessionStates.FINISHED: 'Finished'
}

class RestServer(object):
    """
    An object that wraps a DataObjectManager and exposes its methods via a REST
    interface. The server is started via the `start` method in a separate thread
    and runs until the process is shut down.
    """

    def __init__(self, dom):
        super(RestServer, self).__init__()
        app = Bottle()
        app.route('/static/<filepath:path>', callback=self.server_static)
        app.put(   '/<sessionId>', callback=self.createSession)
        app.post(  '/<sessionId>/deploy', callback=self.deploy)
        app.post(  '/<sessionId>/quick_deploy', callback=self.quickDeploy)
        app.post(  '/<sessionId>/add_graph_spec', callback=self.addGraphSpec)
        app.route( '/<sessionId>/show_graph', callback=self.showGraph)
        app.delete('/<sessionId>', callback=self.destroySession)
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

    def server_static(self, filepath):
        staticRoot = pkg_resources.resource_filename(__name__, '/web/static')  # @UndefinedVariable
        return static_file(filepath, root=staticRoot)

    def deploy(self, sessionId):
        self.dom.deploySession(sessionId)

    def quickDeploy(self, sessionId):
        graphJson = request._get_body_string()
        uris = self.dom.quickDeploy(sessionId, graphJson)
        return [str(uri) for uri in uris]

    def addGraphSpec(self, sessionId):
        graphJson = request._get_body_string()
        self.dom.addGraphSpec(sessionId, graphJson)

    def showGraph(self, sessionId):
        tpl = pkg_resources.resource_string(__name__, 'web/graph_display.html')  # @UndefinedVariable
        sessionStatus = sessionStatusStrings[self.dom.getSessionStatus(sessionId)]
        dataObjects = self.toJson(sessionId)
        return template(tpl, dataObjects=dataObjects, sessionId=sessionId, sessionStatus=sessionStatus)

    def createSession(self, sessionId):
        self.dom.createSession(sessionId)

    def destroySession(self, sessionId):
        self.dom.destroySession(sessionId)

    def toJson(self, sessionId, formatted=False):
        allDOsDict = {}
        for rootDO in self.dom.getRoots(sessionId):
            self.to_json_obj(rootDO, allDOsDict)
        return json.dumps(allDOsDict, sort_keys=formatted)

    def get_type_code(self, dataObject):
        if isinstance(dataObject, AppDataObject):
            return 0
        elif isinstance(dataObject, ContainerDataObject):
            return 1
        elif isinstance(dataObject, SocketListener):
            return 2
        else:
            return 3

    def to_json_obj(self, dataObject, visited):
        """
        JSON serialisation of a DataObject for displaying with dagreD3. Its
        implementation should be similar to the DataObjectTask for Luigi, since both
        should represent the same dependencies
        """
        # Already visited
        if dataObject.oid in visited:
            return

        doDict = {
            'type':     self.get_type_code(dataObject),
            'location': dataObject.location,
            'status' :  dataObject.status
        }
        dependencies = [{'oid': uobj.oid} for uobj in doutils.getUpstreamObjects(dataObject)]
        if dependencies:
            doDict['dependencies'] = dependencies

        visited[dataObject.oid] = doDict

        for dob in doutils.getDownstreamObjects(dataObject):
            self.to_json_obj(dob, visited)