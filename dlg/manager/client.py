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
import logging
import os

from six.moves import urllib_parse as urllib  # @UnresolvedImport

from . import constants
from ..restutils import RestClient


logger = logging.getLogger(__name__)
compress = os.environ.get('DALIUGE_COMPRESSED_JSON', True)

class BaseDROPManagerClient(RestClient):
    """
    Base class for REST clients that talk to the DROP managers.
    """

    def _request(self, url, method, content=None, headers={}):
        # Normalize first
        if not url.startswith('/'):
            url = '/' + url
        url = '/api' + url
        return RestClient._request(self, url, method, content=content, headers=headers)

    def create_session(self, sessionId):
        """
        Creates a session with `sessionId`
        """
        self._post_json('/sessions', {'sessionId': sessionId})
        logger.debug('Successfully created session %s on %s:%s', sessionId, self.host, self.port)

    def deploy_session(self, sessionId, completed_uids=[]):
        """
        Deploys session `sessionId`, effectively creating its DROPs and triggering
        the execution of the graph
        """
        content = None
        if completed_uids:
            content = {'completed': ','.join(completed_uids)}
        self._post_form('/sessions/%s/deploy' % (urllib.quote(sessionId),), content)
        logger.debug('Successfully deployed session %s on %s:%s', sessionId, self.host, self.port)

    def append_graph(self, sessionId, graphSpec):
        """
        Appends a graph to session `sessionId`, without creating its DROPs yet,
        but checking that the graph looks correct
        """
        self._post_json('/sessions/%s/graph/append' % (urllib.quote(sessionId),), graphSpec, compress=compress)
        logger.debug('Successfully appended graph to session %s on %s:%s', sessionId, self.host, self.port)

    def destroy_session(self, sessionId):
        """
        Destroys session `sessionId`
        """
        self._DELETE('/sessions/%s' % (urllib.quote(sessionId),))
        logger.debug('Successfully deleted session %s on %s:%s', sessionId, self.host, self.port)

    def graph_status(self, sessionId):
        """
        Returns a dictionary where the keys are DROP UIDs and the values are
        their corresponding status.
        """
        ret = self._get_json('/sessions/%s/graph/status' % (urllib.quote(sessionId),))
        logger.debug('Successfully read graph status from session %s on %s:%s', sessionId, self.host, self.port)
        return ret

    def graph(self, sessionId):
        """
        Returns a dictionary where the key are the DROP UIDs, and the values are
        the DROP specifications.
        """
        graph = self._get_json('/sessions/%s/graph' % (urllib.quote(sessionId),))
        logger.debug('Successfully read graph (%d nodes) from session %s on %s:%s', len(graph), sessionId, self.host, self.port)
        return graph

    def sessions(self):
        """
        Returns a list of all the sessions currently held by the DROP Manager
        """
        sessions = self._get_json('/sessions')
        logger.debug('Successfully read %d sessions from %s:%s', len(sessions), self.host, self.port)
        return sessions

    def session(self, sessionId):
        """
        Returns the details of sessions `sessionId`
        """
        session = self._get_json('/sessions/%s' % (urllib.quote(sessionId),))
        logger.debug('Successfully read session %s from %s:%s', sessionId, self.host, self.port)
        return session

    def session_status(self, sessionId):
        """
        Returns the status of session `sessionId`
        """
        status = self._get_json('/sessions/%s/status' % (urllib.quote(sessionId),))
        logger.debug('Successfully read session %s status (%s) from %s:%s', sessionId, status, self.host, self.port)
        return status

    def graph_size(self, sessionId):
        """
        Returns the size of the graph of session `sessionId`
        """
        count = self._get_json('/sessions/%s/graph/size' % (urllib.quote(sessionId)))
        logger.debug('Successfully read session %s graph size (%d) from %s:%s', sessionId, count, self.host, self.port)
        return count

    # Offer an API similar to that exposed by Drop Managers objects
    createSession = create_session
    destroySession = destroy_session
    getSessionStatus = session_status
    addGraphSpec = append_graph
    deploySession = deploy_session
    getGraphStatus = graph_status
    getGraphSize = graph_size
    getGraph = graph

class NodeManagerClient(BaseDROPManagerClient):
    """
    A NodeManager REST client
    """
    def __init__(self, host='localhost', port=constants.NODE_DEFAULT_REST_PORT, timeout=10):
        super(NodeManagerClient, self).__init__(host=host, port=port, timeout=timeout)

    def add_node_subscriptions(self, sessionId, node_subscriptions):
        self._post_json('/sessions/%s/subscriptions' % (urllib.quote(sessionId),), node_subscriptions)

    def trigger_drops(self, sessionId, drop_uids):
        self._post_json('/sessions/%s/trigger' % (urllib.quote(sessionId),), drop_uids)

    def shutdown_node_manager(self):
        self._GET('/shutdown')

class CompositeManagerClient(BaseDROPManagerClient):

    def nodes(self):
        return self._get_json('/nodes')

    def add_node(self, node):
        self._POST('/nodes/%s' % (node,), content=None)

    def remove_node(self, node):
        self._DELETE('/nodes/%s' % (node,))

class DataIslandManagerClient(CompositeManagerClient):
    """
    A DataIslandManager REST client
    """
    def __init__(self, host='localhost', port=constants.ISLAND_DEFAULT_REST_PORT, timeout=10):
        super(DataIslandManagerClient, self).__init__(host=host, port=port, timeout=timeout)

class MasterManagerClient(CompositeManagerClient):
    """
    A MasterManager REST client
    """
    def __init__(self, host='localhost', port=constants.MASTER_DEFAULT_REST_PORT, timeout=10):
        super(MasterManagerClient, self).__init__(host=host, port=port, timeout=timeout)

    def create_island(self, island_host, nodes):
        self._post_json('/managers/%s/dataisland' % (urllib.quote(island_host)), {'nodes': nodes})