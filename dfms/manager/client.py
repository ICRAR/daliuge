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
import httplib
import json
import logging
import urllib

from dfms import utils
from dfms.manager import constants


logger = logging.getLogger(__name__)

class ManagerClientException(Exception):
    pass

class BaseDROPManagerClient(object):
    """
    Base class for REST clients that talk to the DROP managers.
    """

    def __init__(self, host, port, timeout):
        self.host = host
        self.port = port
        self.timeout = timeout
        self._conn = None
        self._resp = None

    def _close(self):
        if self._resp:
            self._resp.close()
        if self._conn:
            self._conn.close()

    __del__ = _close
    def __enter__(self):
        return self
    def __exit__(self, typ, value, traceback):
        self._close()
        if typ:
            raise value

    def create_session(self, sessionId):
        """
        Creates a session with `sessionId`
        """
        self._post_json('/sessions', {'sessionId': sessionId})
        logger.info('Successfully created session %s on %s:%s' % (sessionId, self.host, self.port))

    def deploy_session(self, sessionId, completed_uids=[]):
        """
        Deploys session `sessionId`, effectively creating its DROPs and triggering
        the execution of the graph
        """
        content = None
        if completed_uids:
            content = {'completed': ','.join(completed_uids)}
        ret = self._post_form('/sessions/%s/deploy' % (sessionId), content)
        logger.info('Successfully deployed session %s on %s:%s' % (sessionId, self.host, self.port))
        return ret

    def append_graph(self, sessionId, graphSpec):
        """
        Appends a graph to session `sessionId`, without creating its DROPs yet,
        but checking that the graph looks correct
        """
        self._post_json('/sessions/%s/graph/append' % (sessionId), graphSpec)
        logger.info('Successfully appended graph to session %s on %s:%s' % (sessionId, self.host, self.port))

    def destroy_session(self, sessionId):
        """
        Destroys session `sessionId`
        """
        self._DELETE('/sessions/%s' % (sessionId,))
        logger.info('Successfully deleted session %s on %s:%s' % (sessionId, self.host, self.port))

    def graph_status(self, sessionId):
        """
        Returns a dictionary where the keys are DROP UIDs and the values are
        their corresponding status.
        """
        ret = self._get_json('/sessions/%s/graph/status' % (sessionId))
        logger.info('Successfully read graph status from session %s on %s:%s' % (sessionId, self.host, self.port))
        return ret

    def graph(self, sessionId):
        """
        Returns a dictionary where the key are the DROP UIDs, and the values are
        the DROP specifications.
        """
        graph = self._get_json('/sessions/%s/graph' % (sessionId))
        logger.info('Successfully read graph (%d nodes) from session %s on %s:%s' % (len(graph), sessionId, self.host, self.port))
        return graph

    def sessions(self):
        """
        Returns a list of all the sessions currently held by the DROP Manager
        """
        sessions = self._get_json('/sessions')
        logger.info('Successfully read %d sessions from %s:%s' % (len(sessions), self.host, self.port))
        return sessions

    def session(self, sessionId):
        """
        Returns the details of sessions `sessionId`
        """
        session = self._get_json('/sessions/%s' % (sessionId,))
        logger.info('Successfully read session %s from %s:%s' % (sessionId, self.host, self.port))
        return session

    def session_status(self, sessionId):
        """
        Returns the status of session `sessionId`
        """
        status = self._get_json('/sessions/%s/status' % (sessionId,))
        logger.info('Successfully read session %s status (%s) from %s:%s' % (sessionId, status, self.host, self.port))
        return status

    # Offer an API similar to that exposed by Drop Managers objects
    createSession = create_session
    destroySession = destroy_session
    getSessionStatus = session_status
    addGraphSpec = append_graph
    deploySession = deploy_session
    getGraphStatus = graph_status
    getGraph = graph

    def _get_json(self, url):
        return json.loads(self._GET(url))

    def _post_form(self, url, content=None):
        if content is not None:
            content = urllib.urlencode(content)
        ret = self._POST(url, content, 'application/x-www-form-urlencoded')
        return json.loads(ret) if ret else None

    def _post_json(self, url, content):
        if not isinstance(content, basestring):
            content = json.dumps(content)
        ret = self._POST(url, content, 'application/json')
        return json.loads(ret) if ret else None

    def _GET(self, url):
        return self._request(url, 'GET')

    def _POST(self, url, content=None, content_type=None):
        headers = {}
        if content_type:
            headers['Content-Type'] = content_type
        return self._request(url, 'POST', content, headers)

    def _DELETE(self, url):
        return self._request(url, 'DELETE')

    def _request(self, url, method, content=None, headers={}):

        # Normalize first
        if not url.startswith('/'):
            url = '/' + url
        url = '/api' + url

        # Do the HTTP stuff...
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Sending %s request to %s:%d%s" % (method, self.host, self.port, url))

        if not utils.portIsOpen(self.host, self.port, self.timeout):
            raise ManagerClientException("Cannot connect to %s:%d after %.2f [s]" % (self.host, self.port, self.timeout))

        self._conn = httplib.HTTPConnection(self.host, self.port)
        self._conn.request(method, url, content, headers)
        self._resp = self._conn.getresponse()

        # Server errors are encoded in the body as json content
        if self._resp.status == httplib.INTERNAL_SERVER_ERROR:
            msg = json.loads(self._resp.read())['err_str']
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('Error found while requesting %s:%d%s: %s' % (self.host, self.port, url, msg))
            raise ManagerClientException(msg)
        elif self._resp.status != httplib.OK:
            msg = 'Unexpected error while processing %s request for %s:%s%s (status %d): %s' % \
                  (method, self.host, self.port, url, self._resp.status, self._resp.read())
            raise ManagerClientException(msg)

        return self._resp.read()

class NodeManagerClient(BaseDROPManagerClient):
    """
    A NodeManager REST client
    """
    def __init__(self, host='localhost', port=constants.NODE_DEFAULT_REST_PORT, timeout=10):
        super(NodeManagerClient, self).__init__(host=host, port=port, timeout=timeout)

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