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

from dfms.manager import constants


logger = logging.getLogger(__name__)

class BaseDROPManagerClient(object):
    """
    Base class for REST clients that talk to the DROP managers.
    """

    def __init__(self, host, port):
        self.host = host
        self.port = port

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
        self._post_form('/sessions/%s/deploy' % (sessionId), content)
        logger.info('Successfully deployed session %s on %s:%s' % (sessionId, self.host, self.port))

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

    def _get_json(self, url):
        return json.loads(self._GET(url))

    def _post_form(self, url, content=None):
        if content is not None:
            content = urllib.urlencode(content)
        self._POST(url, content, 'application/x-www-form-urlencoded')

    def _post_json(self, url, content):
        if not isinstance(content, basestring):
            content = json.dumps(content, cls=SetEncoder)
        self._POST(url, content, 'application/json')

    def _GET(self, url):
        return self._request(url, 'GET')

    def _POST(self, url, content, content_type):
        headers = {'Content-Type': content_type}
        self._request(url, 'POST', content, headers)

    def _DELETE(self, url):
        return self._request(url, 'DELETE')

    def _request(self, url, method, content=None, headers={}):

        # Normalize first
        if not url.startswith('/'):
            url = '/' + url
        url = '/api' + url

        # Do the HTTP stuff...
        connection = httplib.HTTPConnection(self.host, self.port)
        connection.request(method, url, content, headers)
        response = connection.getresponse()
        if response.status != httplib.OK:
            msg = 'Error while processing %s request for %s:%s%s (status %d): %s' % \
                  (method, self.host, self.port, url, response.status, response.read())
            raise Exception(msg)

        return response.read()

class NodeManagerClient(BaseDROPManagerClient):
    """
    A NodeManager REST client
    """
    def __init__(self, host='localhost', port=constants.NODE_DEFAULT_REST_PORT):
        super(NodeManagerClient, self).__init__(host=host, port=port)

class DataIslandManagerClient(BaseDROPManagerClient):
    """
    A DataIslandManager REST client
    """
    def __init__(self, host='localhost', port=constants.ISLAND_DEFAULT_REST_PORT):
        super(DataIslandManagerClient, self).__init__(host=host, port=port)

class MasterManagerClient(BaseDROPManagerClient):
    """
    A MasterManager REST client
    """
    def __init__(self, host='localhost', port=constants.MASTER_DEFAULT_REST_PORT):
        super(MasterManagerClient, self).__init__(host=host, port=port)


class SetEncoder(json.JSONEncoder):
    """
    The dictdrop uses a set which is not serializable into JSON, so this encoder helps
    perform the serialization
    """
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)
