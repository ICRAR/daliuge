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
import json
import logging
import urllib

import bottle
from paste import httpserver
import six.moves.http_client as httplib  # @UnresolvedImport

from dfms import utils
from dfms.exceptions import DaliugeException


# HTTP headers used by daliuge
DALIUGE_HDR_ERR = 'X-Daliuge-Error'

logger = logging.getLogger(__name__)

class RestServer(object):
    """
    The base class for our REST servers
    """

    def __init__(self):
        self._server = None
        self._server_thr = None
        self.app = bottle.Bottle()

    def start(self, host, port):
        if host is None:
            host = 'localhost'
        if port is None:
            port = 8080

        # It seems it's not trivial to stop a running bottle server, so we use
        # tornado's IOLoop directly instead
        logger.info("Starting REST server on %s:%d" % (host, port))

        self._server = httpserver.serve(self.app, host=host, port=port, start_loop=False)
        self._server.serve_forever()

    def stop(self, timeout=None):
        if self._server:
            logger.info("Stopping REST server")
            self._server.server_close()
            self._server = None
            self.app.close()

class RestClientException(DaliugeException):
    """
    Exception thrown by the RestClient
    """
    pass


class RestClient(object):
    """
    The base class for our REST clients
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

        # Do the HTTP stuff...
        logger.debug("Sending %s request to %s:%d%s", method, self.host, self.port, url)

        if not utils.portIsOpen(self.host, self.port, self.timeout):
            raise RestClientException("Cannot connect to %s:%d after %.2f [s]" % (self.host, self.port, self.timeout))

        self._conn = httplib.HTTPConnection(self.host, self.port)
        self._conn.request(method, url, content, headers)
        self._resp = self._conn.getresponse()

        # Server errors are encoded in the body as json content
        if self._resp.status != httplib.OK:
            err = self._resp.getheader(DALIUGE_HDR_ERR) or self._resp.reason or self._resp.msg
            msg = 'Unexpected error while processing %s request for %s:%s%s (status %d): %s' % \
                  (method, self.host, self.port, url, self._resp.status, err)
            raise RestClientException(msg)

        return self._resp.read()
