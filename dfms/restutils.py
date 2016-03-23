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
import threading
import urllib

import bottle
import tornado.httpserver
import tornado.ioloop
import tornado.wsgi

from dfms import utils


logger = logging.getLogger(__name__)

class RestServer(object):
    """
    The base class for our REST servers
    """

    def __init__(self, jsonified_errors=[500]):

        self._ioloop = None
        self.app = app = bottle.Bottle()

        # Error are returned as strings in a dictionary
        if jsonified_errors:
            def jsonify_error(e):
                bottle.response.content_type = 'application/json'
                return json.dumps({'err_str': e.body})
            for err_code in jsonified_errors:
                app.error_handler[err_code] = jsonify_error

    def start(self, host, port):
        if host is None:
            host = 'localhost'
        if port is None:
            port = 8080

        # It seems it's not trivial to stop a running bottle server, so we use
        # tornado's IOLoop directly instead
        logger.info("Starting REST server on %s:%d" % (host, port))
        self._ioloop = tornado.ioloop.IOLoop()
        self._ioloop.make_current()
        self._server = tornado.httpserver.HTTPServer(tornado.wsgi.WSGIContainer(self.app), io_loop=self._ioloop)
        self._server.listen(port=port,address=host)
        self._ioloop.start()

    def stop(self, timeout=None):

        if self._ioloop:

            logger.info("Stopping REST server")

            self.app.close()

            # Submit a callback to the IOLoop to stop itself and wait until it's
            # done with it
            ioloop_stopped = threading.Event()
            def stop_ioloop():
                self._ioloop.stop()
                ioloop_stopped.set()

            self._ioloop.add_callback(stop_ioloop)
            if not ioloop_stopped.wait(timeout):
                logger.warning("Timed out while waiting for the server to stop")

            self._server.stop()

class RestClientException(Exception):
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
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Sending %s request to %s:%d%s" % (method, self.host, self.port, url))

        if not utils.portIsOpen(self.host, self.port, self.timeout):
            raise RestClientException("Cannot connect to %s:%d after %.2f [s]" % (self.host, self.port, self.timeout))

        self._conn = httplib.HTTPConnection(self.host, self.port)
        self._conn.request(method, url, content, headers)
        self._resp = self._conn.getresponse()

        # Server errors are encoded in the body as json content
        if self._resp.status == httplib.INTERNAL_SERVER_ERROR:
            msg = json.loads(self._resp.read())['err_str']
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('Error found while requesting %s:%d%s: %s' % (self.host, self.port, url, msg))
            raise RestClientException(msg)
        elif self._resp.status != httplib.OK:
            msg = 'Unexpected error while processing %s request for %s:%s%s (status %d): %s' % \
                  (method, self.host, self.port, url, self._resp.status, self._resp.read())
            raise RestClientException(msg)

        return self._resp.read()
