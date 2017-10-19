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
import codecs
import json
import logging
from wsgiref.simple_server import make_server, WSGIServer, WSGIRequestHandler

import bottle
import six
import six.moves.http_client as httplib  # @UnresolvedImport
import six.moves.socketserver as SocketServer  # @UnresolvedImport
import six.moves.urllib_parse as urllib  # @UnresolvedImport

from . import exceptions
from . import utils
from .exceptions import DaliugeException, SubManagerException


logger = logging.getLogger(__name__)

class ThreadingWSGIServer(SocketServer.ThreadingMixIn, WSGIServer):
    daemon_threads = True
    allow_reuse_address = True

class LoggingWSGIRequestHandler(WSGIRequestHandler):
    def log_message(self, fmt, *args):
        logger.debug(fmt, *args)

class RestServerWSGIServer:
    def __init__(self, wsgi_app, listen = '127.0.0.1', port = 8080):
        self.wsgi_app = wsgi_app
        self.listen = listen
        self.port = port
        self.server = make_server(self.listen, self.port, self.wsgi_app,
                                  server_class=ThreadingWSGIServer,
                                  handler_class=LoggingWSGIRequestHandler)

    def serve_forever(self):
        self.server.serve_forever()

    def server_close(self):
        self.server.shutdown()
        self.server.server_close()

class RestServer(object):
    """
    The base class for our REST servers
    """

    def __init__(self):
        self._server = None
        self._server_thr = None
        self.app = bottle.Bottle()

    def start(self, host, port):
        host = host or 'localhost'
        port = port or 8080

        logger.info("Starting REST server on %s:%d" % (host, port))

        self._server = RestServerWSGIServer(self.app, host, port)
        self._server.serve_forever()

    def stop(self, timeout=None):
        if self._server:
            logger.info("Stopping REST server")
            self._server.server_close()
            self.app.close()
            self._server = None

class RestClientException(DaliugeException):
    """
    Exception thrown by the RestClient
    """

def hexdigits(n):
    digits = 0
    while n:
        digits += 1
        n //= 16
    return digits

def chunk(data):
    return ("%x" % len(data)).encode('ascii') + b'\r\n' + data + b'\r\n'

class chunked(object):
    """
    A reader that returns chunked HTTP content
    """
    def __init__(self, content):
        self.content = content
        self.finished = False
    def read(self, n):
        if self.finished:
            return b''
        n = n - hexdigits(n) - 4
        data = self.content.read(n)
        if not data:
            self.finished = True
            return b"0\r\n\r\n"
        return chunk(data)

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
        ret = self._GET(url)
        return json.load(ret) if ret else None

    def _post_form(self, url, content=None):
        if content is not None:
            content = urllib.urlencode(content)
        ret = self._POST(url, content, content_type='application/x-www-form-urlencoded')
        return json.load(ret) if ret else None

    def _post_json(self, url, content, compress=False):
        if not isinstance(content, (six.text_type, six.binary_type)):
            content = utils.JSONStream(content)
        ret = self._POST(url, content, content_type='application/json', compress=compress)
        return json.load(ret) if ret else None

    def _GET(self, url):
        return self._request(url, 'GET')

    def _POST(self, url, content=None, content_type=None, compress=False):
        headers = {}
        if content_type:
            headers['Content-Type'] = content_type
        if compress and content:
            headers['Content-Encoding'] = 'gzip'
            if isinstance(content, six.text_type):
                content = codecs.getencoder('utf8')(content)[0]
            if not hasattr(content, 'read'):
                content = six.BytesIO(content)
            content = utils.ZlibCompressedStream(content)
        return self._request(url, 'POST', content, headers)

    def _DELETE(self, url):
        return self._request(url, 'DELETE')

    def _request(self, url, method, content=None, headers={}):

        # Do the HTTP stuff...
        logger.debug("Sending %s request to %s:%d%s", method, self.host, self.port, url)

        if not utils.portIsOpen(self.host, self.port, self.timeout):
            raise RestClientException("Cannot connect to %s:%d after %.2f [s]" % (self.host, self.port, self.timeout))

        if content and hasattr(content, 'read'):
            headers['Transfer-Encoding'] = 'chunked'
            content = chunked(content)

        self._conn = httplib.HTTPConnection(self.host, self.port)
        self._conn.request(method, url, content, headers)
        self._resp = self._conn.getresponse()

        # Server errors are encoded in the body as json content
        if self._resp.status != httplib.OK:

            msg = 'Error on remote %s@%s:%s%s (status %d): ' % \
                  (method, self.host, self.port, url, self._resp.status)

            try:
                error = json.loads(self._resp.read().decode('utf-8'))
                etype = getattr(exceptions, error['type'])
                eargs = error['args']

                if etype == SubManagerException:
                    for host,args in eargs.items():
                        subetype = getattr(exceptions, args['type'])
                        subargs = args['args']
                        eargs[host] = subetype(*subargs)
                    ex = etype(eargs)
                else:
                    ex = etype(*eargs)
                if hasattr(ex, 'msg'):
                    ex.msg = msg + ex.msg
            except Exception:
                ex = RestClientException(msg + "Unknown")

            raise ex

        if not self._resp.length:
            return None
        return codecs.getreader('utf-8')(self._resp)