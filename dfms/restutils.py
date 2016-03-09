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
import threading

import bottle
import tornado.httpserver
import tornado.ioloop
import tornado.wsgi


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