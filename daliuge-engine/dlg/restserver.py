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

import bottle

from .restutils import RestServerWSGIServer

logger = logging.getLogger(f"dlg.{__name__}")


class RestServer(object):
    """
    The base class for our REST servers
    """

    def __init__(self):
        self._server = None
        self._server_thr = None
        self.app = bottle.Bottle()

    def start(self, host, port):
        host = host or "localhost"
        port = port or 8080

        logger.info("Starting REST server on %s:%d", host, port)

        self._server = RestServerWSGIServer(self.app, host, port)
        self._server.serve_forever()

    def stop(self):
        if self._server:
            logger.info("Stopping REST server")
            self._server.server_close()
            self.app.close()
            self._server = None
