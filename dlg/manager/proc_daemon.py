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
"""
Module containing the DALiuGE Daemon class and command-line entry point to use it
"""

import functools
import json
import logging
import signal
import socket
import sys
import threading

import bottle
import zeroconf as zc

from . import constants, client
from .. import utils
from ..restutils import RestServer


logger = logging.getLogger(__name__)

def get_tool():
    # This import is performed at runtime to avoid a circular dependency
    # at import time with the tool module, which imports this module
    # to make it available as a 'dlg' command
    from .. import tool
    return tool

class DlgDaemon(RestServer):
    """
    The DALiuGE Daemon

    The DALiuGE Daemon is the long-running process that we assume is always
    available for contacting, and that acts as the bootstrapping of the whole
    system. It exposes a REST API through which users can start the different
    Drop Managers (node, dataisland and master) and query their status.
    Optionally it can also start automatically the node manager (default: yes)
    and the master manager (default: no) at creation time.
    """

    def __init__(self, master=False, noNM=False, disable_zeroconf=False, verbosity=0):

        super(DlgDaemon, self).__init__()

        self._shutting_down = False
        self._verbosity = verbosity

        # The three processes we run
        self._nm_proc = None
        self._dim_proc = None
        self._mm_proc = None

        # Zeroconf for NM and MM
        self._zeroconf = None if disable_zeroconf else zc.Zeroconf()
        self._nm_info = None
        self._mm_browser = None

        # Starting managers
        app = self.app
        app.post('/managers/node',       callback=self.rest_startNM)
        app.post('/managers/dataisland', callback=self.rest_startDIM)
        app.post('/managers/master',     callback=self.rest_startMM)

        # Querying about managers
        app.get('/managers/node',       callback=self.rest_getNMInfo)
        app.get('/managers/dataisland', callback=self.rest_getDIMInfo)
        app.get('/managers/master',     callback=self.rest_getMMInfo)

        # Automatically start those that we need
        if master:
            self.startMM()
        if not noNM:
            self.startNM()

    def stop(self, timeout=None):
        """
        Stops this DALiuGE Daemon, terminating all its child processes and its REST
        server.
        """
        self._shutting_down = True
        super(DlgDaemon, self).stop(timeout)
        self._stop_zeroconf()
        self.stopNM(timeout)
        self.stopDIM(timeout)
        self.stopMM(timeout)
        logger.info('DALiuGE Daemon stopped')

    def _stop_zeroconf(self):

        if not self._zeroconf:
            return

        # Stop the MM service browser, the NM registration, and ZC itself
        if self._mm_browser:
            self._mm_browser.cancel()
            self._mm_browser.join()
        if self._nm_info:
            utils.deregister_service(self._zeroconf, self._nm_info)
        self._zeroconf.close()

    def _stop_rest_server(self, timeout):
        if self._ioloop:

            self.app.close()

            # Submit a callback to the IOLoop to stop itself and wait until it's
            # done with it
            logger.debug("Stopping the web server")
            ioloop_stopped = threading.Event()
            def stop_ioloop():
                self._ioloop.stop()
                ioloop_stopped.set()

            self._ioloop.add_callback(stop_ioloop)
            if not ioloop_stopped.wait(timeout):
                logger.warning("Timed out while waiting for the server to stop")
            self._server.stop()

            self._started = False

    def _stop_manager(self, name, timeout):
        proc = getattr(self, name)
        if proc:
            utils.terminate_or_kill(proc, timeout)
            setattr(self, name, None)

    def stopNM(self, timeout=None):
        self._stop_manager('_nm_proc', timeout)

    def stopDIM(self, timeout=None):
        self._stop_manager('_dim_proc', timeout)

    def stopMM(self, timeout=None):
        self._stop_manager('_mm_proc', timeout)

    # Methods to start and stop the individual managers
    def startNM(self):
        tool = get_tool()
        args = ['--host', '0.0.0.0']
        args += self._verbosity_as_cmdline()
        logger.info("Starting Node Drop Manager with args: %s" % (" ".join(args)))
        self._nm_proc = tool.start_process('nm', args)
        logger.info("Started Node Drop Manager with PID %d" % (self._nm_proc.pid))

        # Registering the new NodeManager via zeroconf so it gets discovered
        # by the Master Manager
        if self._zeroconf:
            addrs = utils.get_local_ip_addr()
            self._nm_info = utils.register_service(self._zeroconf, 'NodeManager', socket.gethostname(), addrs[0][0], constants.NODE_DEFAULT_REST_PORT)

    def startDIM(self, nodes):
        tool = get_tool()
        args  = ['--host', '0.0.0.0']
        args += self._verbosity_as_cmdline()
        if nodes:
            args += ['--nodes', ",".join(nodes)]
        logger.info("Starting Data Island Drop Manager with args: %s" % (" ".join(args)))
        self._dim_proc = tool.start_process('dim', args)
        logger.info("Started Data Island Drop Manager with PID %d" % (self._dim_proc.pid))

    def startMM(self):
        tool = get_tool()
        args  = ['--host', '0.0.0.0']
        args += self._verbosity_as_cmdline()
        logger.info("Starting Master Drop Manager with args: %s" % (" ".join(args)))
        self._mm_proc = tool.start_process('mm', args)
        logger.info("Started Master Drop Manager with PID %d" % (self._mm_proc.pid))

        # Also subscribe to zeroconf events coming from NodeManagers and feed
        # the Master Manager with the new hosts we find
        if self._zeroconf:
            mm_client = client.MasterManagerClient()
            node_managers = {}
            def nm_callback(zeroconf, service_type, name, state_change):
                info = zeroconf.get_service_info(service_type, name)
                if state_change is zc.ServiceStateChange.Added:
                    server = socket.inet_ntoa(info.address)
                    port = info.port
                    node_managers[name] = (server, port)
                    logger.info("Found a new Node Manager on %s:%d, will add it to the MM" % (server, port))
                    mm_client.add_node(server)
                elif state_change is zc.ServiceStateChange.Removed:
                    server,port = node_managers[name]
                    logger.info("Node Manager on %s:%d disappeared, removing it from the MM" % (server, port))

                    # Don't bother to remove it if we're shutting down. This way
                    # we avoid hanging in here if the MM is down already but
                    # we are trying to remove our NM who has just disappeared
                    if not self._shutting_down:
                        try:
                            mm_client.remove_node(server)
                        finally:
                            del node_managers[name]

            self._mm_browser = utils.browse_service(self._zeroconf, 'NodeManager', 'tcp', nm_callback)

    def _verbosity_as_cmdline(self):
        if self._verbosity > 0:
            return ["-" + "v"*self._verbosity]
        elif self._verbosity < 0:
            return ["-" + "q"*(-self._verbosity)]
        return ()

    # Rest interface
    def _rest_start_manager(self, proc, start_method):
        if proc is not None:
            bottle.abort(409, 'The Drop Manager is already running') # Conflict
        start_method()

    def _rest_get_manager_info(self, proc):
        if proc:
            bottle.response.content_type = 'application/json'
            return json.dumps({'pid': proc.pid})
        bottle.abort(404, 'The Drop Manager is not running')

    def rest_startNM(self):
        self._rest_start_manager(self._nm_proc, self.startNM)

    def rest_startDIM(self):
        body = bottle.request.json
        if not body or 'nodes' not in body:
            bottle.response.status = 400
            bottle.response.body = 'JSON content is expected with a "nodes" list in it'
            return
        nodes = body['nodes']
        self._rest_start_manager(self._dim_proc, functools.partial(self.startDIM, nodes))

    def rest_startMM(self):
        self._rest_start_manager(self._mm_proc, self.startMM)

    def rest_getNMInfo(self):
        return self._rest_get_manager_info(self._nm_proc)

    def rest_getDIMInfo(self):
        return self._rest_get_manager_info(self._dim_proc)

    def rest_getMMInfo(self):
        return self._rest_get_manager_info(self._mm_proc)


terminating = False
def run_with_cmdline(parser, args):
    parser.add_option('-m', '--master', action='store_true',
                      dest="master", help="Start this DALiuGE daemon as the master daemon", default=False)
    parser.add_option("--no-nm", action="store_true",
                      dest="noNM", help = "Don't start a NodeDropManager by default", default=False)
    parser.add_option("--no-zeroconf", action="store_true",
                      dest="noZC", help = "Don't enable zeroconf on this DALiuGE daemon", default=False)
    parser.add_option("-v", "--verbose", action="count",
                      dest="verbose", help="Become more verbose. The more flags, the more verbose", default=0)
    parser.add_option("-q", "--quiet", action="count",
                      dest="quiet", help="Be less verbose. The more flags, the quieter", default=0)

    (opts, args) = parser.parse_args(args)

    # -v and -q are exclusive
    if opts.verbose and opts.quiet:
        parser.error('-v and -q cannot be specified together')

    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    daemon = DlgDaemon(opts.master, opts.noNM, opts.noZC, opts.verbose - opts.quiet)

    # Signal handling, which stops the daemon
    def handle_signal(signalNo, stack_frame):
        global terminating
        if terminating:
            return
        logger.info("Received signal %d, will stop the daemon now" % (signalNo,))
        terminating = True
        daemon.stop(10)
    signal.signal(signal.SIGINT,  handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # Go, go, go!
    t = threading.Thread(target=daemon.start, args=('0.0.0.0', constants.DAEMON_DEFAULT_REST_PORT))
    t.start()
    signal.pause()