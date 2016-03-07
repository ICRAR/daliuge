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
Module containing the DFMS Daemon class and command-line entry point to use it
"""

import functools
import json
import logging
import optparse
import signal
import socket
import subprocess
import sys
import threading
import time

import bottle
import tornado.httpserver
import tornado.ioloop
import tornado.wsgi
from zeroconf import ServiceStateChange

from dfms import utils
from dfms.manager import constants, client


logger = logging.getLogger(__name__)

class DfmsDaemon(object):
    """
    The DFMS Daemon

    The DFMS Daemon is the long-running process that we assume is always
    available for contacting, and that acts as the bootstrapping of the whole
    system. It exposes a REST API through which users can start the different
    Drop Managers (node, dataisland and master) and query their status.
    Optionally it can also start automatically the node manager (default: yes)
    and the master manager (default: no) at creation time.
    """

    def __init__(self, master=False, noNM=False, disable_zeroconf=False):

        # The three processes we run
        self._nm_proc = None
        self._dim_proc = None
        self._mm_proc = None

        # Zeroconf for NM and MM
        self._enable_zeroconf = not disable_zeroconf
        self._nm_zc = None
        self._nm_info = None
        self._mm_zc = None
        self._mm_browser = None

        # Set up our REST interface
        self._ioloop = None
        self.app = app = bottle.Bottle()

        # Starting managers
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

    def run(self, host=None, port=None):
        """
        Runs the DFMS Daemon binding its REST interface to the given host and
        port. The binding defaults to 0.0.0.0:9000
        """
        if host is None:
            host = '0.0.0.0'
        if port is None:
            port = 9000

        # Start the web server and listen for requests until we're done
        self._ioloop = tornado.ioloop.IOLoop.instance()
        self._server = tornado.httpserver.HTTPServer(tornado.wsgi.WSGIContainer(self.app), io_loop=self._ioloop)
        self._server.listen(port=port,address=host)
        self._ioloop.start()

    def stop(self):
        """
        Stops this DFMS Daemon, terminating all its child processes and its REST
        server.
        """
        timeout = 10

        self._stop_rest_server(timeout)
        self.stopNM(timeout)
        self.stopDIM(timeout)
        self.stopMM(timeout)
        logger.info('DFMS Daemon stopped')

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

            # Terminate; if it doesn't go away kill it
            pid = proc.pid
            logger.info('Terminating %d' % (pid,))
            proc.terminate()

            waitLoops = 0
            max_loops = timeout/0.1
            while proc.poll() is None and waitLoops < max_loops:
                time.sleep(0.1)
                waitLoops += 1

            kill9 = waitLoops == max_loops
            if kill9:
                logger.info('Killing %s by brute force after waiting %.2f [s], BANG! :-(' % (timeout, pid,))
                proc.kill()
            proc.wait()

            setattr(self, name, None)

    def stopNM(self, timeout=None):
        self._stop_manager('_nm_proc', timeout)
        if self._enable_zeroconf and self._nm_zc:
            utils.deregister_service(self._nm_zc, self._nm_info)

    def stopDIM(self, timeout=None):
        self._stop_manager('_dim_proc', timeout)

    def stopMM(self, timeout=None):
        self._stop_manager('_mm_proc', timeout)
        if self._enable_zeroconf and self._mm_browser:
            self._mm_browser.cancel()

    # Methods to start and stop the individual managers
    def startNM(self):

        args  = [sys.executable, '-m', 'dfms.manager.cmdline', 'dfmsNM']
        args += ['--rest', '-i', 'nm', '--host', '0.0.0.0']
        logger.info("Starting Node Drop Manager with args: %s" % (" ".join(args)))
        self._nm_proc = subprocess.Popen(args)
        logger.info("Started Node Drop Manager with PID %d" % (self._nm_proc.pid))

        # Registering the new NodeManager via zeroconf so it gets discovered
        # by the Master Manager
        if self._enable_zeroconf:
            addrs = utils.get_local_ip_addr()
            self._nm_zc, self._nm_info = utils.register_service('NodeManager', addrs[0], 'tcp', addrs[0][0], constants.NODE_DEFAULT_REST_PORT)

    def startDIM(self, nodes):
        args  = [sys.executable, '-m', 'dfms.manager.cmdline', 'dfmsDIM']
        args += ['--rest', '-i', 'dim', '--host', '0.0.0.0']
        if nodes:
            args += ['--nodes', ",".join(nodes)]
        logger.info("Starting Data Island Drop Manager with args: %s" % (" ".join(args)))
        self._dim_proc = subprocess.Popen(args)
        logger.info("Started Data Island Drop Manager with PID %d" % (self._dim_proc.pid))

    def startMM(self):

        args  = [sys.executable, '-m', 'dfms.manager.cmdline', 'dfmsMM']
        args += ['--rest', '-i', 'mm', '--host', '0.0.0.0']
        logger.info("Starting Master Drop Manager with args: %s" % (" ".join(args)))
        self._mm_proc = subprocess.Popen(args)
        logger.info("Started Master Drop Manager with PID %d" % (self._mm_proc.pid))

        # Also subscribe to zeroconf events coming from NodeManagers and feed
        # the Master Manager with the new hosts we find
        if self._enable_zeroconf:
            mm_client = client.MasterManagerClient()
            node_managers = {}
            def nm_callback(zeroconf, service_type, name, state_change):
                info = zeroconf.get_service_info(service_type, name)
                if state_change is ServiceStateChange.Added:
                    server = socket.inet_ntoa(info.address)
                    port = info.port
                    node_managers[name] = (server, port)
                    logger.info("Found a new Node Manager on %s:%d, will add it to the MM" % (server, port))
                    mm_client.add_node(server)
                elif state_change is ServiceStateChange.Removed:
                    server,port = node_managers[name]
                    logger.info("Node Manager on %s:%d disappeared, removing it from the MM" % (server, port))
                    mm_client.remove_node(server)
                    del node_managers[name]
            self._mm_zc, self._mm_browser = utils.browse_service('NodeManager', 'tcp', nm_callback)

    # Rest interface
    def _rest_start_manager(self, proc, start_method):
        if proc is not None:
            bottle.response.status = 409 # Conflict
            return
        start_method()

    def _rest_get_manager_info(self, proc):
        if proc:
            bottle.response.content_type = 'application/json'
            return json.dumps({'pid': proc.pid})
        bottle.response.status = 404

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
def run_with_cmdline(args=sys.argv):

    parser = optparse.OptionParser()
    parser.add_option('-m', '--master', action='store_true',
                      dest="master", help="Start this DFMS daemon as the master daemon", default=False)
    parser.add_option("--no-nm", action="store_true",
                      dest="noNM", help = "Don't start a NodeDropManager by default", default=False)
    parser.add_option("--no-zeroconf", action="store_true",
                      dest="noZC", help = "Don't enable zeroconf on this DFMS daemon", default=False)
    (opts, args) = parser.parse_args(args)

    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    daemon = DfmsDaemon(opts.master, opts.noNM, opts.noZC)

    # Signal handling, which stops the daemon
    def handle_signal(signalNo, stack_frame):
        global terminating
        if terminating:
            return
        logger.info("Received signal %d, will stop the daemon now" % (signalNo,))
        terminating = True
        daemon.stop()
    signal.signal(signal.SIGINT,  handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # Go, go, go!
    t = threading.Thread(target=daemon.run)
    t.start()
    signal.pause()

if __name__ == '__main__':
    # In case we get called directly...
    run_with_cmdline(sys.argv)