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

from dlg import constants
from .. import utils
from ..restserver import RestServer
from dlg.nm_dim_assigner import NMAssigner

logger = logging.getLogger(f"dlg.{__name__}")


def get_tool():
    # This import is performed at runtime to avoid a circular dependency
    # at import time with the tool module, which imports this module
    # to make it available as a 'dlg' command
    from ..common import tool

    return tool


def _get_address(zeroconf_service_info):
    if tuple(map(int, zc.__version__.split(".")))[:2] >= (0, 23):
        return zeroconf_service_info.addresses[0]
    return zeroconf_service_info.address


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
        self._dim_info = None
        self._mm_nm_browser = None
        self._mm_dim_browser = None

        # Starting managers
        app = self.app
        app.post("/managers/node/start", callback=self.rest_startNM)
        app.post("/managers/node/stop", callback=self.rest_stopNM)
        app.post("/managers/island/start", callback=self.rest_startDIM)
        app.post("/managers/island/stop", callback=self.rest_stopDIM)
        app.post("/managers/master/start", callback=self.rest_startMM)
        app.post("/managers/master/stop", callback=self.rest_stopMM)

        # Querying about managers
        app.get("/", callback=self.rest_getMgrs)
        app.get("/managers", callback=self.rest_getMgrs)
        app.get("/managers/master", callback=self.rest_getMMInfo)
        app.get("/managers/island", callback=self.rest_getDIMInfo)
        app.get("/managers/node", callback=self.rest_getNMInfo)

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
        super(DlgDaemon, self).stop()
        self.stopNM(timeout)
        self.stopDIM(timeout)
        self.stopMM(timeout)
        self._stop_zeroconf()
        logger.info("DALiuGE Daemon stopped")

    def _stop_zeroconf(self):

        if not self._zeroconf:
            return

        # Stop the MM service browser, the NM registration, and ZC itself
        if self._mm_nm_browser:
            self._mm_nm_browser.cancel()
            self._mm_nm_browser.join()
        if self._mm_dim_browser:
            self._mm_dim_browser.cancel()
            self._mm_dim_browser.join()
        self._zeroconf.close()
        logger.info("Zeroconf stopped")

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
        logger.debug("Stopping manager %s", name)
        if proc:
            utils.terminate_or_kill(proc, timeout)
            pid = proc.pid
            setattr(self, name, None)
            return {"terminated": pid}
        else:
            logger.warning("No %s manager found!", name)
            return {}

    def stopNM(self, timeout=10):
        if self._nm_info:
            utils.deregister_service(self._zeroconf, self._nm_info)
        return self._stop_manager("_nm_proc", timeout)

    def stopDIM(self, timeout=10):
        if self._dim_info:
            utils.deregister_service(self._zeroconf, self._dim_info)
        self._stop_manager("_dim_proc", timeout)

    def stopMM(self, timeout=10):
        self._stop_manager("_mm_proc", timeout)

    # Methods to start and stop the individual managers
    def startNM(self):
        tool = get_tool()
        args = ["--host", "0.0.0.0"]
        args += self._verbosity_as_cmdline()
        logger.info("Starting Node Drop Manager with args: %s", (" ".join(args)))
        self._nm_proc = tool.start_process("nm", args)
        logger.info("Started Node Drop Manager with PID %d", self._nm_proc.pid)

        # Registering the new NodeManager via zeroconf so it gets discovered
        # by the Master Manager
        if self._zeroconf:
            addrs = utils.get_local_ip_addr()
            logger.info("Registering this NM with zeroconf: %s", addrs)
            self._nm_info = utils.register_service(
                self._zeroconf,
                "NodeManager",
                socket.gethostname(),
                addrs[0][0],
                constants.NODE_DEFAULT_REST_PORT,
            )
        return

    def startDIM(self, nodes):
        tool = get_tool()
        args = ["--host", "0.0.0.0"]
        args += self._verbosity_as_cmdline()
        if nodes:
            args += ["--nodes", ",".join(nodes)]
        logger.info("Starting Data Island Drop Manager with args: %s", (" ".join(args)))
        self._dim_proc = tool.start_process("dim", args)
        logger.info("Started Data Island Drop Manager with PID %d", self._dim_proc.pid)

        # Registering the new DIM via zeroconf so it gets discovered
        # by the Master Manager
        if self._zeroconf:
            addrs = utils.get_local_ip_addr()
            logger.info("Registering this DIM with zeroconf: %s", addrs)
            self._dim_info = utils.register_service(
                self._zeroconf,
                "DIM",
                socket.gethostname(),
                addrs[0][0],
                constants.ISLAND_DEFAULT_REST_PORT,
            )
        return

    def startMM(self):
        tool = get_tool()
        args = ["--host", "0.0.0.0"]
        args += self._verbosity_as_cmdline()
        logger.info("Starting Master Drop Manager with args: %s", (" ".join(args)))
        self._mm_proc = tool.start_process("mm", args)
        logger.info("Started Master Drop Manager with PID %d", self._mm_proc.pid)

        # Also subscribe to zeroconf events coming from NodeManagers and feed
        # the Master Manager with the new hosts we find
        if self._zeroconf:
            nm_assigner = NMAssigner()

            def _callback(
                zeroconf, service_type, name, state_change, adder, remover, accessor
            ):
                info = zeroconf.get_service_info(service_type, name)
                if state_change is zc.ServiceStateChange.Added:
                    server = socket.inet_ntoa(_get_address(info))
                    port = info.port
                    adder(name, server, port)
                    logger.info(
                        "Found a new %s on %s:%d, will add it to the MM",
                        service_type,
                        server,
                        port,
                    )
                elif state_change is zc.ServiceStateChange.Removed:
                    server, port = accessor(name)
                    logger.info(
                        "%s on %s:%d disappeared, removing it from the MM",
                        service_type,
                        server,
                        port,
                    )

                    # Don't bother to remove it if we're shutting down. This way
                    # we avoid hanging in here if the MM is down already but
                    # we are trying to remove our NM who has just disappeared
                    if not self._shutting_down:
                        remover(name)

            nm_callback = functools.partial(
                _callback,
                adder=nm_assigner.add_nm,
                remover=nm_assigner.remove_nm,
                accessor=nm_assigner.get_nm,
            )

            dim_callback = functools.partial(
                _callback,
                adder=nm_assigner.add_dim,
                remover=nm_assigner.remove_dim,
                accessor=nm_assigner.get_dim,
            )

            self._mm_nm_browser = utils.browse_service(
                self._zeroconf, "NodeManager", "tcp", nm_callback
            )
            self._mm_dim_browser = utils.browse_service(
                self._zeroconf,
                "DIM",
                "tcp",
                dim_callback,  # DIM since name must be < 15 bytes
            )
            logger.info("Zeroconf started")
            return

    def _verbosity_as_cmdline(self):
        if self._verbosity > 0:
            return ["-" + "v" * self._verbosity]
        elif self._verbosity < 0:
            return ["-" + "q" * (-self._verbosity)]
        return ()

    # Rest interface
    def _rest_start_manager(self, proc, start_method):
        if proc is not None:
            bottle.abort(409, "The Drop Manager is already running")  # Conflict
        start_method()
        return

    def _rest_stop_manager(self, proc, stop_method):
        if proc is None:
            bottle.abort(409, "The Drop Manager is not running")  # Conflict
        logger.debug("Calling %s", stop_method)
        return json.dumps(stop_method())

    def _rest_get_manager_info(self, proc):
        if proc:
            bottle.response.content_type = "application/json"
            logger.info("Sending response: %s", json.dumps({"pid": proc.pid}))
            return json.dumps({"pid": proc.pid})
        else:
            return json.dumps({"pid": None})

    def rest_getMgrs(self):
        mgrs = {
            "master": self._mm_proc,
            "island": self._dim_proc,
            "node": self._nm_proc,
        }
        if mgrs["master"]:
            mgrs["master"] = self._mm_proc.pid
        if mgrs["island"]:
            mgrs["island"] = self._dim_proc.pid
        if mgrs["node"]:
            mgrs["node"] = self._nm_proc.pid

        logger.info("Sending response: %s", json.dumps(mgrs))
        return json.dumps(mgrs)

    def rest_startNM(self):
        self._rest_start_manager(self._nm_proc, self.startNM)
        return self.rest_getNMInfo()

    def rest_stopNM(self):
        self._rest_stop_manager(self._nm_proc, self.stopNM)

    def rest_startDIM(self):
        body = bottle.request.json
        if not body or "nodes" not in body:
            # if nothing else is specified we simply add this host
            nodes = {}
        else:
            nodes = body["nodes"]
        self._rest_start_manager(
            self._dim_proc, functools.partial(self.startDIM, nodes)
        )
        return self.rest_getDIMInfo()

    def rest_stopDIM(self):
        self._rest_stop_manager(self._dim_proc, self.stopDIM)

    def rest_startMM(self):
        self._rest_start_manager(self._mm_proc, self.startMM)
        return self.rest_getMMInfo()

    def rest_stopMM(self):
        self._rest_stop_manager(self._mm_proc, self.stopMM)

    def rest_getNMInfo(self):
        return self._rest_get_manager_info(self._nm_proc)

    def rest_getDIMInfo(self):
        return self._rest_get_manager_info(self._dim_proc)

    def rest_getMMInfo(self):
        return self._rest_get_manager_info(self._mm_proc)


terminating = False


def run_with_cmdline(parser, args):
    parser.add_option(
        "-m",
        "--master",
        action="store_true",
        dest="master",
        help="Start this DALiuGE daemon as the master daemon",
        default=False,
    )
    parser.add_option(
        "--no-nm",
        action="store_true",
        dest="noNM",
        help="Don't start a NodeDropManager by default",
        default=False,
    )
    parser.add_option(
        "--no-zeroconf",
        action="store_true",
        dest="noZC",
        help="Don't enable zeroconf on this DALiuGE daemon",
        default=False,
    )
    parser.add_option(
        "-v",
        "--verbose",
        action="count",
        dest="verbose",
        help="Become more verbose. The more flags, the more verbose",
        default=0,
    )
    parser.add_option(
        "-q",
        "--quiet",
        action="count",
        dest="quiet",
        help="Be less verbose. The more flags, the quieter",
        default=0,
    )

    (opts, args) = parser.parse_args(args)

    # -v and -q are exclusive
    if opts.verbose and opts.quiet:
        parser.error("-v and -q cannot be specified together")

    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    daemon = DlgDaemon(opts.master, opts.noNM, opts.noZC, opts.verbose - opts.quiet)

    # Signal handling, which stops the daemon
    def handle_signal(signalNo, stack_frame):
        global terminating # pylint: disable=global-statement
        if terminating:
            return
        logger.info("Received signal %d in frame %s, will stop the daemon now",
                    signalNo, stack_frame)
        terminating = True
        daemon.stop(10)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # Go, go, go!
    t = threading.Thread(
        target=daemon.start, args=("0.0.0.0", constants.DAEMON_DEFAULT_REST_PORT)
    )
    t.start()
    signal.pause()
