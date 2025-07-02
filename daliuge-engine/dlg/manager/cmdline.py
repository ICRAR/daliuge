#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2015
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
Module containing command-line entry points to launch Data Manager instances
like DMs and DIMs.
"""

import logging
import os
import signal
import subprocess
import sys
import time
import re

import daemon
from lockfile.pidlockfile import PIDLockFile
from multiprocessing import Process

from .composite_manager import DataIslandManager, MasterManager
from dlg.constants import (
    NODE_DEFAULT_REST_PORT,
    ISLAND_DEFAULT_REST_PORT,
    MASTER_DEFAULT_REST_PORT,
    REPLAY_DEFAULT_REST_PORT,
    NODE_DEFAULT_RPC_PORT,
    NODE_DEFAULT_EVENTS_PORT,
)
from .node_manager import NodeManager
from .replay import ReplayManager, ReplayManagerServer
from .rest import (
    NMRestServer,
    CompositeManagerRestServer,
    MasterManagerRestServer,
)
from dlg import utils
from ..runtime import version


_terminating = False
MAX_WATCHDOG_RESTART = 10

class DlgFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        arg_pattern = re.compile(r"%\((\w+)\)")
        arg_names = [x.group(1) for x in arg_pattern.finditer(self._fmt)]
        for field in arg_names:
            if field not in record.__dict__:
                record.__dict__[field] = None

        return super().format(record)


def run_server(server, host, port):
    """
    Run the server using the options. Non-nested to avoid issues if needing to be pickled.
    :param opts: Command line options
    :return: None
    """
    server.start(host, port)


def launchServer(opts):
    # we might be called via __main__, but we want a nice logger name
    logger = logging.getLogger(f"dlg.{__name__}")
    dmName = opts.dmType.__name__

    logger.info("DALiuGE version %s running at %s", version.full_version, os.getcwd())
    logger.info("Creating %s", dmName)
    try:
        dm = opts.dmType(*opts.dmArgs, **opts.dmKwargs)
    except Exception as e:
        logger.exception(
            "Error while creating/starting our %s, exiting in shame :-(",
            dmName,
        )
        raise RuntimeError from e

    server = opts.restType(dm, opts.maxreqsize)

    # Signal handling
    def handle_signal(signNo, stack_frame):
        global _terminating # pylint: disable=global-statement
        if _terminating:
            return
        _terminating = True
        logger.info("Exiting from %s after receiving signal %s in frame %s",
                    dmName, signNo, stack_frame)

        server.stop_manager()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    # signal.signal(signal.SIGSEGV, handle_signal)

    if opts.watchdog_enabled:
        start_watchdog(server, opts, logger)
    else:
        run_server(server, opts.host, opts.port)


def start_watchdog(server, opts, logger):
    """
    Run the server in a separate process to keep it under observation.

    This is an experimental feature intended to secure complete runtime shutdown when we
    run potentially memory-unsafe code in child threads of the server.

    If we detect a SIGSEGV, we warn the user and attempt to re-run the server on a new
    port so that the NodeManager doesn't completely disappear and we avoid any issues with
    phantom threads using the old port.

    If we recieve any other exitcodes, we do not attempt a restart and instead following
    existing exit protocols.

    This method only acts as the watchdog, and does not support Session management or
    any explicit reconnect, and instead leaves that up to the respective manager
    implementations.

    :param server: server object that we are watching
    :param opts: runtime options
    :param logger: runtime logger
    """

    watchdog = True
    wait_period = 30
    current_restart = 0
    while watchdog:
        p = Process(target=run_server, args=(server, opts.host, opts.port))
        p.start()
        p.join()
        if p.exitcode == signal.SIGSEGV or p.exitcode == -signal.SIGSEGV:
            logger.warning(
                "Threaded server crashed with SIGSEGV (signal 11)."
                "Restarting in %s seconds...",
                wait_period
            )
            opts.port += 27 # Avoid collisions with more obvious ports, e.g. 8080 or 8888
            if current_restart < MAX_WATCHDOG_RESTART:
                current_restart +=1
            else:
                watchdog=False
                logger.warning(
                    "Watchdog service reached maximum restarts. Shutting Down..."
            )
        else:
            watchdog = False


def addCommonOptions(parser, defaultPort):
    parser.add_option(
        "-H",
        "--host",
        action="store",
        type="string",
        dest="host",
        help="The host to bind this instance on",
        default="localhost",
    )
    parser.add_option(
        "-P",
        "--port",
        action="store",
        type="int",
        dest="port",
        help="The port to bind this instance on",
        default=defaultPort,
    )
    parser.add_option(
        "-m",
        "--max-request-size",
        action="store",
        type="int",
        dest="maxreqsize",
        help="The maximum allowed HTTP request size, in MB",
        default=10,
    )
    parser.add_option(
        "-d",
        "--daemon",
        action="store_true",
        dest="daemon",
        help="Run as daemon",
        default=False,
    )
    parser.add_option(
        "-s",
        "--stop",
        action="store_true",
        dest="stop",
        help="Stop an instance running as daemon",
        default=False,
    )
    parser.add_option(
        "--status",
        action="store_true",
        dest="status",
        help="Checks if there is daemon process actively running",
        default=False,
    )
    parser.add_option(
        "-T",
        "--timeout",
        action="store",
        dest="timeout",
        type="float",
        help="Timeout used when checking for the daemon process",
        default=10,
    )
    parser.add_option(
        "-v",
        "--verbose",
        action="count",
        dest="verbose",
        help="Become more verbose. The more flags, the more verbose",
    )
    parser.add_option(
        "-q",
        "--quiet",
        action="count",
        dest="quiet",
        help="Be less verbose. The more flags, the quieter",
    )
    parser.add_option(
        "-l",
        "--log-dir",
        action="store",
        type="string",
        dest="logdir",
        help="The directory where the logging files will be stored",
        default=utils.getDlgLogsDir(),
    )
    parser.add_option(
        "--watchdog",
        action="store_true",
        dest="watchdog_enabled",
        help="Enable watchdog process wrapper for server (WARNING: Experimental feature)"
    )

    parser.add_option(
        "--local-time",
        action="store_true",
        help="Use local system time when logging",
        default=False,
    )


def commonOptionsCheck(options, parser):
    # These are all exclusive
    if options.daemon and options.stop:
        parser.error("-d and -s cannot be specified together")
    if options.daemon and options.status:
        parser.error("-d and --status cannot be specified together")
    if options.stop and options.status:
        parser.error("-s and --status cannot be specified together")
    # -v and -q are exclusive
    if options.verbose and options.quiet:
        parser.error("-v and -q cannot be specified together")


def start(options, parser):
    # Perform common option checks
    commonOptionsCheck(options, parser)

    # Setup the loggers
    fileHandler = setupLogging(options)

    # Start daemon?
    if options.daemon:
        # Make sure the PID file will be created without problems
        pidDir = utils.getDlgPidDir()
        utils.createDirIfMissing(pidDir)
        pidfile = os.path.join(pidDir, "dlg%s.pid" % (options.dmAcronym))

        with daemon.DaemonContext(
            pidfile=PIDLockFile(pidfile, 1),
            files_preserve=[fileHandler.stream],
            working_directory=utils.getDlgWorkDir(),
        ):
            launchServer(options)

    # Stop daemon?
    elif options.stop:
        pidDir = utils.getDlgPidDir()
        pidfile = os.path.join(pidDir, "dlg%s.pid" % (options.dmAcronym))
        pid = PIDLockFile(pidfile).read_pid()
        if pid is None:
            sys.stderr.write("Cannot read PID file, is there an instance running?\n")
        else:
            utils.terminate_or_kill(utils.ExistingProcess(pid), 5)
            if os.path.exists(pidfile):
                sys.stderr.write(
                    "Process %d does not exist, removing PID file\n" % (pid,)
                )
                os.unlink(pidfile)

    # Check status
    elif options.status:
        socket_is_listening = utils.check_port(
            options.host, options.port, options.timeout
        )
        sys.exit(socket_is_listening is False)

    # Start directly
    else:
        working_dir = utils.getDlgWorkDir()
        tree = "/settings"
        utils.createDirIfMissing(working_dir + tree)
        os.chdir(working_dir)
        launchServer(options)


def setupLogging(opts):
    if logging.root.handlers:
        # Mmmm, somebody already did some logging, it shouldn't have been us
        # Let's reset the root handlers
        for h in logging.root.handlers[:]:
            logging.root.removeHandler(h)

    levels = [
        logging.NOTSET,
        logging.DEBUG,
        logging.INFO,
        logging.WARNING,
        logging.ERROR,
        logging.CRITICAL,
    ]

    # Default is WARNING
    lidx = 3
    if opts.verbose:
        lidx -= min((opts.verbose, 3))
    elif opts.quiet:
        lidx += min((opts.quiet, 2))
    level = levels[lidx]

    # Output to files/stdout uses a command format, which can or not contain
    # optionally a session_id and drop_uid to indicate what is currently being
    # executed. This only applies to the NodeManager logs though, for which a
    # 'no_log_ids' option exists (but can be set to True).
    # We also skip logging IDs when stopping a daemon, as the infrastructure
    # won't have been set
    log_ids = opts.dmType == NodeManager and not opts.no_log_ids and not opts.stop
    fmt = "%(asctime)-15s [%(levelname)5.5s] [%(threadName)15.15s] "
    if log_ids:
        fmt += "[%(session_id)10.10s] [%(drop_uid)10.10s] "
    fmt += "%(name)s#%(funcName)s:%(lineno)s %(message)s"
    fmt = DlgFormatter(fmt)
    fmt.converter = time.localtime if opts.local_time else time.gmtime
    time_fmt =  "Local" if opts.local_time else "GMT"
    # Let's configure logging now
    # Daemons don't output stuff to the stdout
    is_daemon_argument = opts.daemon or opts.stop
    if not is_daemon_argument:
        streamHdlr = logging.StreamHandler(sys.stdout)
        streamHdlr.setFormatter(fmt)
        logging.root.addHandler(streamHdlr)

    # This is the logfile we'll use from now on
    logdir = opts.logdir
    utils.createDirIfMissing(logdir)
    logfile = os.path.join(logdir, "dlg%s.log" % (opts.dmAcronym))
    fileHandler = logging.FileHandler(logfile)
    fileHandler.setFormatter(fmt)
    logging.root.addHandler(fileHandler)

    # Per-package/module specific levels
    logging.root.setLevel(level)
    logging.getLogger("dlg").setLevel(level)
    logging.getLogger("zerorpc").setLevel(logging.WARN)

    # Assuming we have selected the default, info-level messages will not show to the
    # user. A Warning message here let's the user know something is happening without
    # us needing to modify the default logging level.
    logging.warning("Starting with level: %s...", logging.getLevelName(level))
    logging.warning("Using %s Time for logging...", time_fmt)

    return fileHandler


def dlgNM(parser, args):
    """
    Entry point for the dlg nm command
    """

    # Parse command-line and check options
    addCommonOptions(parser, NODE_DEFAULT_REST_PORT)
    parser.add_option(
        "-I",
        "--no-log-ids",
        action="store_true",
        dest="no_log_ids",
        help="Do not add associated session IDs and Drop UIDs to log statements",
        default=False,
    )
    parser.add_option(
        "--no-dlm",
        action="store_true",
        dest="noDLM",
        help="(DEPRECATED) Don't start the Data Lifecycle Manager on this NodeManager",
    )
    parser.add_option(
        "--dlm-check-period",
        type="float",
        help="Time in seconds between background DLM drop status checks (defaults to 10)",
        default=10,
    )
    parser.add_option(
        "--dlm-cleanup-period",
        type="float",
        help="Time in seconds between background DLM drop automatic cleanups (defaults to 30)",
        default=30,
    )
    parser.add_option(
        "--dlm-enable-replication",
        action="store_true",
        help="Turn on data drop automatic replication (off by default)",
    )
    parser.add_option(
        "--dlg-path",
        action="store",
        type="string",
        dest="dlgPath",
        help="Path where more DALiuGE-related libraries can be found",
        default=utils.getDlgPath(),
    )
    parser.add_option(
        "--error-listener",
        action="store",
        type="string",
        dest="errorListener",
        help="The error listener class to be used",
        default=None,
    )
    parser.add_option(
        "--event-listeners",
        action="store",
        type="string",
        dest="event_listeners",
        help="A colon-separated list of event listener classes to be used",
        default="",
    )
    parser.add_option(
        "-t",
        "--max-threads",
        action="store",
        type="int",
        dest="max_threads",
        help="Max thread pool size used for executing drops. <= 0 means use all (physical) CPUs. Default is 0.",
        default=0,
    )
    parser.add_option(
        "-p",
        "--processes",
        action="store_true",
        dest="use_processes",
        help="Use processes instead of threads to execute app drops, defaults to False",
    )
    parser.add_option(
        "--rpc_port",
        action="store",
        type="int",
        dest="rpc_port",
        help="Set the port number for the NodeManager RPC client",
        default=NODE_DEFAULT_RPC_PORT,
    )
    parser.add_option(
        "--event_port",
        action="store",
        type="int",
        dest="events_port",
        help="Set the port number for the NodeManager ZMQ client",
        default=NODE_DEFAULT_EVENTS_PORT,
    )

    (options, args) = parser.parse_args(args)

    # No logging setup at this point yet
    if options.noDLM:
        print("WARNING: --no-dlm is deprecated, use the --dlm-* options instead")
        options.dlm_check_period = 0
        options.dlm_cleanup_period = 0
        options.dlm_enable_replication = False

    # Add DM-specific options
    # Note that the host we use to expose the NodeManager itself through Pyro is
    # also used to expose the Sessions it creates
    options.dmType = NodeManager
    options.dmArgs = ()
    options.dmKwargs = {
        "dlm_check_period": options.dlm_check_period,
        "dlm_cleanup_period": options.dlm_cleanup_period,
        "dlm_enable_replication": options.dlm_enable_replication,
        "dlgPath": options.dlgPath,
        "host": options.host,
        "rpc_port": options.rpc_port,
        "events_port": options.events_port,
        "error_listener": options.errorListener,
        "event_listeners": list(filter(None, options.event_listeners.split(":"))),
        "max_threads": options.max_threads,
        "use_processes": options.use_processes,
        "logdir": options.logdir,
        "use_local_time": options.local_time,
    }
    options.dmAcronym = "NM"
    options.restType = NMRestServer

    start(options, parser)


def dlgCompositeManager(parser, args, dmType, acronym, dmPort, dmRestServer):
    """
    Common entry point for the dlgDIM and dlgMM command-line scripts. It
    starts the corresponding CompositeManager and exposes it through a
    REST interface.
    """

    # Parse command-line and check options
    addCommonOptions(parser, dmPort)
    parser.add_option(
        "-N",
        "--nodes",
        action="store",
        type="string",
        dest="nodes",
        help="Comma-separated list of node names managed by this %s" % (acronym),
        default="",
    )
    parser.add_option(
        "-k",
        "--ssh-pkey-path",
        action="store",
        type="string",
        dest="pkeyPath",
        help="Path to the private SSH key to use when connecting to the nodes",
        default=None,
    )
    parser.add_option(
        "--dmCheckTimeout",
        action="store",
        type="int",
        dest="dmCheckTimeout",
        help="Maximum timeout used when automatically checking for DM presence",
        default=10,
    )
    parser.add_option(
        "--dump_graphs",
        action="store_true",
        dest="dump_graphs",
        help="Store physical graphs submitted to the manager in the workspace directory",
        default=False,
    )
    (options, args) = parser.parse_args(args)

    # Add DIM-specific options
    options.dmType = dmType
    options.dmArgs = ([s for s in options.nodes.split(",") if s],)

    options.dmKwargs = {
        "pkeyPath": options.pkeyPath,
        "dmCheckTimeout": options.dmCheckTimeout,
        "dump_graphs": options.dump_graphs,
    }
    options.dmAcronym = acronym
    options.restType = dmRestServer

    start(options, parser)


def dlgDIM(parser, args):
    """
    Entry point for the dlg dim command
    """
    dlgCompositeManager(
        parser,
        args,
        DataIslandManager,
        "DIM",
        ISLAND_DEFAULT_REST_PORT,
        CompositeManagerRestServer,
    )


def dlgMM(parser, args):
    """
    Entry point for the dlg mm command
    """
    dlgCompositeManager(
        parser,
        args,
        MasterManager,
        "MM",
        MASTER_DEFAULT_REST_PORT,
        MasterManagerRestServer,
    )


def dlgReplay(parser, args):
    """
    Entry point for the dlg replay command
    """

    # Parse command-line and check options
    addCommonOptions(parser, REPLAY_DEFAULT_REST_PORT)
    parser.add_option(
        "-S",
        "--status-file",
        action="store",
        dest="status_file",
        help="File containing a continuous graph status dump",
        default=None,
    )
    parser.add_option(
        "-g",
        "--graph-file",
        action="store",
        type="string",
        dest="graph_file",
        help="File containing a physical graph dump",
        default=None,
    )
    (options, args) = parser.parse_args(args)

    if not options.graph_file:
        parser.error("Missing mandatory graph file")
    if not options.status_file:
        parser.error("Missing mandatory status file")

    # Add DM-specific options
    options.dmType = ReplayManager
    options.dmArgs = (options.graph_file, options.status_file)
    options.dmKwargs = {}
    options.dmAcronym = "RP"
    options.restType = ReplayManagerServer

    start(options, parser)
