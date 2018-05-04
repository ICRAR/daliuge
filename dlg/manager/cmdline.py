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

import errno
import logging
import os
import signal
import sys
import threading
import time

import daemon
from lockfile.pidlockfile import PIDLockFile

from .composite_manager import DataIslandManager, MasterManager
from .constants import NODE_DEFAULT_REST_PORT, \
    ISLAND_DEFAULT_REST_PORT, MASTER_DEFAULT_REST_PORT, REPLAY_DEFAULT_REST_PORT
from .node_manager import NodeManager
from .replay import ReplayManager, ReplayManagerServer
from .rest import NMRestServer, CompositeManagerRestServer, \
    MasterManagerRestServer
from .. import version, utils


_terminating = False
def launchServer(opts):

    # we might be called via __main__, but we want a nice logger name
    logger = logging.getLogger(__name__)
    dmName = opts.dmType.__name__

    logger.info('DALiuGE version %s running at %s', version.full_version, os.getcwd())
    logger.info('Creating %s' % (dmName))
    dm = opts.dmType(*opts.dmArgs, **opts.dmKwargs)

    server = opts.restType(dm, opts.maxreqsize)

    # Signal handling
    def handle_signal(signNo, stack_frame):
        global _terminating
        if _terminating:
            return
        _terminating = True
        logger.info("Exiting from %s" % (dmName))

        dm.shutdown()
        server.stop()

        logger.info("Thanks for using our %s, come back again :-)" % (dmName))

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    server_t = threading.Thread(target=server.start, args=(opts.host, opts.port), name="HTTP server")
    server_t.start()
    # Now simply wait...
    signal.pause()


def addCommonOptions(parser, defaultPort):
    parser.add_option("-H", "--host", action="store", type="string",
                      dest="host", help = "The host to bind this instance on", default='localhost')
    parser.add_option("-P", "--port", action="store", type="int",
                      dest="port", help = "The port to bind this instance on", default=defaultPort)
    parser.add_option("-m", "--max-request-size", action="store", type="int",
                      dest="maxreqsize", help="The maximum allowed HTTP request size, in MB", default=10)
    parser.add_option("-d", "--daemon", action="store_true",
                      dest="daemon", help="Run as daemon", default=False)
    parser.add_option(      "--cwd", action="store_true",
                      dest="cwd", help="Short for '-w .'", default=False)
    parser.add_option("-w", "--work-dir",
                      help="Working directory, defaults to '/' in daemon mode, '.' in interactive mode", default=None)
    parser.add_option("-s", "--stop", action="store_true",
                      dest="stop", help="Stop an instance running as daemon", default=False)
    parser.add_option(      "--status", action="store_true",
                      dest="status", help="Checks if there is daemon process actively running", default=False)
    parser.add_option("-T", "--timeout", action="store",
                      dest="timeout", type="float", help="Timeout used when checking for the daemon process", default=10)
    parser.add_option("-v", "--verbose", action="count",
                      dest="verbose", help="Become more verbose. The more flags, the more verbose")
    parser.add_option("-q", "--quiet", action="count",
                      dest="quiet", help="Be less verbose. The more flags, the quieter")
    parser.add_option("-l", "--log-dir", action="store", type="string",
                      dest="logdir", help="The directory where the logging files will be stored", default=utils.getDlgLogsDir())

def commonOptionsCheck(options, parser):
    # These are all exclusive
    if options.daemon and options.stop:
        parser.error('-d and -s cannot be specified together')
    if options.daemon and options.status:
        parser.error('-d and --status cannot be specified together')
    if options.stop and options.status:
        parser.error('-s and --status cannot be specified together')
    # -v and -q are exclusive
    if options.verbose and options.quiet:
        parser.error('-v and -q cannot be specified together')
    if options.cwd and options.work_dir:
        parser.error("--cwd and -w/--work-dir cannot be specified together. Prefer -w")

def start(options, parser):

    # Perform common option checks
    commonOptionsCheck(options, parser)

    # Setup the loggers
    fileHandler = setupLogging(options)

    # Start daemon?
    if options.daemon:

        # Make sure the PID file will be created without problems
        pidDir  = utils.getDlgPidDir()
        utils.createDirIfMissing(pidDir)
        pidfile = os.path.join(pidDir,  "dlg%s.pid"    % (options.dmAcronym))

        working_dir = options.work_dir
        if not working_dir:
            if options.cwd:
                print('The --cwd option is deprecated, prefer -w/--work-dir, continuing anyway')
                working_dir = '.'
            else:
                working_dir = '/'
        with daemon.DaemonContext(pidfile=PIDLockFile(pidfile, 1), files_preserve=[fileHandler.stream],
                                  working_directory=working_dir):
            launchServer(options)

    # Stop daemon?
    elif options.stop:
        pidDir = utils.getDlgPidDir()
        pidfile = os.path.join(pidDir,  "dlg%s.pid"    % (options.dmAcronym))
        pid = PIDLockFile(pidfile).read_pid()
        if pid is None:
            sys.stderr.write('Cannot read PID file, is there an instance running?\n')
        else:
            try:
                os.kill(pid, signal.SIGTERM)
            except OSError as e:
                # Process is gone and file was left dangling,
                # let's clean it up ourselves
                if e.errno == errno.ESRCH:
                    sys.stderr.write('Process %d does not exist, removing PID file')
                    os.unlink(pidfile)

    # Check status
    elif options.status:
        socket_is_listening = utils.check_port(options.host, options.port, options.timeout)
        sys.exit(socket_is_listening is False)

    # Start directly
    else:
        working_dir = options.work_dir or '.'
        os.chdir(working_dir)
        launchServer(options)

def setupLogging(opts):
    if logging.root.handlers:
        # Mmmm, somebody already did some logging, it shouldn't have been us
        # Let's reset the root handlers
        for h in logging.root.handlers[:]:
            logging.root.removeHandler(h)
        pass

    levels = [
        logging.NOTSET,
        logging.DEBUG,
        logging.INFO,
        logging.WARNING,
        logging.ERROR,
        logging.CRITICAL
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
    fmt = '%(asctime)-15s [%(levelname)5.5s] [%(threadName)15.15s] '
    if opts.dmType == NodeManager and not opts.no_log_ids:
        fmt += '[%(session_id)10.10s] [%(drop_uid)10.10s] '
    fmt += '%(name)s#%(funcName)s:%(lineno)s %(message)s'
    fmt = logging.Formatter(fmt)
    fmt.converter = time.gmtime

    # Let's configure logging now
    # Daemons don't output stuff to the stdout
    if not opts.daemon:
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

    return fileHandler

def dlgNM(parser, args):
    """
    Entry point for the dlg nm command
    """

    # Parse command-line and check options
    addCommonOptions(parser, NODE_DEFAULT_REST_PORT)
    parser.add_option("-I", "--no-log-ids", action="store_true",
                  dest="no_log_ids", help="Do not add associated session IDs and Drop UIDs to log statements", default=False)
    parser.add_option("--no-dlm", action="store_true",
                      dest="noDLM", help="Don't start the Data Lifecycle Manager on this NodeManager", default=False)
    parser.add_option("--dlg-path", action="store", type="string",
                      dest="dlgPath", help="Path where more DALiuGE-related libraries can be found", default="~/.dlg/lib")
    parser.add_option("--error-listener", action="store", type="string",
                      dest="errorListener", help="The error listener class to be used", default=None)
    parser.add_option("--event-listeners", action="store", type="string",
                      dest="event_listeners", help="A colon-separated list of event listener classes to be used", default='')
    parser.add_option("-t", "--max-threads", action="store", type="int",
                      dest="max_threads", help="Max thread pool size used for executing drops. 0 (default) means no pool.", default=0)
    (options, args) = parser.parse_args(args)

    # Add DM-specific options
    # Note that the host we use to expose the NodeManager itself through Pyro is
    # also used to expose the Sessions it creates
    options.dmType = NodeManager
    options.dmArgs = ()
    options.dmKwargs = {'useDLM': not options.noDLM,
                        'dlgPath': options.dlgPath,
                        'host': options.host,
                        'error_listener': options.errorListener,
                        'event_listeners': list(filter(None, options.event_listeners.split(":"))),
                        'max_threads': options.max_threads}
    options.dmAcronym = 'NM'
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
    parser.add_option("-N", "--nodes", action="store", type="string",
                      dest="nodes", help = "Comma-separated list of node names managed by this %s" % (acronym), default="")
    parser.add_option("-k", "--ssh-pkey-path", action="store", type="string",
                      dest="pkeyPath", help = "Path to the private SSH key to use when connecting to the nodes", default=None)
    parser.add_option("--dmCheckTimeout", action="store", type="int",
                      dest="dmCheckTimeout", help="Maximum timeout used when automatically checking for DM presence", default=10)
    (options, args) = parser.parse_args(args)

    # Add DIM-specific options
    options.dmType = dmType
    options.dmArgs = ([s for s in options.nodes.split(',') if s],)
    options.dmKwargs = {'pkeyPath': options.pkeyPath, 'dmCheckTimeout': options.dmCheckTimeout}
    options.dmAcronym = acronym
    options.restType = dmRestServer

    start(options, parser)

def dlgDIM(parser, args):
    """
    Entry point for the dlg dim command
    """
    dlgCompositeManager(parser, args, DataIslandManager, 'DIM', ISLAND_DEFAULT_REST_PORT, CompositeManagerRestServer)

def dlgMM(parser, args):
    """
    Entry point for the dlg mm command
    """
    dlgCompositeManager(parser, args, MasterManager, 'MM', MASTER_DEFAULT_REST_PORT, MasterManagerRestServer)

def dlgReplay(parser, args):
    """
    Entry point for the dlg replay command
    """

    # Parse command-line and check options
    addCommonOptions(parser, REPLAY_DEFAULT_REST_PORT)
    parser.add_option("-S", "--status-file", action="store",
                      dest="status_file", help="File containing a continuous graph status dump", default=None)
    parser.add_option("-g", "--graph-file", action="store", type="string",
                      dest="graph_file", help="File containing a physical graph dump", default=None)
    (options, args) = parser.parse_args(args)

    if not options.graph_file:
        parser.error("Missing mandatory graph file")
    if not options.status_file:
        parser.error("Missing mandatory status file")

    # Add DM-specific options
    options.dmType = ReplayManager
    options.dmArgs = (options.graph_file, options.status_file)
    options.dmKwargs = {}
    options.dmAcronym = 'RP'
    options.restType = ReplayManagerServer

    start(options, parser)