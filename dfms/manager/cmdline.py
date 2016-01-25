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
from dfms.manager.constants import NODE_DEFAULT_REST_PORT,\
    ISLAND_DEFAULT_REST_PORT, MASTER_DEFAULT_REST_PORT
"""
Module containing command-line entry points to launch Data Manager instances
like DMs and DIMs.
"""

import logging
import optparse
import os
import signal
import sys
import time

import Pyro4
import daemon
from lockfile.pidlockfile import PIDLockFile

from dfms.manager.composite_manager import DataIslandManager, MasterManager
from dfms.manager.node_manager import NodeManager
from dfms.manager.rest import NMRestServer, CompositeManagerRestServer
from dfms.utils import getDfmsPidDir, getDfmsLogsDir


def launchServer(opts):
    if (opts.host is None):
        Pyro4.config.HOST = 'localhost'
    else:
        Pyro4.config.HOST = opts.host

    logger = logging.getLogger(__name__)
    dmName = opts.dmType.__name__

    logger.info('Creating %s %s' % (dmName, opts.id))
    dm = opts.dmType(*opts.dmArgs, **opts.dmKwargs)

    if opts.rest:
        server = opts.restType(dm)
        server.start(opts.restHost, opts.restPort)

    if not opts.noPyro:
        daemon = Pyro4.Daemon(port=opts.port)
        uri = daemon.register(dm, objectId=opts.id)
        logger.info("Made %s available via %s" % (opts.dmAcronym, str(uri)))

    if not opts.noPyro:
        daemon.requestLoop()
    else:
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            pass

    # kind of a hack, but it's sufficient for the time being
    if hasattr(dm, 'shutdown'):
        dm.shutdown()

def addCommonOptions(parser, defaultRestPort):
    parser.add_option("--no-pyro", action="store_true",
                      dest="noPyro", help="Don't start a Pyro daemon to expose this instance", default=False)
    parser.add_option("-H", "--host", action="store", type="string",
                      dest="host", help = "The host to bind this instance on", default='localhost')
    parser.add_option("-P", "--port", action="store", type="int",
                      dest="port", help = "The port to bind this instance on", default=0)
    parser.add_option("-i", "--id", action="store", type="string",
                      dest="id", help = "The Data Manager ID")
    parser.add_option("-d", "--daemon", action="store_true",
                      dest="daemon", help="Run as daemon", default=False)
    parser.add_option("-s", "--stop", action="store_true",
                      dest="stop", help="Stop an instance running as daemon", default=False)
    parser.add_option("--rest", action="store_true",
                      dest="rest", help="Start the REST interface to receive external commands", default=False)
    parser.add_option("--restHost", action="store",
                      dest="restHost", help="The host to bind the REST server on")
    parser.add_option("--restPort", action="store", type="int",
                      dest="restPort", help="The port to bind the REST server on", default=defaultRestPort)
    parser.add_option("-v", "--verbose", action="store_true",
                      dest="verbose", help="Be verbose, including debugging information", default=False)
    parser.add_option("-q", "--quiet", action="store_true",
                      dest="quiet", help="Limit output to warnings and errors", default=False)

def commonOptionsCheck(options, parser):
    # ID is mandatory
    if not options.id:
        parser.error('Must provide a %s ID via the -i command-line flag' % (options.dmType.__name__))
    # -d and -s are exclusive
    if options.daemon and options.stop:
        parser.error('-d and -s cannot be specified together')
    # -v and -q are exclusive
    if options.verbose and options.quiet:
        parser.error('-v and -q cannot be specified together')

def start(options, parser):

    # Perform common option checks
    commonOptionsCheck(options, parser)

    # Setup the loggers
    setupLogging(options)

    # Start daemon?
    if options.daemon:

        pidDir  = getDfmsPidDir(createIfMissing=True)
        logsDir = getDfmsLogsDir(createIfMissing=True)
        pidfile = os.path.join(pidDir,  "dfms%s_%s.pid"    % (options.dmAcronym, options.id))
        stdout  = os.path.join(logsDir, "dfms%s_%s_stdout" % (options.dmAcronym, options.id))
        stderr  = os.path.join(logsDir, "dfms%s_%s_stderr" % (options.dmAcronym, options.id))
        with daemon.DaemonContext(pidfile=PIDLockFile(pidfile, 1),
                                  stdout=open(stdout, 'w+'),
                                  stderr=open(stderr, 'w+')):
            launchServer(options)

    # Stop daemon?
    elif options.stop:
        pidDir = getDfmsPidDir()
        pidfile = os.path.join(pidDir,  "dfms%s_%s.pid"    % (options.dmAcronym, options.id))
        pid = PIDLockFile(pidfile).read_pid()
        if pid is None:
            sys.stderr.write('Cannot read PID file, is there an instance running?\n')
        else:
            os.kill(pid, signal.SIGTERM)

    # Start directly
    else:
        launchServer(options)

def setupLogging(opts):
    if logging.root.handlers:
        # Mmmm, somebody already did some logging, it shouldn't have been us
        # Let's reset the root handlers
        for h in logging.root.handlers[:]:
            logging.root.removeHandler(h)
        pass

    if opts.verbose and opts.quiet:
        raise Exception
    if opts.verbose:
        level = logging.DEBUG
    elif opts.quiet:
        level = logging.WARN
    else:
        level = logging.INFO

    # Let's configure logging now
    logging.basicConfig(format="%(asctime)-15s [%(levelname)5.5s] [%(threadName)15.15s] %(name)s#%(funcName)s:%(lineno)s %(msg)s", stream=sys.stdout, level=logging.INFO)
    logging.getLogger("dfms").setLevel(level)
    logging.getLogger("tornado").setLevel(logging.WARN)
    logging.getLogger("luigi-interface").setLevel(logging.WARN)

# Entry-point function for the dfmsNM script
def dfmsNM(args=sys.argv):
    """
    Entry point for the dfmsNM command-line script, which starts a
    NodeManager and exposes it through Pyro and a REST interface.
    """

    # Parse command-line and check options
    parser = optparse.OptionParser()
    addCommonOptions(parser, NODE_DEFAULT_REST_PORT)
    parser.add_option("--no-dlm", action="store_true",
                      dest="noDLM", help="Don't start the Data Lifecycle Manager on this NodeManager", default=False)
    parser.add_option("--dfms-path", action="store", type="string",
                      dest="dfmsPath", help="Path where more dfms-related libraries can be found", default="~/.dfms/lib")
    (options, args) = parser.parse_args(args)

    # Add DM-specific options
    options.dmType = NodeManager
    options.dmArgs = (options.id,)
    options.dmKwargs = {'useDLM': not options.noDLM, 'dfmsPath': options.dfmsPath}
    options.dmAcronym = 'NM'
    options.restType = NMRestServer

    start(options, parser)

def dfmsCompositeManager(args, dmType, acronym, dmRestPort):
    """
    Common entry point for the dfmsDIM and dfmsMM command-line scripts. It
    starts the corresponding CompositeManager and exposes it through Pyro and a
    REST interface.
    """

    # Parse command-line and check options
    parser = optparse.OptionParser()
    addCommonOptions(parser, dmRestPort)
    parser.add_option("-N", "--nodes", action="store", type="string",
                      dest="nodes", help = "Comma-separated list of node names managed by this %s" % (acronym), default='localhost')
    parser.add_option("-k", "--ssh-pkey-path", action="store", type="string",
                      dest="pkeyPath", help = "Path to the private SSH key to use when connecting to the nodes", default=None)
    parser.add_option("--dmCheckTimeout", action="store", type="int",
                      dest="dmCheckTimeout", help="Maximum timeout used when automatically checking for DM presence", default=10)
    (options, args) = parser.parse_args(args)

    # Add DIM-specific options
    options.dmType = dmType
    options.dmArgs = (options.id, options.nodes.split(','))
    options.dmKwargs = {'pkeyPath': options.pkeyPath, 'dmCheckTimeout': options.dmCheckTimeout}
    options.dmAcronym = acronym
    options.restType = CompositeManagerRestServer

    start(options, parser)

# Entry-point function for the dfmsDIM script
def dfmsDIM(args=sys.argv):
    """
    Entry point for the dfmsDIM command-line script.
    """
    dfmsCompositeManager(args, DataIslandManager, 'DIM', ISLAND_DEFAULT_REST_PORT)

# Entry-point function for the dfmsDIM script
def dfmsMM(args=sys.argv):
    """
    Entry point for the dfmsMM command-line script.
    """
    dfmsCompositeManager(args, MasterManager, 'MM', MASTER_DEFAULT_REST_PORT)


if __name__ == '__main__':
    # If this module is called directly, the first argument must be dfmsMM,
    # dfmsNM or dfmsDIM, the rest of the arguments are the normal ones
    if len(sys.argv) == 1:
        print 'Usage: %s [dfmsNM|dfmsDIM|dfmsMM] [options]' % (sys.argv[0])
        sys.exit(1)
    dm = sys.argv.pop(1)
    if dm == 'dfmsNM':
        dfmsNM()
    elif dm == 'dfmsDIM':
        dfmsDIM()
    elif dm == 'dfmsMM':
        dfmsMM()
    else:
        print 'Usage: %s [dfmsNM|dfmsDIM|dfmsMM] [options]' % (sys.argv[0])
        sys.exit(1)