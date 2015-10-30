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
like DOMs and DIMs.
"""

import logging
import optparse
import os
import sys
import threading

import Pyro4

from dfms.daemon import Daemon
from dfms.manager.data_island_manager import DataIslandManager
from dfms.manager.data_object_manager import DataObjectManager
from dfms.manager.rest import DOMRestServer, DIMRestServer
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
            threading.Event().wait()
        except KeyboardInterrupt:
            pass

class DMDaemon(Daemon):
    def __init__(self, options):
        logsDir = getDfmsLogsDir(createIfMissing=True)
        pidDir  = getDfmsPidDir(createIfMissing=True)
        pidfile = os.path.join(pidDir,  "dfms%s_%s.pid"    % (options.dmAcronym, options.id))
        stdout  = os.path.join(logsDir, "dfms%s_%s_stdout" % (options.dmAcronym, options.id))
        stderr  = os.path.join(logsDir, "dfms%s_%s_stderr" % (options.dmAcronym, options.id))
        super(DMDaemon, self).__init__(pidfile, stdout=stdout, stderr=stderr)
        self._options = options
    def run(self):
        launchServer(self._options)

def addCommonOptions(parser):
    parser.add_option("--no-pyro", action="store_true",
                      dest="noPyro", help="Don't start a Pyro daemon to expose this DM instance", default=False)
    parser.add_option("-H", "--host", action="store", type="string",
                      dest="host", help = "The host to bind this DM on", default='localhost')
    parser.add_option("-P", "--port", action="store", type="int",
                      dest="port", help = "The port to bind this DM on", default=0)
    parser.add_option("-i", "--id", action="store", type="string",
                      dest="id", help = "The Data Manager ID")
    parser.add_option("-d", "--daemon", action="store_true",
                      dest="daemon", help="Run as daemon", default=False)
    parser.add_option("-s", "--stop", action="store_true",
                      dest="stop", help="Stop a DM instance running as daemon", default=False)
    parser.add_option("--rest", action="store_true",
                      dest="rest", help="Start the REST interface to receive external commands", default=False)
    parser.add_option("--restHost", action="store",
                      dest="restHost", help="The host to bind the REST server on")
    parser.add_option("--restPort", action="store", type="int",
                      dest="restPort", help="The port to bind the REST server on")
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

def start(options):
    # Start daemon
    if options.daemon:
        daemon = DMDaemon(options)
        daemon.start()
    # Stop daemon
    elif options.stop:
        daemon = DMDaemon(options)
        daemon.stop()
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

# Entry-point function for the dfmsDOM script
def dfmsDOM(args=sys.argv):
    """
    Entry point for the dfmsDIM command-line script, which starts a
    DataObjectManager and exposes it through Pyro and a REST interface.
    """

    # Parse command-line and check options
    parser = optparse.OptionParser()
    addCommonOptions(parser)
    parser.add_option("--no-dlm", action="store_true",
                      dest="noDLM", help="Don't start the Data Lifecycle Manager on this DOM", default=False)
    parser.add_option("--dfms-path", action="store", type="string",
                      dest="dfmsPath", help="Path where more dfms-related libraries can be found", default="~/.dfms/")
    (options, args) = parser.parse_args(args)

    commonOptionsCheck(options, parser)
    setupLogging(options)

    # Add DOM-specific options
    options.dmType = DataObjectManager
    options.dmArgs = (options.id,)
    options.dmKwargs = {'useDLM': not options.noDLM}
    options.dmAcronym = 'DOM'
    options.restType = DOMRestServer


    # dfmsPath might contain code the user is adding
    logger = logging.getLogger(__name__)
    dfmsPath = os.path.expanduser(options.dfmsPath)
    if os.path.isdir(dfmsPath):
        if logger.isEnabledFor(logging.INFO):
            logger.info("Adding %s to the system path" % (dfmsPath))
        sys.path.append(dfmsPath)

    start(options)

# Entry-point function for the dfmsDOM script
def dfmsDIM(args=sys.argv):
    """
    Entry point for the dfmsDIM command-line script, which starts a
    DataIslandManager and exposes it through Pyro and a REST interface.
    """

    # Parse command-line and check options
    parser = optparse.OptionParser()
    addCommonOptions(parser)
    parser.add_option("-N", "--nodes", action="store", type="string",
                      dest="nodes", help = "Comma-separated list of node names managed by this DIM", default='localhost')
    parser.add_option("-k", "--ssh-pkey-path", action="store", type="string",
                      dest="pkeyPath", help = "Path to the private SSH key to use when connecting to the nodes", default=None)
    parser.add_option("--domRestPort", action="store", type="int",
                      dest="domRestPort", help = "Port used by DOMs started by this DIM to expose their REST interface", default=None)
    (options, args) = parser.parse_args(args)

    commonOptionsCheck(options, parser)
    setupLogging(options)

    # Add DIM-specific options
    options.dmType = DataIslandManager
    options.dmArgs = (options.id, options.nodes.split(','))
    options.dmKwargs = {'pkeyPath': options.pkeyPath, 'domRestPort': options.domRestPort}
    options.dmAcronym = 'DIM'
    options.restType = DIMRestServer

    start(options)

if __name__ == '__main__':
    # If this module is called directly, the first argument must be either
    # dfmsDOM or dfmsDIM, the rest of the arguments are the normal ones
    if len(sys.argv) == 1:
        print 'Usage: %s [dfmsDOM|dfmsDIM] [options]' % (sys.argv[0])
        sys.exit(1)
    dm = sys.argv.pop(1)
    if dm == 'dfmsDOM':
        dfmsDOM()
    elif dm == 'dfmsDIM':
        dfmsDIM()
    else:
        print 'Usage: %s [dfmsDOM|dfmsDIM] [options]' % (sys.argv[0])
        sys.exit(1)