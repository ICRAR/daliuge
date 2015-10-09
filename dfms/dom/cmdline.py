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
Command-line entry point to launch a DOM instance
"""

import logging
import optparse
import os
import sys
import threading

import Pyro4

from dfms.daemon import Daemon
from dfms.dom.data_object_mgr import DataObjectMgr
from dfms.dom.rest import RestServer
from dfms.utils import getDfmsPidDir, getDfmsLogsDir


def launchServer(opts):
    if (opts.host is None):
        Pyro4.config.HOST = 'localhost'
    else:
        Pyro4.config.HOST = opts.host

    logger = logging.getLogger(__name__)

    # dfmsPath might contain code the user is adding
    dfmsPath = os.path.expanduser(opts.dfmsPath)
    if os.path.isdir(dfmsPath):
        if logger.isEnabledFor(logging.INFO):
            logger.info("Adding %s to the system path" % (dfmsPath))
        sys.path.append(dfmsPath)

    logger.info('Creating DataObjectManager %s' % (opts.domId))
    dom = DataObjectMgr(opts.domId, not opts.noDLM)

    if opts.rest:
        server = RestServer(dom)
        server.start(opts.restHost, opts.restPort)

    if not opts.noPyro:
        dom_daemon = Pyro4.Daemon(port=opts.port)
        uri = dom_daemon.register(dom)

        logger.info('Registering DataObjectManager %s to NameServer' % (opts.domId))
        try:
            ns = Pyro4.locateNS(host=opts.nsHost, port=opts.nsPort)
            ns.register(opts.domId, uri)
        except:
            logger.warning("Failed to register the DOM with Pyro's NameServer, life continues anyway")

    if not opts.noPyro:
        dom_daemon.requestLoop()
    else:
        try:
            threading.Event().wait()
        except KeyboardInterrupt:
            pass

class DOMDaemon(Daemon):
    def __init__(self, options):
        logsDir = getDfmsLogsDir(createIfMissing=True)
        pidDir  = getDfmsPidDir(createIfMissing=True)
        pidfile = os.path.join(pidDir,  "dfmsDOM_%s.pid" % (options.domId))
        stdout  = os.path.join(logsDir, "dfmsDOM_%s_stdout" % (options.domId))
        stderr  = os.path.join(logsDir, "dfmsDOM_%s_stderr" % (options.domId))
        super(DOMDaemon, self).__init__(pidfile, stdout=stdout, stderr=stderr)
        self._options = options
    def run(self):
        launchServer(self._options)

# Function used as entry point by setuptools
def main(args=sys.argv):

    parser = optparse.OptionParser()
    parser.add_option("--no-pyro", action="store_true",
                      dest="noPyro", help="Don't start a Pyro daemon to expose this DOM instance", default=False)
    parser.add_option("-H", "--host", action="store", type="string",
                      dest="host", help = "The host to bind this DOM on", default='localhost')
    parser.add_option("-P", "--port", action="store", type="int",
                      dest="port", help = "The port to bind this DOM on", default=0)
    parser.add_option("-n", "--nsHost", action="store", type="string",
                      dest="nsHost", help = "Name service host", default='localhost')
    parser.add_option("-p", "--nsPort", action="store", type="int",
                      dest="nsPort", help = "Name service port", default=9090)
    parser.add_option("-i", "--domId", action="store", type="string",
                      dest="domId", help = "The Data Object Manager ID")
    parser.add_option("-d", "--daemon", action="store_true",
                      dest="daemon", help="Run as daemon", default=False)
    parser.add_option("-s", "--stop", action="store_true",
                      dest="stop", help="Stop a DOM instance running as daemon", default=False)
    parser.add_option("--rest", action="store_true",
                      dest="rest", help="Start the REST interface to receive external commands", default=False)
    parser.add_option("--restHost", action="store",
                      dest="restHost", help="The host to bind the REST server on")
    parser.add_option("--restPort", action="store", type="int",
                      dest="restPort", help="The port to bind the REST server on")
    parser.add_option("--no-dlm", action="store_true",
                      dest="noDLM", help="Don't start the Data Lifecycle Manager on this DOM", default=False)
    parser.add_option("--dfms-path", action="store", type="string",
                      dest="dfmsPath", help="Path where more dfms-related libraries can be found", default="~/.dfms/")
    (options, args) = parser.parse_args(args)

    if not options.domId:
        parser.error('Must provide a DOM ID via the -i command-line flag')

    # -d and -s are exclusive
    if options.daemon and options.stop:
        parser.error('-d and -s cannot be specified together')

    # Start daemon
    if options.daemon:
        daemon = DOMDaemon(options)
        daemon.start()
    # Stop daemon
    elif options.stop:
        daemon = DOMDaemon(options)
        daemon.stop()
    # Start directly
    else:
        launchServer(options)

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    logging.getLogger('tornado').setLevel(logging.WARN)
    main()