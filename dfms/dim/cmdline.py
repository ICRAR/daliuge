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
import logging
import optparse
import os
import sys
import threading

import Pyro4

from dfms.daemon import Daemon
from dfms.dim.data_island_manager import DataIslandManager


def launchServer(opts):
    if (opts.host is None):
        Pyro4.config.HOST = 'localhost'
    else:
        Pyro4.config.HOST = opts.host

    logger = logging.getLogger(__name__)

    logger.info('Creating DataIslandManager %s' % (opts.dimId))
    dim = DataIslandManager(opts.dimId, opts.nodes.split(','))

    if not opts.noPyro:
        dim_daemon = Pyro4.Daemon(port=opts.port)
        uri = dim_daemon.register(dim)

        logger.info('Registering DataIslandManager %s to NameServer' % (opts.dimId))
        try:
            ns = Pyro4.locateNS(host=opts.nsHost, port=opts.nsPort)
            ns.register(opts.dimId, uri)
        except:
            logger.warning("Failed to register the DIM with Pyro's NameServer, life continues anyway")

    if not opts.noPyro:
        dim_daemon.requestLoop()
    else:
        try:
            threading.Event().wait()
        except KeyboardInterrupt:
            pass



def getPidFilename(domId):
    if 'XDG_RUNTIME_DIR' in os.environ:
        pidfile = os.path.join(os.environ['XDG_RUNTIME_DIR'], "dfmsDIM_%s.pid" % (domId))
    else:
        pidfile = os.path.join(os.path.expanduser("~"), ".dfmsDIM_%s.pid" % (domId))
    return pidfile

class DIMDaemon(Daemon):
    def __init__(self, options, *args, **kwargs):
        pidfile = getPidFilename(options.dimId)
        super(DIMDaemon, self).__init__(pidfile, *args, **kwargs)
        self._options = options
    def run(self):
        launchServer(self._options)


# Function used as entry point by setuptools
def main(args=sys.argv):

    parser = optparse.OptionParser()
    parser.add_option("--no-pyro", action="store_true",
                      dest="noPyro", help="Don't start a Pyro daemon to expose this DIM instance", default=False)
    parser.add_option("-H", "--host", action="store", type="string",
                      dest="host", help = "The host to bind this DIM on", default='localhost')
    parser.add_option("-P", "--port", action="store", type="int",
                      dest="port", help = "The port to bind this DIM on", default=0)
    parser.add_option("-n", "--nsHost", action="store", type="string",
                      dest="nsHost", help = "Name service host", default='localhost')
    parser.add_option("-N", "--nodes", action="store", type="string",
                      dest="nodes", help = "Comma-separated list of node names managed by this DIM", default='localhost')
    parser.add_option("-p", "--nsPort", action="store", type="int",
                      dest="nsPort", help = "Name service port", default=9090)
    parser.add_option("-i", "--domId", action="store", type="string",
                      dest="domId", help = "The Data Island Manager ID")
    parser.add_option("-d", "--daemon", action="store_true",
                      dest="daemon", help="Run as daemon", default=False)
    parser.add_option("-s", "--stop", action="store_true",
                      dest="stop", help="Stop a DIM instance running as daemon", default=False)
    (options, args) = parser.parse_args(args)

    if not options.dimId:
        parser.error('Must provide a DIM ID via the -i command-line flag')

    # -d and -s are exclusive
    if options.daemon and options.stop:
        parser.error('-d and -s cannot be specified together')

    if not options.nodes.strip():
        parser.error('Invalid list of nodes given via -N: "%s"' % (options.nodes))

    # Start daemon
    if options.daemon:
        daemon = DIMDaemon(options)
        daemon.start()
    # Stop daemon
    elif options.stop:
        daemon = DIMDaemon(options)
        daemon.stop()
    # Start directly
    else:
        launchServer(options)


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    main()