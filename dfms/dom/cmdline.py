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
from optparse import OptionParser
import os
import sys

import Pyro4

from dfms.daemon import Daemon
from dfms.ddap_protocol import CST_NS_DOM
from dfms.dom.data_object_mgr import DataObjectMgr
from dfms.dom.rest import RestServer


_logger = logging.getLogger(__name__)

def launchServer(opts):
    if (opts.host is None):
        Pyro4.config.HOST = 'localhost'
    else:
        Pyro4.config.HOST = opts.host

    _logger.info('Creating data object manager daemon')
    dom = DataObjectMgr()
    dom_daemon = Pyro4.Daemon(port=opts.port)
    uri = dom_daemon.register(dom)
    dom.setURI(str(uri))

    _logger.info('Locating Naming Service...')
    ns = Pyro4.locateNS(host=opts.nsHost, port=opts.nsPort)

    _logger.info('Registering %s to NS...' % uri)
    ns.register("%s_%s" % (CST_NS_DOM, opts.domId), uri)

    if opts.rest:
        server = RestServer(dom)
        server.start(opts.restHost, opts.restPort)

    _logger.info('Launching DOM service as a process')
    dom_daemon.requestLoop()

class DOMDaemon(Daemon):
    def __init__(self, options, *args, **kwargs):
        pidfile = os.path.expanduser("~/.dfmsDOM_%s.pid" % (options.domId))
        super(DOMDaemon, self).__init__(pidfile, *args, **kwargs)
        self._options = options
    def run(self):
        launchServer(self._options)

# Function used as entry point by setuptools
def main(args=sys.argv):

    parser = OptionParser()
    parser.add_option("-n", "--nsHost", action="store", type="string",
                      dest="nsHost", help = "Name service host", default='localhost')
    parser.add_option("-p", "--nsPort", action="store", type="int",
                      dest="nsPort", help = "Name service port", default=9090)
    parser.add_option("-H", "--host", action="store", type="string",
                      dest="host", help = "The host to bind this DOM on", default='localhost')
    parser.add_option("-P", "--port", action="store", type="int",
                      dest="port", help = "The port to bind this DOM on", default=0)
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
    parser.add_option("--restPort", action="store",
                      dest="restPort", help="The port to bind the REST server on")
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
    main()