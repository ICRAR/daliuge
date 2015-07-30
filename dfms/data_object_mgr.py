#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2014
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
# Who                   When          What
# ------------------------------------------------
# chen.wu@icrar.org   10/12/2014     Created as compute island mgr
#                     13/04/2015     change name to DOMgr
#
from dfms import graph_loader, doutils
"""
A data object managers manages all local Data Object instances
on a single address space
"""

from collections import defaultdict
import logging
from optparse import OptionParser
import os
import sys, threading

import Pyro4

from dfms.daemon import Daemon
from dfms.data_object import InMemoryCRCResultDataObject, InMemoryDataObject
from dfms.ddap_protocol import CST_NS_DOM, DOLinkType
from dfms.events.event_broadcaster import LocalEventBroadcaster
from dfms.lifecycle.dlm import DataLifecycleManager

_logger = logging.getLogger(__name__)

def _startDobDaemonThread(daemon):
    daemon.requestLoop()

class DataObjectMgr(object):

    def __init__(self, useDLM=True):
        """
        Constructor:
        """
        self.useDLM = useDLM
        self.dlm_dict = {} # key - sessionId, value - DataLifecycleManager
        self.daemon_dict = {} # key - sessionId, value - daemon
        self.daemon_thd_dict = {} # key - sessionId, value - daemon thread
        self.daemon_dob_dict = defaultdict(dict) # key - sessionId, value - a dictionary of Data Objects (key - obj uri, val - obj)
        self.eventbc = LocalEventBroadcaster()

    def getURI(self):
        return self._uri

    def setURI(self, uri):
        self._uri = uri

    def _getSessionObjects(self, sessionId):

        # Get the DLM for the session
        dlm = None
        if self.useDLM:
            if self.dlm_dict.has_key(sessionId):
                dlm = self.dlm_dict[sessionId]
            else:
                dlm = DataLifecycleManager()
                dlm.startup()
                self.dlm_dict[sessionId] = dlm

        # Get the DO daemon for this session, and that
        # will host the new DO
        if (self.daemon_dict.has_key(sessionId)):
            dob_daemon = self.daemon_dict[sessionId]
        else:
            dob_daemon = Pyro4.Daemon()
            self.daemon_dict[sessionId] = dob_daemon

        return dob_daemon, dlm

    def _registerNewDataObject(self, dob_daemon, dlm, dataObject, sessionId):
        # Create, register, and let the DLM manage its lifecycle
        uri = dob_daemon.register(dataObject)
        dataObject.uri = uri
        self.daemon_dob_dict[sessionId][uri] = dataObject
        if dlm:
            dlm.addDataObject(dataObject)
        return uri

    def createDataObject(self, oid, uid, sessionId, appDataObj = False, lifespan=3600):
        """
        This dummy implementation of 'createDataObject' creates either a data-only
        DataObject in the form of a FileDataObject, or an application DataObject,
        in the form of an InMemoryCRCResultDataObject.

        This method returns the URI of the data object created
        """
        if (appDataObj):
            mydo = InMemoryCRCResultDataObject(oid, uid, self.eventbc)
        else:
            mydo = InMemoryDataObject(oid, uid, self.eventbc)

        dob_daemon, dlm = self._getSessionObjects(sessionId)
        return self._registerNewDataObject(dob_daemon, dlm, mydo, sessionId)

    def createDataObjectGraph(self, sessionId, graphSpec):
        """
        Creates a full DataObject graph hosted by this DataObjectManager.
        The graph to be created is specified by `graphSpec`, and all its objects
        will be managed in the context of session `sessionId`.

        This method returns a list with all the URIs of the newly created
        DataObjects in bread-first order starting from the roots of the graph,
        which are in turn found in the same order as they are present in
        `graphSpec`
        """

        # Create and register
        roots = graph_loader.readObjectGraphS(graphSpec)
        dob_daemon, dlm = self._getSessionObjects(sessionId)
        doutils.breadFirstTraverse(roots, lambda do: self._registerNewDataObject(dob_daemon, dlm, do, sessionId))

        # Collect all URIs and return
        uris = []
        doutils.breadFirstTraverse(roots, lambda do: uris.append(do.uri))
        return uris

    def linkDataObjects(self, ldoUri, rdoUri, lcmiUri, rcmiUri, linkType, sessionId):
        """
        This method is useless if we can directly manipulate graph using proxy objects

        After calling this function:
            left data object becomes right data object's linkType
        e.g. A is B's consumer
        e.g. B is A's producer
        """
        if ((not self.daemon_dict.has_key(sessionId)) or (not self.daemon_dob_dict.has_key(sessionId))):
            raise Exception('Unknown sessionId %s' % sessionId)

        if (lcmiUri == self._uri and rcmiUri == self._uri): # both are local data objects
            ldo = self.daemon_dob_dict[sessionId][ldoUri]
            rdo = self.daemon_dob_dict[sessionId][rdoUri]
        elif (lcmiUri == self._uri and rcmiUri != self._uri):
            ldo = self.daemon_dob_dict[sessionId][ldoUri]
            rdo = Pyro4.Proxy(rdoUri)
        elif (lcmiUri != self._uri and rcmiUri == self._uri):
            ldo = Pyro4.Proxy(ldoUri)
            rdo = self.daemon_dob_dict[sessionId][rdoUri]
        else: # neither is local data object
            raise Exception('At least one data object should be on my compute island!')

        if (linkType == DOLinkType.CONSUMER):
            rdo.addConsumer(ldo)
        elif (linkType == DOLinkType.PARENT):
            rdo.setParent(ldo)
        elif (linkType == DOLinkType.CHILD):
            rdo.addChild(ldo)
        else:
            raise Exception('Unknown link type %s' % (str(linkType)))

    def startDOBDaemon(self, sessionId):
        """
        Starts the Pyro Daemon that serves requests for the given sessionId.
        TODO: We could start this automatically when the daemon is created in
        `self._getSessionObjects`
        """
        if (self.daemon_dict.has_key(sessionId)):
            dae = self.daemon_dict[sessionId]
            args = (dae,)
            thref = threading.Thread(None, _startDobDaemonThread, 'DOBThrd' + str(sessionId), args)
            thref.setDaemon(1)
            if _logger.isEnabledFor(logging.INFO):
                _logger.info('Launching daemon %s' % sessionId)
            thref.start()
            self.daemon_thd_dict[sessionId] = thref
            return 0
        else:
            return -1

    def shutdownDOBDaemon(self, sessionId):
        if (self.daemon_dict.has_key(sessionId)):
            print 'Shutting down daemon %s' % sessionId
            self.daemon_dict[sessionId].shutdown()
            d = self.daemon_dict.pop(sessionId)
            del d
            d = self.daemon_thd_dict.pop(sessionId)
            del d
            d = self.daemon_dob_dict.pop(sessionId)
            del d
            return 0
        else:
            return -1

    def ping(self):
        return "Hello, this is DOM %s" % self.getURI()

def _startDaemonThread(daemon):
    daemon.requestLoop()

def launchServer(dom_id, as_daemon=False, host=None, port=0, nsHost=None, nsPort=9090):
    if (host is None):
        Pyro4.config.HOST = 'localhost'
    else:
        Pyro4.config.HOST = host

    _logger.info('Creating data object manager daemon')
    dom = DataObjectMgr()
    dom_daemon = Pyro4.Daemon(port=port)
    uri = dom_daemon.register(dom)
    dom.setURI(str(uri))

    _logger.info('Locating Naming Service...')
    ns = Pyro4.locateNS(host=nsHost, port=nsPort)

    _logger.info('Registering %s to NS...' % uri)
    ns.register("%s_%s" % (CST_NS_DOM, dom_id), uri)

    if (as_daemon):
        _logger.info('Launching DOM service as a daemon')
        args = (dom_daemon,)
        thref = threading.Thread(None, _startDaemonThread, 'DOMDaemonThrd', args)
        thref.setDaemon(1)
        thref.start() # TODO - change to multiprocessing
    else:
        _logger.info('Launching DOM service as a process')
        dom_daemon.requestLoop()

def testClient(dom_id, nsHost = None):
    # 1. found the dom service by the naming service
    print 'Locating Naming Service...'
    ns = Pyro4.locateNS(host = nsHost)
    uriObj = ns.lookup("%s_%s" % (CST_NS_DOM, dom_id))

    print 'Getting DOM service handle...'
    domServ = Pyro4.Proxy(uriObj.asString())
    print domServ.ping()

    # 2. create two data objects remotely
    ssid = 'session0001'
    uri01 = domServ.createDataObject('oid001', 'uid001', ssid)
    print 'uri of the first object: %s' % uri01

    uri02 = domServ.createDataObject('oid002', 'uid002', ssid)
    print 'uri of the second object: %s' % uri02

    # 3. start the DOB daemon
    ret = domServ.startDOBDaemon(ssid)
    print "start DOB Daemon result %d" % ret

    # 4. use the DOB daemon to "ping" the newly created remote data objects
    dob1 = Pyro4.Proxy(uri01)
    print dob1.ping()
    print dob1.getURI()

    dob2 = Pyro4.Proxy(uri02)
    print dob2.ping()
    print dob2.getURI()

    # 5. shutdown the DOB daemon
    ret = domServ.shutdownDOBDaemon(ssid)
    print "shutdown DOB Daemon result %d" % ret

def usage():
    print "**************************************************************"
    print "Example of launching a DOM service:"
    print "python data_object_mgr.py -n storage01.icrar.org -i 001"
    print
    print "Example of launching a DOM service on a specified host"
    print "python data_object_mgr.py -n storage01.icrar.org -i 001 -m storage01.icrar.org"
    print
    print "Example of start a test client:"
    print "python data_object_mgr.py -n storage01.icrar.org -i 001 -c"
    print
    print "Example of starting a name service on a host:"
    print "pyro4-ns -n storage01.icrar.org"
    print "**************************************************************"

class DOMDaemon(Daemon):
    def __init__(self, options, *args, **kwargs):
        pidfile = os.path.expanduser("~/.dfmsDOM_%s.pid" % (options.domId))
        super(DOMDaemon, self).__init__(pidfile, *args, **kwargs)
        self._options = options
    def run(self):
        opt = self._options
        launchServer(opt.domId, host=opt.host, port=opt.port, nsHost=opt.nsHost, nsPort=opt.nsPort)

# Function used as entry point by setuptools
def main(args=sys.argv):

    parser = OptionParser()
    parser.add_option("-c", "--client", action="store_true",
                      dest="test_client", help="Run test DOM client", default = False)
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
    (options, args) = parser.parse_args(args)

    if not options.domId:
        sys.stderr.write('Must provide a DOM ID via the -i command-line flag\n')
        sys.exit(1)

    # -d and -s are exclusive
    if options.daemon and options.stop:
        sys.stderr.write('%s: -d and -s cannot be specified together\n' % (args[0]))
        sys.exit(1)

    # Test client
    if (options.test_client):
        testClient(options.domId, nsHost = options.nsHost)
    # Server
    else:
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
            launchServer(options.domId, host=options.host, port=options.port, nsHost=options.nsHost, nsPort=options.nsPort)

if __name__ == '__main__':
    main()