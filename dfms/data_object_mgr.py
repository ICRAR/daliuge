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
from dfms.data_object import InMemoryCRCResultDataObject
from dfms.events.pyro.pyro_event_broadcaster import PyroEventBroadcaster
import types

"""
A data object managers manages all local Data Object instances
on a single address space
"""
from optparse import OptionParser
import sys, os, threading
from collections import defaultdict
import Pyro4
import logging
from data_object import FileDataObject
from ddap_protocol import CST_NS_DOM, DOLinkType
from dfms.lifecycle.dlm import DataLifecycleManager

_logger = logging.getLogger(__name__)

def _startDobDaemonThread(daemon):
    daemon.requestLoop()

class DataObjectMgr(object):

    def __init__(self):
        """
        Constructor:
        """
        self.dlm_dict = {} # key - sessionId, value - DataLifecycleManager
        self.daemon_dict = {} # key - sessionId, value - daemon
        self.daemon_thd_dict = {} # key - sessionId, value - daemon thread
        self.daemon_dob_dict = defaultdict(dict) # key - sessionId, value - a dictionary of Data Objects (key - obj uri, val - obj)
        self.eventbc = PyroEventBroadcaster()

    def getURI(self):
        return self._uri

    def setURI(self, uri):
        self._uri = uri

    def createDataObject(self, oid, uid, sessionId, appDataObj = False, lifespan=3600):
        """
        This dummy implementation of 'createDataObject' creates either a data-only
        DataObject in the form of a FileDataObject, or an application DataObject,
        in the form of an InMemoryCRCResultDataObject.

        This method returns the URI of the data object created
        """

        # Get the DLM for the session
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

        # What kind of DO we need to create?
        # TODO: In the future I guess that by the time this method will be
        #       called with more specific parameters, mainly to know which kind
        #       of DO should be created depending on the node of the PG that
        #       is being created (i.e., which kind of storage do we need, which
        #       application should be run, which are its parameters, etc).
        #       Also, when creating the DO it should be known the storage layer
        #       (in the HSM) it belongs to, because the information is needed
        #       later to make decisions regarding data movement (we need to know
        #       in which later each DO is currently sitting, and where does it
        #       need to be moved)
        if (appDataObj):
            mydo = InMemoryCRCResultDataObject(oid, uid, self.eventbc)
        else:
            mydo = FileDataObject(oid, uid, self.eventbc)

        # Create, register, and let the DLM manage its lifecycle
        uri = dob_daemon.register(mydo)
        mydo.uri = uri
        mydo._getDataObject = types.MethodType(lambda self, do: Pyro4.Proxy(do.uri), mydo)
        self.daemon_dob_dict[sessionId][uri] = mydo
        dlm.addDataObject(mydo)
        return uri

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
        elif (linkType == DOLinkType.PRODUCER):
            rdo.addProducer(ldo)
        elif (linkType == DOLinkType.PARENT):
            rdo.setParent(ldo)
        elif (linkType == DOLinkType.CHILD):
            rdo.addChild(ldo)
        else:
            raise Exception('Unknown link type %s' % (str(linkType)))

    def startDOBDaemon(self, sessionId):
        if (self.daemon_dict.has_key(sessionId)):
            dae = self.daemon_dict[sessionId]
            args = (dae,)
            thref = threading.Thread(None, _startDobDaemonThread, 'DOBThrd' + str(sessionId), args)
            thref.setDaemon(1)
            print 'Launching daemon %s' % sessionId
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

def launchServer(dom_id, as_daemon=False, nsHost=None, myHost=None, port=9090):
    if (myHost is None):
        Pyro4.config.HOST = os.uname()[1] # won't work on Windows
    else:
        Pyro4.config.HOST = myHost
    print 'Creating data object manager daemon...'
    dom = DataObjectMgr()
    dom_daemon = Pyro4.Daemon()
    uri = dom_daemon.register(dom)
    dom.setURI(str(uri))

    print 'Locating Naming Service...'
    ns = Pyro4.locateNS(host=nsHost, port=port)

    print 'Registering %s to NS...' % uri
    ns.register("%s_%s" % (CST_NS_DOM, dom_id), uri)

    if (as_daemon):
        print 'Launching DOM service as a daemon...'
        args = (dom_daemon,)
        thref = threading.Thread(None, _startDaemonThread, 'DOMDaemonThrd', args)
        thref.setDaemon(1)
        thref.start() # TODO - change to multiprocessing
    else:
        print 'Launching DOM service as a process...'
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

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-c", "--client",
                  action="store_true", dest="test_client", default = False,
                  help="Only test as a DOM client (default is False)")

    parser.add_option("-e", "--example",
                  action="store_true", dest="help_msg", default = False,
                  help="Show some examples")

    parser.add_option("-n", "--nshost", action="store", type="string",
                      dest="ns_host", help = "Name service host id")

    parser.add_option("-m", "--myhost", action="store", type="string",
                      dest="my_host", help = "The current service host id")

    parser.add_option("-i", "--domid", action="store", type="string",
                      dest="dom_id", help = "Compute Island Id")
    (options, args) = parser.parse_args()

    if (options.help_msg):
        usage()
        sys.exit(0)

    if (options.dom_id is None):
        print 'Must provide a unique Island ID in this prototype'
        print 'We could use IP/port, but you may launch multiple compute islands managers on a single node for testing...'
        print 'e.g. python compute_island_mgr.py -i 001'
        sys.exit(1)

    if (options.test_client):
        testClient(options.dom_id, nsHost = options.ns_host)
    else:
        launchServer(options.dom_id, nsHost = options.ns_host, myHost = options.my_host)



