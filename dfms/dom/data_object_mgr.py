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
"""
A data object managers manages all local Data Object instances
on a single address space
"""

from collections import defaultdict
import logging
import threading

import Pyro4

from dfms import graph_loader, doutils
from dfms.data_object import InMemoryDataObject, CRCAppDataObject
from dfms.ddap_protocol import DOLinkType
from dfms.lifecycle.dlm import DataLifecycleManager

logger = logging.getLogger(__name__)

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
            mydo = CRCAppDataObject(oid, uid)
        else:
            mydo = InMemoryDataObject(oid, uid)

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
            thref = threading.Thread(None, lambda daemon: daemon.requestLoop(), 'DOBThrd' + str(sessionId), [dae])
            thref.setDaemon(1)
            if logger.isEnabledFor(logging.INFO):
                logger.info('Launching daemon %s' % sessionId)
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