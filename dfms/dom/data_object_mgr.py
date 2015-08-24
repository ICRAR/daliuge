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

import logging

from dfms import doutils
from dfms.dom.session import Session
from dfms.lifecycle.dlm import DataLifecycleManager


logger = logging.getLogger(__name__)

class DataObjectMgr(object):
    """
    The DataObjectManager.

    A DataObjectManager, as the name states, manages DataObjects. It does so not
    directly, but via Sessions, which represent and encapsulate separate,
    independent DataObject graph executions. All DataObjects created by the
    different Sessions are also given to a common DataLifecycleManager, which
    takes care of expiring them when needed, replicating them, and moving them
    across the HSM.
    """

    def __init__(self, domId, useDLM=True):
        self.domId = domId
        self.dlm = DataLifecycleManager() if useDLM else None
        self._sessions = {}

    def getURI(self):
        return self._uri

    def setURI(self, uri):
        self._uri = uri

    def createSession(self, sessionId):
        if sessionId in self._sessions:
            raise Exception('A session already exists for sessionId %s' % (str(sessionId)))
        self._sessions[sessionId] = Session(sessionId)
        if logger.isEnabledFor(logging.INFO):
            logger.info('Created session %s' % (sessionId))

    def getSessionStatus(self, sessionId):
        return self._sessions[sessionId].status

    def quickDeploy(self, sessionId, graphSpec):
        self.createSession(sessionId)
        self.addGraphSpec(sessionId, graphSpec)
        return self.deploySession(sessionId)

    def linkDataObjects(self, sessionId, lhOID, rhOID, linkType):
        self._sessions[sessionId].linkDataObjects(lhOID, rhOID, linkType)

    def addGraphSpec(self, sessionId, graphSpec):
        self._sessions[sessionId].addGraphSpec(graphSpec)

    def getGraphStatus(self, sessionId):
        return self._sessions[sessionId].getGraphStatus()

    def getGraph(self, sessionId):
        return self._sessions[sessionId].getGraph()

    def deploySession(self, sessionId):
        session = self._sessions[sessionId]
        session.deploy()
        roots = session.roots

        # We register the new DOs with the DLM if there is one
        if self.dlm:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('Registering new DOs with the DataLifecycleManager')
            doutils.breadFirstTraverse(roots, lambda do: self.dlm.addDataObject(do))

        # Finally, we also collect the Pyro URIs of our DOs and return them
        uris = []
        doutils.breadFirstTraverse(roots, lambda do: uris.append(do.uri))
        return uris

    def destroySession(self, sessionId):
        session = self._sessions.pop(sessionId)
        session.destroy()