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
"""
A data object managers manages all local Data Object instances
on a single address space
"""

import importlib
import inspect
import logging
import os
import sys

from dfms import droputils
from dfms.lifecycle.dlm import DataLifecycleManager
from dfms.manager import repository
from dfms.manager.session import Session


logger = logging.getLogger(__name__)

def _functionAsTemplate(f):
    args, _, _, defaults = inspect.getargspec(f)

    # 'defaults' might be shorter than 'args' if some of the arguments
    # are not optional. In the general case anyway the optional
    # arguments go at the end of the method declaration, and therefore
    # a reverse iteration should yield the correct match between
    # arguments and their defaults
    defaults = list(defaults) if defaults else []
    defaults.reverse()
    argsList = []
    for i, arg in enumerate(reversed(args)):
        if i >= len(defaults):
            # mandatory argument
            argsList.append({'name':arg})
        else:
            # optional with default value
            argsList.append({'name':arg, 'default':defaults[i]})

    return {'name': inspect.getmodule(f).__name__ + "." + f.__name__, 'args': argsList}

class DROPManager(object):
    """
    The DROPManager.

    A DROPManager, as the name states, manages DROPs. It does so not
    directly, but via Sessions, which represent and encapsulate separate,
    independent DROP graph executions. All DROPs created by the
    different Sessions are also given to a common DataLifecycleManager, which
    takes care of expiring them when needed, replicating them, and moving them
    across the HSM.
    """

    def __init__(self, dmId, useDLM=True, dfmsPath=None):
        self._dmId = dmId
        self._dlm = DataLifecycleManager() if useDLM else None
        self._sessions = {}

        # dfmsPath contains code added by the user with possible
        # DROP applications
        if dfmsPath:
            dfmsPath = os.path.expanduser(dfmsPath)
            if os.path.isdir(dfmsPath):
                if logger.isEnabledFor(logging.INFO):
                    logger.info("Adding %s to the system path" % (dfmsPath))
                sys.path.append(dfmsPath)

    @property
    def dmId(self):
        return self._dmId

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

    def linkGraphParts(self, sessionId, lhOID, rhOID, linkType):
        self._sessions[sessionId].linkGraphParts(lhOID, rhOID, linkType)

    def addGraphSpec(self, sessionId, graphSpec):
        self._sessions[sessionId].addGraphSpec(graphSpec)

    def getGraphStatus(self, sessionId):
        return self._sessions[sessionId].getGraphStatus()

    def getGraph(self, sessionId):
        return self._sessions[sessionId].getGraph()

    def deploySession(self, sessionId, completedDrops=[]):
        session = self._sessions[sessionId]
        session.deploy(completedDrops=completedDrops)
        roots = session.roots

        # We register the new DROPs with the DLM if there is one
        if self._dlm:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('Registering new DROPs with the DataLifecycleManager')
            droputils.breadFirstTraverse(roots, lambda drop: self._dlm.addDrop(drop))

        # Finally, we also collect the Pyro URIs of our DROPs and return them
        uris = {}
        droputils.breadFirstTraverse(roots, lambda drop: uris.__setitem__(drop.uid, drop.uri))
        return uris

    def destroySession(self, sessionId):
        session = self._sessions.pop(sessionId)
        session.destroy()

    def getSessionIds(self):
        return self._sessions.keys()

    def getTemplates(self):

        # TODO: we currently have a hardcoded list of functions, but we should
        #       load these repositories in a different way, like in this
        #       commented code
        #tplDir = os.path.expanduser("~/.dfms/templates")
        #if not os.path.isdir(tplDir):
        #    logger.warning('%s directory not found, no templates available' % (tplDir))
        #    return []
        #
        #templates = []
        #for fname in os.listdir(tplDir):
        #    if not  os.path.isfile(fname): continue
        #    if fname[-3:] != '.py': continue
        #
        #    with open(fname) as f:
        #        m = imp.load_module(fname[-3:], f, fname)
        #        functions = m.list_templates()
        #        for f in functions:
        #            templates.append(_functionAsTemplate(f))

        templates = []
        for f in repository.complex_graph, repository.pip_cont_img_pg, repository.archiving_app:
            templates.append(_functionAsTemplate(f))
        return templates

    def materializeTemplate(self, tpl, sessionId, **tplParams):
        # tpl currently has the form <full.mod.path.functionName>
        parts = tpl.split('.')
        module = importlib.import_module('.'.join(parts[:-1]))
        tplFunction = getattr(module, parts[-1])

        # invoke the template function with the given parameters
        # and add the new graph spec to the session
        graphSpec = tplFunction(**tplParams)
        self.addGraphSpec(sessionId, graphSpec)

        if logger.isEnabledFor(logging.INFO):
            logger.info('Added graph from template %s to session %s with params: %s' % (tpl, sessionId, tplParams))