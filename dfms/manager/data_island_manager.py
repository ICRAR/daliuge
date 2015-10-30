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
import collections
import logging
import threading

import Pyro4

from dfms import remote, graph_loader, data_object
from dfms.utils import CountDownLatch, portIsOpen


logger = logging.getLogger(__name__)
DOM_PORT = 4000

class DataIslandManager(object):
    """
    The DataIslandManager

    A DataIslandManager manages a number of DataObjectManagers, one per node
    in the Data Island. It offers roughly the same methods as those offered
    by the DOM since it offers the same capabilities.

    One of the key aspects of the DIM is that it receives a physical graph for
    the whole island, which it must distribute among the different DOMs. For
    this it requires that all the nodes in the graph declare a node (a host
    name), which should be part of the Data Island. This way the DIM breaks down
    the physical graph in parts that belong to the different DOMs, creates them
    individually, and links them later at deployment time.
    """

    def __init__(self, dimId, nodes=['localhost'], pkeyPath=None, domRestPort=8888):
        self._dimId = dimId
        self._nodes = nodes
        self._connectTimeout = 100
        self._interDOMRelations = collections.defaultdict(list)
        self._sessionIds = [] # TODO: it's still unclear how sessions are managed at the DIM level
        self._pkeyPath = pkeyPath
        self._domRestPort = domRestPort
        self.startNodeChecker()
        logger.info('Created DataIslandManager for nodes: %r' % (self._nodes))

    def startNodeChecker(self):
        self._nodeCheckerEvt = threading.Event()
        self._nodeCheckerThread = threading.Thread(name='Node checker Thread', target=self._checkNodes)
        self._nodeCheckerThread.daemon = True
        self._nodeCheckerThread.start()

    def stopNodeChecker(self):
        self._nodeCheckerEvt.set()
        self._nodeCheckerThread.join()

    __del__ = stopNodeChecker

    def _checkNodes(self):
        while True:
            for n in self._nodes:
                self.ensureDOM(n)
            if self._nodeCheckerEvt.wait(10000):
                break

    @property
    def dimId(self):
        return self._dimId

    @property
    def nodes(self):
        return self._nodes[:]

    @property
    def domRestPort(self):
        return self._domRestPort

    def dfmsDOMCommandLine(self, host, port):
        cmdline = 'dfmsDOM --rest -i dom_{0} -P {1} -d --host {0}'.format(host, port)
        if self._domRestPort:
            cmdline += ' --restPort {0}'.format(self._domRestPort)
        return cmdline

    def startDOM(self, host, port):
        client = remote.createClient(host, pkeyPath=self._pkeyPath)
        if logger.isEnabledFor(logging.INFO):
            logger.info("DOM not present at %s:%d, starting it" % (host, port))
        out, err, status = remote.execRemoteWithClient(client, self.dfmsDOMCommandLine(host, port))
        if status != 0:
            logger.error("Failed to start the DOM on %s:%d, stdout/stderr follow:\n==STDOUT==\n%s\n==STDERR==\n%s" % (host, port, out, err))
            raise Exception("Failed to start the DOM on %s:%d" % (host, port))
        if logger.isEnabledFor(logging.INFO):
            logger.info("DOM successfully started at %s:%d" % (host, port))

    def ensureDOM(self, host, port=DOM_PORT):
        # We rely on having ssh keys for this, since we're using
        # the dfms.remote module, which authenticates using public keys
        if logger.isEnabledFor(logging.DEBUG):
                logger.debug("Checking DOM presence at %s:%d" % (host, port))

        if portIsOpen(host, port, 10):
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("DOM already present at %s:%d" % (host, port))
            return

        self.startDOM(host, port)

        # Wait a bit until the DOM starts; if it doesn't we fail
        if not portIsOpen(host, port, 10):
            raise Exception("DOM started at %s:%d, but couldn't connect to it" % (host, port))

    def domAt(self, node):
        return Pyro4.Proxy("PYRO:dom_{0}@{0}:{1}".format(node, DOM_PORT))

    def getSessionIds(self):
        return self._sessionIds;

    def _createSession(self, sessionId, node, latch, exceptions):
        try:
            self.ensureDOM(node)
            with self.domAt(node) as dom:
                dom.createSession(sessionId)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('Successfully created session %s in %s' % (sessionId, node))
        except Exception as e:
            logger.error("Failed to create a session on node %s" % (node))
            exceptions[node] = e
            raise # so it gets printed
        finally:
            latch.countDown()

    def createSession(self, sessionId):
        """
        Creates a session in all underlying DOMs.
        """

        logger.info('Creating Session %s in all nodes' % (sessionId))
        latch = CountDownLatch(len(self._nodes))
        thrExs = {}
        for node in self._nodes:
            t = threading.Thread(target=self._createSession, args=(sessionId, node, latch, thrExs))
            t.start()
        latch.await()
        if thrExs:
            raise Exception("One or more errors occurred while creating sessions", thrExs)
        self._sessionIds.append(sessionId)

    def _destroySession(self, sessionId, node, latch, exceptions):
        try:
            self.ensureDOM(node)
            with self.domAt(node) as dom:
                dom.destroySession(sessionId)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('Successfully destroyed session %s in %s' % (sessionId, node))
        except Exception as e:
            logger.error("Failed to destroy a session on node %s" % (node))
            exceptions[node] = e
            raise # so it gets printed
        finally:
            latch.countDown()

    def destroySession(self, sessionId):
        """
        Destroy a session in all underlying DOMs.
        """
        logger.info('Destroying Session %s in all nodes' % (sessionId))
        thrExs = {}

        latch = CountDownLatch(len(self._nodes))
        for node in self._nodes:
            t = threading.Thread(target=self._destroySession, args=(sessionId, node, latch, thrExs))
            t.start()
        latch.await()

        if thrExs:
            raise Exception("One or more errors occurred while destroying sessions", thrExs)
        self._sessionIds.remove(sessionId)

    def _addGraphSpec(self, sessionId, node, graphSpec, latch, exceptions):
        try:
            with self.domAt(node) as dom:
                dom.addGraphSpec(sessionId, graphSpec)
            pass
        except Exception as e:
            logger.error("Failed to append graphSpec for session %s on node %s" % (sessionId, node))
            exceptions[node] = e
            raise # so it gets printed
        finally:
            latch.countDown()

    def addGraphSpec(self, sessionId, graphSpec):

        # The first step is to break down the graph into smaller graphs that
        # belong to the same node, so we can submit that graph into the individual
        # DOMs. For this we need to make sure that our graph has a 'node'
        # attribute set
        perNode = collections.defaultdict(list)
        for doSpec in graphSpec:
            if 'node' not in doSpec:
                raise Exception("DataObject %s doesn't specify a node attribute" % (doSpec['oid']))

            loc = doSpec['node']
            if loc not in self._nodes:
                raise Exception("DataObject %s's node %s does not belong to this DIM" % (doSpec['oid'], loc))

            perNode[loc].append(doSpec)

        # At each node the relationships between DOs should be local at the
        # moment of submitting the graph; thus we record the inter-DOM
        # relationships separately and remove them from the original graph spec
        interDOMRelations = []
        for loc,doSpecs in perNode.viewitems():
            interDOMRelations.extend(graph_loader.removeUnmetRelationships(doSpecs))

        # Create the individual graphs on each DOM now that they are correctly
        # separated.
        if logger.isEnabledFor(logging.INFO):
            logger.info('Adding individual graphSpec of session %s to each node' % (sessionId))
        latch = CountDownLatch(len(self._nodes))
        thrExs = {}
        for node in self._nodes:
            t = threading.Thread(target=self._addGraphSpec, args=(sessionId, node, perNode[node], latch, thrExs))
            t.start()
        latch.await()

        if thrExs:
            raise Exception("One or more errors occurred while adding the graphSpec to the individual DOMs", thrExs)

        self._interDOMRelations[sessionId].extend(interDOMRelations)

    def _deploySession(self, sessionId, node, allUris, latch, exceptions):
        try:
            with self.domAt(node) as dom:
                uris = dom.deploySession(sessionId)
                allUris.update(uris)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('Successfully deployed session %s in %s' % (sessionId, node))
        except Exception as e:
            exceptions[node] = e
            logger.error("An exception occurred while deploying session %s in %s" % (sessionId, node))
            raise # so it gets printed
        finally:
            latch.countDown()

    def _triggerDataObject(self, do, uid, latch, exceptions):
        try:
            if hasattr(do, 'execute'):
                t = threading.Thread(target=lambda:do.execute())
                t.daemon = True
                t.start()
            else:
                do.setCompleted()
        except Exception as e:
            exceptions[do.uid] = e
            logger.error("An exception occurred while moving DO %s to COMPLETED" % (uid))
            raise # so it gets printed
        finally:
            latch.countDown()

    def deploySession(self, sessionId, completedDOs=[]):

        logger.info('Deploying Session %s in all nodes' % (sessionId))

        allUris = {}
        thrExs = {}

        # Deploy all individual graphs in parallel
        latch = CountDownLatch(len(self._nodes))
        for node in self._nodes:
            t = threading.Thread(target=self._deploySession, args=(sessionId, node, allUris, latch, thrExs))
            t.start()
        latch.await()

        if thrExs:
            raise Exception("One ore more exceptions occurred while deploying session %s" % (sessionId), thrExs)

        # Retrieve all necessary proxies we'll need afterward
        # (i.e., those present in inter-DOM relationships and in completedDOs)
        # Creating proxies beforehand and reusing them means that we won't need
        # to establish that many TCP connections once and over again
        proxies = {}
        for rel in self._interDOMRelations[sessionId]:
            if rel.rhs not in proxies:
                proxies[rel.rhs] = Pyro4.Proxy(allUris[rel.rhs])
            if rel.lhs not in proxies:
                proxies[rel.lhs] = Pyro4.Proxy(allUris[rel.lhs])
        for uid in completedDOs:
            if uid not in proxies:
                proxies[uid] = Pyro4.Proxy(allUris[uid])

        # Establish the inter-DOM relationships between DataObjects.
        # DORel tuples are read: "lhs is rel of rhs" (e.g., A is PRODUCER of B)
        for rel in self._interDOMRelations[sessionId]:
            relType = rel.rel
            rhsDO = proxies[rel.rhs]
            lhsDO = proxies[rel.lhs]

            if relType in data_object.LINKTYPE_1TON_APPEND_METHOD:
                methodName = data_object.LINKTYPE_1TON_APPEND_METHOD[relType]
                rhsDO._pyroInvoke(methodName, (lhsDO,), {})
            else:
                relPropName = data_object.LINKTYPE_NTO1_PROPERTY[relType]
                setattr(rhsDO, relPropName, lhsDO)

        # Now that everything is wired up we move the requested DOs to COMPLETED
        # (instead of doing it at the DOM-level deployment time, in which case
        # we would certainly miss most of the events)
        if logger.isEnabledFor(logging.INFO):
            logger.info('Moving following DataObjects to COMPLETED right away: %r' % (completedDOs,))

        thrExs = {}
        latch = CountDownLatch(len(completedDOs))
        for uid in completedDOs:
            t = threading.Thread(target=self._triggerDataObject, args=(proxies[uid],uid, latch, thrExs))
            t.start()
        latch.await()

        if thrExs:
            raise Exception("One ore more exceptions occurred while moving DOs to COMPLETED: %s" % (sessionId), thrExs)

        return allUris

    def _getGraphStatus(self, sessionId, node, allStatus, latch, exceptions):
        try:
            with self.domAt(node) as dom:
                allStatus.update(dom.getGraphStatus(sessionId))
        except Exception as e:
            exceptions[node] = e
            logger.error("An exception occurred while getting the graph status for session %s in node %s" % (sessionId, node))
            raise # so it gets printed
        finally:
            latch.countDown()

    def getGraphStatus(self, sessionId):

        allStatus = {}
        thrExs = {}

        latch = CountDownLatch(len(self._nodes))
        for node in self._nodes:
            t = threading.Thread(target=self._getGraphStatus, args=(sessionId, node, allStatus, latch, thrExs))
            t.start()
        latch.await()

        if thrExs:
            raise Exception("One ore more exceptions occurred while getting the graph status for session %s" % (sessionId), thrExs)
        return allStatus

    def _getGraph(self, sessionId, node, latch, allGraphs, exceptions):
        try:
            with self.domAt(node) as dom:
                allGraphs.update(dom.getGraph(sessionId))
        except Exception as e:
            exceptions[node] = e
            logger.error("An exception occurred while getting the graph for session %s in node %s" % (sessionId, node))
            raise # so it gets printed
        finally:
            latch.countDown()

    def getGraph(self, sessionId):

        allGraphs = {}
        thrExs = {}

        latch = CountDownLatch(len(self._nodes))
        for node in self._nodes:
            t = threading.Thread(target=self._getGraph, args=(sessionId, node, latch, allGraphs, thrExs))
            t.start()
        latch.await()

        if thrExs:
            raise Exception("One ore more exceptions occurred while getting the graph for session %s" % (sessionId), thrExs)
        return allGraphs

    def _getSessionStatus(self, sessionId, node, allStatus, latch, exceptions):
        try:
            with self.domAt(node) as dom:
                allStatus[node] = dom.getSessionStatus(sessionId)
        except Exception as e:
            exceptions[node] = e
            logger.error("An exception occurred while getting the status of session %s in node %s" % (sessionId, node))
            raise # so it gets printed
        finally:
            latch.countDown()

    def getSessionStatus(self, sessionId):

        allStatus = {}
        thrExs = {}

        latch = CountDownLatch(len(self._nodes))
        for node in self._nodes:
            t = threading.Thread(target=self._getSessionStatus, args=(sessionId, node, allStatus, latch, thrExs))
            t.start()
        latch.await()

        if thrExs:
            raise Exception("One ore more exceptions occurred while getting the graph status for session %s" % (sessionId), thrExs)

        # TODO: Maybe calculate a DIM-wide session status
        return allStatus