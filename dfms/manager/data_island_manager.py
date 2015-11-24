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

from dfms import remote, graph_loader, drop
from dfms.utils import CountDownLatch, portIsOpen


logger = logging.getLogger(__name__)
DM_PORT = 4000

class DataIslandManager(object):
    """
    The DataIslandManager

    A DataIslandManager manages a number of DROPManagers, one per node
    in the Data Island. It offers roughly the same methods as those offered
    by the DM since it offers the same capabilities.

    One of the key aspects of the DIM is that it receives a physical graph for
    the whole island, which it must distribute among the different DMs. For
    this it requires that all the nodes in the graph declare a node (a host
    name), which should be part of the Data Island. This way the DIM breaks down
    the physical graph in parts that belong to the different DMs, creates them
    individually, and links them later at deployment time.
    """

    def __init__(self, dimId, nodes=['localhost'], pkeyPath=None, dmRestPort=8888, dmCheckTimeout=10):
        self._dimId = dimId
        self._nodes = nodes
        self._connectTimeout = 100
        self._interDMRelations = collections.defaultdict(list)
        self._sessionIds = [] # TODO: it's still unclear how sessions are managed at the DIM level
        self._pkeyPath = pkeyPath
        self._dmRestPort = dmRestPort
        self._dmCheckTimeout = dmCheckTimeout
        self.startNodeChecker()
        logger.info('Created DataIslandManager for nodes: %r' % (self._nodes))

    def startNodeChecker(self):
        self._nodeCheckerEvt = threading.Event()
        self._nodeCheckerThread = threading.Thread(name='NodeChecker Thread', target=self._checkNodes)
        self._nodeCheckerThread.start()

    def stopNodeChecker(self):
        if not self._nodeCheckerEvt.isSet():
            self._nodeCheckerEvt.set()
            self._nodeCheckerThread.join()

    # Explicit shutdown
    def shutdown(self):
        self.stopNodeChecker()

    def _checkNodes(self):
        while True:
            for n in self._nodes:
                try:
                    self.ensureDM(n, timeout=self._dmCheckTimeout)
                except:
                    logger.warning("Couldn't ensure a DM for node %s, will try again later" % (n))
            if self._nodeCheckerEvt.wait(60):
                break

    @property
    def dimId(self):
        return self._dimId

    @property
    def nodes(self):
        return self._nodes[:]

    @property
    def dmRestPort(self):
        return self._dmRestPort

    def dfmsDMCommandLine(self, host, port):
        cmdline = 'dfmsDM --rest -i dm_{0} -P {1} -d --host {0}'.format(host, port)
        if self._dmRestPort:
            cmdline += ' --restPort {0}'.format(self._dmRestPort)
        return cmdline

    def startDM(self, host, port):
        client = remote.createClient(host, pkeyPath=self._pkeyPath)
        out, err, status = remote.execRemoteWithClient(client, self.dfmsDMCommandLine(host, port))
        if status != 0:
            logger.error("Failed to start the DM on %s:%d, stdout/stderr follow:\n==STDOUT==\n%s\n==STDERR==\n%s" % (host, port, out, err))
            raise Exception("Failed to start the DM on %s:%d" % (host, port))
        if logger.isEnabledFor(logging.INFO):
            logger.info("DM successfully started at %s:%d" % (host, port))

    def ensureDM(self, host, port=DM_PORT, timeout=10):
        # We rely on having ssh keys for this, since we're using
        # the dfms.remote module, which authenticates using public keys
        if logger.isEnabledFor(logging.DEBUG):
                logger.debug("Checking DM presence at %s:%d" % (host, port))

        if portIsOpen(host, port, timeout):
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("DM already present at %s:%d" % (host, port))
            return

        if logger.isEnabledFor(logging.DEBUG):
                logger.debug("DM not present at %s:%d, will start it now" % (host, port))

        self.startDM(host, port)

        # Wait a bit until the DM starts; if it doesn't we fail
        if not portIsOpen(host, port, timeout):
            raise Exception("DM started at %s:%d, but couldn't connect to it" % (host, port))

    def dmAt(self, node):
        return Pyro4.Proxy("PYRO:dm_{0}@{0}:{1}".format(node, DM_PORT))

    def getSessionIds(self):
        return self._sessionIds;

    def _createSession(self, sessionId, node, latch, exceptions):
        try:
            self.ensureDM(node)
            with self.dmAt(node) as dm:
                dm.createSession(sessionId)
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
        Creates a session in all underlying DMs.
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
            self.ensureDM(node)
            with self.dmAt(node) as dm:
                dm.destroySession(sessionId)
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
        Destroy a session in all underlying DMs.
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
            with self.dmAt(node) as dm:
                dm.addGraphSpec(sessionId, graphSpec)
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
        # DMs. For this we need to make sure that our graph has a 'node'
        # attribute set
        perNode = collections.defaultdict(list)
        for dropSpec in graphSpec:
            if 'node' not in dropSpec:
                raise Exception("DROP %s doesn't specify a node attribute" % (dropSpec['oid']))

            loc = dropSpec['node']
            if loc not in self._nodes:
                raise Exception("DROP %s's node %s does not belong to this DIM" % (dropSpec['oid'], loc))

            perNode[loc].append(dropSpec)

        # At each node the relationships between DROPs should be local at the
        # moment of submitting the graph; thus we record the inter-DM
        # relationships separately and remove them from the original graph spec
        interDMRelations = []
        for loc,dropSpecs in perNode.viewitems():
            interDMRelations.extend(graph_loader.removeUnmetRelationships(dropSpecs))

        # Create the individual graphs on each DM now that they are correctly
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
            raise Exception("One or more errors occurred while adding the graphSpec to the individual DMs", thrExs)

        self._interDMRelations[sessionId].extend(interDMRelations)

    def _deploySession(self, sessionId, node, allUris, latch, exceptions):
        try:
            with self.dmAt(node) as dm:
                uris = dm.deploySession(sessionId)
                allUris.update(uris)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('Successfully deployed session %s in %s' % (sessionId, node))
        except Exception as e:
            exceptions[node] = e
            logger.error("An exception occurred while deploying session %s in %s" % (sessionId, node))
            raise # so it gets printed
        finally:
            latch.countDown()

    def _triggerDrop(self, drop, uid, latch, exceptions):
        try:
            if hasattr(drop, 'execute'):
                t = threading.Thread(target=lambda:drop.execute())
                t.daemon = True
                t.start()
            else:
                drop.setCompleted()
        except Exception as e:
            exceptions[drop.uid] = e
            logger.error("An exception occurred while moving DROP %s to COMPLETED" % (uid))
            raise # so it gets printed
        finally:
            latch.countDown()

    def deploySession(self, sessionId, completedDrops=[]):

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
        # (i.e., those present in inter-DM relationships and in completedDrops)
        # Creating proxies beforehand and reusing them means that we won't need
        # to establish that many TCP connections once and over again
        proxies = {}
        for rel in self._interDMRelations[sessionId]:
            if rel.rhs not in proxies:
                proxies[rel.rhs] = Pyro4.Proxy(allUris[rel.rhs])
            if rel.lhs not in proxies:
                proxies[rel.lhs] = Pyro4.Proxy(allUris[rel.lhs])
        for uid in completedDrops:
            if uid not in proxies:
                proxies[uid] = Pyro4.Proxy(allUris[uid])

        # Establish the inter-DM relationships between DROPs.
        # DROPRel tuples are read: "lhs is rel of rhs" (e.g., A is PRODUCER of B)
        for rel in self._interDMRelations[sessionId]:
            relType = rel.rel
            rhsDrop = proxies[rel.rhs]
            lhsDrop = proxies[rel.lhs]

            if relType in drop.LINKTYPE_1TON_APPEND_METHOD:
                methodName = drop.LINKTYPE_1TON_APPEND_METHOD[relType]
                rhsDrop._pyroInvoke(methodName, (lhsDrop,), {})
            else:
                relPropName = drop.LINKTYPE_NTO1_PROPERTY[relType]
                setattr(rhsDrop, relPropName, lhsDrop)

        # Now that everything is wired up we move the requested DROPs to COMPLETED
        # (instead of doing it at the DM-level deployment time, in which case
        # we would certainly miss most of the events)
        if logger.isEnabledFor(logging.INFO):
            logger.info('Moving following DROPs to COMPLETED right away: %r' % (completedDrops,))

        thrExs = {}
        latch = CountDownLatch(len(completedDrops))
        for uid in completedDrops:
            t = threading.Thread(target=self._triggerDrop, args=(proxies[uid],uid, latch, thrExs))
            t.start()
        latch.await()

        if thrExs:
            raise Exception("One ore more exceptions occurred while moving DROPs to COMPLETED: %s" % (sessionId), thrExs)

        return allUris

    def _getGraphStatus(self, sessionId, node, allStatus, latch, exceptions):
        try:
            with self.dmAt(node) as dm:
                allStatus.update(dm.getGraphStatus(sessionId))
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
            with self.dmAt(node) as dm:
                allGraphs.update(dm.getGraph(sessionId))
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

        # The graphs coming from the DMs are not interconnected, we need to
        # add the missing connections to the graph before returning upstream
        for rel in self._interDMRelations[sessionId]:
            graph_loader.addLink(rel.rel, allGraphs[rel.rhs], rel.lhs)

        return allGraphs

    def _getSessionStatus(self, sessionId, node, allStatus, latch, exceptions):
        try:
            with self.dmAt(node) as dm:
                allStatus[node] = dm.getSessionStatus(sessionId)
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