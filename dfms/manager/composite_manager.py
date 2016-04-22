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
import functools
import logging
import multiprocessing.pool
import threading

import Pyro4

from dfms import remote, graph_loader, drop, utils
from dfms.manager.client import BaseDROPManagerClient
from dfms.manager.constants import ISLAND_DEFAULT_REST_PORT, NODE_DEFAULT_REST_PORT
from dfms.manager.drop_manager import DROPManager
from dfms.utils import portIsOpen


logger = logging.getLogger(__name__)

class CompositeManager(DROPManager):
    """
    A DROPManager that in turn manages DROPManagers (sigh...).

    DROP Managers form a hierarchy where those at the bottom actually hold
    DROPs while those in the levels above rely commands and aggregate results,
    making the system more manageable and scalable. The CompositeManager class
    implements the upper part of this hierarchy in a generic way by holding
    references to a number of sub-DROPManagers and communicating with them to
    complete each operation. The only assumption about sub-DROPManagers is that
    they obey the DROPManager interface, and therefore this CompositeManager
    class allows for multiple levels of hierarchy seamlessly.

    Having different levels of Data Management hierarchy implies that the
    physical graph that is fed into the hierarchy needs to be partitioned at
    each level (except at the bottom of the hierarchy) in order to place each
    DROP in its correct place. The attribute used by a particular
    CompositeManager to partition the graph (from its graphSpec) is given at
    construction time.
    """

    def __init__(self, dmPort, partitionAttr, dmExec, subDmId, dmHosts=[], pkeyPath=None, dmCheckTimeout=10):
        """
        Creates a new CompositeManager. The sub-DMs it manages are to be located
        at `dmHosts`, and should be listening on port `dmPort`.

        :param: dmPort The port at which the sub-DMs expose themselves
        :param: partitionAttr The attribute on each dropSpec that specifies the
                partitioning of the graph at this CompositeManager level.
        :param: dmExec The name of the executable that starts a sub-DM
        :param: subDmId The sub-DM ID.
        :param: dmHosts The list of hosts under which the sub-DMs should be found.
        :param: pkeyPath The path to the SSH private key to be used when connecting
                to the remote hosts to start the sub-DMs if necessary. A value
                of `None` means that the default path should be used
        :param: dmCheckTimeout The timeout used before giving up and declaring
                a sub-DM as not-yet-present in a given host
        """
        self._dmPort = dmPort
        self._partitionAttr = partitionAttr
        self._dmExec = dmExec
        self._subDmId = subDmId
        self._dmHosts = dmHosts
        self._interDMRelations = collections.defaultdict(list)
        self._sessionIds = [] # TODO: it's still unclear how sessions are managed at the composite-manager level
        self._pkeyPath = pkeyPath
        self._dmCheckTimeout = dmCheckTimeout
        n_threads = len(dmHosts*2) if dmHosts else 20
        self._tp = multiprocessing.pool.ThreadPool(n_threads)

        # The list of bottom-level nodes that are covered by this manager
        # This list is different from the dmHosts, which are the machines that
        # are directly managed by this manager (which in turn could manage more
        # machines)
        self._nodes = []

        self.startDMChecker()

    def startDMChecker(self):
        self._dmCheckerEvt = threading.Event()
        self._dmCheckerThread = threading.Thread(name='DMChecker Thread', target=self._checkDM)
        self._dmCheckerThread.start()

    def stopDMChecker(self):
        if not self._dmCheckerEvt.isSet():
            self._dmCheckerEvt.set()
            self._dmCheckerThread.join()

    # Explicit shutdown
    def shutdown(self):
        self.stopDMChecker()
        self._tp.close()
        self._tp.join()

    def _checkDM(self):
        while True:
            for host in self._dmHosts:
                if self._dmCheckerEvt.is_set():
                    break
                try:
                    self.ensureDM(host, timeout=self._dmCheckTimeout)
                except:
                    logger.warning("Couldn't ensure a DM for host %s, will try again later" % (host))
            if self._dmCheckerEvt.wait(60):
                break

    @property
    def id(self):
        return self._id

    @property
    def dmHosts(self):
        return self._dmHosts[:]

    def addDmHost(self, host):
        self._dmHosts.append(host)

    @property
    def nodes(self):
        return self._nodes[:]

    def add_node(self, node):
        self._nodes.append(node)

    def remove_node(self, node):
        self._nodes.remove(node)

    @property
    def dmPort(self):
        return self._dmPort

    def subDMCommandLine(self, host):
        return '{0} -i {1} -P {2} -d --host {3}'.format(self._dmExec, self._subDmId, self._dmPort, host)

    def startDM(self, host):
        client = remote.createClient(host, pkeyPath=self._pkeyPath)
        out, err, status = remote.execRemoteWithClient(client, self.subDMCommandLine(host))
        if status != 0:
            logger.error("Failed to start the DM on %s:%d, stdout/stderr follow:\n==STDOUT==\n%s\n==STDERR==\n%s" % (host, self._dmPort, out, err))
            raise Exception("Failed to start the DM on %s:%d" % (host, self._dmPort))
        if logger.isEnabledFor(logging.INFO):
            logger.info("DM successfully started at %s:%d" % (host, self._dmPort))

    def ensureDM(self, host, timeout=10):

        # We rely on having ssh keys for this, since we're using
        # the dfms.remote module, which authenticates using public keys
        if logger.isEnabledFor(logging.DEBUG):
                logger.debug("Checking DM presence at %s:%d" % (host, self._dmPort))

        if portIsOpen(host, self._dmPort, timeout):
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("DM already present at %s:%d" % (host, self._dmPort))
            return

        if logger.isEnabledFor(logging.DEBUG):
                logger.debug("DM not present at %s:%d, will start it now" % (host, self._dmPort))

        self.startDM(host)

        # Wait a bit until the DM starts; if it doesn't we fail
        if not portIsOpen(host, self._dmPort, timeout):
            raise Exception("DM started at %s:%d, but couldn't connect to it" % (host, self._dmPort))

    def dmAt(self, host):
        return BaseDROPManagerClient(host, self._dmPort, 10)

    def getSessionIds(self):
        return self._sessionIds;

    def _createSession(self, sessionId, exceptions, host):
        try:
            self.ensureDM(host)
            with self.dmAt(host) as dm:
                dm.createSession(sessionId)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('Successfully created session %s in %s' % (sessionId, host))
        except Exception as e:
            logger.error("Failed to create a session on host %s" % (host))
            exceptions[host] = e
            raise # so it gets printed

    def createSession(self, sessionId):
        """
        Creates a session in all underlying DMs.
        """

        logger.info('Creating Session %s in all hosts' % (sessionId))

        thrExs = {}
        self._tp.map(functools.partial(self._createSession, sessionId, thrExs), self._dmHosts)
        if thrExs:
            raise Exception("One or more errors occurred while creating sessions", thrExs)

        self._sessionIds.append(sessionId)


    def _destroySession(self, sessionId, exceptions, host):
        try:
            self.ensureDM(host)
            with self.dmAt(host) as dm:
                dm.destroySession(sessionId)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('Successfully destroyed session %s in %s' % (sessionId, host))
        except Exception as e:
            logger.error("Failed to destroy a session on host %s" % (host))
            exceptions[host] = e
            raise # so it gets printed

    def destroySession(self, sessionId):
        """
        Destroy a session in all underlying DMs.
        """
        logger.info('Destroying Session %s in all hosts' % (sessionId))
        thrExs = {}

        self._tp.map(functools.partial(self._destroySession, sessionId, thrExs), self._dmHosts)
        if thrExs:
            raise Exception("One or more errors occurred while destroying sessions", thrExs)
        self._sessionIds.remove(sessionId)

    def _addGraphSpec(self, sessionId, exceptions, (host, graphSpec)):
        try:
            with self.dmAt(host) as dm:
                dm.addGraphSpec(sessionId, graphSpec)
            pass
        except Exception as e:
            logger.error("Failed to append graphSpec for session %s on host %s" % (sessionId, host))
            exceptions[host] = e
            raise # so it gets printed

    def addGraphSpec(self, sessionId, graphSpec):
        logger.debug('addGraphSpec - sessionId: {0}'.format(sessionId))  # TODO: KV remove
        # The first step is to break down the graph into smaller graphs that
        # belong to the same host, so we can submit that graph into the individual
        # DMs. For this we need to make sure that our graph has a the correct
        # attribute set
        perPartition = collections.defaultdict(list)
        for dropSpec in graphSpec:
            if self._partitionAttr not in dropSpec:
                raise Exception("DROP %s doesn't specify a %s attribute" % (dropSpec['oid'], self._partitionAttr))

            partition = dropSpec[self._partitionAttr]
            if partition not in self._dmHosts:
                raise Exception("DROP %s's %s %s does not belong to this DM" % (dropSpec['oid'], self._partitionAttr, partition))

            perPartition[partition].append(dropSpec)

        # At each partition the relationships between DROPs should be local at the
        # moment of submitting the graph; thus we record the inter-DM
        # relationships separately and remove them from the original graph spec
        logger.debug('addGraphSpec - sessionId: {0}'.format(sessionId))  # TODO: KV remove
        interDMRelations = []
        for dropSpecs in perPartition.viewvalues():
            interDMRelations.extend(graph_loader.removeUnmetRelationships(dropSpecs))

        # Create the individual graphs on each DM now that they are correctly
        # separated.
        if logger.isEnabledFor(logging.INFO):
            logger.info('Adding individual graphSpec of session %s to each DM' % (sessionId))

        thrExs = {}
        self._tp.map(functools.partial(self._addGraphSpec, sessionId, thrExs), [(host, perPartition[host]) for host in self._dmHosts])

        if thrExs:
            raise Exception("One or more errors occurred while adding the graphSpec to the individual DMs", thrExs)

        self._interDMRelations[sessionId].extend(interDMRelations)

    def _deploySession(self, sessionId, allUris, exceptions, host):
        try:
            with self.dmAt(host) as dm:
                uris = dm.deploySession(sessionId)

                # Perform some URI cosmetics. If the remote host is binding the
                # Pyro Daemons to all interfaces, the URIs will look like
                # PYRO:objID@0.0.0.0:port, and any proxy initialized with such
                # URI will try to contact 0.0.0.0:port *locally*
                # We thus replace any 0.0.0.0s we see by the `host`
                # For reference see:
                #
                # https://pythonhosted.org/Pyro4/tipstricks.html#multiple-network-interfaces
                for uid, origUri in uris.items():
                    if utils.isLocalhost(host) or \
                       '0.0.0.0' not in origUri:
                        continue
                    uri = Pyro4.URI(origUri)
                    uri.host = host
                    uri = uri.asString()
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug('Sanitized Pyro uri for DROP %s: %s -> %s' % (uid, origUri, uri))
                    uris[uid] = uri

                allUris.update(uris)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('Successfully deployed session %s in %s' % (sessionId, host))
        except Exception as e:
            exceptions[host] = e
            logger.error("An exception occurred while deploying session %s in %s" % (sessionId, host))
            raise # so it gets printed

    def _triggerDrop(self, exceptions, (drop, uid)):

        # Call "async_execute" for InputFiredAppDROPs, "setCompleted" otherwise
        method = 'setCompleted'
        if hasattr(drop, 'async_execute'):
            method = 'async_execute'

        try:
            m = getattr(drop, method)
            m()

            # Eagerly release the pyro connection used by this Drop proxy
            # If we don't do, and lots of Drops have to be initially
            # triggered at the same time we'll arrive to a deadlock since
            # all threads will need a connection, but connections won't be
            # available until all threads are finished
            drop._pyroRelease()

        except Exception as e:
            exceptions[drop.uid] = e
            logger.error("An exception occurred while moving DROP %s to COMPLETED" % (uid))
            raise # so it gets printed

    def deploySession(self, sessionId, completedDrops=[]):

        logger.info('Deploying Session %s in all hosts' % (sessionId))

        allUris = {}
        thrExs = {}

        # Deploy all individual graphs in parallel
        self._tp.map(functools.partial(self._deploySession, sessionId, allUris, thrExs), self._dmHosts)
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
        # TODO: This could be parallelized like the rest...
        for rel in self._interDMRelations[sessionId]:
            relType = rel.rel
            rhsDrop = proxies[rel.rhs]
            lhsDrop = proxies[rel.lhs]

            logger.debug("Establishing link %r", rel)
            try:
                if relType in drop.LINKTYPE_1TON_APPEND_METHOD:
                    methodName = drop.LINKTYPE_1TON_APPEND_METHOD[relType]
                    backMethodName = drop.LINKTYPE_1TON_BACK_APPEND_METHOD[relType]
                    rhsDrop._pyroInvoke(methodName, (lhsDrop,False), {})
                    lhsDrop._pyroInvoke(backMethodName, (rhsDrop,False), {})
                else:
                    relPropName = drop.LINKTYPE_NTO1_PROPERTY[relType]
                    backMethodName = drop.LINKTYPE_NTO1_BACK_APPEND_METHOD[relType]
                    setattr(rhsDrop, relPropName, lhsDrop)
                    lhsDrop._pyroInvoke(backMethodName, (rhsDrop,False), {})

                # Eagerly release the pyro connection used by this Drop proxy
                # See comment on self._triggerDrop
                rhsDrop._pyroRelease()
                lhsDrop._pyroRelease()
            except Exception:
                logger.exception("Error while establishing link %r" % (rel,))
                raise

        # Now that everything is wired up we move the requested DROPs to COMPLETED
        # (instead of doing it at the DM-level deployment time, in which case
        # we would certainly miss most of the events)
        if logger.isEnabledFor(logging.INFO):
            logger.info('Moving following DROPs to COMPLETED right away: %r' % (completedDrops,))

        if completedDrops:
            thrExs = {}
            self._tp.map(functools.partial(self._triggerDrop, thrExs), [(proxies[uid],uid) for uid in completedDrops])
            if thrExs:
                raise Exception("One ore more exceptions occurred while moving DROPs to COMPLETED: %s" % (sessionId), thrExs)

        return allUris

    def _getGraphStatus(self, sessionId, allStatus, exceptions, host):
        try:
            with self.dmAt(host) as dm:
                allStatus.update(dm.getGraphStatus(sessionId))
        except Exception as e:
            exceptions[host] = e
            logger.error("An exception occurred while getting the graph status for session %s in host %s" % (sessionId, host))
            raise # so it gets printed

    def getGraphStatus(self, sessionId):

        allStatus = {}
        thrExs = {}

        self._tp.map(functools.partial(self._getGraphStatus, sessionId, allStatus, thrExs), self._dmHosts)

        if thrExs:
            raise Exception("One ore more exceptions occurred while getting the graph status for session %s" % (sessionId), thrExs)
        return allStatus

    def _getGraph(self, sessionId, allGraphs, exceptions, host):
        try:
            with self.dmAt(host) as dm:
                allGraphs.update(dm.getGraph(sessionId))
        except Exception as e:
            exceptions[host] = e
            logger.error("An exception occurred while getting the graph for session %s in host %s" % (sessionId, host))
            raise # so it gets printed

    def getGraph(self, sessionId):

        allGraphs = {}
        thrExs = {}

        self._tp.map(functools.partial(self._getGraph, sessionId, allGraphs, thrExs), self._dmHosts)

        if thrExs:
            raise Exception("One ore more exceptions occurred while getting the graph for session %s" % (sessionId), thrExs)

        # The graphs coming from the DMs are not interconnected, we need to
        # add the missing connections to the graph before returning upstream
        for rel in self._interDMRelations[sessionId]:
            graph_loader.addLink(rel.rel, allGraphs[rel.rhs], rel.lhs)

        return allGraphs

    def _getSessionStatus(self, sessionId, allStatus, exceptions, host):
        try:
            with self.dmAt(host) as dm:
                allStatus[host] = dm.getSessionStatus(sessionId)
        except Exception as e:
            exceptions[host] = e
            logger.error("An exception occurred while getting the status of session %s in host %s" % (sessionId, host))
            raise # so it gets printed

    def getSessionStatus(self, sessionId):

        allStatus = {}
        thrExs = {}

        self._tp.map(functools.partial(self._getSessionStatus, sessionId, allStatus, thrExs), self._dmHosts)

        if thrExs:
            raise Exception("One ore more exceptions occurred while getting the graph status for session %s" % (sessionId), thrExs)

        # TODO: Maybe calculate a wider session status?
        return allStatus

class DataIslandManager(CompositeManager):
    """
    The DataIslandManager, which manages a number of NodeManagers.
    """

    def __init__(self, dmHosts=[], pkeyPath=None, dmCheckTimeout=10):
        super(DataIslandManager, self).__init__(NODE_DEFAULT_REST_PORT,
                                                'node',
                                                'dfmsNM',
                                                'nm',
                                                dmHosts=dmHosts,
                                                pkeyPath=pkeyPath,
                                                dmCheckTimeout=dmCheckTimeout)

        # In the case of the Data Island the dmHosts are the final nodes as well
        self._nodes = dmHosts
        logger.info('Created DataIslandManager for hosts: %r' % (self._dmHosts))

    def add_node(self, node):
        CompositeManager.add_node(self, node)
        self._dmHosts.append(node)

class MasterManager(CompositeManager):
    """
    The MasterManager, which manages a number of DataIslandManagers.
    """

    def __init__(self, dmHosts=[], pkeyPath=None, dmCheckTimeout=10):
        super(MasterManager, self).__init__(ISLAND_DEFAULT_REST_PORT,
                                            'island',
                                            'dfmsDIM',
                                            'dim',
                                            dmHosts=dmHosts,
                                            pkeyPath=pkeyPath,
                                            dmCheckTimeout=dmCheckTimeout)
        logger.info('Created MasterManager for hosts: %r' % (self._dmHosts))
