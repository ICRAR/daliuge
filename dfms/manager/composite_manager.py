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
import time

import Pyro4

from dfms import remote, graph_loader, drop, utils
from dfms.ddap_protocol import DROPRel
from dfms.exceptions import InvalidGraphException, DaliugeException, \
    SubManagerException
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
        n_threads = max(1,min(len(dmHosts),20))
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
                    logger.warning("Couldn't ensure a DM for host %s, will try again later", host)
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
            raise DaliugeException("Failed to start the DM on %s:%d" % (host, self._dmPort))
        logger.info("DM successfully started at %s:%d", host, self._dmPort)

    def ensureDM(self, host, timeout=10):

        logger.debug("Checking DM presence at %s:%d", host, self._dmPort)
        if portIsOpen(host, self._dmPort, timeout):
            logger.debug("DM already present at %s:%d", host, self._dmPort)
            return

        # We rely on having ssh keys for this, since we're using
        # the dfms.remote module, which authenticates using public keys
        logger.debug("DM not present at %s:%d, will start it now", host, self._dmPort)
        self.startDM(host)

        # Wait a bit until the DM starts; if it doesn't we fail
        if not portIsOpen(host, self._dmPort, timeout):
            raise DaliugeException("DM started at %s:%d, but couldn't connect to it" % (host, self._dmPort))

    def dmAt(self, host):
        return BaseDROPManagerClient(host, self._dmPort, 10)

    def getSessionIds(self):
        return self._sessionIds;

    #
    # Replication of commands to underlying drop managers
    # If "collect" is given, then individual results are also kept in the given
    # structure, which is either a dictionary or a list
    #
    def _do_in_host(self, sessionId, exceptions, f, collect, iterable):

        host = iterable
        if isinstance(iterable, (list, tuple)):
            host = iterable[0]

        try:
            self.ensureDM(host)
            with self.dmAt(host) as dm:
                res = f(dm, iterable, sessionId)

            if isinstance(collect, dict):
                collect.update(res)
            elif isinstance(collect, list):
                collect.append(res)

        except Exception as e:
            exceptions[host] = e
            raise # so it gets printed

    def replicate(self, sessionId, f, action, collect=None, iterable=None):
        """
        Replicates the given function call on each of the underlying drop managers
        """
        thrExs = {}
        iterable = iterable or self._dmHosts
        self._tp.map(functools.partial(self._do_in_host, sessionId, thrExs, f, collect), iterable)
        if thrExs:
            msg = "One or more errors occurred while %s on session %s" % (action, sessionId)
            raise SubManagerException(msg, thrExs)

    #
    # Commands and their per-underlying-drop-manager functions
    #
    def _createSession(self, dm, host, sessionId):
        dm.createSession(sessionId)
        logger.debug('Successfully created session %s on %s', sessionId, host)

    def createSession(self, sessionId):
        """
        Creates a session in all underlying DMs.
        """
        logger.info('Creating Session %s in all hosts', sessionId)
        self.replicate(sessionId, self._createSession, "creating sessions")
        self._sessionIds.append(sessionId)

    def _destroySession(self, dm, host, sessionId):
        dm.destroySession(sessionId)
        logger.debug('Successfully destroyed session %s on %s', sessionId, host)

    def destroySession(self, sessionId):
        """
        Destroy a session in all underlying DMs.
        """
        logger.info('Destroying Session %s in all hosts', sessionId)
        self.replicate(sessionId, self._destroySession, "creating sessions")
        self._sessionIds.remove(sessionId)

    def _addGraphSpec(self, dm, (host, graphSpec), sessionId):
        dm.addGraphSpec(sessionId, graphSpec)
        logger.info("Successfully appended graph to session %s on %s", sessionId, host)

    def addGraphSpec(self, sessionId, graphSpec):
        # The first step is to break down the graph into smaller graphs that
        # belong to the same host, so we can submit that graph into the individual
        # DMs. For this we need to make sure that our graph has a the correct
        # attribute set
        perPartition = collections.defaultdict(list)
        for dropSpec in graphSpec:
            if self._partitionAttr not in dropSpec:
                msg = "DROP %s doesn't specify a %s attribute" % (dropSpec['oid'], self._partitionAttr)
                raise InvalidGraphException(msg)

            partition = dropSpec[self._partitionAttr]
            if partition not in self._dmHosts:
                msg = "DROP %s's %s %s does not belong to this DM" % (dropSpec['oid'], self._partitionAttr, partition)
                raise InvalidGraphException(msg)

            perPartition[partition].append(dropSpec)

        # At each partition the relationships between DROPs should be local at the
        # moment of submitting the graph; thus we record the inter-DM
        # relationships separately and remove them from the original graph spec
        interDMRelations = []
        for dropSpecs in perPartition.viewvalues():
            interDMRelations.extend(graph_loader.removeUnmetRelationships(dropSpecs))

        # TODO: Big change required to remove this hack here
        #
        # Values in the interDMRelations array use OIDs to identify drops.
        # This is because so far we have told users to that OIDs are required
        # in the physical graph description, while UIDs are optional
        # (and copied over from the OID if not given).
        # On the other hand, once drops are actually created in deploySession()
        # we access the values in interDMRelations as if they had UIDs inside,
        # which causes problems everywhere because everything else is indexed
        # on UIDs.
        # In order to not break the current physical graph constrains and keep
        # things simple we'll simply replace the values of the interDMRelations
        # array here to use the corresponding UID for the given OIDs.
        # Because UIDs are globally unique across drop instances it makes sense
        # to always index things by UID and not by OID. Thus, in the future we
        # should probably change the requirement on the physical graphs sent by
        # users to always require an UID, and optionally an OID, and then change
        # all this code to immediately use those UIDs instead.
        def uid_for_drop(dropSpec):
            if 'uid' in dropSpec:
                return dropSpec['uid']
            return dropSpec['oid']
        newDMRelations = []
        graphDict = {dropSpec['oid']: dropSpec for dropSpec in graphSpec}
        for rel in interDMRelations:
            lhs = uid_for_drop(graphDict[rel.lhs])
            rhs = uid_for_drop(graphDict[rel.rhs])
            new_rel = DROPRel(lhs, rel.rel, rhs)
            newDMRelations.append(new_rel)
        interDMRelations[:] = newDMRelations
        logger.debug('Removed (and sanitized) %d inter-dm relationships', len(interDMRelations))

        # Create the individual graphs on each DM now that they are correctly
        # separated.
        logger.info('Adding individual graphSpec of session %s to each DM', sessionId)

        partitions = [(host, perPartition[host]) for host in self._dmHosts]
        self.replicate(sessionId, self._addGraphSpec, "appending graphSpec to individual DMs", iterable=partitions)

        self._interDMRelations[sessionId].extend(interDMRelations)

    def _deploySession(self, dm, host, sessionId):

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
            logger.debug('Sanitized Pyro uri for DROP %s: %s -> %s', uid, origUri, uri)
            uris[uid] = uri

        logger.debug('Successfully deployed session %s on %s', sessionId, host)
        return uris

    def _establish_drop_rhsrel(self, proxy, allUris, rel):

        # DROPRel tuples are read: "lhs is rel of rhs" (e.g., A is PRODUCER of B)
        relType = rel.rel
        rhsDrop = proxy
        lhsDrop = Pyro4.Proxy(allUris[rel.lhs])

        rhsDrop._pyroReconnect(tries=10)
        if relType in drop.LINKTYPE_1TON_APPEND_METHOD:
            methodName = drop.LINKTYPE_1TON_APPEND_METHOD[relType]
            rhsDrop._pyroInvoke(methodName, (lhsDrop,False), {})
        else:
            relPropName = drop.LINKTYPE_NTO1_PROPERTY[relType]
            setattr(rhsDrop, relPropName, lhsDrop)

    def _establish_drop_lhsrel(self, proxy, allUris, rel):

        # DROPRel tuples are read: "lhs is rel of rhs" (e.g., A is PRODUCER of B)
        relType = rel.rel
        rhsDrop = Pyro4.Proxy(allUris[rel.rhs])
        lhsDrop = proxy

        if relType in drop.LINKTYPE_1TON_APPEND_METHOD:
            backMethodName = drop.LINKTYPE_1TON_BACK_APPEND_METHOD[relType]
            lhsDrop._pyroInvoke(backMethodName, (rhsDrop,False), {})
        else:
            backMethodName = drop.LINKTYPE_NTO1_BACK_APPEND_METHOD[relType]
            lhsDrop._pyroInvoke(backMethodName, (rhsDrop,False), {})

    def _establish_drop_rels(self, allUris, exceptions, uids_rel_pairs):

        # rels is a list of (uid,rel) tuples, index by uid
        by_uid = collections.defaultdict(list)
        for uid, rel in uids_rel_pairs:
            by_uid[uid].append(rel)

        for uid, rels in by_uid.items():

            # Later on proxy will correspond either to the rhsDrop or the lhsDrop
            # Each thread uses a fresh Proxy thus avoiding race conditions when
            # connecting to the same remote object from different threads, but
            # at the same time reusing a single connection from each thread.
            proxy = Pyro4.Proxy(allUris[uid])

            try:
                proxy._pyroReconnect(tries=10)
                for rel in rels:
                    logger.debug("Establishing link %r", rel)
                    if uid == rel.rhs:
                        self._establish_drop_rhsrel(proxy, allUris, rel)
                    else:
                        self._establish_drop_lhsrel(proxy, allUris, rel)
                    logger.debug("Done establishing link %r", rel)
            except Exception as e:
                exceptions[rel] = e
                logger.exception("An exception establishing link %r", rel)
                raise # so it gets printed
            finally:
                proxy._pyroRelease()

    def _establish_all_rels(self, sessionId, allUris):

        # For each DROPRel element we establish the link both ways so both drops
        # can see each other. This is automatically done by the add* methods
        # of the drop classes, but we do it manually here (thus the "False"
        # argument on the _pyroInvoke call later on) to have full control over
        # the connections being opened/closed on the pyro deamons hosting the
        # drops.
        #
        # Moreover, in order to be able to safely establish these links in
        # parallel and avoid exhausting all the threads on the pyro deamons
        # we try to group together those relationships that originate in the
        # same drop and establish them all from the same thread, and using a
        # single connection. At the same time we try to work-balance all threads
        uids_rel_pairs = []
        for rel in self._interDMRelations[sessionId]:
            uids_rel_pairs.append((rel.lhs,rel))
            uids_rel_pairs.append((rel.rhs,rel))
        uids_rel_pairs.sort(key=lambda x: x[0])

        n = self._tp._processes
        uids_rel_pairs = [uids_rel_pairs[i:i+n] for i in range(0, len(uids_rel_pairs), n)]

        thrExs = {}
        self._tp.map(functools.partial(self._establish_drop_rels, allUris, thrExs), uids_rel_pairs)
        if thrExs:
            raise Exception("One or more exceptions occurred while establishing links on session %s" % (sessionId,), thrExs)

    def _triggerDrop(self, exceptions, (drop, uid)):

        # Call "async_execute" for InputFiredAppDROPs, "setCompleted" otherwise
        method = 'setCompleted'
        if hasattr(drop, 'async_execute'):
            method = 'async_execute'

        try:
            m = getattr(drop, method)
            m()

        except Exception as e:
            exceptions[drop.uid] = e
            logger.exception("An exception occurred while moving DROP %s to COMPLETED", uid)
            raise # so it gets printed
        finally:
            drop._pyroRelease()

    def deploySession(self, sessionId, completedDrops=[]):

        logger.info('Deploying Session %s in all hosts', sessionId)

        allUris = {}
        self.replicate(sessionId, self._deploySession, "deploying session", collect=allUris)

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
        if self._interDMRelations[sessionId]:
            now = time.time()
            logger.info("Establishing %d drop relationships", len(self._interDMRelations[sessionId]))
            self._establish_all_rels(sessionId, allUris)
            logger.info("Established all drop relationships (%d) in %.3f [s]", len(self._interDMRelations[sessionId]), time.time() - now)

        # Now that everything is wired up we move the requested DROPs to COMPLETED
        # (instead of doing it at the DM-level deployment time, in which case
        # we would certainly miss most of the events)
        if completedDrops:
            logger.debug('Moving following DROPs to COMPLETED right away: %r', completedDrops)
            thrExs = {}
            self._tp.map(functools.partial(self._triggerDrop, thrExs), [(proxies[uid],uid) for uid in completedDrops])
            if thrExs:
                raise Exception("One or more exceptions occurred while moving DROPs to COMPLETED: %s" % (sessionId), thrExs)

        return allUris

    def _getGraphStatus(self, dm, host, sessionId):
        return dm.getGraphStatus(sessionId)

    def getGraphStatus(self, sessionId):
        allStatus = {}
        self.replicate(sessionId, self._getGraphStatus, "getting graph status", collect=allStatus)
        return allStatus

    def _getGraph(self, dm, host, sessionId):
        return dm.getGraph(sessionId)

    def getGraph(self, sessionId):

        allGraphs = {}
        self.replicate(sessionId, self._getGraph, "getting the graph", collect=allGraphs)

        # The graphs coming from the DMs are not interconnected, we need to
        # add the missing connections to the graph before returning upstream
        for rel in self._interDMRelations[sessionId]:
            graph_loader.addLink(rel.rel, allGraphs[rel.rhs], rel.lhs)

        return allGraphs

    def _getSessionStatus(self, dm, host, sessionId):
        return {host: dm.getSessionStatus(sessionId)}

    def getSessionStatus(self, sessionId):
        allStatus = {}
        self.replicate(sessionId, self._getSessionStatus, "getting the graph status", collect=allStatus)
        return allStatus

    def _getGraphSize(self, dm, host, sessionId):
        return dm.getGraphSize(sessionId)

    def getGraphSize(self, sessionId):
        allCounts = []
        self.replicate(sessionId, self._getGraphSize, "getting the graph size", collect=allCounts)
        return sum(allCounts)

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
        logger.info('Created DataIslandManager for hosts: %r', self._dmHosts)

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
        logger.info('Created MasterManager for hosts: %r', self._dmHosts)