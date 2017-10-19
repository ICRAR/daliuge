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
import abc
import collections
import functools
import logging
import multiprocessing.pool
import threading

from . import constants
from .client import NodeManagerClient
from .constants import ISLAND_DEFAULT_REST_PORT, NODE_DEFAULT_REST_PORT
from .drop_manager import DROPManager
from .. import remote, graph_loader
from ..ddap_protocol import DROPRel
from ..exceptions import InvalidGraphException, DaliugeException, \
    SubManagerException
from ..utils import portIsOpen


logger = logging.getLogger(__name__)

def uid_for_drop(dropSpec):
    if 'uid' in dropSpec:
        return dropSpec['uid']
    return dropSpec['oid']

def sanitize_relations(interDMRelations, graph):

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
    newDMRelations = []
    for rel in interDMRelations:
        lhs = uid_for_drop(graph[rel.lhs])
        rhs = uid_for_drop(graph[rel.rhs])
        new_rel = DROPRel(lhs, rel.rel, rhs)
        newDMRelations.append(new_rel)
    interDMRelations[:] = newDMRelations

def group_by_node(uids, graph):
    uids_by_node = collections.defaultdict(list)
    for uid in uids:
        uids_by_node[graph[uid]['node']].append(uid)
    return uids_by_node

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

    __metaclass__ = abc.ABCMeta

    def __init__(self, dmPort, partitionAttr, subDmId, dmHosts=[], pkeyPath=None, dmCheckTimeout=10):
        """
        Creates a new CompositeManager. The sub-DMs it manages are to be located
        at `dmHosts`, and should be listening on port `dmPort`.

        :param: dmPort The port at which the sub-DMs expose themselves
        :param: partitionAttr The attribute on each dropSpec that specifies the
                partitioning of the graph at this CompositeManager level.
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
        self._subDmId = subDmId
        self._dmHosts = dmHosts
        self._graph = {}
        self._drop_rels = {}
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
                if not self.check_dm(host, timeout=self._dmCheckTimeout):
                    logger.error("Couldn't contact manager for host %s, will try again later", host)
            if self._dmCheckerEvt.wait(60):
                break

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

    def check_dm(self, host, port=None, timeout=10):
        port = port or self._dmPort
        logger.debug("Checking DM presence at %s:%d", host, port)
        dm_is_there = portIsOpen(host, port, timeout)
        return dm_is_there

    def dmAt(self, host, port=None):

        if not self.check_dm(host, port):
            raise SubManagerException('Manager expected but not running in %s:%d' % (host, port))

        port = port or self._dmPort
        return NodeManagerClient(host, port, 10)

    def getSessionIds(self):
        return self._sessionIds;

    #
    # Replication of commands to underlying drop managers
    # If "collect" is given, then individual results are also kept in the given
    # structure, which is either a dictionary or a list
    #
    def _do_in_host(self, action, sessionId, exceptions, f, collect, port, iterable):

        host = iterable
        if isinstance(iterable, (list, tuple)):
            host = iterable[0]

        try:
            with self.dmAt(host, port) as dm:
                res = f(dm, iterable, sessionId)

            if isinstance(collect, dict):
                collect.update(res)
            elif isinstance(collect, list):
                collect.append(res)

        except Exception as e:
            exceptions[host] = e
            logger.exception("Error while %s on host %s, session %s", action, host, sessionId)

    def replicate(self, sessionId, f, action, collect=None, iterable=None, port=None):
        """
        Replicates the given function call on each of the underlying drop managers
        """
        thrExs = {}
        iterable = iterable or self._dmHosts
        port = port or self._dmPort
        self._tp.map(functools.partial(self._do_in_host, action, sessionId, thrExs, f, collect, port), iterable)
        if thrExs:
            msg = "More than one error occurred while %s on session %s" % (action, sessionId)
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
        logger.info('Successfully created session %s in all hosts', sessionId)
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

    def _add_node_subscriptions(self, dm, host_and_subscriptions, sessionId):
        host, subscriptions = host_and_subscriptions
        dm.add_node_subscriptions(sessionId, subscriptions)
        logger.debug("Successfully added relationship info to session %s on %s", sessionId, host)

    def _addGraphSpec(self, dm, host_and_graphspec, sessionId):
        host, graphSpec = host_and_graphspec
        dm.addGraphSpec(sessionId, graphSpec)
        logger.info("Successfully appended graph to session %s on %s", sessionId, host)

    def addGraphSpec(self, sessionId, graphSpec):

        # The first step is to break down the graph into smaller graphs that
        # belong to the same host, so we can submit that graph into the individual
        # DMs. For this we need to make sure that our graph has a the correct
        # attribute set
        logger.info('Separating graph')
        perPartition = collections.defaultdict(list)
        for dropSpec in graphSpec:
            if self._partitionAttr not in dropSpec:
                msg = "Drop %s doesn't specify a %s attribute" % (dropSpec['oid'], self._partitionAttr)
                raise InvalidGraphException(msg)

            partition = dropSpec[self._partitionAttr]
            if partition not in self._dmHosts:
                msg = "Drop %s's %s %s does not belong to this DM" % (dropSpec['oid'], self._partitionAttr, partition)
                raise InvalidGraphException(msg)

            perPartition[partition].append(dropSpec)

            # Add the drop specs to our graph
            self._graph[uid_for_drop(dropSpec)] = dropSpec

        # At each partition the relationships between DROPs should be local at the
        # moment of submitting the graph; thus we record the inter-partition
        # relationships separately and remove them from the original graph spec
        inter_partition_rels = []
        for dropSpecs in perPartition.values():
            inter_partition_rels += graph_loader.removeUnmetRelationships(dropSpecs)
        sanitize_relations(inter_partition_rels, self._graph)
        logger.info('Removed (and sanitized) %d inter-dm relationships', len(inter_partition_rels))

        # Store the inter-partition relationships; later on they have to be
        # communicated to the NMs so they can establish them as needed.
        drop_rels = collections.defaultdict(functools.partial(collections.defaultdict, list))
        for rel in inter_partition_rels:
            rhn = self._graph[rel.rhs]['node']
            lhn = self._graph[rel.lhs]['node']
            drop_rels[lhn][rhn].append(rel)
            drop_rels[rhn][lhn].append(rel)

        self._drop_rels[sessionId] = drop_rels
        logger.debug("Calculated NM-level drop relationships: %r", drop_rels)

        # Create the individual graphs on each DM now that they are correctly
        # separated.
        logger.info('Adding individual graphSpec of session %s to each DM', sessionId)
        self.replicate(sessionId, self._addGraphSpec, "appending graphSpec to individual DMs", iterable=perPartition.items())
        logger.info('Successfully added individual graphSpec of session %s to each DM', sessionId)

    def _deploySession(self, dm, host, sessionId):
        dm.deploySession(sessionId)
        logger.debug('Successfully deployed session %s on %s', sessionId, host)

    def _triggerDrops(self, dm, host_and_uids, sessionId):
        host, uids = host_and_uids
        dm.trigger_drops(sessionId, uids)
        logger.info("Successfully triggered drops for session %s on %s", sessionId, host)

    def deploySession(self, sessionId, completedDrops=[]):

        # Indicate the node managers that they have to subscribe to events
        # published by some nodes
        if self._drop_rels.get(sessionId, None):
            self.replicate(sessionId, self._add_node_subscriptions, "adding relationship information",
                           port=constants.NODE_DEFAULT_REST_PORT,
                           iterable=self._drop_rels[sessionId].items())
            logger.info("Delivered node subscription list to node managers")

        logger.info('Deploying Session %s in all hosts', sessionId)
        self.replicate(sessionId, self._deploySession, "deploying session")
        logger.info('Successfully deployed session %s in all hosts', sessionId)

        # Now that everything is wired up we move the requested DROPs to COMPLETED
        # (instead of doing it at the DM-level deployment time, in which case
        # we would certainly miss most of the events)
        if completedDrops:
            not_found = set(completedDrops) - set(self._graph)
            if not_found:
                raise DaliugeException("UIDs for completed drops not found: %r", not_found)
            logger.info('Moving Drops to COMPLETED right away: %r', completedDrops)
            completed_by_host = group_by_node(completedDrops, self._graph)
            self.replicate(sessionId, self._triggerDrops, "triggering drops",
                           port=constants.NODE_DEFAULT_REST_PORT,
                           iterable=completed_by_host.items())
            logger.info('Successfully triggered drops')

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
        rels = set([z for x in self._drop_rels[sessionId].values() for y in x.values() for z in y])
        for rel in rels:
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
                                            'dim',
                                            dmHosts=dmHosts,
                                            pkeyPath=pkeyPath,
                                            dmCheckTimeout=dmCheckTimeout)
        logger.info('Created MasterManager for hosts: %r', self._dmHosts)
