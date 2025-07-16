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
from __future__ import annotations

import abc
import collections
import functools
import json
import logging
import multiprocessing.pool
import threading

from pathlib import Path

from dlg.manager.client import NodeManagerClient
from dlg.manager.drop_manager import DROPManager
from dlg.manager.manager_data import Node

from dlg.constants import ISLAND_DEFAULT_REST_PORT, NODE_DEFAULT_REST_PORT
from dlg import graph_loader
from dlg.common.reproducibility.reproducibility import init_pg_repro_data
from dlg.ddap_protocol import DROPRel
from dlg.exceptions import (
    InvalidGraphException,
    DaliugeException,
    SubManagerException,
)
from dlg.manager.past_sessions import PastSessionManager
from dlg.utils import portIsOpen, getDlgWorkDir

logger = logging.getLogger(f"dlg.{__name__}")


def uid_for_drop(dropSpec):
    if "uid" in dropSpec:
        return dropSpec["uid"]
    return dropSpec["oid"]


def sanitize_link(link):
    """
    Links can now be dictionaries, but we only need
    the key.
    """
    return list(link.keys())[0] if isinstance(link, dict) else link


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
    #
    # NOTE: It seems that the comment above is the result of a misunderstasnding
    # of the concept of OIDs and UIDs. OIDs are objectIDs provided by the
    # user or rather the translation system, they can't be UIDs, since those
    # have to be created by the underlying system implementing the actual drops.
    # The reason for that is that the UIDs are required to be unique within
    # the system runtime, the OIDs only have to be unique for a certain object.
    # In fact there could be multiple drops using to the same OID, but having
    # different UIDs. The idea would be that system generates the UIDs during
    # generation of the drops. In fact the user does not need to and should not
    # know about the UIDs at all and in general the system does not need to
    # know about the OIDs.
    newDMRelations = []
    for rel in interDMRelations:
        lhs = rel.lhs
        lhs = sanitize_link(rel.lhs)
        lhs = uid_for_drop(graph[lhs])
        rhs = sanitize_link(rel.rhs)
        rhs = uid_for_drop(graph[rhs])
        new_rel = DROPRel(lhs, rel.rel, rhs)
        newDMRelations.append(new_rel)
    interDMRelations[:] = newDMRelations


def group_by_node(uids, graph):
    uids_by_node = collections.defaultdict(list)
    for uid in uids:
        uids_by_node[Node(graph[uid]["node"])].append(uid)
    logger.info(uids_by_node)
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

    def __init__(
        self,
        dmPort,
        partitionAttr,
        subDmId,
        dmHosts: list[str] = None,
        pkeyPath=None,
        dmCheckTimeout=10,
        dump_graphs=False,
        hosts_are_dim=False,
    ):
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
        self._dmHosts = [Node(host, dim=hosts_are_dim) for host in dmHosts]
        if self._dmHosts and self._dmHosts[0].rest_port_specified:
            dmPort = -1
        self._dmPort = dmPort
        self._partitionAttr = partitionAttr
        self._subDmId = subDmId
        self._graph = {}
        self._drop_rels = {}
        self._sessionIds = (
            []
        )  # TODO: it's still unclear how sessions are managed at the composite-manager level
        self._pkeyPath = pkeyPath
        self._dmCheckTimeout = dmCheckTimeout
        n_threads = max(1, min(len(dmHosts), 20))
        self._tp = multiprocessing.pool.ThreadPool(n_threads)
        self._dump_graphs = dump_graphs
        self._past_session_manager = PastSessionManager(getDlgWorkDir())
        # The list of bottom-level nodes that are covered by this manager
        # This list is different from the dmHosts, which are the machines that
        # are directly managed by this manager (which in turn could manage more
        # machines)
        self.use_dmHosts = False
        self._nodes = []

        self.startDMChecker()

    def startDMChecker(self):
        self._dmCheckerEvt = threading.Event()
        self._dmCheckerThread = threading.Thread(
            name="DMChecker Thread", target=self._checkDM
        )
        self._dmCheckerThread.start()

    def stopDMChecker(self):
        if not self._dmCheckerEvt.is_set():
            self._dmCheckerEvt.set()
            self._dmCheckerThread.join()

    # Explicit shutdown
    def shutdown(self):
        self.stopDMChecker()
        self._tp.close()
        self._tp.terminate()
        self._tp.join()

    def _checkDM(self):
        while True:
            for host in self._dmHosts:
                if self._dmCheckerEvt.is_set():
                    break
                if not self.check_dm(host, self._dmPort, timeout=self._dmCheckTimeout):
                    logger.error(
                        "Couldn't contact manager for host %s with dmPort %d, will try again later",
                        host,
                        self._dmPort,
                    )
            if self._dmCheckerEvt.wait(60):
                break

    @property
    def dmHosts(self):
        return [str(n) for n in self._dmHosts[:]]

    def addDmHost(self, host_str: str):
        host = Node(host_str)
        if host not in self._dmHosts:
            self._dmHosts.append(host)
            logger.debug("Added sub-manager %s", host)
        else:
            logger.warning("Host %s already registered.", host)

    def removeDmHost(self, host_str):
        host = Node(host_str)
        if host in self._dmHosts:
            self._dmHosts.remove(host)

    @property
    def nodes(self):
        if self.use_dmHosts:
            return [str(n) for n in self._dmHosts[:]]
        else:
            return self._nodes

    def add_node(self, node: Node):
        if self.use_dmHosts:
            return self._dmHosts.append(node)
        else:
            self._nodes.append(node)

    def remove_node(self, node):
        if self.use_dmHosts:
            self._dmHosts.remove(node)
        else:
            self._nodes.remove(node)

    def get_node_from_json(self, node_str):
        """
        Given a node str, return the Node we have stored
        Return: Node
        Raises: ValueError if there is no existing Node added to the CompositeManager
        """

        idx = self._nodes.index(Node(node_str))
        return self._nodes[idx]

    @property
    def dmPort(self):
        return self._dmPort

    def check_dm(self, host: Node, port: int = None, timeout=10):
        host_name = host.host
        if host.rest_port_specified:
            port = host.port
        else:
            port = port or self._dmPort

        logger.debug("Checking DM presence at %s port %d", host, port)
        return portIsOpen(host_name, port, timeout)

    def dmAt(self, host):
        if not self.check_dm(host):
            raise SubManagerException(
                f"Manager expected but not running in {host.host}:{host.port}"
            )

        return NodeManagerClient(host.host, host.port, 10)

    def getSessionIds(self):
        return self._sessionIds

    def getPastSessionIds(self) -> list[str]:
        """
        Get past sessions stored in the composite managers current working directory.

        :return: List of path names (str)
        """
        return [
            path.name
            for path in self._past_session_manager.past_sessions(self._sessionIds)
        ]

    def _do_in_host(self, action, sessionId, exceptions, f, collect, iterable, **kwargs):
        """
        Replication of commands to underlying drop managers
        If "collect" is given, then individual results are also kept in the given
        structure, which is either a dictionary or a list
        """

        host = iterable
        if isinstance(iterable, (list, tuple)):
            host = iterable[0]  # What's going on here?
        if isinstance(host, str):
            host = Node(host)

        try:
            with self.dmAt(host) as dm:
                res = f(dm, iterable, sessionId, **kwargs)

            if isinstance(collect, dict):
                collect.update(res)
            elif isinstance(collect, list):
                collect.append(res)

        except Exception as e: # pylint: disable=broad-exception-caught
            exceptions[str(host)] = e
            logger.exception(
                "Error while %s on host %s:%d, session %s, when executing %s",
                action,
                host.host,
                host.port,
                sessionId,
                f,
            )

    def replicate(self, sessionId, f, action, collect=None, iterable=None, **kwargs):
        """
        Replicates the given function call on each of the underlying drop managers
        """
        thrExs = {}
        iterable = iterable or self._dmHosts
        logger.debug("Replicating command: %s on hosts: %s", f, iterable)
        self._tp.map(
            functools.partial(
                self._do_in_host, action, sessionId, thrExs, f, collect,
                **kwargs
            ),
            iterable,
        )
        if thrExs:
            msg = f"ERRROR(s) occurred while {action} for session {sessionId}"
            raise SubManagerException(msg, thrExs)

    #
    # Commands and their per-underlying-drop-manager functions
    #
    def _createSession(self, dm, host, sessionId):
        dm.createSession(sessionId)
        logger.debug("Successfully created session %s on %s", sessionId, host)

    def createSession(self, sessionId):
        """
        Creates a session in all underlying DMs.
        """
        logger.info("Creating Session %s in all hosts", sessionId)
        self.replicate(sessionId, self._createSession, "creating sessions")
        logger.info("Successfully created session %s in all hosts", sessionId)
        self._sessionIds.append(sessionId)

    def _cancelSession(self, dm, host, sessionId):
        dm.cancelSession(sessionId)
        logger.debug("Successfully cancelled session %s on %s", sessionId, host)

    def cancelSession(self, sessionId):
        """
        Cancels a session in all underlying DMs.
        """
        logger.info("Cancelled session %s in all hosts", sessionId)
        self.replicate(sessionId, self._cancelSession, "cancelling sessions")

    def _destroySession(self, dm, host, sessionId):
        dm.destroySession(sessionId)
        logger.debug("Successfully destroyed session %s on %s", sessionId, host)

    def destroySession(self, sessionId):
        """
        Destroy a session in all underlying DMs.
        """
        logger.info("Destroying Session %s in all hosts", sessionId)
        self.replicate(sessionId, self._destroySession, "creating sessions")
        self._sessionIds.remove(sessionId)

    def _add_node_subscriptions(self, dm, host_and_subscriptions, sessionId):
        host, subscriptions = host_and_subscriptions
        dm.add_node_subscriptions(sessionId, subscriptions)
        logger.debug(
            "Successfully added relationship info to session %s on %s",
            sessionId,
            host,
        )

    @staticmethod
    def _dump_graph_to_file(sessionId: str, graphSpec: dict):
        """
        Store the partitioned Physcial Graph in the DALiuGE working directory. The graph
        is associated with the session in which the graph will be run.

        This directory will be present on the compute node that either the MasterManager
        or DataIslandManager is running, depending on the run-time setup.

        :param: sessionId, the Id of the session in which the graph is run
        :param: graphSpec, the JSON-representation of drops that form the
        complete physical graph.
        """
        session_dir = Path(getDlgWorkDir()) / sessionId
        # Make directory in case CompositeManagers are using different working directory
        # to NodeManagers.
        Path.mkdir(session_dir, exist_ok=True)
        graph_path = session_dir / f"{sessionId}.graph"

        try:
            with graph_path.open("w") as fp:
                json.dump(graphSpec, fp, indent=2)
            logger.debug("Graph saved at %s", graph_path)
        except NotADirectoryError:
            logger.error("Session directory %s does not exist", graph_path.parent)

    def _addGraphSpec(self, dm, host_and_graphspec, sessionId):
        host, graphSpec = host_and_graphspec
        dm.addGraphSpec(sessionId, graphSpec)
        logger.info("Successfully appended graph to session %s on %s", sessionId, host)

    def addGraphSpec(self, sessionId, graphSpec):
        # The first step is to break down the graph into smaller graphs that
        # belong to the same host, so we can submit that graph into the individual
        # DMs. For this we need to make sure that our graph has a the correct
        # attribute set
        if self._dump_graphs:
            self._dump_graph_to_file(sessionId, graphSpec)

        logger.info(
            "Separating graph using partition attribute '%s'",
            self._partitionAttr,
        )
        perPartition = collections.defaultdict(list)
        if "rmode" in graphSpec[-1]:
            init_pg_repro_data(graphSpec)
            self._graph["reprodata"] = graphSpec.pop()
            logger.debug(
                "Composite manager found reprodata in dropspecList, rmode=%s",
                self._graph["reprodata"]["rmode"],
            )
        if graphSpec[-1] == {}:
            graphSpec.pop()
        for dropSpec in graphSpec:
            if self._partitionAttr not in dropSpec:
                msg = (
                    f"Drop {dropSpec.get('oid', None)} doesn't specify a {self._partitionAttr} "
                    f"attribute"
                )
                raise InvalidGraphException(msg)

            partition = Node(dropSpec[self._partitionAttr])
            if partition not in self._dmHosts:
                msg = (
                    f"Drop {dropSpec.get('oid', None)}'s {self._partitionAttr} {partition} "
                    f"does not belong to this DM"
                )
                raise InvalidGraphException(msg)

            perPartition[partition].append(dropSpec)

            # Add the drop specs to our graph
            self._graph[uid_for_drop(dropSpec)] = dropSpec
        # At each partition the relationships between DROPs should be local at the
        # moment of submitting the graph; thus we record the inter-partition
        # relationships separately and remove them from the original graph spec
        logger.info("Graph split into %r", perPartition.keys())
        inter_partition_rels = []
        for dropSpecs in perPartition.values():
            inter_partition_rels += graph_loader.removeUnmetRelationships(dropSpecs)
        sanitize_relations(inter_partition_rels, self._graph)
        logger.info(
            "Removed (and sanitized) %d inter-dm relationships",
            len(inter_partition_rels),
        )

        # Store the inter-partition relationships; later on they have to be
        # communicated to the NMs so they can establish them as needed.
        drop_rels = collections.defaultdict(
            functools.partial(collections.defaultdict, list)
        )
        for rel in inter_partition_rels:
            # rhn = self._graph[rel.rhs]["node"].split(":")[0]
            # lhn = self._graph[rel.lhs]["node"].split(":")[0]
            rhn = self._graph[rel.rhs]["node"]
            lhn = self._graph[rel.lhs]["node"]
            drop_rels[lhn][rhn].append(rel)
            drop_rels[rhn][lhn].append(rel)

        self._drop_rels[sessionId] = drop_rels
        logger.debug("Calculated NM-level drop relationships: %r", drop_rels)

        # Create the individual graphs on each DM now that they are correctly
        # separated.
        logger.info("Adding individual graphSpec of session %s to each DM", sessionId)
        for partition in perPartition:
            if self._graph.get("reprodata") is not None:
                perPartition[partition].append(self._graph["reprodata"])
        self.replicate(
            sessionId,
            self._addGraphSpec,
            "appending graphSpec to individual DMs",
            iterable=perPartition.items(),
        )
        logger.info(
            "Successfully added individual graphSpec of session %s to each DM",
            sessionId,
        )

    def _deploySession(self, dm, host, sessionId):
        dm.deploySession(sessionId)
        logger.debug("Successfully deployed session %s on %s", sessionId, host)

    def _triggerDrops(self, dm, host_and_uids, sessionId):
        host, uids = host_and_uids

        dm.trigger_drops(sessionId, uids)
        logger.info(
            "Successfully triggered drops for session %s on %s",
            sessionId,
            host,
        )

    def deploySession(self, sessionId, completedDrops: list[str] = None):
        # Indicate the node managers that they have to subscribe to events
        # published by some nodes
        if self._drop_rels.get(sessionId, None):
            self.replicate(
                sessionId,
                self._add_node_subscriptions,
                "adding relationship information",
                iterable=self._drop_rels[sessionId].items(),
            )
            logger.info("Delivered node subscription list to node managers")
            logger.debug(
                "Number of subscriptions: %s",
                len(self._drop_rels[sessionId].items()),
            )

        logger.info("Deploying Session %s in all hosts", sessionId)
        self.replicate(sessionId, self._deploySession, "deploying session")
        logger.info("Successfully deployed session %s in all hosts", sessionId)

        # Now that everything is wired up we move the requested DROPs to COMPLETED
        # (instead of doing it at the DM-level deployment time, in which case
        # we would certainly miss most of the events)
        if completedDrops:
            not_found = set(completedDrops) - set(self._graph)
            if not_found:
                raise DaliugeException(
                    f"UIDs for completed drops not found: {str(not_found)}"
                )
            logger.info(
                "Moving graph root Drops to COMPLETED right away: %s",
                completedDrops,
            )
            completed_by_host = group_by_node(completedDrops, self._graph)
            self.replicate(
                sessionId,
                self._triggerDrops,
                "triggering drops",
                iterable=completed_by_host.items(),
            )
            logger.info("Successfully triggered drops")

    def _getGraphStatus(self, dm, host, sessionId):
        logger.info("Getting graph status from %s", host)
        return dm.getGraphStatus(sessionId)

    def getGraphStatus(self, sessionId):
        allStatus = {}
        self.replicate(
            sessionId,
            self._getGraphStatus,
            "getting graph status",
            collect=allStatus,
        )
        return allStatus

    def getDropStatus(self, sessionId, dropId):
        allstatus = {}
        self.replicate(
            sessionId,
            # {"session": sessionId, "drop": dropId},
            self._getDropStatus,
            "getting graph status",
            collect=allstatus,
            dropId=dropId
        )
        return allstatus

    def _getSessionDir(self, dm, host, sessionId):
        logger.debug("Retrieving directory for session %s on %s", sessionId, host)
        return dm.getSessionDir(sessionId)

    def getSessionDir(self, sessionId):
        """
        Get the session directory for the DMs.
        """
        logger.info("Get the directory the in which session data is stored")
        session_dir = {}
        self.replicate(
            sessionId,
            self._getSessionDir,
            "Getting logs",
            collect=session_dir)
        return session_dir

    def _getDropStatus(self, dm, host, sessionId, dropId ):
        """
        See session.getDropLogs()

        :param dm:
        :param host:
        :param sessionId:
        :param dropId:
        :return: JSON of status logs and DROP information
        """
        logger.info("Getting drop status from %s", host)
        return dm.getDropStatus(sessionId, dropId)

    def _getGraph(self, dm, host, sessionId):
        logger.info("Getting graph from %s", host)
        return dm.getGraph(sessionId)

    def getGraph(self, sessionId):
        allGraphs = {}
        self.replicate(
            sessionId, self._getGraph, "getting the graph", collect=allGraphs
        )

        # The graphs coming from the DMs are not interconnected, we need to
        # add the missing connections to the graph befor/ returninr upstream
        rels = set(
            [
                z
                for x in self._drop_rels[sessionId].values()
                for y in x.values()
                for z in y
            ]
        )
        for rel in rels:
            graph_loader.addLink(rel.rel, allGraphs[rel.rhs], rel.lhs)

        return allGraphs

    def _getSessionStatus(self, dm, host, sessionId):
        return {str(host): dm.getSessionStatus(sessionId)}

    def getSessionStatus(self, sessionId):
        allStatus = {}
        self.replicate(
            sessionId,
            self._getSessionStatus,
            "getting the graph status",
            collect=allStatus,
        )
        return allStatus

    def _getGraphSize(self, dm, host, sessionId):
        logger.info("Getting graph size from %s", host)
        return dm.getGraphSize(sessionId)

    def getGraphSize(self, sessionId):
        allCounts = []
        self.replicate(
            sessionId,
            self._getGraphSize,
            "getting the graph size",
            collect=allCounts,
        )
        return sum(allCounts)

    def getGraphReproData(self, sessionId):
        raise NotImplementedError

    def getSessionReproStatus(self, sessionId):
        raise NotImplementedError


class DataIslandManager(CompositeManager):
    """
    The DataIslandManager, which manages a number of NodeManagers.
    """

    def __init__(
        self,
        dmHosts: list[str] = None,
        pkeyPath=None,
        dmCheckTimeout=10,
        dump_graphs=False,
    ):
        super(DataIslandManager, self).__init__(
            NODE_DEFAULT_REST_PORT,
            "node",
            "nm",  # Node manager
            dmHosts=dmHosts,
            pkeyPath=pkeyPath,
            dmCheckTimeout=dmCheckTimeout,
            dump_graphs=dump_graphs,
        )

        # In the case of the Data Island the dmHosts are the final nodes as well
        self.use_dmHosts = True
        # self._nodes = dmHosts
        logger.info("Created DataIslandManager for hosts: %r", self._dmHosts)

    def getGraphReproData(self, sessionId):
        raise NotImplementedError

    def getSessionReproStatus(self, sessionId):
        raise NotImplementedError

class MasterManager(CompositeManager):
    """
    The MasterManager, which manages a number of DataIslandManagers.
    """

    def __init__(
        self,
        dmHosts: list[str] = None,
        pkeyPath=None,
        dmCheckTimeout=10,
        dump_graphs=False,
    ):
        super(MasterManager, self).__init__(
            ISLAND_DEFAULT_REST_PORT,
            "island",
            "dim",
            dmHosts=dmHosts,
            pkeyPath=pkeyPath,
            dmCheckTimeout=dmCheckTimeout,
            dump_graphs=dump_graphs,
            hosts_are_dim=True,
        )
        logger.info("Created MasterManager for DIM hosts: %r", self._dmHosts)

    def getGraphReproData(self, sessionId):
        raise NotImplementedError

    def getSessionReproStatus(self, sessionId):
        raise NotImplementedError
