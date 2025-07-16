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

import copy
import importlib.resources
import logging
import os
import platform
import time
from collections import defaultdict

import networkx as nx
import numpy as np
from pyswarm import pso

from .utils.antichains import get_max_weighted_antichain
from ..common import dropdict, get_roots, CategoryType

logger = logging.getLogger(f"dlg.{__name__}")

DEBUG = 0


class SchedulerException(Exception):
    pass


class Schedule(object):
    """
    The scheduling solution with schedule-related properties
    """

    def __init__(self, dag, max_dop):
        self._dag = dag
        self._max_dop = max_dop if type(max_dop) == int else max_dop.get("num_cpus", 1)
        DAGUtil.label_schedule(self._dag)
        self._lpl = DAGUtil.get_longest_path(
            self._dag, default_weight=0, show_path=True
        )
        self._wkl = None
        self._sma = None

    @property
    def dag(self):
        return self._dag

    @property
    def makespan(self):
        return self._lpl[1]

    @property
    def longest_path(self):
        return self._lpl[0]

    @property
    def schedule_matrix(self):
        """
        Return: a self._lpl x self._max_dop matrix
                (X - time, Y - resource unit / parallel lane)
        """
        if self._sma is not None:
            return self._sma
        else:
            N = max(self.makespan, 1)
            if DEBUG:
                lpl_str = []
                lpl_c = 0
                for lpn in self.longest_path:
                    ww = self._dag.nodes[lpn].get("num_cpus", 0)
                    lpl_str.append("{0}({1})".format(lpn, ww))
                    lpl_c += ww
                logger.debug("lpl: %s", " -> ".join(lpl_str))
                logger.debug("lplt = %d", int(lpl_c))

            # print("N (makespan) is ", N, "M is ", M)
            ma = np.zeros((self._max_dop, N), dtype=int)
            pr = np.zeros((self._max_dop), dtype=int)
            last_pid = -1
            prev_n = None

            for n in nx.topological_sort(self._dag):
                node = self._dag.nodes[n]
                try:
                    stt = node["stt"]
                    edt = node["edt"]
                except KeyError as ke:
                    raise SchedulerException(
                        "No schedule labels found: {0}".format(str(ke))
                    ) from ke
                if edt == stt:
                    continue
                if prev_n in self._dag.predecessors(n):
                    curr_pid = last_pid
                else:
                    found = None
                    for i in range(self._max_dop):
                        if pr[i] <= stt:
                            found = i
                            break
                    if found is None:
                        raise SchedulerException(
                            "Cannot find a idle PID, max_dop provided: {0}, actual max_dop: {1}\n Graph: {2}".format(
                                self._max_dop,
                                "DAGUtil.get_max_dop(G)",
                                self._dag.nodes(data=True),
                            )
                        )
                        # DAGUtil.get_max_dop(G), G.nodes(data=True)))
                    curr_pid = found
                ma[curr_pid, stt:edt] = n
                pr[curr_pid] = edt
                last_pid = curr_pid
                prev_n = n
            self._sma = ma
            return self._sma

    @property
    def workload(self):
        """
        Return: (integer)
            the mean # of resource units per time unit consumed by the graph/partition
        """
        if self._wkl is None:
            c = []
            for i in range(self.schedule_matrix.shape[1]):
                c.append(np.count_nonzero(self.schedule_matrix[:, i]))
            self._wkl = int(np.mean(np.array(c)))  # since METIS only accepts integer
        return self._wkl

    @property
    def efficiency(self):
        """
        resource usage percentage (integer)
        """
        return int(float(self.workload) / self._max_dop * 100)


class Partition(object):
    """
    Logical partition, multiple (1 ~ N) of these can be placed onto a single
    physical resource unit

    Logical partition can be nested, and it somewhat resembles the `dlg.manager.drop_manager`
    """

    def __init__(self, gid, max_dop):
        """
        gid:        cluster/partition id (string)
        max_dop:    maximum allowed degree of parallelism in this partition (int)
        """
        self._gid = gid
        self._dag = nx.DiGraph()
        self._ask_max_dop = max_dop
        self._max_antichains = None  # a list of max (width) antichains
        self._lpl = None
        self._schedule = None
        self._max_dop = None
        self._parent_id = None
        self._child_parts = None
        self._tmp_merge_dag = None
        self._tmp_new_ac = None

    @property
    def parent_id(self):
        return self._parent_id

    @parent_id.setter
    def parent_id(self, value):
        self._parent_id = value

    @property
    def partition_id(self):
        return self._gid

    @partition_id.setter
    def partition_id(self, gid):
        self._gid = gid

    @property
    def schedule(self):
        """
        Get the schedule assocaited with this partition
        """
        if self._schedule is None:
            self._schedule = Schedule(self._dag, self._max_dop)
        return self._schedule

    @property
    def max_dop(self):
        return self._max_dop

    @property
    def dag(self):
        return self._dag

    def recompute_schedule(self):
        self._schedule = None
        return self.schedule

    def can_merge(self, that):
        if self._max_dop + that.max_dop <= self._ask_max_dop:
            return True
        else:
            return False

    def merge(self, that):
        if self._tmp_merge_dag is not None:
            self._dag = self._tmp_merge_dag
            self._tmp_merge_dag = None
        else:
            self._dag = nx.compose(self._dag, that.dag)

        # self._max_dop

        # TODO add this performance hog!
        # self._max_antichains = None

    def can_add(self, u, v, gu, gv):
        """
        Check if nodes u and/or v can join this partition
        A node may be rejected due to reasons such as: DoP overflow or
        completion time deadline overdue, etc.
        """
        uw = gu["weight"]
        vw = gv["weight"]
        if len(self._dag.nodes()) == 0:
            return (True, False, False)

        unew = u not in self._dag.nodes
        vnew = v not in self._dag.nodes

        if DEBUG:
            slow_max = DAGUtil.get_max_antichains(self._dag)
            info = "Before: {0} - slow max: {1}, fast max: {2}, u: {3}, v: {4}, unew:{5}, vnew:{6}".format(
                self._dag.edges(), slow_max, self._max_antichains, u, v, unew, vnew
            )
            logger.debug(info)
            if len(slow_max) != len(self._max_antichains):
                raise SchedulerException("ERROR - {0}".format(info))

        self._dag.add_node(u, weight=uw)
        self._dag.add_node(v, weight=vw)
        self._dag.add_edge(u, v)

        if unew and vnew:
            mydop = DAGUtil.get_max_dop(self._dag)
        else:
            mydop = self.probe_max_dop(u, v, unew, vnew)
            # TODO - put the following code in a unit test!
            if DEBUG:
                mydop_slow = DAGUtil.get_max_dop(self._dag)  #
                if mydop_slow != mydop:
                    err_msg = "u = {0}, v = {1}, unew = {2}, vnew = {3}".format(
                        u, v, unew, vnew
                    )
                    raise SchedulerException(
                        "{2}: mydop = {0}, mydop_slow = {1}".format(
                            mydop, mydop_slow, err_msg
                        )
                    )
        ret = False if mydop > self._ask_max_dop.get("num_cpus", 1) else True
        if unew:
            self.remove(u)
        if vnew:
            self.remove(v)
        return (ret, unew, vnew)

    def add(self, u, v, gu, gv, sequential=False, global_dag=None):
        """
        Add nodes u and/or v to the partition
        if sequential is True, break antichains to sequential chains
        """
        # if (self.partition_id == 180):
        #     logger.debug("u = ", u, ", v = ", v, ", partition = ", self.partition_id)
        uw = gu["weight"]
        vw = gv["weight"]
        unew = u not in self._dag.nodes
        vnew = v not in self._dag.nodes
        self._dag.add_node(u, weight=uw, num_cpus=gu["num_cpus"])
        self._dag.add_node(v, weight=vw, num_cpus=gv["num_cpus"])
        self._dag.add_edge(u, v)

        if unew and vnew:  # we know this is fast
            self._max_antichains = DAGUtil.get_max_antichains(self._dag)
            self._max_dop = 1
        else:
            if sequential and (global_dag is not None):
                # break potential antichain to sequential chain
                if unew:
                    v_ups = nx.ancestors(self._dag, v)
                    for vup in v_ups:
                        if u == vup:
                            continue
                        if len(list(self._dag.predecessors(vup))) == 0:
                            # link u to "root" parent of v to break antichain
                            self._dag.add_edge(u, vup)
                            # change the original global graph
                            global_dag.add_edge(u, vup, weight=0)
                            if not nx.is_directed_acyclic_graph(global_dag):
                                global_dag.remove_edge(u, vup)
                else:
                    u_downs = nx.descendants(self._dag, u)
                    for udo in u_downs:
                        if udo == v:
                            continue
                        if len(list(self._dag.successors(udo))) == 0:
                            # link "leaf" children of u to v to break antichain
                            self._dag.add_edge(udo, v)
                            # change the original global graph
                            global_dag.add_edge(udo, v, weight=0)
                            if not nx.is_directed_acyclic_graph(global_dag):
                                global_dag.remove_edge(udo, v)

            self._max_dop = self.probe_max_dop(u, v, unew, vnew, update=True)
            # self._max_dop = DAGUtil.get_max_dop(self._dag)# this is too slow!

    def remove(self, n):
        """
        Remove node n from the partition
        """
        self._dag.remove_node(n)

    def add_node(self, u, weight):
        """
        Add a single node u to the partition
        """
        self._dag.add_node(u, weight=weight)
        self._max_dop = 1

    def probe_max_dop(self, u, v, unew, vnew, update=False):
        """
        An incremental antichain (which appears significantly more efficient than the networkx antichains)
        But only works for DoP, not for weighted width
        """
        if self._max_antichains is None:
            new_ac = DAGUtil.get_max_antichains(self._dag)
            if update:
                self._max_antichains = new_ac
            if len(new_ac) == 0:
                if update:
                    self._max_antichains = None
                return 0
            else:
                return len(new_ac[0])
        else:
            if update and self._tmp_new_ac is not None:
                self._max_antichains, md = self._tmp_new_ac
                self._tmp_new_ac = None
                return md

            if unew:
                ups = nx.descendants(self._dag, u)
                new_node = u
            elif vnew:
                ups = nx.ancestors(self._dag, v)
                new_node = v
            else:
                raise SchedulerException("u v are both new/old")
            new_ac = []
            md = 1
            for (
                ma
            ) in (
                self._max_antichains
            ):  # missing elements in the current max_antichains!
                # incremental updates
                found = False
                for n in ma:
                    if n in ups:
                        found = True
                        break
                if not found:
                    mma = list(ma)
                    mma.append(new_node)
                    new_ac.append(mma)
                    if len(mma) > md:
                        md = len(mma)
                elif len(ma) > md:
                    md = len(ma)
                new_ac.append(ma)  # carry over, then prune it
            if len(new_ac) > 0:
                self._tmp_new_ac = (new_ac, md)
                if update:
                    self._max_antichains = new_ac
                return md
            else:
                raise SchedulerException("No antichains")

    @property
    def cardinality(self):
        return len(self._dag.nodes())


class KFamilyPartition(Partition):
    """
    A special case (K = 1) of the Maximum Weighted K-families based on
    the Theorem 3.1 in
    http://fmdb.cs.ucla.edu/Treports/930014.pdf
    """

    def __init__(self, gid, max_dop, global_dag=None):
        """
        max_dop:    dict with key:   resource_attributes (string)
                              value: resource_capacity (integer)
        """
        mtype = type(max_dop)
        if mtype == int:
            # backward compatible
            max_dop = {"num_cpus": max_dop}
        elif mtype == dict:
            pass
        else:
            raise SchedulerException("Invalid max_dop type: %r" % mtype)

        super(KFamilyPartition, self).__init__(gid, max_dop)
        self._bpg = nx.DiGraph()
        self._global_dag = global_dag
        self._check_global_dag = global_dag is not None
        self._w_attr = max_dop.keys()
        self._tc = defaultdict(set)
        self.tmp_max_dop = {}

    def add_node(self, u): #pylint: disable=arguments-differ
        """
        Add a single node u to the partition
        """
        kwargs = {
            _w_attr: self._global_dag.nodes[u].get(_w_attr, 1)
            for _w_attr in self._w_attr
        }
        kwargs["weight"] = self._global_dag.nodes[u].get("weight", 5)
        self._dag.add_node(u, **kwargs)
        for k in self._w_attr:
            self.tmp_max_dop[k] = get_max_weighted_antichain(self._dag, w_attr=k)[0]
        self._max_dop = self.tmp_max_dop

    def can_merge(self, that, u, v): #pylint: disable=arguments-differ
        """"""
        dag = nx.compose(self._dag, that.dag)
        if u is not None:
            dag.add_edge(u, v)
        tmp_max_dop = copy.deepcopy(self.tmp_max_dop)

        for _w_attr in self._w_attr:
            ask_max_dop = (
                self._ask_max_dop[_w_attr]
                if self._ask_max_dop[_w_attr] is not None
                else 1
            )
            mydop = get_max_weighted_antichain(dag, w_attr=_w_attr)[0]
            curr_max = max(ask_max_dop, that.max_dop[_w_attr])

            if mydop <= curr_max:
                # if you don't increase DoP, we accept that immediately
                tmp_max_dop[_w_attr] = curr_max
            elif mydop > ask_max_dop:
                return False
            else:
                tmp_max_dop[_w_attr] = mydop

        self.tmp_max_dop = tmp_max_dop  # only change it when returning True
        return True

    def merge(self, that, u, v): #pylint: disable=arguments-differ
        self._dag = nx.compose(self._dag, that.dag)
        if u is not None:
            self._dag.add_edge(u, v)
        if self.tmp_max_dop is not None:
            self._max_dop = self.tmp_max_dop
            # print("Gid %d just merged with DoP %d" % (self._gid, self._tmp_max_dop))
        else:
            # we could recalcuate it again, but we are lazy!
            raise SchedulerException("can_merge was not probed before add()")


class Scheduler(object):
    """
    Static Scheduling consists of three steps:
    1. partition the DAG into an optimal number (M) of partitions
    goal - minimising execution time while maintaining intra-partition DoP
    2. merge partitions into a given number (N) of partitions (if M > N)
    goal - minimise logical communication cost while maintaining load balancing
    3. map each merged partition to a resource unit
    goal - minimise physical communication cost amongst resource units
    """

    def __init__(self, drop_list, max_dop=8, dag=None):
        """
        turn drop_list into DAG, and check its validity
        """
        self._drop_list = drop_list
        if dag is None:
            self._dag = DAGUtil.build_dag_from_drops(self._drop_list)
        else:
            self._dag = dag
        self._max_dop = max_dop
        self._parts = []  # partitions
        self._part_dict = dict()  # {gid : part}
        self._part_edges = []  # edges amongst all partitions

    def partition_dag(self):
        raise SchedulerException("Not implemented. Try subclass instead")

    def merge_partitions(self, num_partitions, bal_cond=1):
        """
        Merge M partitions into N partitions where N < M
            implemented using METIS for now

        bal_cond:  load balance condition (integer):
                    0 - workload,
                    1 - CPU count (faster to evaluate than workload)
        """
        # 1. build the bi-directional graph (each partition is a node)
        metis = DAGUtil.import_metis()
        G = nx.Graph()
        st_gid = len(self._drop_list) + len(self._parts) + 1
        if bal_cond == 0:
            G.graph["node_weight_attr"] = ["wkl", "eff"]
            for part in self._parts:
                sc = part.schedule
                G.add_node(part.partition_id, wkl=sc.workload, eff=sc.efficiency)
        else:
            G.graph["node_weight_attr"] = "cc"
            for part in self._parts:
                # sc = part.schedule
                pdop = part.max_dop
                # TODO add memory as one of the LB condition too
                cc_eval = pdop if type(pdop) == int else pdop.get("num_cpus", 1)
                G.add_node(part.partition_id, cc=cc_eval)

        for e in self._part_edges:
            u = e[0]
            v = e[1]
            ugid = self._dag.nodes[u].get("gid", None)
            vgid = self._dag.nodes[v].get("gid", None)
            G.add_edge(ugid, vgid)  # repeating is fine
            ew = self._dag.adj[u][v]["weight"]
            try:
                G[ugid][vgid]["weight"] += ew
            except KeyError:
                G[ugid][vgid]["weight"] = ew
        # since METIS does not allow zero edge weight, reset them to one
        for e in G.edges(data=True):
            if e[2]["weight"] == 0:
                e[2]["weight"] = 1
        # logger.debug(G.nodes(data=True))
        (edgecuts, metis_parts) = metis.part_graph(G, nparts=num_partitions, ufactor=1)

        for node, pt in zip(G.nodes(), metis_parts):  # note min(pt) == 0
            parent_id = pt + st_gid
            child_part = self._part_dict[node]
            child_part.parent_id = parent_id
            # logger.debug("Part {0} --> Cluster {1}".format(child_part.partition_id, parent_id))
            # parent_part = Partition(parent_id, None)
            # self._parts.append(parent_part)
        # logger.debug("Edgecuts of merged partitions: ", edgecuts)
        return edgecuts

    def map_partitions(self):
        """
        map logical partitions to physical resources
        """


class MySarkarScheduler(Scheduler):
    """
    Based on "V. Sarkar, Partitioning and Scheduling Parallel Programs for Execution on
    Multiprocessors. Cambridge, MA: MIT Press, 1989."

    Main change
    We do not order independent tasks within the same cluster. This could blow the cluster, therefore
    we allow for a cost constraint on the number of concurrent tasks (e.g. # of cores) within each cluster

    Why
    1. we only need to topologically sort the DAG once since we do not add new edges in the cluster
    2. closer to requirements
    3. adjustable for local schedulers

    Similar ideas:
    http://stackoverflow.com/questions/3974731
    """

    def __init__(self, drop_list, max_dop=8, dag=None, dump_progress=False):
        super(MySarkarScheduler, self).__init__(drop_list, max_dop=max_dop, dag=dag)
        self._sspace = [3] * len(self._dag.edges())  # all edges are not zeroed
        self._dump_progress = dump_progress

    def override_cannot_add(self):
        """
        Whether this scheduler will override the False result from `Partition.can_add()`
        """
        return False

    def _merge_two_parts(self, ugid, vgid, u, v, gu, gv, g_dict, parts, G):
        """
        Merge two parts associated with u and v respectively

        Return: None if these two parts cannot be merged
                due to reasons such as DoP overflow
                A ``Part`` instance
        """
        # get the new part should we go ahead
        # the new part should be one  of partu or partv
        print("\nMerging ugid %s and vgid %s, u %s and v %s" % (ugid, vgid, u, v))
        logger.info("Parts include %s %s", gu, gv)
        l_gid = min(ugid, vgid)
        r_gid = max(ugid, vgid)
        part_new = g_dict[l_gid]
        part_removed = g_dict[r_gid]

        if not part_new.can_merge(part_removed, u, v):
            return None

        part_new.merge(part_removed, u, v)

        # Get hold of all gnodes that belong to "part_removed"
        # and re-assign them to the new partitions
        for n in part_removed.dag.nodes():
            G.nodes[n]["gid"] = l_gid

        index = None
        for i, part in enumerate(parts):
            p_gid = part.partition_id
            if p_gid > r_gid:
                g_dict[p_gid - 1] = part
                part.partition_id -= 1
                for n in part.dag.nodes():
                    G.nodes[n]["gid"] = part.partition_id
            elif p_gid == r_gid:
                # index = len(parts) - i - 1
                index = i
                del g_dict[p_gid]

        if index is None:
            raise SchedulerException("Failed to find r_gid")
        parts[:] = parts[:index] + parts[index + 1 :]

        return part_new

    def reduce_partitions(self, parts, g_dict, G):
        """
        further reduce the number of partitions by merging partitions whose max_dop
        is less than capacity

        step 1 - sort partition list based on their
                 _max_dop of num_cpus as default
        step 2 - enumerate each partition p to see merging
                 between p and its neighbour is feasible
        """
        done_reduction = False
        num_reductions = 0
        # TODO consider other w_attrs other than CPUs!
        parts.sort(key=lambda x: x.max_dop["num_cpus"])
        while not done_reduction:
            for i, partA in enumerate(parts):
                if i < len(parts) - 1:
                    partB = parts[i + 1]
                    new_part = self._merge_two_parts(
                        partA.partition_id,
                        partB.partition_id,
                        None,
                        None,
                        None,
                        None,
                        g_dict,
                        parts,
                        G,
                    )
                    if new_part is not None:
                        num_reductions += 1
                        break  # force re-sorting
                else:
                    done_reduction = True
                    logger.info("Performed reductions %d times", num_reductions)
                    break

    def partition_dag(self):
        """
        Return a tuple of
            1. the # of partitions formed (int)
            2. the parallel time (longest path, int)
            3. partition time (seconds, float)
        """
        st_gid = len(self._drop_list) + 1
        init_c = st_gid
        stt = time.time()
        topo_sorted = nx.topological_sort(self._dag)
        curr_lpl = None
        parts = []
        plots_data = []

        for n in self._dag.nodes(data=True):
            n[1]["gid"] = st_gid
            part = KFamilyPartition(st_gid, self._max_dop, global_dag=self._dag)
            part.add_node(n[0])
            self._part_dict[st_gid] = part
            parts.append(part)  # will it get rejected?
            st_gid += 1

        for i, e in enumerate(
            sorted(self._dag.edges(data=True), key=lambda ed: ed[2]["weight"] * -1)
        ):
            u = e[0]
            v = e[1]
            gu = self._dag.nodes[u]
            gv = self._dag.nodes[v]
            ow = self._dag.adj[u][v]["weight"]
            self._dag.adj[u][v]["weight"] = 0  # edge zeroing
            ugid = gu.get("gid", None)
            vgid = gv.get("gid", None)
            if ugid != vgid:  # merge existing parts
                part = self._merge_two_parts(
                    ugid, vgid, u, v, gu, gv, self._part_dict, parts, self._dag
                )
                if part is not None:
                    st_gid -= 1
                    self._sspace[i] = 1
                else:
                    self._dag.adj[u][v]["weight"] = ow
                    self._part_edges.append(e)
            if self._dump_progress:
                bb = np.median([pp.tmp_max_dop for pp in parts])
                _, curr_lpl = DAGUtil.get_longest_path(
                    self._dag, show_path=False, topo_sort=topo_sorted
                )
                plots_data.append("%d,%d,%d" % (curr_lpl, len(parts), bb))
        self.reduce_partitions(parts, self._part_dict, self._dag)
        edt = time.time() - stt
        self._parts = parts
        if self._dump_progress:
            with open("/tmp/%.3f_lpl_parts.csv" % time.time(), "w") as of:
                of.writelines(os.linesep.join(plots_data))
        if curr_lpl is None:
            _, curr_lpl = DAGUtil.get_longest_path(
                self._dag, show_path=False, topo_sort=topo_sorted
            )
        return (st_gid - init_c), curr_lpl, edt, parts


class MinNumPartsScheduler(MySarkarScheduler):
    """
    A special type of partition that aims to schedule the DAG on time but at minimum cost.
    In this particular case, the cost is the number of partitions that will be generated.
    The assumption is # of partitions (with certain DoP) more or less represents resource footprint.
    """

    def __init__(self, drop_list, deadline, max_dop=8, dag=None, optimistic_factor=0.5):
        super(MinNumPartsScheduler, self).__init__(drop_list, max_dop=max_dop, dag=dag)
        self._deadline = deadline
        self._optimistic_factor = optimistic_factor

    def override_cannot_add(self):
        return True


class PSOScheduler(Scheduler):
    """
    Use the Particle Swarm Optimisation to guide the Sarkar algorithm
    https://en.wikipedia.org/wiki/Particle_swarm_optimization

    The idea is to let "edgezeroing" becomes the search variable X
    The number of dimensions of X is the number of edges in DAG
    Possible values for each dimension is a discrete set {1, 2, 3}
    where:
    * 10 - no zero (2 in base10) + 1
    * 00 - zero w/o linearisation (0 in base10) + 1
    * 01 - zero with linearisation (1 in base10) + 1

    if (deadline is present):
        the objective function sets up a partition scheme such that
            (1) DoP constrints for each partiiton are satisfied
                based on X[i] value, reject or linearisation
            (2) returns num_of_partitions

        constrain function:
            1. makespan < deadline
    else:
        the objective function sets up a partition scheme such that
            (1) DoP constrints for each partiiton are satisfied
                based on X[i] value, reject or linearisation
            (2) returns makespan
    """

    def __init__(
        self,
        drop_list,
        max_dop=8,
        dag=None,
        deadline=None,
        topk=30,
        swarm_size=40,
    ):
        super(PSOScheduler, self).__init__(drop_list, max_dop=max_dop, dag=dag)
        self._deadline = deadline
        # search space: key - combination of X[i] (string),
        # val - a tuple of (critical_path (int), num_parts (int))
        self._sspace_dict = dict()
        self._topk = topk
        self._swarm_size = swarm_size if swarm_size is not None else 40
        self._lite_dag = DAGUtil.build_dag_from_drops(self._drop_list, embed_drop=False)
        self._call_counts = 0
        leng = len(self._lite_dag.edges())
        self._leng = leng
        self._topk = leng if self._topk is None or leng < self._topk else self._topk

    def partition_dag(self):
        """
        Returns a tuple of:
            1. the # of partitions formed (int)
            2. the parallel time (longest path, int)
            3. partition time (seconds, float)
            4. a list of partitions (Partition)
        """
        # trigger the PSO algorithm
        lb = [0.99] * self._leng
        ub = [3.01] * self._leng
        stt = time.time()
        if self._deadline is None:
            xopt, _ = pso(self.objective_func, lb, ub, swarmsize=self._swarm_size)
        else:
            xopt, _ = pso(
                self.objective_func,
                lb,
                ub,
                ieqcons=[self.constrain_func],
                swarmsize=self._swarm_size,
            )

        curr_lpl, num_parts, parts, g_dict = self._partition_G(self._dag, xopt)
        # curr_lpl, num_parts, parts, g_dict = self.objective_func(xopt)
        self._part_dict = g_dict
        edt = time.time()
        # print "PSO scheduler took {0} seconds".format(edt - stt)
        st_gid = len(self._drop_list) + 1 + num_parts
        for n in self._dag.nodes(data=True):
            if not "gid" in n[1]:
                n[1]["gid"] = st_gid
                part = Partition(st_gid, self._max_dop)
                part.add_node(n[0], n[1].get("weight", 1))
                g_dict[st_gid] = part
                parts.append(part)  # will it get rejected?
                num_parts += 1
        self._parts = parts
        # print "call counts ", self._call_counts
        return (num_parts, curr_lpl, edt - stt, parts)

    def _partition_G(self, G, x):
        """
        A helper function to partition G based on a given scheme x
        subject to constraints imposed by each partition's DoP
        """
        # print x
        st_gid = len(self._drop_list) + 1
        el = sorted(G.edges(data=True), key=lambda ed: ed[2]["weight"] * -1)
        g_dict = dict()
        parts = []
        for i, e in enumerate(el):
            pos = int(round(x[i]))
            if pos == 3:  # 10 non_zero + 1
                continue
            elif pos == 2:  # 01 zero with linearisation + 1
                linear = True
            elif pos == 1:  # 00 zero without linearisation + 1
                linear = False
            else:
                raise SchedulerException("PSO position out of bound: {0}".format(pos))

            u = e[0]
            gu = G.nodes[u]
            v = e[1]
            gv = G.nodes[v]
            ow = G.adj[u][v]["weight"]
            G.adj[u][v]["weight"] = 0  # edge zeroing
            recover_edge = False

            ugid = gu.get("gid", None)
            vgid = gv.get("gid", None)
            if ugid and (not vgid):
                part = g_dict[ugid]
            elif (not ugid) and vgid:
                part = g_dict[vgid]
            elif not ugid and (not vgid):
                part = Partition(st_gid, self._max_dop)
                g_dict[st_gid] = part
                parts.append(part)  # will it get rejected?
                st_gid += 1
            else:  # elif (ugid and vgid):
                # cannot change Partition once is in!
                part = None
            # uw = gu['weight']
            # vw = gv['weight']

            if part is None:
                recover_edge = True
            else:
                ca, _, _ = part.can_add(u, v, gu, gv)
                if ca:
                    # ignore linear flag, add it anyway
                    part.add(u, v, gu, gv)
                    gu["gid"] = part.partition_id
                    gv["gid"] = part.partition_id
                else:
                    if linear:
                        part.add(u, v, gu, gv, sequential=True, global_dag=G)
                        gu["gid"] = part.partition_id
                        gv["gid"] = part.partition_id
                    else:
                        recover_edge = True  # outright rejection
            if recover_edge:
                G.adj[u][v]["weight"] = ow
                self._part_edges.append(e)
        self._call_counts += 1
        # print "called {0} times, len parts = {1}".format(self._call_counts, len(parts))
        return (
            DAGUtil.get_longest_path(G, show_path=False)[1],
            len(parts),
            parts,
            g_dict,
        )

    def constrain_func(self, x):
        """
        Deadline - critical_path >= 0
        """
        if self._deadline is None:
            raise SchedulerException("Deadline is None, cannot apply constraints!")

        sk = "".join([str(int(round(xi))) for xi in x[: self._topk]])
        stuff = self._sspace_dict.get(sk, None)
        if not stuff:
            G = self._lite_dag.copy()
            stuff = self._partition_G(G, x)
            self._sspace_dict[sk] = stuff[:2]
            del G
        return self._deadline - stuff[0]

    def objective_func(self, x):
        """
        x is a list of values, each taking one of the 3 integers: 0,1,2 for an edge
        indices of x is identical to the indices in G.edges().sort(key='weight')
        """
        # first check if the solution is already available in the search space
        sk = "".join([str(int(round(xi))) for xi in x[: self._topk]])
        stuff = self._sspace_dict.get(sk, None)  # TODO is this atomic operation?
        if not stuff:
            # make a deep copy to avoid mix up multiple particles,
            # each of which has multiple iterations
            G = self._lite_dag.copy()
            stuff = self._partition_G(G, x)
            self._sspace_dict[sk] = stuff[:2]
            del G
        if self._deadline is None:
            return stuff[0]
        else:
            return stuff[1]


class DAGUtil(object):
    """
    Helper functions dealing with DAG
    """

    @staticmethod
    def get_longest_path(
        G, weight="weight", default_weight=1, show_path=True, topo_sort=None
    ):
        """
        Ported from:
        https://github.com/networkx/networkx/blob/master/networkx/algorithms/dag.py
        Added node weight

        Returns the longest path in a DAG
        If G has edges with 'weight' attribute the edge data are used as weight values.
        :param: G Graph (NetworkX DiGraph)
        :param: weight Edge data key to use for weight (string)
        :param: default_weight The weight of edges that do not have a weight attribute (integer)
        :return: a tuple with two elements: `path` (list), the longest path, and
        `path_length` (float) the length of the longest path.
        """
        dist = {}  # stores {v : (length, u)}
        if topo_sort is None:
            topo_sort = nx.topological_sort(G)
        for v in topo_sort:
            us = [
                (
                    dist[u][0]
                    + data.get(weight, default_weight)  # accumulate
                    + G.nodes[u].get(weight, 0)  # edge weight
                    + (  # u node weight
                        G.nodes[v].get(weight, 0)
                        if len(list(G.successors(v))) == 0
                        else 0
                    ),  # v node weight if no successor
                    u,
                )
                for u, data in G.pred[v].items()
            ]
            # Use the best predecessor if there is one and its distance is non-negative, otherwise terminate.
            maxu = max(us) if us else (0, v)
            dist[v] = maxu if maxu[0] >= 0 else (0, v)
        u = None
        v = max(dist, key=dist.get)
        lp = dist[v][0]
        if not show_path:
            path = None
        else:
            path = []
            while u != v:
                path.append(v)
                u = v
                v = dist[v][1]
            path.reverse()
        return (path, lp)

    @staticmethod
    def get_max_width(G, weight="weight", default_weight=1):
        """
        Get the antichain with the maximum "weighted" width of this DAG
        weight: float (for example, it could be RAM consumption in GB)
        Return : float
        """
        max_width = 0
        for antichain in nx.antichains(G):
            t = 0
            for n in antichain:
                t += G.nodes[n].get(weight, default_weight)
            if t > max_width:
                max_width = t
        return max_width

    @staticmethod
    def get_max_dop(G):
        """
        Get the maximum degree of parallelism of this DAG
        return : int
        """
        return max([len(antichain) for antichain in nx.antichains(G)])

    @staticmethod
    def get_max_antichains(G):
        """
        return a list of antichains with Top-2 lengths
        """
        return DAGUtil.prune_antichains(nx.antichains(G))

    @staticmethod
    def prune_antichains(antichains):
        """
        Prune a list of antichains to keep those with Top-2 lengths
        antichains is a Generator (not a list!)
        """
        todo = []
        for antichain in antichains:
            todo.append(antichain)
        todo.sort(key=lambda x: len(x), reverse=True) # pylint: disable=unnecessary-lambda
        return todo

    @staticmethod
    def label_schedule(G, weight="weight", topo_sort=None):
        """
        for each node, label its start and end time
        """
        if topo_sort is None:
            topo_sort = nx.topological_sort(G)
        for v in topo_sort:
            gv = G.nodes[v]
            parents = list(G.predecessors(v))
            if len(parents) == 0:
                gv["stt"] = 0
            else:
                # get the latest end time of one of its parents
                ledt = -1
                for parent in parents:
                    pedt = G.nodes[parent]["edt"] + G.adj[parent][v].get(weight, 0)
                    if pedt > ledt:
                        ledt = pedt
                gv["stt"] = ledt
            gv["edt"] = gv["stt"] + gv.get(weight, 0)

    @staticmethod
    def ganttchart_matrix(G, topo_sort=None):
        """
        Return a M (# of DROPs) by N (longest path length) matrix
        """
        lpl = DAGUtil.get_longest_path(G, show_path=True)
        # N = lpl[1] - (len(lpl[0]) - 1)
        N = lpl[1]
        M = G.number_of_nodes()
        ma = np.zeros((M, N), dtype=np.int)
        if topo_sort is None:
            topo_sort = nx.topological_sort(G)
        for i, n in enumerate(topo_sort):
            node = G.nodes[n]
            try:
                stt = node["stt"]
                edt = node["edt"]
            except KeyError as ke:
                raise SchedulerException(
                    "No schedule labels found: {0}".format(str(ke))
                ) from ke
            # print i, n, stt, edt
            leng = edt - stt
            if edt == stt:
                continue
            try:
                ma[i, stt:edt] = np.ones((1, leng))
            except:
                logger.error("i, stt, edt, leng = %d, %d, %d, %d", i, stt, edt, leng)
                logger.error("N, M = %d, %d", M, N)
                raise
            # print ma[i, :]
        return ma

    @staticmethod
    def import_metis():
        try:
            import metis as mt
        except RuntimeError:
            pl = platform.platform()
            if pl.startswith("Darwin"):  # a clumsy way
                ext = "dylib"
            else:
                ext = "so"  # what about Microsoft??!!
            os.environ["METIS_DLL"] = str(
                importlib.resources.files("dlg.dropmake") / f"lib/libmetis.{ext}"
            )
            import metis as mt
        if not hasattr(mt, "dlg_patched"):
            mt.tmp_part_graph = mt.part_graph

            def logged_part_graph(*args, **kwargs):
                logger.info("Starting metis partitioning")
                start = time.time()
                ret = mt.tmp_part_graph(*args, **kwargs)  # @UndefinedVariable
                logger.info(
                    "Finished metis partitioning in %.3f [s]",
                    time.time() - start,
                )
                return ret

            mt.part_graph = logged_part_graph
            mt.dlg_patched = True
        return mt

    @staticmethod
    def build_dag_from_drops(drop_list, embed_drop=True, fake_super_root=False):
        """
        return a networkx Digraph (DAG)
        :param: fake_super_root whether to create a fake super root node in the DAG
        If set to True, it enables edge zero-based scheduling agorithms to make
        more aggressive merging
        """
        # tw - task weight
        # dw - data weight / volume
        key_dict = dict()  # {oid : node_id}
        drop_dict = dict()  # {oid : drop}
        out_bound_keys = ["streamingConsumers", "consumers", "outputs"]
        for i, drop in enumerate(drop_list):
            oid = drop["oid"]
            key_dict[oid] = i + 1  # starting from 1
            drop_dict[oid] = drop
        G = nx.DiGraph()
        for i, drop in enumerate(drop_list):
            oid = drop["oid"]
            myk = i + 1
            tt = drop["categoryType"]
            if tt in [CategoryType.DATA, "data"]:
                tw = 0
                dtp = 0
            elif tt in [CategoryType.APPLICATION, "app"]:
                # obk = 'outputs'
                try:
                    tw = int(drop["weight"])
                except (ValueError, KeyError):
                    tw = 1
                dtp = 1
            elif tt in [CategoryType.SERVICE, "serviceapp"]:
                try:
                    tw = int(drop["weight"])
                except (ValueError, KeyError):
                    tw = 1
                dtp = 1
            else:
                raise SchedulerException("Drop Type '{0}' not supported".format(tt))
            num_cpus = drop.get("num_cpus", 1)
            if embed_drop:
                G.add_node(
                    myk,
                    weight=tw,
                    text=drop["name"],
                    drop_type=dtp,
                    drop_spec=drop,
                    num_cpus=num_cpus,
                )
            else:
                G.add_node(
                    myk,
                    weight=tw,
                    text=drop["name"],
                    drop_type=dtp,
                    num_cpus=num_cpus,
                )
            for obk in out_bound_keys:
                if obk in drop:
                    for oup in drop[obk]:
                        key = list(oup.keys())[0] if isinstance(oup, dict) else oup
                        if CategoryType.DATA == tt:
                            G.add_weighted_edges_from(
                                [(myk, key_dict[key], int(drop["weight"]))]
                            )
                        elif CategoryType.APPLICATION == tt:
                            G.add_weighted_edges_from(
                                [
                                    (
                                        myk,
                                        key_dict[key],
                                        int(drop_dict[key].get("weight", 5)),
                                    )
                                ]
                            )

        if fake_super_root:
            super_root = dropdict(
                {
                    "oid": "-92",
                    "categoryType": CategoryType.DATA,
                    "dropclass": "dlg.data.drops.data_base.NullDROP",
                }
            )
            super_k = len(drop_list) + 1
            G.add_node(
                super_k,
                weight=0,
                drop_type=0,
                drop_spec=super_root,
                num_cpus=0,
                text="fake_super_root",
            )

            for oup in get_roots(drop_list):
                G.add_weighted_edges_from([(super_k, key_dict[oup], 1)])

        return G
