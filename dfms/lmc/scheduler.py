
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

import networkx as nx
import numpy as np
from collections import defaultdict
import time, math

class SchedulerException(Exception):
    pass

class Schedule(object):
    """
    The scheduling solution with schedule-related properties
    """
    def __init__(self, dag, max_dop):
        self._dag = dag
        self._max_dop = max_dop
        self._topo_sort = nx.topological_sort(self._dag)
        DAGUtil.label_schedule(self._dag, topo_sort=self._topo_sort)
        #self._gantt_mat = DAGUtil.ganttchart_matrix(self._dag, topo_sort=self._topo_sort))
        self._lpl = None
        self._wkldist = None
        self._wkl = None

    @property
    def makespan(self):
        if (self._lpl is None):
            self._lpl = DAGUtil.get_longest_path(self._dag, show_path=False)[1]
        return self._lpl

    @property
    def schedule_matrix(self):
        """
        Return: a self._lpl x self._max_dop matrix
                (X - time, Y - resource unit / parallel lane)
        """
        G = self._dag
        N = int(math.ceil(DAGUtil.get_longest_path(G, show_path=False)[1]))
        M = self._max_dop
        ma = np.zeros((M, N), dtype=int)
        pr = np.zeros((M), dtype=int)
        last_pid = -1
        prev_n = None
        for n in self._topo_sort:
            node = G.node[n]
            try:
                stt = node['stt']
                edt = node['edt']
            except KeyError, ke:
                raise SchedulerException("No schedule labels found: {0}".format(str(ke)))
            #print i, n, stt, edt
            if (edt == stt):
                continue
            if (prev_n in G.predecessors(n)):
                curr_pid = last_pid
            else:
                found = None
                for i in range(M):
                    if (i != last_pid and pr[i] <= stt):
                        found = i
                        break
                if (found is None):
                    raise SchedulerException("Cannot find a idle PID, max_dop could be wrong")
                curr_pid = found
            ma[curr_pid, stt:edt + 1] = np.ones((1, edt + 1 - stt)) * n
            pr[curr_pid] = edt
            last_pid = curr_pid
            prev_n = n
        return ma

    @property
    def workload(self):
        """
        Integrated (weighted average) Workload
        Return: Integer
        """
        pass

    def workload_dist(self):
        """
        Workload distribution over the entire makespan
        1. colummn peek
        2. each node will be tagged "start/finish" time
        3. for each v, find a u with the latest finish time as the start time (st)
        4. column peek st across all lanes, get the first idle lane, schedule v
            (this may mean go back in time)
        """
        pass

class Partition(object):
    """
    Logical partition, multiple (1 ~ N) of these will be placed onto a single
    physical partition
    """
    def __init__(self, gid, max_dop):
        """
        gid:        cluster/partition id (string)
        max_dop:    maximum allowed degree of parallelism in this partition (int)
        """
        self._gid = gid
        self._dag = nx.DiGraph()
        self._ask_max_dop = max_dop
        self._max_antichains = None # a list of max (width) antichains
        self._lpl = None
        self._schedule = None
        self._max_dop = None
        #print "My dop = {0}".format(self._ask_max_dop)

    @property
    def schedule(self):
        if (self._schedule is None):
            self._schedule = Schedule(self._dag, self._max_dop)
        return self._schedule

    def recompute_schedule(self):
        self._schedule = None
        return self.schedule

    def can_add(self, u, uw, v, vw):
        if (len(self._dag.nodes()) == 0):
            return True

        if (self._dag.node.has_key(u)):
            unew = False
        else:
            unew = True
        if (self._dag.node.has_key(v)):
            vnew = False
        else:
            vnew = True

        # print "Before: {0} - slow max antichains: {1}, fast max: {2}".format(self._dag.edges(),
        # DAGUtil.get_max_antichains(self._dag), self._max_antichains)

        self._dag.add_node(u, weight=uw)
        self._dag.add_node(v, weight=vw)
        self._dag.add_edge(u, v)

        if (unew and vnew):
            mydop = DAGUtil.get_max_dop(self._dag)
        else:
            mydop = self.probe_max_dop(u, v, unew, vnew)
            #TODO - put the following code in a unit test!
            # mydop_slow = DAGUtil.get_max_dop(self._dag)#
            # if (mydop_slow != mydop):
            #     err_msg = "u = {0}, v = {1}, unew = {2}, vnew = {3}".format(u, v, unew, vnew)
            #     raise SchedulerException("{2}: mydop = {0}, mydop_slow = {1}".format(mydop, mydop_slow, err_msg))

        if (mydop > self._ask_max_dop):
            ret = False
        else:
            ret = True

        if (unew):
            self.remove(u)
        if (vnew):
            self.remove(v)

        return ret

    def add(self, u, uw, v, vw):
        if (self._dag.node.has_key(u)):
            unew = False
        else:
            unew = True
        if (self._dag.node.has_key(v)):
            vnew = False
        else:
            vnew = True
        self._dag.add_node(u, weight=uw)
        self._dag.add_node(v, weight=vw)
        self._dag.add_edge(u, v)
        if (unew and vnew): # we know this is fast
            self._max_antichains = DAGUtil.get_max_antichains(self._dag)
            self._max_dop = 1
        else:
            self._max_dop = self.probe_max_dop(u, v, unew, vnew, update=True)
            #DAGUtil.get_max_dop(self._dag)#

    def remove(self, n):
        self._dag.remove_node(n)

    def probe_max_dop(self, u, v, unew, vnew, update=False):
        """
        an incremental antichain (which appears significantly more efficient than the networkx antichains)
        But only works for DoP, not for weighted width
        """
        if (self._max_antichains is None):
            new_ac = DAGUtil.get_max_antichains(self._dag)
            if (update):
                self._max_antichains = new_ac
            if (len(new_ac) == 0):
                if (update):
                    self._max_antichains = None
                return 0
            else:
                return len(new_ac[0])
        else:
            if (unew):
                ups = nx.descendants(self._dag, u) #TODO this could be accumulated!
                new_node = u
            elif (vnew):
                ups = nx.ancestors(self._dag, v)
                new_node = v
            else:
                raise SchedulerException("u v are both new/old")
            new_ac = [[new_node]] # the new node will be the first default antichain
            md = 1
            for ma in self._max_antichains: # missing elements in the current max_antichains!
                mma = list(ma)
                """
                incremental updates
                """
                found = False
                for n in mma:
                    if (n in ups):
                        found = True
                        break
                if (not found):
                    mma.append(new_node)
                if (len(mma) > md):
                    md = len(mma)
                new_ac.append(mma) # carry over, then prune it
            new_acs = DAGUtil.prune_antichains(new_ac)
            if (len(new_acs) > 0):
                if (update):
                    self._max_antichains = new_acs
                return md
            else:
                raise SchedulerException("No antichains")
    @property
    def cardinality(self):
        return len(self._dag.nodes())

class Scheduler(object):
    """
    Static Scheduling consists of three steps:
    1. partition the DAG into an optimal number (M) of clusters
        goal - minimising execution time while maintaining intra-partition DoP
    2. merge partitions into a given number (N) of resource units (if M > N)
        goal - minimise logical communication cost while maintaining load balancing
    3. map each cluster to a resource unit
        goal - minimise physical communication cost amongst physical partitions
    """

    def __init__(self, drop_list, max_dop=8):
        """
        turn drop_list into DAG, and check its validity
        """
        self._drop_list = drop_list
        self._key_dict = dict() # {oid : node_id}
        self._drop_dict = dict() # {oid : drop}
        for i, drop in enumerate(self._drop_list):
            oid = drop['oid']
            self._key_dict[oid] = i + 1 # starting from 1
            self._drop_dict[oid] = drop
        self._dag = self._build_dag()#
        self._max_dop = max_dop

    def _build_dag(self):
        """
        return a networkx Digraph (DAG)

        tw - task weight
        dw - data weight / volume
        """
        G = nx.DiGraph()
        for i, drop in enumerate(self._drop_list):
            oid = drop['oid']
            myk = i + 1
            tt = drop['type']
            if ('plain' == tt):
                obk = 'consumers' # outbound keyword
                tw = 0
            elif ('app' == tt):
                obk = 'outputs'
                tw = drop['tw']
            G.add_node(myk, weight=tw)
            if (drop.has_key(obk)):
                for oup in drop[obk]:
                    if ('plain' == tt):
                        G.add_weighted_edges_from([(myk, self._key_dict[oup], drop['dw'])])
                    elif ('app' == tt):
                        G.add_weighted_edges_from([(myk, self._key_dict[oup], self._drop_dict[oup]['dw'])])
        return G

    def partition_dag(self):
        pass

    def merge_cluster(self, num_partitions):
        """
        Return a schedule
        """
        pass

    def map_cluster(self):
        pass

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
    def __init__(self, drop_list, max_dop=8):
        super(MySarkarScheduler, self).__init__(drop_list, max_dop=max_dop)

    def partition_dag(self):
        """
        Return a tuple of
            1. the # of partitions formed (int)
            2. the parallel time (longest path, int)
            3. partition time (seconds, float)
        """
        G = self._dag
        st_gid = len(self._drop_list) + 1
        init_c = st_gid
        el = G.edges(data=True)
        el.sort(key=lambda ed: ed[2]['weight'] * -1)
        stt = time.time()
        topo_sorted = nx.topological_sort(G)
        g_dict = dict() #{gid : Partition}
        curr_lpl = DAGUtil.get_longest_path(G, show_path=False, topo_sort=topo_sorted)[1]
        parts = []
        for e in el:
            u = e[0]
            gu = G.node[u]
            v = e[1]
            gv = G.node[v]
            ow = G.edge[u][v]['weight']
            G.edge[u][v]['weight'] = 0 #edge zeroing
            new_lpl = DAGUtil.get_longest_path(G, show_path=False, topo_sort=topo_sorted)[1]
            #print "{2} --> {3}, curr lpl = {0}, new lpl = {1}".format(curr_lpl, new_lpl, u, v)
            if (new_lpl <= curr_lpl): #try to accept the edge zeroing
                ugid = gu.get('gid', None)
                vgid = gv.get('gid', None)
                if (ugid and (not vgid)):
                    part = g_dict[ugid]
                elif ((not ugid) and vgid):
                    part = g_dict[vgid]
                elif (not ugid and (not vgid)):
                    part = Partition(st_gid, self._max_dop)
                    g_dict[st_gid] = part
                    st_gid += 1
                    parts.append(part)
                else: #elif (ugid and vgid):
                    # cannot change Partition once is in!
                    part = None
                uw = gu['weight']
                vw = gv['weight']
                if (part is not None and part.can_add(u, uw, v, vw)):
                    part.add(u, uw, v, vw)
                    gu['gid'] = part._gid
                    gv['gid'] = part._gid
                    curr_lpl = new_lpl
                else:
                    G.edge[u][v]['weight'] = ow
            else:
                G.edge[u][v]['weight'] = ow

        #for an unallocated node, it forms its own partition
        edt = time.time() - stt
        for n in G.nodes(data=True):
            if (not n[1].has_key('gid')):
                n[1]['gid'] = st_gid
                st_gid += 1

        return ((st_gid - init_c), curr_lpl, edt, parts)

    def merge_cluster(self, num_partitions):
        pass

    def map_cluster(self):
        pass

class DSCScheduler(Schedule):
    """
    Based on
    T. Yang and A. Gerasoulis, "DSC: scheduling parallel tasks on an
    unbounded number of processors," in IEEE Transactions on
    Parallel and Distributed Systems, vol.5, no.9, pp.951-967, Sep 1994
    """
    def __init__(self, drop_list):
        super(DSCScheduler, self).__init__(drop_list)

    def partition_dag(self):
        pass

    def merge_cluster(self, num_partitions):
        pass

    def map_cluster(self):
        pass

class DAGUtil(object):
    """
    Helper functions dealing with DAG
    """
    @staticmethod
    def get_longest_path(G, weight='weight', default_weight=1, show_path=True, topo_sort=None):
        """
        Ported from:
        https://github.com/networkx/networkx/blob/master/networkx/algorithms/dag.py
        Added node weight

        Returns the longest path in a DAG
        If G has edges with 'weight' attribute the edge data are used as weight values.
        Parameters
        ----------
        G : NetworkX DiGraph
            Graph
        weight : string (default 'weight')
            Edge data key to use for weight
        default_weight : integer (default 1)
            The weight of edges that do not have a weight attribute
        Returns a tuple with two elements
        -------
        path : list
            Longest path
        path_length : float
            The length of the longest path

        """
        dist = {} # stores {v : (length, u)}
        if (topo_sort is None):
            topo_sort = nx.topological_sort(G)
        for v in topo_sort:
            us = [
            (dist[u][0] + #accumulate
            data.get(weight, default_weight) + #edge weight
            G.node[u].get(weight, 0) + # u node weight
            (G.node[v].get(weight, 0) if len(G.successors(v)) == 0 else 0), # v node weight if no successor
            u)
                for u, data in G.pred[v].items()]
            # Use the best predecessor if there is one and its distance is non-negative, otherwise terminate.
            maxu = max(us) if us else (0, v)
            dist[v] = maxu if maxu[0] >= 0 else (0, v)
        u = None
        v = max(dist, key=dist.get)
        lp = dist[v][0]
        if (not show_path):
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
    def get_max_width(G, weight='weight', default_weight=1):
        """
        Get the antichain with the maximum "weighted" width of this DAG
        weight: float (for example, it could be RAM consumption in GB)
        Return : float
        """
        max_width = 0
        for antichain in nx.antichains(G):
            t = 0
            for n in antichain:
                t += G.node[n].get(weight, default_weight)
            if (t > max_width):
                max_width = t
        return max_width

    @staticmethod
    def get_max_dop(G):
        """
        Get the maximum degree of parallelism of this DAG
        return : int
        """
        max_dop = 0
        for antichain in nx.antichains(G):
            leng = len(antichain)
            if (leng > max_dop):
                max_dop = leng
        return max_dop

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
        """
        ret = []
        leng_dict = defaultdict(list)
        for antichain in antichains:
            leng = len(antichain)
            if (0 == leng):
                continue
            leng_dict[leng].append(antichain)
        ll = list(reversed(sorted(leng_dict.keys())))
        lengl = len(ll)
        if (lengl == 0):
            pass
        elif (leng == 1):
            ret = leng_dict[ll[0]]
        else:
            ret = leng_dict[ll[0]] + leng_dict[ll[1]]
        return ret

    @staticmethod
    def label_schedule(G, weight='weight', topo_sort=None):
        """
        for each node, label its start and end time
        """
        if (topo_sort is None):
            topo_sort = nx.topological_sort(G)
        for v in topo_sort:
            gv = G.node[v]
            parents = G.predecessors(v)
            if (len(parents) == 0):
                gv['stt'] = 0
                edge = 0
            else:
                # get the latest end time of one of its parents
                ledt = -1
                edge = 0
                for parent in parents:
                    pedt = G.node[parent]['edt']
                    if (pedt > ledt):
                        ledt = pedt
                        edge = G.edge[parent][v].get(weight, 0)
                gv['stt'] = int(math.ceil(ledt)) + int(math.ceil(edge))
            gv['edt'] = gv['stt'] + int(math.ceil(gv.get(weight, 0)))

    @staticmethod
    def ganttchart_matrix(G, topo_sort=None):
        """
        a M by N matrix
        """
        N = int(math.ceil(DAGUtil.get_longest_path(G, show_path=False)[1]))
        M = len(G.nodes())
        ma = np.zeros((M, N), dtype=np.int)
        if (topo_sort is None):
            topo_sort = nx.topological_sort(G)
        for i, n in enumerate(topo_sort):
            node = G.node[n]
            try:
                stt = node['stt']
                edt = node['edt']
            except KeyError, ke:
                raise SchedulerException("No schedule labels found: {0}".format(str(ke)))
            #print i, n, stt, edt
            leng = edt + 1 - stt
            if (edt == stt):
                continue
            ma[i, stt:edt + 1] = np.ones((1, leng))
            print ma[i, :]
        return ma

if __name__ == "__main__":
    G = nx.DiGraph()
    G.add_weighted_edges_from([(4,3,1.2), (3,2,4), (2,1,2), (5,3,1.1)])
    G.add_weighted_edges_from([(3,6,5.2), (6,7,2)])
    G.add_weighted_edges_from([(9,12,2.1)]) # testing independent nodes
    G.node[3]['weight'] = 65
    print G.pred[12].items()
    print G.node[G.predecessors(12)[0]]

    # print "prepre"
    # print len(G.pred[7].items())
    # print G.predecessors(7)
    # print G.pred[7].items()
    # print ""
    #
    # print G.nodes(data=True)
    # print G.edges(data=True)

    print "topological sort\n"
    print nx.topological_sort(G)
    # for i, v in enumerate(nx.topological_sort(G)):
    #     print i, v

    lp = DAGUtil.get_longest_path(G)
    print "The longest path is {0} with a length of {1}".format(lp[0], lp[1])
    mw = DAGUtil.get_max_width(G)
    dop = DAGUtil.get_max_dop(G)
    print "The max (weighted) width = {0}, and the max degree of parallelism = {1}".format(mw, dop)
    DAGUtil.label_schedule(G)
    print G.nodes(data=True)
    gantt_matrix = DAGUtil.ganttchart_matrix(G)
    print gantt_matrix
