
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
import os, time, math, pkg_resources, platform

DEBUG = 0

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
        self._lpl = None
        self._wkl = None
        self._sma = None

    @property
    def makespan(self):
        if (self._lpl is None):
            lpl = DAGUtil.get_longest_path(self._dag, show_path=True)
            self._lpl = lpl[1] - (len(lpl[0]) - 1) #TODO find out why
        return self._lpl

    @property
    def schedule_matrix(self):
        """
        Return: a self._lpl x self._max_dop matrix
                (X - time, Y - resource unit / parallel lane)
        """
        if (self._sma is None):
            G = self._dag
            if (DEBUG):
                lpl_str = []
                lpl_c = 0
                for lpn in lpl[0]:
                    ww = G.node[lpn].get('weight', 0)
                    lpl_str.append("{0}({1})".format(lpn, ww))
                    lpl_c += ww
                print "lpl: ", " -> ".join(lpl_str)
                print "lplt = ", lpl_c

            N = self.makespan
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
                if (edt == stt):
                    continue
                if (prev_n in G.predecessors(n)):
                    curr_pid = last_pid
                else:
                    found = None
                    for i in range(M):
                        if (pr[i] <= stt):
                            found = i
                            break
                    if (found is None):
                        raise SchedulerException("Cannot find a idle PID, max_dop provided: {0}, actual max_dop: {1}\n Graph: {2}".format(M,
                        DAGUtil.get_max_dop(G), G.nodes(data=True)))
                    curr_pid = found
                ma[curr_pid, stt:edt] = np.ones((1, edt - stt)) * n
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
        if (self._wkl is None):
            ma = self.schedule_matrix
            c = []
            for i in range(ma.shape[1]):
                c.append(np.count_nonzero(ma[:,i]))
            self._wkl = int(np.mean(np.array(c))) # since METIS only accepts integer
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

    Logical partition can be nested, and it somewhat resembles the DataDROPManager
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
        self._parent_id = None
        self._child_parts = None
        #print "My dop = {0}".format(self._ask_max_dop)

    @property
    def parent_id(self):
        return self._parent_id

    @parent_id.setter
    def parent_id(self, value):
        self._parent_id = value

    @property
    def partition_id(self):
        return self._gid

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

        unew = False if self._dag.node.has_key(u) else True
        vnew = False if self._dag.node.has_key(v) else True

        if (DEBUG):
            slow_max = DAGUtil.get_max_antichains(self._dag)
            fast_max = self._max_antichains
            info = "Before: {0} - slow max: {1}, fast max: {2}, u: {3}, v: {4}, unew:{5}, vnew:{6}".format(self._dag.edges(),
            slow_max, fast_max, u, v, unew, vnew)
            print info
            if (len(slow_max) != len(fast_max)):
                raise SchedulerException("ERROR - {0}".format(info))

        self._dag.add_node(u, weight=uw)
        self._dag.add_node(v, weight=vw)
        self._dag.add_edge(u, v)

        if (unew and vnew):
            mydop = DAGUtil.get_max_dop(self._dag)
        else:
            mydop = self.probe_max_dop(u, v, unew, vnew)
            #TODO - put the following code in a unit test!
            if (DEBUG):
                mydop_slow = DAGUtil.get_max_dop(self._dag)#
                if (mydop_slow != mydop):
                    err_msg = "u = {0}, v = {1}, unew = {2}, vnew = {3}".format(u, v, unew, vnew)
                    raise SchedulerException("{2}: mydop = {0}, mydop_slow = {1}".format(mydop, mydop_slow, err_msg))
        ret = False if mydop > self._ask_max_dop else True
        if (unew):
            self.remove(u)
        if (vnew):
            self.remove(v)
        return ret

    def add(self, u, uw, v, vw):
        unew = False if self._dag.node.has_key(u) else True
        vnew = False if self._dag.node.has_key(v) else True
        self._dag.add_node(u, weight=uw)
        self._dag.add_node(v, weight=vw)
        self._dag.add_edge(u, v)

        if (unew and vnew): # we know this is fast
            self._max_antichains = DAGUtil.get_max_antichains(self._dag)
            self._max_dop = 1
        else:
            self._max_dop = self.probe_max_dop(u, v, unew, vnew, update=True)
            #self._max_dop = DAGUtil.get_max_dop(self._dag)# this is too slow!

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
                ups = nx.descendants(self._dag, u)
                new_node = u
            elif (vnew):
                ups = nx.ancestors(self._dag, v)
                new_node = v
            else:
                raise SchedulerException("u v are both new/old")
            new_ac = []
            md = 1
            for ma in self._max_antichains: # missing elements in the current max_antichains!
                """
                incremental updates
                """
                found = False
                for n in ma:
                    if (n in ups):
                        found = True
                        break
                if (not found):
                    mma = list(ma)
                    mma.append(new_node)
                    new_ac.append(mma)
                    if (len(mma) > md):
                        md = len(mma)
                elif (len(ma) > md):
                    md = len(ma)
                new_ac.append(ma) # carry over, then prune it
            if (len(new_ac) > 0):
                if (update):
                    self._max_antichains = new_ac
                return md
            else:
                raise SchedulerException("No antichains")
    @property
    def cardinality(self):
        return len(self._dag.nodes())

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
        self._parts = None # partitions
        self._part_dict = dict() #{gid : part}
        self._part_edges = [] # edges amongst all partitions

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
                tw = int(drop['tw'])
            G.add_node(myk, weight=tw)
            if (drop.has_key(obk)):
                for oup in drop[obk]:
                    if ('plain' == tt):
                        G.add_weighted_edges_from([(myk, self._key_dict[oup], int(drop['dw']))])
                    elif ('app' == tt):
                        G.add_weighted_edges_from([(myk, self._key_dict[oup], int(self._drop_dict[oup]['dw']))])
        return G

    def partition_dag(self):
        raise SchedulerException("Not implemented. Try subclass instead")

    def merge_partitions(self, num_partitions):
        """
        Merge M partitions into N partitions where N < M
            implemented using METIS for now
        """
        # 1. build the bi-directional graph (each partition is a node)
        metis = DAGUtil.import_metis()
        G = nx.Graph()
        G.graph['edge_weight_attr'] = 'weight'
        G.graph['node_weight_attr'] = ['wkl', 'eff']

        st_gid = len(self._drop_list) + len(self._parts) + 1

        for part in self._parts:
            sc = part.schedule
            G.add_node(part.partition_id, wkl=sc.workload, eff=sc.efficiency)

        for e in self._part_edges:
            u = e[0]
            v = e[1]
            ugid = self._dag.node[u].get('gid', None)
            vgid = self._dag.node[v].get('gid', None)
            G.add_edge(ugid, vgid) # repeating is fine
            ew = self._dag.edge[u][v]['weight']
            try:
                G[ugid][vgid]['weight'] += ew
            except KeyError, ke:
                G[ugid][vgid]['weight'] = ew
        #DAGUtil.metis_part(G, 15)
        # since METIS does not allow zero edge weight, reset them to one
        for e in G.edges(data=True):
            if (e[2]['weight'] == 0):
                e[2]['weight'] = 1
        #print G.nodes(data=True)
        (edgecuts, metis_parts) = metis.part_graph(G, nparts=num_partitions)
        #assert(len(metis_parts) == len(G.nodes())) #test only
        parent_parts = []
        for i, pt in enumerate(metis_parts):
            parent_id = pt + st_gid
            child_part = self._part_dict[G.nodes()[i]]
            child_part.parent_id = parent_id
            #print "Part {0} --> Cluster {1}".format(child_part.partition_id, parent_id)
            #parent_part = Partition(parent_id, None)
            #self._parts.append(parent_part)
        print "Edgecuts of merged partitions: ", edgecuts
        return edgecuts

    def map_partitions(self):
        """
        map logical partitions to physical resources
        """
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
                    parts.append(part) # will it get rejected?
                    self._part_dict[st_gid] = part
                    st_gid += 1
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
                    self._part_edges.append(e)
            else:
                G.edge[u][v]['weight'] = ow
                self._part_edges.append(e)

        #for an unallocated node, it forms its own partition
        edt = time.time() - stt
        for n in G.nodes(data=True):
            if (not n[1].has_key('gid')):
                n[1]['gid'] = st_gid
                st_gid += 1
        self._parts = parts
        return ((st_gid - init_c), curr_lpl, edt, parts)

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
        return a list of antichains with Top-3 lengths
        """
        return DAGUtil.prune_antichains(nx.antichains(G))

    @staticmethod
    def prune_antichains(antichains):
        """
        Prune a list of antichains to keep those with Top-3 lengths
        """
        todo = []
        leng_dict = defaultdict(list)
        for antichain in antichains:
            todo.append(antichain)
        return todo

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
            else:
                # get the latest end time of one of its parents
                ledt = -1
                for parent in parents:
                    pedt = G.node[parent]['edt'] + G.edge[parent][v].get(weight, 0)
                    if (pedt > ledt):
                        ledt = pedt
                gv['stt'] = ledt
            gv['edt'] = gv['stt'] + gv.get(weight, 0)

    @staticmethod
    def ganttchart_matrix(G, topo_sort=None):
        """
        a M by N matrix
        """
        lpl = DAGUtil.get_longest_path(G, show_path=True)
        N = lpl[1] - (len(lpl[0]) - 1)
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
            leng = edt - stt
            if (edt == stt):
                continue
            ma[i, stt:edt] = np.ones((1, leng))
            #print ma[i, :]
        return ma

    @staticmethod
    def import_metis():
        try:
            import metis as mt
        except:
            pl = platform.platform()
            if (pl.startswith('Darwin')): # a clumsy way
                ext = 'dylib'
            else:
                ext = 'so' # what about Microsoft??!!
            os.environ["METIS_DLL"] = pkg_resources.resource_filename('dfms.lmc', 'lib/libmetis.{0}'.format(ext))
            import metis as mt
        return mt

    @staticmethod
    def metis_part(G, num_partitions):
        """
        Use metis binary executable (instead of library)
        This is used only for testing when libmetis halts unexpectedly
        """
        outf = '/tmp/mm'
        lines = []
        part_id_line_dict = dict() # {part_id: line_num}
        line_part_id_dict = dict()
        for i, n in enumerate(G.nodes()):
            part_id_line_dict[n] = i + 1
            line_part_id_dict[i + 1] = n

        for i, node in enumerate(G.nodes(data=True)):
            n = node[0]
            line = []
            line.append(str(node[1]['wkl']))
            line.append(str(node[1]['eff']))
            for m in G.neighbors(n):
                line.append(str(part_id_line_dict[m]))
                a = G[m][n]['weight']
                if (0 == a):
                    print "G[{0}][{1}]['weight'] = {2}".format(m, n, a)
                line.append(str(G[m][n]['weight']))
            lines.append(" ".join(line))

        header = "{0} {1} 011 2".format(len(G.nodes()), len(G.edges()))
        lines.insert(0, header)
        with open(outf, "w") as f:
            f.write("\n".join(lines))

if __name__ == "__main__":
    G = nx.DiGraph()
    G.add_weighted_edges_from([(4,3,1), (3,2,4), (2,1,2), (5,3,1)])
    G.add_weighted_edges_from([(3,6,5), (6,7,2)])
    G.add_weighted_edges_from([(9,12,2)]) # testing independent nodes
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
    print gantt_matrix.shape
    # sch = Schedule(G, 5)
    # sch_mat = sch.schedule_matrix
    # print sch_mat
    # print sch_mat.shape

    #print DAGUtil.prune_antichains([[], [64], [62], [62, 64], [61], [61, 64], [61, 62], [61, 62, 64], [5], [1]])
