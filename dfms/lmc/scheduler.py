
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
from collections import defaultdict
import time

class Schedule(object):
    """
    The scheduling solution object, which contains:
    which drop is executed at where [and when]
    """
    @property
    def makespan(self):
        pass

class Partition(object):
    """
    Resource cluster
    """
    def __init__(self, gid, max_dop):
        """
        gid:    cluster/partition id (string)
        max_dop:    maximum allowed degree of parallelism in this partition (int)
        """
        self._gid = gid
        self._dag = nx.DiGraph()
        self._max_dop = max_dop
        #print "My dop = {0}".format(self._max_dop)

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

        self.add(u, uw, v, vw)

        mydop = DAGUtil.get_max_dop(self._dag)
        if (mydop > self._max_dop):
            ret = False
        else:
            ret = True

        if (unew):
            self.remove(u)
        if (vnew):
            self.remove(v)

        return ret

    def add(self, u, uw, v, vw):
        self._dag.add_node(u, weight=uw)
        self._dag.add_node(v, weight=vw)
        self._dag.add_edge(u, v)

    def remove(self, n):
        self._dag.remove_node(n)

    @property
    def cardinality(self):
        return len(self._dag.nodes())

class Scheduler(object):
    """
    Static Scheduling consists of three steps:
    1. partition the DAG into an optimal number (M) of clusters
    2. merge clusters into a given number (N) of resource units (if M > N)
    3. map each cluster to a resource unit
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
    we allow for a cost constraint on the number of parallel tasks (e.g. # of cores) within each cluster

    Why
    1. we only need to topologically sort the DAG once since we do not add new edges in the cluster
    2. closer to requirements
    3. adjustable for local schedulers
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

        return ((st_gid - init_c), curr_lpl, edt)

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

if __name__ == "__main__":
    G = nx.DiGraph()
    G.add_weighted_edges_from([(4,3,1.2), (3,2,4), (2,1,2)])
    G.add_weighted_edges_from([(3,6,5.2), (6,7,2)])
    G.node[7]['weight'] = 65
    print G.nodes(data=True)
    print G.edges(data=True)

    lp = DAGUtil.get_longest_path(G)
    print "The longest path is {0} with a length of {1}".format(lp[0], lp[1])
    mw = DAGUtil.get_max_width(G)
    dop = DAGUtil.get_max_dop(G)
    print "The max (weighted) width = {0}, and the max degree of parallelism = {1}".format(mw, dop)
