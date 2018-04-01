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

"""
Weighted antichain agorithm based on

A special case (K = 1) of the Maximum Weighted K-families based on
    the Theorem 3.1 in
    http://fmdb.cs.ucla.edu/Treports/930014.pdf

A detailed proof can be found on Page 2 (Corollary)
    https://link.springer.com/article/10.1007/BF00333130
"""
import sys

import networkx as nx

def _create_split_graph(dag, w_attr='weight'):
    """
    Given a normal DiGraph, create its equivalent split graph
    """
    bpg = nx.DiGraph()
    for el in dag.nodes(data=True):
        xi = '{0}_x'.format(el[0])
        yi = '{0}_y'.format(el[0])
        #print(el)
        bpg.add_edge('s', xi, capacity=el[1].get(w_attr, 1), weight=0)
        bpg.add_edge(xi, yi, capacity=sys.maxsize, weight=1)
        bpg.add_edge(yi, 't', capacity=el[1].get(w_attr, 1), weight=0)

        el_des = nx.descendants(dag, el[0])
        el_pred = nx.ancestors(dag, el[0])

        for udown in el_des:
            bpg.add_edge(xi, '{0}_y'.format(udown),
            capacity=sys.maxsize, weight=0)
        for uup in el_pred:
            bpg.add_edge('{0}_x'.format(uup), yi,
            capacity=sys.maxsize, weight=0)

    return bpg

def _get_pi_solution(split_graph):
        """
        1. create H (admissable graph) based on Section 3
        http://fmdb.cs.ucla.edu/Treports/930014.pdf

        2. calculate the max flow f' on H using networkx

        3. construct Residual graph from f' on H based on
        https://www.topcoder.com/community/data-science/\
        data-science-tutorials/minimum-cost-flow-part-two-algorithms/

        4. calculate Pi based on Section 3 again
        """
        # Step 1
        H = nx.DiGraph()
        H.add_nodes_from(split_graph)
        for ed in split_graph.edges(data=True):
            Cxy = ed[2].get('capacity', sys.maxsize)
            Axy = ed[2]['weight']
            if (Axy == 0 and Cxy > 0):
                H.add_edge(ed[0], ed[1], capacity=Cxy, weight=Axy)

        # Step 2
        flow_value, flow_dict = nx.maximum_flow(H, 's', 't')

        # Step 3
        R = nx.DiGraph()
        R.add_nodes_from(H)
        for ed in H.edges(data=True):
            Xij = flow_dict[ed[0]][ed[1]]
            Uij = ed[2].get('capacity', sys.maxsize)
            Cij = ed[2]['weight']
            if (Uij - Xij) > 0:
                R.add_edge(ed[0], ed[1], weight=Cij)
            if (Xij > 0):
                R.add_edge(ed[1], ed[0], weight=-1 * Cij)

        # Step 4
        pai = dict()
        for n in R.nodes():
            if (nx.has_path(R, 's', n)):
                pai[n] = 0
            else:
                pai[n] = 1
        return pai

def get_max_weighted_antichain(dag, w_attr='weight'):
    """
    Given a a nextworkx DiGraph `dag`, return a tuple.
    The first element is the length of the max_weighted_antichain
    The second element is the node list of the antichain

    Assume each node in `dag` has a field "weight", which is, well, the weight
    """

    # Step 1 - Create the "split" graph
    bpg = _create_split_graph(dag, w_attr=w_attr)
    pai = _get_pi_solution(bpg)

    w_antichain_len = 0 #weighted antichain length
    antichain_names = []
    for h in range(2):
        for nd in bpg.nodes():
            if (nd.endswith('_x')):
                y_nd = nd.split('_x')[0] + '_y'
                if ((1 - pai[nd] + pai['s'] == h) and
                (pai[y_nd] - pai[nd] == 1)):
                    w_antichain_len += bpg.adj['s'][nd]['capacity']
                    #print(' *** %d' % bpg.edge['s'][nd]['capacity'])
                    antichain_names.append(nd)

    return w_antichain_len, antichain_names

def create_small_seq_graph():
    G = nx.DiGraph()
    G.add_edge(1, 2)
    G.add_edge(2, 3)
    G.node[1]['weight'] = 5
    G.node[2]['weight'] = 4
    G.node[3]['weight'] = 7
    #print("")
    return G, 7

def create_medium_seq_graph():
    G = create_small_seq_graph()[0]
    G.add_edge(3, 4)
    G.add_edge(3, 5)
    G.add_edge(4, 6)
    G.add_edge(5, 6)
    G.add_edge(6, 7)
    G.node[4]['weight'] = 3
    G.node[5]['weight'] = 2
    G.node[6]['weight'] = 6
    G.node[7]['weight'] = 1
    return G, 7

def create_small_parral_graph():
    G = nx.DiGraph()
    G.add_edge(1, 2)
    G.add_edge(1, 3)

    G.node[1]['weight'] = 5
    G.node[2]['weight'] = 6
    G.node[3]['weight'] = 7
    return G, 13

def create_medium_parral_graph():
    G = create_small_parral_graph()[0]
    G.add_edge(2, 4)
    G.add_edge(2, 5)
    G.node[4]['weight'] = 3
    G.node[5]['weight'] = 4
    return G, 14

if __name__ == "__main__":
    gs = [create_small_seq_graph(),
          create_medium_seq_graph(),
          create_small_parral_graph(),
          create_medium_parral_graph()]
    for g, lt in gs:
        w, l = get_max_weighted_antichain(g)
        print(w)
        print(l)
        if (w != lt):
            print("Calculated %d != %d, which is the ground-truth" % (w, lt))
        print('')
