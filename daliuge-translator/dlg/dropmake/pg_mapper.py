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
A new mapping scheme that deviates from the previous two-phase approach such as
clustering-based algorithms (e.g. MetisPGTP, MySarkarPGTP) that have two issues:

1. Cannot re-use resources that subsequently become idling after their Drops
   have completed executions. This is due to (1) the nature of "static" graph
   partitioning algorithm, which cannot cope with "status evolution" of both the
   Drop graphs and resources graph; and (2) the nature of the "two-phase"
   approach where the first phase does not consider any resources.
   Traditionally to address this issue, in Pagasus[1] for example, it uses some
   threshold to control partial mapping (e.g. mapping/scheuling "horizon").
   But this adds yet another hyperparameter, which is hard to determine.
   More importantly, such partial graph deployment is currently not supported
   unless we also change DropManager's behaviours in receiving PGs.

2. The complexity of maintaining the DoP constraint within each partition is too
   high as the # of Drops goes up (exponential), and will only get worse after
   adding more constraints such as CPUs or memory consumption
   The key issue here is that we tried to use complex graph models (antichains)
   to prevent something bad from hapening in the future. This
   in turn requires significant computation (e.g. complete transitive closures).
   Instead, we could just "go" to the future, disallow bad things via
   simulations by playing out the mapping algorithm step by step

[1] Deelman, Ewa, et al. "Pegasus: A framework for mapping complex scientific
workflows onto distributed systems." Scientific Programming 13.3 (2005): 219-237.

In addition, for there is a more practical issue associated with the most
popular METIS algorithm --- cannot minimise completion time, does not really
support DAG, and cannot impose the DoP constraint on each partition

The mapping algorithm will be based on the HEFT algorithm
http://ieeexplore.ieee.org/xpls/abs_all.jsp?arnumber=993206

But with the following modifications:
1   The user rank can be directly derived from the logical graphs
2   Use lookahead strategies when assigning a high priority task to an idling
    processor
3   The definition of "idling" for a processor is no longer binary
"""
from .pg_generator import GraphException

class ResourceCapability(object):
    """
    Currently assume compute nodes are all homogeneous as
    defined in SDP Compute Island
    """
    def __init__(self, num_island, num_nodes_per_island,
                num_cores_per_node, intra_inter_ratio):
        """
        intra_inter_ratio:  ratio between intra-island bandwith and inter-island
                            bandwidth (integer and should be >= 1)
        """
        if (intra_inter_ratio < 1):
            raise GraphException('Invalid intra_inter_ratio {0}'\
            .format(intra_inter_ratio))
        self._num_island = num_island
        self._num_nodes_pi = num_nodes_per_island
        self._num_cores_pn = num_cores_per_node # per compute node
        self._island_ratio = float(intra_inter_ratio)
        self._ttnodes = num_island * num_nodes_per_island

    def bandwidth(self, nodeA, nodeB):
        """
        nodeA, nodeB:  integer (starting from 0)
        """
        if (nodeA >= self._ttnodes or nodeB >= self._ttnodes):
            raise GraphException('Invalid node id')
        gap = nodeA // self._num_nodes_pi - nodeB // self._num_nodes_pi
        return self._island_ratio  if gap == 0 else 1.0

    def avg_comp_cost(self, drop, comp_node):
        """
        Comput cost in time, assuming homogeneity for now
        """
        return float(drop['tw'])

    def comm_cost(self, drop_edge_weight, nodeA, nodeB):
        """
        Communication cost in time
        """
        return drop_edge_weight / self.bandwidth(nodeA, nodeB)

    def avg_comm_cost(self, drop_edge_weight):
        return (drop_edge_weight / 1.0 +
                drop_edge_weight / self._island_ratio) / 2

class ResourceAvailability(ResourceCapability):
    pass
