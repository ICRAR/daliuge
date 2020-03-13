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
Partially unroll the physical map into a series of stages, each of which is
then secheduled separately while considering data locality.
This is inspired by the following factors:
1. a fully-unrolled physical graph (e.g. 50 million Drops) is hard to handle
2. a fully-unrolled physical graph is likely to under-utilise resources
3. partially-unrolled physical graph is more suitable for multi-graph solution

The implementation is also partly inspired by
Spark's "Stage-Oriented Scheduler":
https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache\
/spark/scheduler/DAGScheduler.scala
"""

class Stage(object):
    def __init__(self, id):
        self._id = id #normally this is the group construct key
        self._lg_nodes = []

    def __hash__(self):
        return hash(self._id)

    def add_node(lgnode):
        self._lg_nodes.append(lgnode)

def parse_node(curr_node, done_dict, stg_set):
    node_list = curr_node.outputs
    for n in node_list:
        if (n.id not in done_dict):
            if (curr_node.h_level != n.h_level):
                stg = Stage(str(n.gid))
                c = 0
                while stg in stg_set:
                    c += 1
                    stg._id = '%d_%d' % (n.gid, c)
                stg_set.add(stg)
                done_dict[n.id] = stg
                stg.add_node(n)
            else:
                done_dict[n.id] = done_dict[curr_node.id]
        parse_node(n, done_dict, stg_set)

def produce_stages(lg):
    """
    output a list of stages sorted by the scheduling order
    lg
    """
    lgn_list = lg._lgn_list
    dag_roots = [x for x in lgn_list if x.is_dag_root()]
    done_dict = dict()
    #for n in dag_roots:
