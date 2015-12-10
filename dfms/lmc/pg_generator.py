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
https://confluence.ska-sdp.org/display/PRODUCTTREE/C.1.2.4.4.2+DFM+Resource+Manager

DFM resource managr uses the requested logical graphs, the available resources and
the profiling information and turns it into the partitioned physical graph,
which will then be deployed and monitored by the Physical Graph Manager

Examples of logical graph node JSON representation

{ u'category': u'Data',
  u'data_volume': 25,
  u'group': -58,
  u'key': -59,
  u'loc': u'40.96484375000006 -250.53115793863992',
  u'text': u'Channel @ \nAll Day'},

{ u'Arg01': u'',
  u'Arg02': u'',
  u'Arg03': u'',
  u'Arg04': u'',
  u'category': u'Component',
  u'execution_time': 20,
  u'group': -60,
  u'key': -56,
  u'loc': u'571.6718750000005 268.0000000000004',
  u'text': u'DD Calibration'}

"""
import json, os, datetime, time
from dfms.drop import dropdict
from collections import defaultdict

class GraphException(Exception):
    pass

class LGNode():
    def __init__(self, jd, group_q, done_dict, ssid):
        """
        jd: json_dict (dict)
        group_q: group queue (defaultdict)
        done_dict: LGNode that have been processed (Dict)
        ssid:   session id (string)
        """
        self._jd = jd
        self._children = []
        self._outs = [] # event flow target
        self._inputs = [] # event flow source
        self.group = None
        self._id = jd['key']
        self._ssid = ssid
        self._isgrp = False
        self._converted = False
        if (jd.has_key('isGroup') and jd['isGroup'] == True):
            self._isgrp = True
            for wn in group_q[self.id]:
                wn.group = self
                self.add_child(wn)
            group_q.pop(self.id) # not thread safe

        if (jd.has_key('group')):
            grp_id = jd['group']
            if (done_dict.has_key(grp_id)):
                grp_nd = done_dict[grp_id]
                self.group = grp_nd
                grp_nd.add_child(self)
            else:
                group_q[grp_id].append(self)

        done_dict[self.id] = self

    @property
    def jd(self):
        return self._jd

    @property
    def id(self):
        return self._id

    @property
    def group(self):
        return self._grp

    @group.setter
    def group(self, value):
        self._grp = value

    def has_converted(self):
        return self._converted

    def complete_conversion(self):
        self._converted = True

    @property
    def gid(self):
        if (self.group is None):
            return 0
        else:
            return self.group.id

    def add_output(self, lg_node):
        self._outs.append(lg_node)

    def add_input(self, lg_node):
        self._inputs.append(lg_node)

    def add_child(self, lg_node):
        """
        Add a group member
        """
        self._children.append(lg_node)

    @property
    def children(self):
        return self._children

    @property
    def outputs(self):
        return self._outs

    @property
    def inputs(self):
        return self._inputs

    def has_child(self):
        return len(self._children) > 0

    def has_output(self):
        return len(self._outs) > 0

    def is_start(self):
        return (self._jd['category'] == 'Start')

    def is_group(self):
        return self._isgrp

    def is_scatter(self):
        return (self._isgrp and self._jd['category'] == 'SplitData')

    def is_gather(self):
        return (self._jd['category'] == 'DataGather')

    def is_barrier(self):
        return (self._jd['category'] == 'GroupBy')

    @property
    def dop(self):
        """
        Degree of Parallelism:  integer

        dummy impl.
        """
        return 3

    def make_oid(self, iid=0):
        """
        ssid_id_iid
        iid:    instance id
        """
        return "{0}_{1}_{2}".format(self._ssid, self.id, iid)

    def make_single_drop(self, iid=0, **kwargs):
        """
        make only one drop from a LG nodes
        one-one mapping

        Dummy implementation as of 09/12/15
        """
        oid = self.make_oid(iid)
        dropSpec = dropdict({'oid':oid, 'type':'plain', 'storage':'file'})
        kwargs['iid'] = iid
        dropSpec.update(kwargs)
        return dropSpec

class LG():
    """
    An object representation of Logical Graph
    """

    def __init__(self, json_path, ssid=None):
        """
        parse JSON into LG object graph first
        """
        if (not os.path.exists(json_path)):
            raise GraphException("Logical graph {0} not found".format(json_path))
        if (ssid is None):
            ts = time.time()
            ssid = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%dT%H:%M:%S')
        with open(json_path) as df:
            lg = json.load(df)
            self._done_dict = dict()
            self._group_q = defaultdict(list)
            self._output_q = defaultdict(list)
            start_list = []
            for jd in lg['nodeDataArray']:
                lgn = LGNode(jd, self._group_q, self._done_dict, ssid)
                if (lgn.is_start()):
                    start_list.append(lgn)

            st_len = len(start_list)
            if (st_len != 1):
                raise GraphException("This logical graph has {0} 'Start' node.".format(st_len))
            self._start_node = start_list[0]

            for lk in lg['linkDataArray']:
                src = self._done_dict[lk['from']]
                tgt = self._done_dict[lk['to']]
                src.add_output(tgt)
                tgt.add_input(src)

        self._drop_list = []

    def to_pg_tpl(self, input_dict, curr_node=None, iid=0):
        """
        convert lg to physical graph template (pg_tpl)

        curr_node:  is always LG node
        input_dict: key - next LGNode id, val - prodcued DROP, which is pre-filled when
                    processing the input LGNode (defaultdict)
        iid:        instance id (integer)

        1. create (parallel) DROP instances of curr_node
        2. establish links from input DROPs to each DROP instances created in 1
            2.1 those input DROPs are obtained from input_dict

        Difference between LG links and PG links:
            LG links for traversing the LG
            PG links are created amongst DROPs

        1. traverse the graph from start
        """
        if (curr_node is None):
            curr_node = self.start_node

        for input_node in curr_node.inputs:
            if (not input_node.has_converted()):
                self.to_pg_tpl(input_dict, input_node)

        # This appears wrong, what if curr_node is also a scatter
        if (curr_node.group.is_scatter() and (not curr_node.group.has_converted())):
            curr_node = curr_node.group

        #depth first traversal
        if (curr_node.is_group()):
            curr_node.complete_conversion()
            if (curr_node.is_scatter()):
                for i in range(curr_node.dop):
                    #go over each child
                    for ch_node in curr_node.children:
                        self.to_pg_tpl(input_dict, ch_node, i)
            elif (curr_node.is_groupby()):
                """
                Implement a dict / hashtable
                use input DROP's iid as the key for the dict
                the keys in this dict determines the number of groups
                """
                grpby_dict = defaultdict(list)
                prod_list = input_dict[curr_node.id]
                for prod in prod_list:
                    #prod_iid = prod['iid']
                    grpby_dict[prod['iid']].append(prod)
                for prod_iid, drop_list in grpby_dict.iteritems():
                    # this should be a BarrierDrop
                    grpby_drop = curr_node.make_single_drop(iid=prod_iid)
                    for drp in drop_list:
                        drp.addOutput(grpby_drop)
                        grpby_drop.addInput(drp)
            elif (curr_node.is_gather()):
                """
                if gather is inside a scatter, need to identify the iid
                """
                prod_list = input_dict[curr_node.id]
                # this should be a BarrierDrop
                gather_drop = curr_node.make_single_drop(iid=curr_node.iid)
                try:
                    gather_width = int(curr_node.jd['num_of_inputs'])
                except:
                    gather_width = len(prod_list) # by default, gather all
                for pri in range(gather_width * curr_node.iid, gather_width * (curr_node.iid + 1)):
                    prodd = prod_list[pri]
                    prodd.addOutput(gather_drop)
                    gather_drop.addInput(prodd)
            else:
                # do gathering
                pass
        else:
            curr_drop = curr_node.make_single_drop(iid)
            curr_node.complete_conversion()
            # find curr_node's producers (DROPs)
            prod_list = input_dict[curr_node.id]
            #establish the PG links
            for prod in prod_list:
                prod.addOutput(curr_drop)
                curr_drop.addInput(prod)

            # prefil a list of producers for each "next" LGNode
            for next_node in curr_node.outputs:
                input_dict[next_node.id].append(curr_drop)
                self.to_pg_tpl(input_dict, next_node)

    @property
    def start_node(self):
        return self._start_node

    def _parse_local(self, in_dict, lg):
        """
        Parse out all DROPs at the local level (each level represents a box)
        Four types of boxes:
        1. "Diagram" root level
        2. "Group By" box
        3. "Gather" box
        4. "Scatter" box

        Levels can be nested and hierarchical
        """
        if (in_dict is None):
            """
            Entire diagram level
            """
            pass
        else:
            pass

    def get_ds_list(self):
        """
        get a list of DSS
        """
        pass

def para_lg(lg):
    """
    Parallelise the logical graph
    1. Parse lg into LG_node_graph,
    2 list all Data Scatters (DS),
    3. for each DS, determine DoP
    4. fork out new DROPs based on DoP for each DS
    5. produce pg (DROP definitions)
    """
    pass

def l2g(lg, num_procs):
    """
    A simple implementation to do the mapping with the following features:

    1. parallelise lg into lg_para
    2. convert lg_para into pyrros input matrix and pg (without partitions)
    3. run pyrros
    4. convert pyrros output matrix into pgp directly (physical graph partition)
    5. see chilesdospec.py examples for producing the final PG specification
    """
    pass
