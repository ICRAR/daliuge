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

import collections
import datetime
import json
import logging
import math
import os
import random
import subprocess
import time

import networkx as nx
import numpy as np

from dfms.drop import dropdict
from dfms.dropmake.scheduler import MySarkarScheduler, DAGUtil, MinNumPartsScheduler, PSOScheduler


logger = logging.getLogger(__name__)

class GraphException(Exception):
    pass

class GInvalidLink(GraphException):
    pass

class GInvalidNode(GraphException):
    pass

class GPGTException(GraphException):
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
        self._id = '{0}#{1}'.format(self.text.replace('\n', '_'), jd['key'])
        self._ssid = ssid
        self._isgrp = False
        self._converted = False
        self._h_level = None
        self._g_h = None
        self._dop = None
        self._gaw = None
        self._grpw = None
        if 'isGroup' in jd and jd['isGroup'] == True:
            self._isgrp = True
            for wn in group_q[self.id]:
                wn.group = self
                self.add_child(wn)
            group_q.pop(self.id) # not thread safe

        if 'group' in jd:
            grp_id = jd['group']
            if grp_id in done_dict:
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
    def text(self):
        return self.jd.get('text', '')

    @property
    def group(self):
        return self._grp

    @group.setter
    def group(self, value):
        self._grp = value

    def has_group(self):
        return self.group is not None

    def has_converted(self):
        return self._converted
        """
        if (self.is_group()):
            return self._converted
        return False
        """

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
        if (lg_node.is_group() and (not (lg_node.is_scatter())) and (not (lg_node.is_loop())) and (not (lg_node.is_groupby()))):
            raise GInvalidNode("Only Scatters or Loops can be nested, but {0} is not Scatter".format(lg_node.id))
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

    @property
    def h_level(self):
        if (self._h_level is None):
            l = 0
            cg = self
            while(cg.has_group()):
                cg = cg.group
                l += 1
            self._h_level = l
        return self._h_level

    @property
    def group_hierarchy(self):
        if (self._g_h is None):
            glist = []
            cg = self
            while(cg.has_group()):
                glist.append(str(cg.gid))
                cg = cg.group
            glist.append('0')
            self._g_h = '/'.join(reversed(glist))
        return self._g_h

    def dop_diff(self, that_lgn):
        """
        dop difference between inner node/group and outer group
        e.g for each outer group, how many instances of inner nodes/groups
        """
        # if (self.is_group() or that_lgn.is_group()):
        #     raise GraphException("Cannot compute dop diff between groups.")
        if (self.h_related(that_lgn)):
            il = self.h_level
            al = that_lgn.h_level
            if (il == al):
                return 1
            elif (il > al):
                oln = that_lgn
                iln = self
            else:
                iln = that_lgn
                oln = self
            re_dop = 1
            cg = iln
            while(cg.gid != oln.gid and cg.has_group()):
                re_dop *= cg.group.dop
                cg = cg.group
            return re_dop
        else:
            raise GInvalidLink("{0} and {1} are not hierarchically related".format(self.id, that_lgn.id))

    def h_related(self, that_lgn):
        that_gh = that_lgn.group_hierarchy
        this_gh = self.group_hierarchy
        if (len(that_gh) + len(this_gh) <= 1):
            # at least one is at the root level
            return True

        return (that_gh.find(this_gh) > -1 or this_gh.find(that_gh) > -1)

    def has_child(self):
        return len(self._children) > 0

    def has_output(self):
        return len(self._outs) > 0

    def is_start_node(self):
        return self.jd['category'] == 'Start'

    def is_end_node(self):
        return self.jd['category'] == 'End'

    def is_start(self):
        return (not self.has_group())

    def is_start_listener(self):
        return (len(self.inputs) == 1 and self.inputs[0].jd['category'] == 'Start' and
        self.jd['category'] == 'Data')

    def is_group_start(self):
        return (self.has_group() and "group_start" in self.jd and 1 == int(self.jd["group_start"]))

    def is_group_end(self):
        return (self.has_group() and "group_end" in self.jd and 1 == int(self.jd["group_end"]))

    def is_group(self):
        return self._isgrp

    def is_scatter(self):
        return (self.is_group() and self._jd['category'] == 'SplitData')

    def is_gather(self):
        return (self._jd['category'] == 'DataGather')

    def is_loop(self):
        return (self.is_group() and self._jd['category'] == 'Loop')

    def is_groupby(self):
        return (self._jd['category'] == 'GroupBy')

    @property
    def group_keys(self):
        """
        Return:
            None or a list of keys (each key is an integer)
        """
        if (not self.is_groupby()):
            return None
        val = str(self.jd.get('group_key', 'None'))
        if (val in ['None', '-1', '']):
            return None
        else:
            try:
                return [int(x) for x in val.split(',')]
            except ValueError as ve:
                raise GraphException("group_key must be an integer or comma-separated integers: {0}".format(ve))

    def is_branch(self):
        return (self._jd['category'] == 'Branch')

    @property
    def gather_width(self):
        """
        Gather width
        """
        if (self.is_gather()):
            if (self._gaw is None):
                try:
                    self._gaw = int(self.jd['num_of_inputs'])
                except:
                    self._gaw = 1
            return self._gaw
        else:
            """
            TODO: use OO style to replace all type-related statements!
            """
            raise GraphException("Non-Gather LGN {0} does not have gather_width".format(self.id))

    @property
    def groupby_width(self):
        """
        GroupBy count
        """
        if (self.is_groupby()):
            if (self._grpw is None):
                tlgn = self.inputs[0]
                re_dop = 1
                cg = tlgn.group # exclude its own group
                while (cg.has_group()):
                    re_dop *= cg.group.dop
                    cg = cg.group
                self._grpw = re_dop
            return self._grpw
        else:
            raise GraphException("Non-GroupBy LGN {0} does not have groupby_width".format(self.id))

    @property
    def group_by_scatter_layers(self):
        """
        Return:
            scatter layers info associated with this group by logical node
            A tuple of three items:
                (1) DoP
                (2) layer indexes (list) from innser scatter to outer scatter
                (3) layers (list)
        """
        if (not self.is_groupby()):
            return None

        tlgn = self.inputs[0]
        grpks = self.group_keys
        ret_dop = 1
        layer_index = [] # from inner to outer
        layers = [] # from inner to outer
        c = 0
        if (tlgn.group.is_groupby()):
            #group by followed by another group by
            if (grpks is None or len(grpks) < 1):
                raise GInvalidNode("Must specify group_key for Group By '{0}'".format(self.text))
            # find the "root" groupby and get all of its scatters
            inputgrp = self
            while ((inputgrp is not None) and inputgrp.inputs[0].group.is_groupby()):
                inputgrp = inputgrp.inputs[0].group
            # inputgrp now is the "root" groupby that follows Scatter immiately
            # move it to Scatter
            inputgrp = inputgrp.inputs[0].group
            # go thru all the scatters
            while ((inputgrp is not None) and inputgrp.is_scatter()):
                if (inputgrp.id in grpks):
                    ret_dop *= inputgrp.dop
                    layer_index.append(c)
                    layers.append(inputgrp)
                inputgrp = inputgrp.group
                c += 1
        else:
            if (grpks is None or len(grpks) < 1):
                ret_dop = tlgn.group.dop
                layer_index.append(0)
                layers.append(tlgn.group)
            else:
                if (len(grpks) == 1):
                    if (grpks[0] == tlgn.group.id):
                        ret_dop = tlgn.group.dop
                        layer_index.append(0)
                        layers.append(tlgn.group)
                    else:
                        raise GInvalidNode("Wrong single group_key for {0}".format(self.text))
                else:
                    inputgrp = tlgn.group
                    # find the "groupby column list" from all layers of scatter loops
                    while ((inputgrp is not None) and inputgrp.is_scatter()):
                        if (inputgrp.id in grpks):
                            ret_dop *= inputgrp.dop
                            layer_index.append(c)
                            layers.append(inputgrp)
                        inputgrp = inputgrp.group
                        c += 1

        return (ret_dop, layer_index, layers)

    @property
    def dop(self):
        """
        Degree of Parallelism:  integer
        default:    1
        """
        if (self._dop is None):
            if (self.is_group()):
                if (self.is_scatter()):
                    for kw in ['num_of_copies', 'num_of_splits']:
                        if kw in self.jd:
                            self._dop = int(self.jd[kw])
                            break
                    if (self._dop is None):
                        self._dop = 4 # dummy impl.
                elif (self.is_gather()):
                    try:
                        tlgn = self.inputs[0]
                    except IndexError:
                        raise GInvalidLink("Gather '{0}' does not have input!".format(self.id))
                    if (tlgn.is_groupby()):
                        tt = tlgn.dop
                    else:
                        tt = self.dop_diff(tlgn)
                    self._dop = int(math.ceil(tt / float(self.gather_width)))
                elif (self.is_groupby()):
                    self._dop = self.group_by_scatter_layers[0]
                elif (self.is_loop()):
                    self._dop = self.jd.get('num_of_iter', 1)
                else:
                    raise GInvalidNode("Unrecognised (Group) Logical Graph Node: '{0}'".format(self._jd['category']))
            else:
                self._dop = 1
        return self._dop

    def make_oid(self, iid=0):
        """
        return:
            ssid_id_iid (string), where
            ssid:   session id
            id:     logical graph node key
            iid:    instance id (for the physical graph node)
        """
        return "{0}_{1}_{2}".format(self._ssid, self.id, iid)

    def _create_test_drop_spec(self, oid, kwargs):
        """
        TODO
        This is a test funciton only
        should be replaced by LGNode class specific methods
        """
        drop_type = self.jd['category']
        if (drop_type in ['Data', 'LSM', 'Metadata', 'GSM']):
            if 'data_volume' in self.jd:
                kwargs['dw'] = int(self.jd['data_volume']) #dw -- data weight
            else:
                kwargs['dw'] = 1
            if (self.is_start_listener()):
                #create socket listener DROP first
                dropSpec = dropdict({'oid':oid, 'type':'plain', 'storage':'memory'})
                dropSpec_socket = dropdict({'oid':"{0}-sock_lstnr".format(oid),
                'type':'app', 'app':'test.graphsRepository.SleepApp', 'nm':'lstnr', 'tw':1, 'sleepTime': 5})
                # tw -- task weight
                dropSpec_socket['autostart'] = 1
                kwargs['listener_drop'] = dropSpec_socket
                dropSpec_socket.addOutput(dropSpec)
            else:
                dropSpec = dropdict({'oid':oid, 'type':'plain', 'storage':'memory'})
                kwargs['dirname'] = '/tmp'
        elif (drop_type == 'Component'): # default generic component becomes "sleep and copy"
            dropSpec = dropdict({'oid':oid, 'type':'app', 'app':'test.graphsRepository.SleepApp'})
            if 'execution_time' in self.jd:
                sleepTime = int(self.jd['execution_time'])
            else:
                sleepTime = random.randint(3, 8)
            kwargs['tw'] = sleepTime
            kwargs['sleepTime'] = sleepTime
            dropSpec.update(kwargs)
        elif (drop_type == 'BashShellApp'):
            dropSpec = dropdict({'oid':oid, 'type':'app', 'app':'dfms.apps.bash_shell_app.BashShellApp'})
            if 'execution_time' in self.jd:
                kwargs['tw'] = int(self.jd['execution_time'])
            else:
                kwargs['tw'] = random.randint(3, 8)
            # add more arguments
            cmds = []
            for i in range(10):
                k = "Arg%02d" % (i + 1,)
                if not k in self.jd:
                    continue
                v = self.jd[k]
                if (v is not None and len(str(v)) > 0):
                    cmds.append(str(v))
            kwargs['command'] = ' '.join(cmds)
            dropSpec.update(kwargs)
        elif (drop_type == 'GroupBy'):
            dropSpec = dropdict({'oid':oid, 'type':'app', 'app':'test.graphsRepository.SleepApp'})
            dw = int(self.inputs[0].jd['data_volume']) * self.groupby_width
            dropSpec_grp = dropdict({'oid':"{0}-grp-data".format(oid), 'type':'plain', 'storage':'memory',
            'nm':'grpdata', 'dw':dw})
            kwargs['grp-data_drop'] = dropSpec_grp
            kwargs['tw'] = 1 # barriar literarlly takes no time for its own computation
            kwargs['sleepTime'] = 1
            dropSpec.addOutput(dropSpec_grp)
            dropSpec_grp.addProducer(dropSpec)
        elif (drop_type == 'DataGather'):
            dropSpec = dropdict({'oid':oid, 'type':'app', 'app':'test.graphsRepository.SleepApp'})
            gi = self.inputs[0]
            if (gi.is_groupby()):
                gii = gi.inputs[0]
                dw = int(gii.jd['data_volume']) * gi.groupby_width * self.gather_width
            else: # data
                dw = int(gi.jd['data_volume']) * self.gather_width
            dropSpec_gather = dropdict({'oid':"{0}-gather-data".format(oid),
            'type':'plain', 'storage':'memory', 'nm':'gthrdt', 'dw':dw})
            kwargs['gather-data_drop'] = dropSpec_gather
            kwargs['tw'] = 1
            kwargs['sleepTime'] = 1
            dropSpec.addOutput(dropSpec_gather)
            dropSpec_gather.addProducer(dropSpec)
        elif (drop_type == 'Branch'):
            # create an App first
            dropSpec = dropdict({'oid':oid, 'type':'app', 'app':'test.graphsRepository.SleepApp'})
            dropSpec_null = dropdict({'oid':"{0}-null_drop".format(oid), 'type':'plain',
            'storage':'null', 'nm':'null', 'dw':0})
            kwargs['null_drop'] = dropSpec_null
            kwargs['tw'] = 0
            kwargs['sleepTime'] = 1
            dropSpec.addOutput(dropSpec_null)
            dropSpec_null.addProducer(dropSpec)
        elif (drop_type in ['Start', 'End']):
            dropSpec = dropdict({'oid':oid, 'type':'plain', 'storage':'null'})
        elif (drop_type == 'Loop'):
            pass
        else:
            raise GraphException("Unknown DROP type: '{0}'".format(drop_type))
        return dropSpec

    def make_single_drop(self, iid='0', **kwargs):
        """
        make only one drop from a LG nodes
        one-one mapping

        Dummy implementation as of 09/12/15
        """
        oid = self.make_oid(iid)
        dropSpec = self._create_test_drop_spec(oid, kwargs)
        kwargs['iid'] = iid
        kwargs['dt'] = self.jd['category']
        kwargs['nm'] = self.text
        dropSpec.update(kwargs)
        return dropSpec

class PGT(object):
    """
    A DROP representation of Physical Graph Template
    """

    def __init__(self, drop_list):
        self._drop_list = drop_list
        self._extra_drops = [] # artifacts DROPs produced during L2G mapping
        self._dag = DAGUtil.build_dag_from_drops(self._drop_list)
        self._json_str = None
        self._oid_gid_map = dict()
        self._gid_island_id_map = dict()
        self._num_parts_done = 0
        self._partition_merged = False

    @property
    def drops(self):
        if (self._extra_drops is None):
            return self._drop_list
        else:
            return self._drop_list + self._extra_drops

    def to_partition_input(self, outf):
        """
        Convert to format for mapping and decomposition
        """
        raise GPGTException("Must be implemented by PGTP sub-class only")

    def get_opt_num_parts(self):
        """
        dummy for now
        """
        leng = len(self._drop_list)
        return int(math.ceil(leng / 10.0))

    def get_partition_info(self):
        return "No partitioning. - Completion time: {0}".format(DAGUtil.get_longest_path(self.dag, show_path=False)[1])

    @property
    def dag(self):
        """
            Return the networkx nx.DiGraph object

        The weight of the same edge (u, v) also depends.
        If it is called after the partitioning, it could have been zeroed
        if both u and v is allocated to the same DropIsland
        """
        return self._dag

    def pred_exec_time(self, app_drop_only=False, wk='weight'):
        """
        Predict execution time using the longest path length
        """
        if (app_drop_only):
            lp = DAGUtil.get_longest_path(self.dag, show_path=True)[0]
            G = self.dag
            return sum(G.node[u].get(wk, 0) for u in lp)
        else:
            return DAGUtil.get_longest_path(self.dag, show_path=False)[1]

    @property
    def json(self):
        """
            Return the JSON string representation of the PGT
        """
        if (self._json_str is None):
            self._json_str = self.to_gojs_json()
        return self._json_str
        # return self.to_gojs_json()

    def to_pg_spec(self, node_list, ret_str=True, num_isla=None):
        """
        convert pgt to pg specification, and map that to the hardware resources

        node_list:
            A list of nodes (list) from ALL islands
        """
        # num_nodes = toplogy.num_nodes
        # if (self._partitions is None):
        #     # but this will change to use HEFT scheduling algorithm for non-partitioned PGT
        #     raise GraphException("The PGT has no partitions, but non-partitioned mapping is not yet implemented.")
        # else:
        #     pass
        if ((node_list is None) or (0 == len(node_list))):
            raise GPGTException("Node list is empty!")
        if (0 == self._num_parts_done):
            raise GPGTException("The graph has not been partitioned yet")
        nodes_len = len(node_list)
        drop_list = self._drop_list + self._extra_drops
        num_parts = self._num_parts_done
        logger.info("Drops count: {0}, partitions count: {1}, nodes count: {2}".format(len(drop_list), num_parts, nodes_len))
        if (nodes_len < num_parts):
            if (isinstance(self, MySarkarPGTP)):
                self.merge_partitions(nodes_len)
                num_parts = nodes_len
            else:
                raise GPGTException("The node list {0} is smaller than {1}".format(nodes_len, num_parts))

        lm = self._oid_gid_map
        for drop in drop_list:
            oid = drop['oid']
            # For now, simply round robin, but need to consider
            # nodes cross islands which has
            #TODO consider distance between a pair of nodes
            pid = lm[oid] % num_parts
            drop['node'] = node_list[pid]

        if (ret_str):
            return json.dumps(drop_list, indent=2)
        else:
            return drop_list

    def to_gojs_json(self, string_rep=True):
        """
        Convert to JSON for visualisation in GOJS
        """
        G = self.dag
        ret = dict()
        ret['class'] = 'go.GraphLinksModel'
        nodes = []
        links = []
        key_dict = dict() # key - oid, value - GOJS key

        for i, drop in enumerate(self._drop_list):
            oid = drop['oid']
            node = dict()
            node['key'] = i + 1
            key_dict[oid] = i + 1
            node['oid'] = oid
            tt = drop['type']
            if ('plain' == tt):
                node['category'] = 'Data'
            elif ('app' == tt):
                node['category'] = 'Component'
            node['text'] = drop['nm']
            nodes.append(node)

        if (self._extra_drops is None):
            extra_drops = []
            remove_edges = []
            add_edges = [] # a list of tuples
            add_nodes = []
            for drop in self._drop_list:
                oid = drop['oid']
                myk = key_dict[oid]
                for i, oup in enumerate(G.successors(myk)):
                    link = dict()
                    link['from'] = myk
                    from_dt = 0 if drop['type'] == 'plain' else 1
                    to_dt = G.node[oup]['dt']
                    if (from_dt == to_dt):
                        to_drop = G.node[oup]['drop_spec']
                        if (from_dt == 0):
                            # add an extra app DROP
                            extra_oid = "{0}_TransApp_{1}".format(oid, i)
                            dropSpec = dropdict({'oid':extra_oid, 'type':'app',
                            'app':'dfms.drop.BarrierAppDROP', 'nm':'go_app', 'tw':1})
                            # create links
                            drop.addConsumer(dropSpec)
                            dropSpec.addInput(drop)
                            dropSpec.addOutput(to_drop)
                            to_drop.addProducer(dropSpec)
                            mydt = 1
                        else:
                            # add an extra data DROP
                            extra_oid = "{0}_TransData_{1}".format(oid, i)
                            dropSpec = dropdict({'oid':extra_oid, 'type':'plain',
                            'storage':'memory', 'nm':'go_data', 'dw':1})
                            drop.addOutput(dropSpec)
                            dropSpec.addProducer(drop)
                            dropSpec.addConsumer(to_drop)
                            to_drop.addInput(dropSpec)
                            mydt = 0
                        extra_drops.append(dropSpec)
                        lid = len(extra_drops) * -1
                        link['to'] = lid
                        endlink = dict()
                        endlink['from'] = lid
                        endlink['to'] = oup
                        links.append(endlink)
                        # global graph updates
                        # the new drop must have the same gid as the to_drop
                        add_nodes.append((lid, 1, mydt, dropSpec, G.node[oup].get('gid', None)))
                        remove_edges.append((myk, oup))
                        add_edges.append((myk, lid))
                        add_edges.append((lid, oup))
                    else:
                        link['to'] = oup
                    links.append(link)
            for gn in add_nodes:
                #logger.debug("added gid = {0} for new node {1}".format(gn[4], gn[0]))
                G.add_node(gn[0], weight=gn[1], dt=gn[2], drop_spec=gn[3], gid=gn[4])
            G.remove_edges_from(remove_edges)
            G.add_edges_from(add_edges)
            self._extra_drops = extra_drops
        else:
            for drop in self._drop_list:
                oid = drop['oid']
                myk = key_dict[oid]
                for oup in G.successors(myk):
                    link = dict()
                    link['from'] = myk
                    link['to'] = oup
                    links.append(link)

        # going through the extra_drops
        for i, drop in enumerate(self._extra_drops):
            oid = drop['oid']
            node = dict()
            node['key'] = (i + 1) * -1
            node['oid'] = oid
            tt = drop['type']
            if ('plain' == tt):
                node['category'] = 'Data'
            elif ('app' == tt):
                node['category'] = 'Component'
            node['text'] = drop['nm']
            nodes.append(node)

        ret['nodeDataArray'] = nodes
        ret['linkDataArray'] = links
        if (string_rep):
            return json.dumps(ret, indent=2)
        else:
            return ret

class MetisPGTP(PGT):
    """
    DROP and GOJS representations of Physical Graph Template with Partitions
    Based on METIS
    http://glaros.dtc.umn.edu/gkhome/metis/metis/overview
    """
    def __init__(self, drop_list, num_partitions=0, min_goal=0, par_label="Partition", ptype=0, ufactor=10):
        """
        num_partitions:  number of partitions supplied by users (int)
        TODO - integrate from within PYTHON module (using C API) soon!
        """
        super(MetisPGTP, self).__init__(drop_list)
        self._metis_path = "gpmetis" # assuming it is installed at the sys path
        if (num_partitions <= 0):
            self._num_parts = self.get_opt_num_parts()
        else:
            self._num_parts = num_partitions
        if (1 == min_goal):
            self._obj_type = 'vol'
        else:
            self._obj_type = 'cut'

        if (0 == ptype):
            self._ptype = 'kway'
        else:
            self._ptype = 'rb'

        self._par_label = par_label
        self._u_factor = ufactor
        self._metis_logs = []
        self._G = self.to_partition_input()
        self._metis = DAGUtil.import_metis()
        self._group_workloads = dict() # k - gid, v - a tuple of (tw, sz)

    def to_partition_input(self, outf=None):
        """
        Convert to METIS format for mapping and decomposition
        NOTE - Since METIS only supports Undirected Graph, we have to produce
        both upstream and downstream nodes to fit its input format
        """
        key_dict = dict() # key - oid, value - GOJS key
        drop_dict = dict() # key - oid, value - drop

        G = nx.Graph()
        G.graph['edge_weight_attr'] = 'weight'
        G.graph['node_weight_attr'] = 'tw'
        G.graph['node_size_attr'] = 'sz'

        for i, drop in enumerate(self._drop_list):
            oid = drop['oid']
            key_dict[oid] = i + 1 #METIS index starts from 1
            drop_dict[oid] = drop
        for i, drop in enumerate(self._drop_list):
            line = []
            oid = drop['oid']
            myk = key_dict[oid]
            if (myk != i + 1):
                raise GPGTException("GOJS key {0} is not ordered: {1}!".format(myk, i + 1))
            tt = drop['type']
            if ('plain' == tt):
                dst = 'consumers' # outbound keyword
                ust = 'producers'
                tw = 1 # task weight is zero for a Data DROP
                sz = drop.get('dw', 1) # size
            elif ('app' == tt):
                dst = 'outputs'
                ust = 'inputs'
                tw = drop['tw']
                sz = 1
            G.add_node(myk, tw=tw, sz=sz)
            adj_drops = [] #adjacent drops (all neighbours)
            if dst in drop:
                adj_drops += drop[dst]
            if ust in drop:
                adj_drops += drop[ust]

            for inp in adj_drops:
                line.append(str(key_dict[inp]))
                if ('plain' == tt):
                    lw = drop['dw']
                elif ('app' == tt):
                    lw = drop_dict[inp].get('dw', 1)
                if (lw <= 0):
                    lw = 1
                G.add_edge(myk, key_dict[inp], weight=lw)
        for e in G.edges(data=True):
            if (e[2]['weight'] == 0):
                e[2]['weight'] = 1
        return G

    def _set_metis_log(self, logtext):
        self._metis_logs = logtext.split("\n")

    def get_partition_info(self, entry_key=[' - Data movement:']):
        """
        partition parameter and log entry
        return a string
        """
        if (self._obj_type == 'vol'):
            min_g = "Total comm. volume"
        else:
            min_g = "Edge cut"
        if (self._ptype == 'kway'):
            pa = "K-way"
        else:
            pa = "Recursive bisect"
        ret = []
        pparam = "{0} partitions (asked) - Algorithm: {2} - Completion time: {4} - Min objective: {1} - Load balancing: {3}%".format(self._num_parts, min_g, pa, 101 - self._u_factor, DAGUtil.get_longest_path(self.dag, show_path=False)[1])
        #pparam = "{0} partitions (asked) - Algorithm: {2} - Min objective: {1} - Load balancing: {3}%".format(self._num_parts, min_g, pa, 101 - self._u_factor)
        ret.append(pparam)
        for l in self._metis_logs:
            for ek in entry_key:
                if (l.startswith(ek)):
                    ret.append(l)
        return " ".join(ret)

    def _parse_metis_output(self, metis_out, jsobj):
        """
        1. parse METIS result, and add group node into the GOJS json
        2. also update edge weight for self._dag
        """

        key_dict = dict() #k - gojs key, v - gojs group id
        groups = set()
        #group_weight = self._group_workloads# k - gid, v - a tuple of (tw, sz)
        #G = self._G
        start_k = len(self._drop_list) + 1
        for i, gid in enumerate(metis_out):
            key_dict[i + 1] = gid
            groups.add(gid)
            """
            myk = G.nodes()[i]
            gnode = G.node[myk]
            gnode['gid'] = gid # write back to the original bigraph
            if gid not in group_weight:
                group_weight[gid] = [0, 0]
            tt = group_weight[gid]
            tt[0] += gnode['tw']
            tt[1] += gnode['sz']
            """

        node_list = jsobj['nodeDataArray']
        gc = 0
        for node in node_list:
            if node['key'] in key_dict:
                gid = key_dict[int(node['key'])]
                node['group'] = gid + start_k
                self._oid_gid_map[node['oid']] = gid
        self._num_parts_done = len(groups)
        for gid in groups:
            gn = dict()
            gn['key'] = start_k + gid
            gn['isGroup'] = True
            gn['text'] = '{1}_{0}'.format(gid, self._par_label)
            node_list.append(gn)

        for e in self.dag.edges(data=True):
            if (key_dict[e[0]] == key_dict[e[1]]):
                e[2]['weight'] = 0

    def to_gojs_json(self, string_rep=True, outdict=None):
        jsobj = super(MetisPGTP, self).to_gojs_json(string_rep=False)
        #uid = uuid.uuid1()
        uid = int(time.time() * 1000)
        recursive_param = False if self._ptype == 'kway' else True
        if (recursive_param and self._obj_type == 'vol'):
            raise GPGTException("Recursive partitioning does not support total volume minimisation.")
        (edgecuts, metis_parts) = self._metis.part_graph(self._G,
        nparts=self._num_parts, recursive=recursive_param,
        objtype=self._obj_type, ufactor=self._u_factor)
        if (outdict is not None):
            outdict['edgecuts'] = edgecuts
        self._set_metis_log(" - Data movement: {0}".format(edgecuts))
        self._parse_metis_output(metis_parts, jsobj)
        if (string_rep):
            return json.dumps(jsobj, indent=2)
        else:
            return jsobj

    def merge_partitions(self, new_num_parts):
        """
        A lightweight partition merging that does not consider GOJS JSON script
        i.e. it works for real (large number of drops) but not for visualisation
        """
        # 0. parse the output and get all the partitions
        GG = self._G
        part_edges = defaultdict(int) #k: from_gid + to_gid, v: sum_of_weight
        for e in GG.edges(data=True):
            from_gid = GG.node[e[0]]['gid']
            to_gid = GG.node[e[1]]['gid']
            k = '{0}**{1}'.format(from_gid, to_gid)
            part_edges[k] += e[2]['weight']

        # 1. build the bi-directional graph again
        # with each partition being a node
        G = nx.Graph()
        G.graph['edge_weight_attr'] = 'weight'
        G.graph['node_weight_attr'] = 'tw'
        G.graph['node_size_attr'] = 'sz'
        for gid, v in self._group_workloads.iteritems():
            G.add_node(gid, tw=v[0], sz=v[1])
        for glinks, v in part_edges.iteritems():
            gl = glinks.split('**')
            G.add_edge(int(gl[0]), int(gl[1]), weight=v)

        (edgecuts, metis_parts) = self._metis.part_graph(G, nparts=new_num_parts)
        for i, island_id in metis_parts:
            gid = G.nodes()[i]
            self._gid_island_id_map[gid] = island_id

        # TODO add GOJS groups for visualisation
        self._partition_merged = True

class MySarkarPGTP(PGT):
    """
    use the MySarkarScheduler to produce the PGTP
    """
    def __init__(self, drop_list, num_partitions=0, par_label="Partition", max_dop=8, merge_parts=False):
        """
        num_partitions: 0 - only do the initial logical partition
                        >1 - does logical partition, partition mergeing and
                        physical mapping
        """
        super(MySarkarPGTP, self).__init__(drop_list)
        self._num_parts = num_partitions
        self._max_dop = max_dop # max dop per partition
        self._par_label = par_label
        self._lpl = None # longest path
        self._ptime = None # partition time
        self._merge_parts = merge_parts
        self._edge_cuts = None
        self._partitions = None

        self.init_scheduler()

    def init_scheduler(self):
        self._scheduler = MySarkarScheduler(self._drop_list, self._max_dop, dag=self.dag)

    def get_partition_info(self, entry_key=None):
        """
        partition parameter and log entry
        return a string
        """
        if (self._merge_parts):
            part_str = "{0} outer partitions requested, ".format(self._num_parts)
            part_str1 = " inner "
        else:
            part_str = ""
            part_str1 = ""
        ed_str = " - Data movement: {0}".format(self._edge_cuts)
        return "{6}{2}{8} partitions produced - Algorithm: {1} - Completion time: {3} - Max DoP: {5}{7}".format(self._num_parts,
        type(self).__name__, self._num_parts_done, self._lpl, self._ptime, self._max_dop, part_str, ed_str, part_str1)

    def to_partition_input(self, outf):
        pass

    def merge_partitions(self, new_num_parts):
        if (self._num_parts_done == 0):
            raise GPGTException("Graph is not yet partitioned, cannot merge partitions")
        if (self._num_parts_done <= new_num_parts or self._partition_merged):
            return
        G = self._scheduler._dag
        node_list = self._node_list
        inner_parts = self._inner_parts
        parts = self._partitions
        groups = self._groups
        key_dict = self._grp_key_dict
        in_out_part_map = dict()
        lengnow = len(node_list)
        outer_groups = set()
        leng = len(self._drop_list)
        if (new_num_parts > 1):
            self._edge_cuts = self._scheduler.merge_partitions(new_num_parts)
        else:
            # all parts share the same outer group (island) when # of island == 1
            ppid = leng + len(groups) + 2
            for part in parts:
                part.parent_id = ppid
            self._edge_cuts = 0
            for e in G.edges(data=True):
                self._edge_cuts += e[2].get('weight', 0)

        for part in parts:
            gid = part.parent_id
            outer_groups.add(gid)
            in_out_part_map[part.partition_id] = gid

        self._gid_island_id_map = in_out_part_map

        for gid in outer_groups:
            gn = dict()
            gn['key'] = gid
            gn['isGroup'] = True
            gn['text'] = 'Out_{0}_{1}'.format(gid - lengnow, self._par_label)
            node_list.append(gn)

        for node in node_list:
            ggid = node.get('group', None)
            if (ggid is not None):
                new_ggid = in_out_part_map[ggid]
                self._oid_gid_map[node['oid']] = new_ggid

        for ip in inner_parts:
            ip['group'] = in_out_part_map[ip['key']]

        for e in self.dag.edges(data=True):
            #zero edeges within the same partition after merging
            if (in_out_part_map.get(key_dict[e[0]], -0.1) == in_out_part_map.get(key_dict[e[1]], -0.2)):
                e[2]['weight'] = 0
        self._lpl = DAGUtil.get_longest_path(self.dag, show_path=False)[1]
        self._partition_merged = True

    def to_gojs_json(self, string_rep=True):
        self._num_parts_done, self._lpl, self._ptime, parts = self._scheduler.partition_dag()
        jsobj = super(MySarkarPGTP, self).to_gojs_json(string_rep=False)
        G = self._scheduler._dag
        self._partitions = parts
        #logger.debug("The same DAG? ", (G == self.dag))
        leng = len(self._drop_list)

        key_dict = dict() #k - gojs key, v - gojs group id
        groups = set()
        node_list = jsobj['nodeDataArray']
        for node in node_list:
            gid = G.node[node['key']]['gid']
            if (gid is None):
                raise GPGTException("Node {0} does not have a Partition".format(node['key']))
            node['group'] = gid
            key_dict[node['key']] = gid
            groups.add(gid)
            self._oid_gid_map[node['oid']] = gid

        inner_parts = []
        for gid in groups:
            gn = dict()
            gn['key'] = gid
            #logger.debug("group key = {0}".format(gid))
            gn['isGroup'] = True
            gn['text'] = '{1}_{0}'.format((gid - leng), self._par_label)
            node_list.append(gn)
            inner_parts.append(gn)

        self._node_list = node_list
        self._inner_parts = inner_parts
        self._groups = groups
        self._grp_key_dict = key_dict

        # if (self._merge_parts):
        #     pass
        # else:
        # get all the edge sum
        self._edge_cuts = 0
        for e in G.edges(data=True):
            self._edge_cuts += e[2].get('weight', 0)

        if (string_rep):
            return json.dumps(jsobj, indent=2)
        else:
            return jsobj

class MinNumPartsPGTP(MySarkarPGTP):
    def __init__(self, drop_list, deadline, num_partitions=0, par_label="Partition", max_dop=8, merge_parts=False, optimistic_factor=0.5):
        """
        num_partitions: 0 - only do the initial logical partition
                        >1 - does logical partition, partition mergeing and
                        physical mapping
        """
        self._deadline = deadline
        self._opf = optimistic_factor
        super(MinNumPartsPGTP, self).__init__(drop_list, num_partitions, par_label, max_dop, merge_parts)
        self._extra_drops = None # force it to re-calculate the extra drops due to extra links during linearisation

    def init_scheduler(self):
        self._scheduler = MinNumPartsScheduler(self._drop_list, self._deadline, max_dop=self._max_dop, dag=self.dag, optimistic_factor=self._opf)

class PSOPGTP(MySarkarPGTP):
    def __init__(self, drop_list, par_label="Partition", max_dop=8,
    deadline=None, topk=30, swarm_size=40):
        """
        PSO-based PGTP
        """
        self._deadline = deadline
        self._topk = topk
        self._swarm_size = swarm_size
        super(PSOPGTP, self).__init__(drop_list, 0, par_label, max_dop, False)
        self._extra_drops = None

    def init_scheduler(self):
        self._scheduler = PSOScheduler(self._drop_list, max_dop=self._max_dop,
        deadline=self._deadline, dag=self.dag, topk=self._topk, swarm_size=self._swarm_size)

class PyrrosPGTP(PGT):
    """
    DROP and GOJS representations of Physical Graph Template with Partitions
    Based on PYRROS
    http://www.cs.ucsb.edu/~tyang/papers/PYRROScode.html
    """

    def __init__(self, drop_list, num_partitions=1):
        """
        num_partitions:  number of partitions supplied by users (int)
        """
        super(PyrrosPGTP, self).__init__(drop_list)
        self._pyrros_path = "pysched1"
        if (num_partitions == 1):
            self._num_parts = self.get_opt_num_parts()
        else:
            self._num_parts = num_partitions

    def get_partition_info(self, entry_key=[' - Edgecut:']):
        """
        partition parameter and log entry
        return a string
        """
        return "{0} partitions (asked) - Algorithm: {1}".format(self._num_parts, "Pyrros")

    def to_partition_input(self, outf):
        """
        Convert to PYRROS format for mapping and decomposition
        """
        key_dict = dict() # key - oid, value - GOJS key
        links_dict = dict()
        drop_dict = dict() # key - oid, value - drop
        for i, drop in enumerate(self._drop_list):
            oid = drop['oid']
            key_dict[oid] = i
            drop_dict[oid] = drop

        with open(outf, "w") as f:
            f.write("{0}\n".format(len(self._drop_list)))
            for i, drop in enumerate(self._drop_list):
                oid = drop['oid']
                myk = key_dict[oid]
                tt = drop['type']
                if ('plain' == tt):
                    obk = 'consumers' # outbound keyword
                    tw = 1
                elif ('app' == tt):
                    obk = 'outputs'
                    tw = drop['tw']
                f.write("{0} {1} {2}\n".format(myk, tw, 0))
                if obk in drop:
                    oel = []
                    for oup in drop[obk]:
                        oel.append(str(key_dict[oup]))
                        if ('plain' == tt):
                            oel.append(str(drop['dw']))
                        elif ('app' == tt):
                            oel.append(str(drop_dict[oup]['dw']))
                    oel.append('-1')
                    f.write("{0}\n".format(" ".join(oel)))
            f.write("-1\n")

    def _parse_pyrros_output(self, pyrros_out, jsobj):
        """
        parse pyrros result, and add group node into the GOJS json
        """

        key_dict = dict() #k - gojs key, v - gojs group id
        groups = set()

        start_k = len(self._drop_list) + 1
        with open(pyrros_out, "r") as f:
            num_pts = int(f.readline())
            if (num_pts != self._num_parts):
                #self._num_parts = num_pts
                raise GPGTException("Inconsistent number of partitions: {0} != {1}".format(num_pts,
                self._num_parts))

            line = f.readline()
            while (len(line) > 0):
                #logger.debug("\t" + line)
                la = line.split()
                key_dict[int(la[0])] = int(la[1])
                groups.add(int(la[1]))
                line = f.readline()

        node_list = jsobj['nodeDataArray']
        for node in node_list:
            if node['key'] in key_dict:
                gid = key_dict[int(node['key'])] + start_k
                node['group'] = gid
                logger.debug("{0} --> {1}".format(node['key'], gid))
            else:
                logger.debug("Node without group: {0}".format(node['key']))

        for gid in groups:
            gn = dict()
            gn['key'] = start_k + gid
            logger.debug("group key = {0}".format(start_k + gid))
            gn['isGroup'] = True
            gn['text'] = 'Island_{0}'.format(gid)
            node_list.append(gn)


    def to_gojs_json(self, string_rep=True):
        jsobj = super(PyrrosPGTP, self).to_gojs_json(string_rep=False)
        #uid = uuid.uuid1()
        uid = "pgtp"

        pyrros_in = "/tmp/{0}_pyrros.in".format(uid)
        pyrros_out = "/tmp/{0}_pyrros.out".format(uid)
        remove_list = [pyrros_in, pyrros_out]
        for f in remove_list:
            if (os.path.exists(f)):
                os.remove(f)
        try:
            self.to_partition_input(pyrros_in)
            if (os.path.exists(pyrros_in) and os.stat(pyrros_in).st_size > 0):
                cmd = "{0} -i {1} -p {2} -x {3}".format(self._pyrros_path,
                pyrros_in, self._num_parts, pyrros_out)
                ret = subprocess.call(cmd)
                if (14848 == ret[0] and
                os.path.exists(pyrros_out) and
                os.stat(pyrros_out).st_size > 0):
                    self._parse_pyrros_output(pyrros_out, jsobj)
                    if (string_rep):
                        return json.dumps(jsobj, indent=2)
                    else:
                        return jsobj
                else:
                    err_msg = "PYRROS Schedule failed:\n'{2}': {0}/\n{1}".format(ret[0], ret[1], cmd)
                    raise GPGTException(err_msg)
        finally:
            for f in remove_list:
                if (os.path.exists(f)):
                    pass
                    #os.remove(f)

class LG():
    """
    An object representation of Logical Graph
    """

    def __init__(self, json_path, ssid=None):
        """
        parse JSON into LG object graph first
        """
        self._g_var = []
        if (not os.path.exists(json_path)):
            raise GraphException("Logical graph {0} not found".format(json_path))
        if (ssid is None):
            ts = time.time()
            ssid = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%dT%H:%M:%S')
        self._session_id = ssid
        with open(json_path) as df:
            lg = json.load(df)
            self._done_dict = dict()
            self._group_q = collections.defaultdict(list)
            self._output_q = collections.defaultdict(list)
            self._start_list = []
            all_list = []
            for jd in lg['nodeDataArray']:
                if (jd['category'] == 'Comment'):
                    continue
                lgn = LGNode(jd, self._group_q, self._done_dict, ssid)
                all_list.append(lgn)

            for lgn in all_list:
                if (lgn.is_start() and lgn.jd["category"] != "Comment"):
                    if (lgn.jd["category"] == "Variables"):
                        self._g_var.append(lgn)
                    else:
                        self._start_list.append(lgn)

            self._lg_links = lg['linkDataArray']

            for lk in self._lg_links:
                src = self._done_dict[lk['from']]
                tgt = self._done_dict[lk['to']]
                self.validate_link(src, tgt)
                src.add_output(tgt)
                tgt.add_input(src)

        # key - lgn id, val - a list of pgns associated with this lgn
        self._drop_dict = collections.defaultdict(list)
        del all_list

    def validate_link(self, src, tgt):
        if (src.is_scatter() or tgt.is_scatter()):
            raise GInvalidLink("Scatter construct {0} or {1} cannot be linked".format(src.text, tgt.text))

        if (src.is_loop() or tgt.is_loop()):
            raise GInvalidLink("Loop construct {0} or {1} cannot be linked".format(src.text, tgt.text))

        if (src.is_gather()):
            if (not (tgt.jd['category'] in ['Component', 'BashShellApp'] and tgt.is_group_start() and src.inputs[0].h_level == tgt.h_level)):
                raise GInvalidLink("Gather {0}'s output {1} must be a Group-Start Component inside a Group with the same H level as Gather's input".format(src.id, tgt.id))
            #raise GInvalidLink("Gather {0} cannot be the input".format(src.id))
        elif (src.is_branch()):
            if (tgt.jd['category'] not in ['Component', 'BashShellApp'] and (not tgt.is_end_node())):
                raise GInvalidLink("Branch {0}'s output {1} must be Component".format(src.id, tgt.id))

        if (tgt.is_groupby()):
            if (src.is_group()):
                raise GInvalidLink("GroupBy {0} input must not be a group {1}".format(tgt.id, src.id))
            elif (len(tgt.inputs) > 0):
                raise GInvalidLink("GroupBy {0} already has input {2} other than {1}".format(tgt.id, src.id, tgt.inputs[0].id))
            elif (src.gid == 0):
                raise GInvalidLink("GroupBy {0} requires at least one Scatter around input {1}".format(tgt.id, src.id))
        elif (tgt.is_gather()):
            if (src.jd['category'] != 'Data' and (not src.is_groupby())):
                raise GInvalidLink("Gather {0}'s input {1} should be either a GroupBy or Data".format(tgt.id, src.id))
        elif (tgt.is_branch()):
            if (src.jd['category'] != 'Data'):
                raise GInvalidLink("Branch {0}'s input {1} should be Data".format(tgt.id, src.id))

        if (src.is_groupby() and not (tgt.is_gather())):
            raise GInvalidLink("Output {1} from GroupBy {0} must be Gather, otherwise embbed {1} inside GroupBy {0}".format(src.id, tgt.id))

        elif (src.is_branch()):
            o = src.outputs
            if (len(o) < 2):
                pass
            else:
                raise GInvalidLink("Branch {0} must have two outputs, but it has {1} now".format(src.id, len(o)))

        if (not src.h_related(tgt)):
            raise GInvalidLink("{0} and {1} are not hierarchically related: {2} and {3}".format(src.id, tgt.id,
            src.group_hierarchy,
            tgt.group_hierarchy))

    def lgn_to_pgn(self, lgn, iid='0'):
        """
        convert logical graph node to physical graph node
        without considering pg links

        iid:    instance id (string)
        """
        if (lgn.is_group()):
            extra_links_drops = (not lgn.is_scatter())
            if (extra_links_drops):
                non_inputs = []
                grp_starts = []
                grp_ends = []
                for child in lgn.children:
                    if (len(child.inputs) == 0):
                        non_inputs.append(child)
                    if (child.is_group_start()):
                        grp_starts.append(child)
                    elif (child.is_group_end()):
                        grp_ends.append(child)
                if (len(grp_starts) == 0):
                    gs_list = non_inputs
                else:
                    gs_list = grp_starts
                if (lgn.is_loop()):
                    if (len(grp_starts) == 0 or len(grp_ends) == 0):
                        raise GInvalidNode("Loop '{0}' should have at least one Start Component and one End Data".format(lgn.text))
                    for ge in grp_ends:
                        for gs in grp_starts: # make an artificial circle
                            ge.add_output(gs)
                            gs.add_input(ge)
                            lk = dict()
                            lk['from'] = ge.id
                            lk['to'] = gs.id
                            self._lg_links.append(lk)
                else:
                    for gs in gs_list: # add artificial logical links to the "first" children
                        lgn.add_input(gs)
                        gs.add_output(lgn)
                        lk = dict()
                        lk['from'] = lgn.id
                        lk['to'] = gs.id
                        self._lg_links.append(lk)

            multikey_grpby = False
            lgk = lgn.group_keys
            if (lgk is not None and len(lgk) > 1):
                multikey_grpby = True
                scatters = lgn.group_by_scatter_layers[2] # inner most scatter to outer most scatter
                shape = [x.dop for x in scatters] # inner most is also the slowest running index

            for i in range(lgn.dop):
                miid = '{0}/{1}'.format(iid, i)
                if (multikey_grpby):
                    #set up more refined hierarchical context for group by with multiple keys
                    # recover multl-dimension indexes from i
                    grp_h = np.unravel_index(i, shape)
                    grp_h = [str(x) for x in grp_h]
                    miid += "${0}".format('-'.join(grp_h))

                if (extra_links_drops and not lgn.is_loop()): # make GroupBy and Gather drops
                    src_gdrop = lgn.make_single_drop(miid)
                    self._drop_dict[lgn.id].append(src_gdrop)
                    if (lgn.is_groupby()):
                        self._drop_dict['new_added'].append(src_gdrop['grp-data_drop'])
                    elif (lgn.is_gather()):
                        self._drop_dict['new_added'].append(src_gdrop['gather-data_drop'])

                for child in lgn.children:
                    self.lgn_to_pgn(child, miid)
        else:
            #TODO !!
            src_drop = lgn.make_single_drop(iid)
            self._drop_dict[lgn.id].append(src_drop)
            if (lgn.is_branch()):
                self._drop_dict['new_added'].append(src_drop['null_drop'])
            elif (lgn.is_start_listener()):
                self._drop_dict['new_added'].append(src_drop['listener_drop'])

    def _split_list(self, l, n):
        """
        Yield successive n-sized chunks from l.
        """
        for i in range(0, len(l), n):
            yield l[i:i+n]

    def _unroll_gather_as_output(self, slgn, tlgn, sdrops, tdrops, chunk_size):
        if (slgn.h_level < tlgn.h_level):
            raise GraphException("Gather {0} has higher h-level than its input {1}".format(tlgn.id, slgn.id))
        # src must be data
        for i, chunk in enumerate(self._split_list(sdrops, chunk_size)):
            for sdrop in chunk:
                self._link_drops(slgn, tlgn, sdrop, tdrops[i])

    def _get_chunk_size(self, s, t):
        """
        Assumption:
        s or t cannot be Scatter as Scatter does not convert into DROPs
        """
        if (t.is_gather()):
            ret = t.gather_width
        elif (t.is_groupby()):
            ret = t.groupby_width
        else:
            ret = s.dop_diff(t)
        return ret

    def _link_drops(self, slgn, tlgn, src_drop, tgt_drop):
        """
        """
        if (slgn.is_branch()):
            sdrop = src_drop['null_drop']
        elif (slgn.is_gather()):
            sdrop = src_drop['gather-data_drop']
        elif (slgn.is_groupby()):
            sdrop = src_drop['grp-data_drop']
        else:
            sdrop = src_drop

        tdrop = tgt_drop
        if (slgn.jd['category'] in ['Component', 'BashShellApp']):
            sdrop.addOutput(tdrop)
            tdrop.addProducer(sdrop)
        else:
            sdrop.addConsumer(tdrop)
            tdrop.addInput(sdrop)

    def unroll_to_tpl(self):
        """
        Not thread-safe!

        1. just create pgn anyway
        2. sort out the links
        """
        # each pg node needs to be taggged with iid
        # based purely on its h-level
        for lgn in self._start_list:
            self.lgn_to_pgn(lgn)

        for lk in self._lg_links:
            sid = lk['from'] # source
            tid = lk['to'] # target
            slgn = self._done_dict[sid]
            tlgn = self._done_dict[tid]
            sdrops = self._drop_dict[sid]
            tdrops = self._drop_dict[tid]
            chunk_size = self._get_chunk_size(slgn, tlgn)
            if (slgn.is_group() and (not tlgn.is_group())):
                # this link must be artifically added (within group link)
                # since
                # 1. GroupBy's "natual" output must be a Scatter (i.e. group)
                # 2. Scatter "naturally" does not have output
                if (slgn.is_gather() and tlgn.gid != sid): # not the artifical link between gather and its own start child
                    # gather iteration case, tgt must be a Group-Start Component
                    for i, ga_drop in enumerate(sdrops):
                        j = (i + 1) * slgn.gather_width
                        if (j >= tlgn.group.dop and j % tlgn.group.dop == 0):
                            continue
                        while (j < (i + 2) * slgn.gather_width and j < tlgn.group.dop * (i + 1)):
                            if 'gather-data_drop' in ga_drop:
                                gddrop = ga_drop['gather-data_drop'] # this is the "true" target (not source!) drop
                                gddrop.addConsumer(tdrops[j])
                                tdrops[j].addInput(gddrop)
                                j += 1
                else:
                    if (len(sdrops) != len(tdrops)):
                        err_info = "For within-group links, # {2} Group Inputs {0} must be the same as # {3} of Component Outputs {1}".format(slgn.id,
                        tlgn.id, len(sdrops), len(tdrops))
                        raise GraphException(err_info)
                    for i, sdrop in enumerate(sdrops):
                        self._link_drops(slgn, tlgn, sdrop, tdrops[i])
            elif (slgn.is_group() and tlgn.is_group()):
                # slgn must be GroupBy and tlgn must be Gather
                self._unroll_gather_as_output(slgn, tlgn, sdrops, tdrops, chunk_size)
            elif (not slgn.is_group() and (not tlgn.is_group())):
                if (slgn.is_start_node() or tlgn.is_end_node()):
                    continue
                elif ((slgn.group is not None) and slgn.group.is_loop() and slgn.gid == tlgn.gid and slgn.is_group_end() and tlgn.is_group_start()):
                    # Re-link to the next iteration's start
                    lsd = len(sdrops)
                    if (lsd != len(tdrops)):
                        raise GraphException("# of sdrops '{0}' != # of tdrops '{1}'for Loop '{2}'".format(slgn.text,
                        tlgn.text, slgn.group.text))
                    # first add the outer construct (scatter, gather, group-by) boundary
                    # oc = slgn.group.group
                    # if (oc is not None and (not oc.is_loop())):
                    #     pass
                    loop_chunk_size = slgn.group.dop
                    for i, chunk in enumerate(self._split_list(sdrops, loop_chunk_size)):
                        #logger.debug("{0} ** {1}".format(i, loop_chunk_size))
                        for j, sdrop in enumerate(chunk):
                            #logger.debug("{0} -- {1}".format(j, loop_chunk_size))
                            if (j < loop_chunk_size - 1):
                                self._link_drops(slgn, tlgn, sdrop, tdrops[i * loop_chunk_size + j + 1])
                                #logger.debug("{0} --> {1}".format(i * loop_chunk_size + j, i * loop_chunk_size + j + 1))

                    # for i, sdrop in enumerate(sdrops):
                    #     if (i < lsd - 1):
                    #         self._link_drops(slgn, tlgn, sdrop, tdrops[i + 1])

                else:
                    if (slgn.h_level >= tlgn.h_level):
                        for i, chunk in enumerate(self._split_list(sdrops, chunk_size)):
                            # distribute slgn evenly to tlgn
                            for sdrop in chunk:
                                self._link_drops(slgn, tlgn, sdrop, tdrops[i])
                    else:
                        for i, chunk in enumerate(self._split_list(tdrops, chunk_size)):
                            # distribute tlgn evenly to slgn
                            for tdrop in chunk:
                                self._link_drops(slgn, tlgn, sdrops[i], tdrop)
            else: # slgn is not group, but tlgn is group
                if (tlgn.is_groupby()):
                    grpby_dict = collections.defaultdict(list)
                    layer_index = tlgn.group_by_scatter_layers[1]
                    for gdd in sdrops:
                        src_ctx = gdd['iid'].split('/')
                        if (tlgn.group_keys is None):
                            # the last bit of iid (current h id) is the local GrougBy key, i.e. inner most loop context id
                            gby = src_ctx[-1]
                            if (slgn.h_level - 2 == tlgn.h_level and tlgn.h_level > 0): #groupby itself is nested inside a scatter
                            # group key consists of group context id + inner most loop context id
                                gctx = '/'.join(src_ctx[0:-2])
                                gby = gctx + '/' + gby
                        else:
                            # find the "group by" scatter level
                            gbylist = []
                            if (slgn.group.is_groupby()): # a chain of group bys
                                try:
                                    src_ctx = gdd['iid'].split('$')[1].split('-')
                                except IndexError:
                                    raise GraphException("The group by hiearchy in the multi-key group by '{0}' is not specified for node '{1}'".format(slgn.group.text, slgn.text))
                            else:
                                src_ctx.reverse()
                            for lid in layer_index:
                                gbylist.append(src_ctx[lid])
                            gby = '/'.join(gbylist)
                        grpby_dict[gby].append(gdd)
                    grp_keys = grpby_dict.keys()
                    if (len(grp_keys) != len(tdrops)):
                        # this happens when groupby itself is nested inside a scatter
                        raise GraphException("# of Group keys {0} != # of Group Drops {1} for LGN {2}".format(len(grp_keys),
                        len(tdrops),
                        tlgn.id))
                    grp_keys = sorted(grp_keys)
                    for i, gk in enumerate(grp_keys):
                        grpby_drop = tdrops[i]
                        drop_list = grpby_dict[gk]
                        for drp in drop_list:
                            self._link_drops(slgn, tlgn, drp, grpby_drop)
                            """
                            drp.addOutput(grpby_drop)
                            grpby_drop.addInput(drp)
                            """
                elif (tlgn.is_gather()):
                    self._unroll_gather_as_output(slgn, tlgn, sdrops, tdrops, chunk_size)
                else:
                    raise GraphException("Unsupported target group {0}".format(tlgn.id))
        #clean up extra drops
        for lid, lgn in self._done_dict.items():
            if ((lgn.is_start_node() or lgn.is_end_node()) and lid in self._drop_dict):
                del self._drop_dict[lid]
            elif (lgn.is_branch()):
                for branch_drop in self._drop_dict[lid]:
                    if 'null_drop' in branch_drop:
                        del branch_drop['null_drop']
            elif (lgn.is_start_listener()):
                for sl_drop in self._drop_dict[lid]:
                    if 'listener_drop'in sl_drop:
                        del sl_drop['listener_drop']
            elif (lgn.is_groupby()):
                for sl_drop in self._drop_dict[lid]:
                    if 'grp-data_drop' in sl_drop:
                        del sl_drop['grp-data_drop']
            elif (lgn.is_gather()):
                for sl_drop in self._drop_dict[lid]:
                    if 'gather-data_drop' in sl_drop:
                        del sl_drop['gather-data_drop']

        ret = []
        for drop_list in self._drop_dict.values():
            ret += drop_list
        return ret

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
