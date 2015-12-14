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

class GLinkException(GraphException):
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
        self._h_level = None
        self._g_h = None
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
            l = '0'
            cg = self
            while(cg.has_group()):
                cg = cg.group
                l += '/{0}'.format(cg.gid)
            self._g_h = l
        return self._g_h

    def dop_diff(self, that_lgn):
        """
        dop difference between inner node/group and outer group
        e.g for each outer group, how many instances of inner nodes/groups
        """
        if (self.is_group() or that_lgn.is_group()):
            raise GraphException("Cannot compute dop diff between groups.")
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
            while(cg.gid != oln.gid):
                re_dop *= cg.group.dop
                cg = cg.group
                #print "re_dop = ", re_dop
            return re_dop
        else:
            raise GraphException("{0} and {1} are not hierarchically related".format(self.id, that_lgn.id))

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

    def is_start(self):
        return (not self.has_group())

    def is_group(self):
        return self._isgrp

    def is_scatter(self):
        return (self._isgrp and self._jd['category'] == 'SplitData')

    def is_gather(self):
        return (self._jd['category'] == 'DataGather')

    def is_groupby(self):
        return (self._jd['category'] == 'GroupBy')

    @property
    def dop(self):
        """
        Degree of Parallelism:  integer
        default:    1
        """
        if (self.is_scatter()):
            return 3 # dummy impl.
        else:
            return 1

    def h_dops(self):
        pass

    def make_oid(self, iid=0):
        """
        ssid_id_iid
        iid:    instance id
        """
        return "{0}_{1}_{2}".format(self._ssid, self.id, iid)

    def make_single_drop(self, iid='0', **kwargs):
        """
        make only one drop from a LG nodes
        one-one mapping

        Dummy implementation as of 09/12/15
        """
        oid = self.make_oid(iid)
        dropSpec = dropdict({'oid':oid, 'type':'plain', 'storage':'file'})
        kwargs['iid'] = iid
        kwargs['dt'] = self.jd['category']
        kwargs['nm'] = self.jd['text']
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
            self._start_list = []
            for jd in lg['nodeDataArray']:
                lgn = LGNode(jd, self._group_q, self._done_dict, ssid)
                if (lgn.is_start()):
                    self._start_list.append(lgn)

            self._lg_links = lg['linkDataArray']

            for lk in self._lg_links:
                src = self._done_dict[lk['from']]
                tgt = self._done_dict[lk['to']]
                self.validate_link(src, tgt)
                src.add_output(tgt)
                tgt.add_input(src)

        self._drop_list = []

        # key - lgn id, val - a list of pgns associated with this lgn
        self._drop_dict = defaultdict(list)

    def validate_link(self, src, tgt):
        if (not src.h_related(tgt)):
            raise GLinkException("{0} and {1} are not hierarchically related".format(src.id, tgt.id))
        if (src.is_scatter() or tgt.is_scatter()):
            raise GLinkException("Scatter construct {0} cannot link to another Scatter {1}".format(src.id, tgt.id))
        if (tgt.is_groupby()):
            if (src.is_group()):
                raise GLinkException("GroupBy {0} input must not be a group {1}".format(tgt.id, src.id))
            elif (len(tgt.inputs) > 0):
                raise GLinkException("GroupBy {0} already has input {2} other than {1}".format(tgt.id, src.id, tgt.inputs[0].id))
            elif (src.gid == 0):
                raise GLinkException("GroupBy {0} requires at least one Scatter around input {1}".format(tgt.id, src.id))
            elif (tgt.gid != 0):
                raise GraphException("GroupBy {0} cannot be embedded inside another group {1}".format(tgt.id, tgt.gid))
        if (src.is_groupby() and not (tgt.is_gather())):
            raise GLinkException("Output {1} from GroupBy {0} must be Gather, otherwise embbed {1} inside GroupBy {0}".format(src.id, tgt.id))


    def lgn_to_pgn(self, lgn, iid='0'):
        """
        convert logical graph node to physical graph node
        without considering pg links

        iid:    instance id (string)
        """
        if (lgn.is_group()):
            # do not create actual DROPs for group constructs
            self._drop_dict[lgn.id].append(iid)
            for i in range(lgn.dop):
                for child in lgn.children:
                    self.lgn_to_pgn(child, '{0}/{1}'.format(iid, i))
        else:
            self._drop_dict[lgn.id].append(lgn.make_single_drop(iid))

    def _split_list(self, l, n):
        """
        Yield successive n-sized chunks from l.
        """
        for i in xrange(0, len(l), n):
            yield l[i:i+n]

    def convert_to_tpl(self):
        """
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

            if (slgn.is_group() and (not tlgn.is_group())):
                #slgn cannot be groupby, since groupby's output must be a Scatter (i.e. group)
                #slgn cannot be scatter, since scatter does not link
                #so, slgn must be gather
                tdrops = self._drop_dict[tlgn.id]

            elif (slgn.is_group() and tlgn.is_group()):
                if (slgn.is_groupby()):
                    pass
                else: #slgn is gather
                    pass
            elif (not slgn.is_group() and (not tlgn.is_group())):
                chunk_size = slgn.dop_diff(tlgn)
                #print "chunk_size = {0}".format(chunk_size)
                sdrops = self._drop_dict[slgn.id]
                tdrops = self._drop_dict[tlgn.id]
                #print "len(tdrop) = ", len(tdrops), "len(sdrop) = ", len(sdrops)
                if (slgn.h_level >= tlgn.h_level):
                    for i, chunk in enumerate(self._split_list(sdrops, chunk_size)):
                        # distribute slgn evenly to tlgn
                        for sdrop in chunk:
                            sdrop.addOutput(tdrops[i])
                            tdrops[i].addInput(sdrop)
                else:
                    for i, chunk in enumerate(self._split_list(tdrops, chunk_size)):
                        # distribute tlgn evenly to slgn
                        for tdrop in chunk:
                            sdrops[i].addOutput(tdrop)
                            tdrop.addInput(sdrops[i])
            else: # slgn is not group, but tlgn is group
                sdrops = self._drop_dict[slgn.id]
                if (tlgn.is_groupby()):
                    grpby_dict = defaultdict(list)
                    for gdd in sdrops:
                        # the last bit of iid (current h id) is GrougBy key
                        gby = gdd['iid'].split('/')[-1] # TODO could be any bit in the future
                        grpby_dict[gby].append(gdd)
                    # for each group, create a Barrier DROP as AppDROP (Consumer)
                    del self._drop_dict[tlgn.id]
                    for gbyid, drop_list in grpby_dict.iteritems():
                        # this should be a BarrierDrop
                        grpby_drop = tlgn.make_single_drop(iid='0-grp_{0}'.format(gbyid)) # TODO make it barrier
                        self._drop_dict[tlgn.id].append(grpby_drop)
                        for drp in drop_list:
                            drp.addOutput(grpby_drop)
                            grpby_drop.addInput(drp)

    def _align_iid(self, lgnode, lgnode_ref, iid, expand_char='*'):
        """
        Helper function

        if expand_char is None, then do not expand iid at all
        """
        l = lgnode.h_level
        r = lgnode_ref.h_level

        if (l == r):
            return iid

        if (l > r):
            # shorten iid
            # e.g. 0/1/2/3 --> 0/1
            iids = iid.split('/')
            niid = ''
            for i in range(l + 1):
                niid += iids[i]
                if (i < l):
                    niid += '/'
            return niid
        elif (expand_char is not None):
            # expand idd with the expand_char
            # e.g. 0/1 --> 0/1/*/*
            for i in range(r - l):
                iid += '/{0}'.format(expand_char)
        else:
            return iid

    def _regex_iid(self, iid, regex_char='*'):
        """
        e.g.
        0/1/2/3 -->
        [0/1/2/*, 0/1/*/*, 0/*/*/*]
        """
        iids = str(iid).split("/")
        if (len(iids) == 1):
            return [str(iid)]

        relist = []
        for i in range(len(iids) - 1):
            iids[-1 * (i + 1)] = regex_char
            relist.append('/'.join(iids))

        return relist

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

        """
        if (curr_node is None):
            curr_node = self.start_node
        """

        # see if any inputs are missing
        for input_node in curr_node.inputs:
            if (input_node.has_converted()):
                continue

            if (not curr_node.h_related(input_node)):
                raise GraphException("{0} and {1} are not hierarchically related".format(curr_node.id, input_node.id))

            self.to_pg_tpl(input_dict, input_node, iid=self._align_iid(curr_node, input_node, iid, expand_char=None))

        if (curr_node.has_group()):
            if ((not curr_node.group.has_converted())):
                curr_node = curr_node.group
        else:
            pass

        #depth first traversal
        if (curr_node.is_group()):
            curr_node.complete_conversion()
            if (curr_node.is_scatter() and curr_node.has_child()):
                for i in range(curr_node.dop):
                    #go over each child
                    #for ch_node in curr_node.children:
                    # get a random one
                    ch_node = curr_node.children[0]
                    self.to_pg_tpl(input_dict, ch_node, '{0}/{1}'.format(iid, i))
            elif (curr_node.is_groupby()):
                """
                Implement a dict / hashtable
                use input DROP's iid as the key for the dict
                the keys in this dict determines the number of groups
                """
                grpby_dict = defaultdict(list)
                prod_list = input_dict[curr_node.make_oid(iid=iid)]
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
                prod_list = input_dict[curr_node.make_oid(iid=iid)]
                # this should be a BarrierDrop
                gather_drop = curr_node.make_single_drop(iid=iid)
                try:
                    gather_width = int(curr_node.jd['num_of_inputs'])
                except:
                    gather_width = len(prod_list) # by default, gather all
                for pri in range(gather_width * curr_node.iid, gather_width * (curr_node.iid + 1)):
                    prodd = prod_list[pri]
                    prodd.addOutput(gather_drop)
                    gather_drop.addInput(prodd)
            else:
                # do nothing
                pass
        else:
            curr_drop = curr_node.make_single_drop(iid)
            curr_node.complete_conversion()
            self._drop_list.append(curr_drop)
            # find curr_node's producers (DROPs)
            _iids = self._regex_iid(iid) + [iid]
            for _iid in _iids:
                prod_list = input_dict[curr_node.make_oid(iid=_iid)]
                #establish the PG links
                for prod in prod_list:
                    prod.addOutput(curr_drop)
                    curr_drop.addInput(prod)

            # prefill a list of producers for each "next" LGNode
            for next_node in curr_node.outputs:
                if (next_node.has_converted()):
                    continue
                niid = self._align_iid(curr_node, next_node, iid)
                input_dict[next_node.make_oid(iid=niid)].append(curr_drop)
                # TODO - need to restore the "target" hierarchy
                self.to_pg_tpl(input_dict, next_node, iid=niid)


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
