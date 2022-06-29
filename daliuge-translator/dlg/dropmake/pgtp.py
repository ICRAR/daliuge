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

from collections import defaultdict
import json
import logging

import networkx as nx

from dlg.dropmake.pgt import PGT, GraphException
from dlg.dropmake.scheduler import (
    MySarkarScheduler,
    DAGUtil,
    MinNumPartsScheduler,
    PSOScheduler,
)
from dlg.common import DropType

logger = logging.getLogger(__name__)


class GPGTException(GraphException):
    pass


class MetisPGTP(PGT):
    """
    DROP and GOJS representations of Physical Graph Template with Partitions
    Based on METIS
    http://glaros.dtc.umn.edu/gkhome/metis/metis/overview
    """

    def __init__(
        self,
        drop_list,
        num_partitions=1,
        min_goal=0,
        par_label="Partition",
        ptype=0,
        ufactor=10,
        merge_parts=False,
    ):
        """
        num_partitions:  number of partitions supplied by users (int)
        TODO - integrate from within PYTHON module (using C API) soon!
        """
        super(MetisPGTP, self).__init__(drop_list, build_dag=False)
        self._metis_path = "gpmetis"  # assuming it is installed at the sys path
        if num_partitions <= 0:
            # self._num_parts = self.get_opt_num_parts()
            raise GPGTException("Invalid num_partitions {0}".format(num_partitions))
        else:
            self._num_parts = num_partitions
        if 1 == min_goal:
            self._obj_type = "vol"
        else:
            self._obj_type = "cut"

        if 0 == ptype:
            self._ptype = "kway"
        else:
            self._ptype = "rb"

        self._par_label = par_label
        self._u_factor = ufactor
        self._metis_logs = []
        self._G = self.to_partition_input()
        self._metis = DAGUtil.import_metis()
        self._group_workloads = dict()  # k - gid, v - a tuple of (tw, sz)
        self._merge_parts = merge_parts
        self._metis_out = None  # initial internal partition result

    def to_partition_input(self, outf=None):
        """
        Convert to METIS format for mapping and decomposition
        NOTE - Since METIS only supports Undirected Graph, we have to produce
        both upstream and downstream nodes to fit its input format
        """
        key_dict = dict()  # key - oid, value - GOJS key
        droplist = self._drop_list

        G = nx.Graph()
        G.graph["edge_weight_attr"] = "weight"
        G.graph["node_weight_attr"] = "tw"
        G.graph["node_size_attr"] = "sz"

        for i, drop in enumerate(droplist):
            oid = drop["oid"]
            key_dict[oid] = i + 1  # METIS index starts from 1

        logger.info("Metis partition input progress - dropdict is built")

        if self._drop_list_len > 1e7:
            import resource

            logger.info(
                "self._drop_list, max RSS: %.2f GB",
                resource.getrusage(resource.RUSAGE_SELF)[2] / 1024.0 ** 2
            )

        for i, drop in enumerate(droplist):
            oid = drop["oid"]
            myk = i + 1
            tt = drop["type"]
            if DropType.PLAIN == tt:
                dst = "consumers"  # outbound keyword
                ust = "producers"
                tw = 1  # task weight is zero for a Data DROP
                sz = drop.get("dw", 1)  # size
            elif DropType.APP == tt:
                dst = "outputs"
                ust = "inputs"
                tw = drop["tw"]
                sz = 1
            G.add_node(myk, tw=tw, sz=sz, oid=oid)
            adj_drops = []  # adjacent drops (all neighbours)
            if dst in drop:
                adj_drops += drop[dst]
            if ust in drop:
                adj_drops += drop[ust]

            for inp in adj_drops:
                key = list(inp.keys())[0] if isinstance(inp, dict) else inp
                if DropType.PLAIN == tt:
                    lw = drop["dw"]
                elif DropType.APP == tt:
                    # lw = drop_dict[inp].get('dw', 1)
                    lw = droplist[key_dict[key] - 1].get("dw", 1)
                if lw <= 0:
                    lw = 1
                G.add_edge(myk, key_dict[key], weight=lw)
        # for e in G.edges(data=True):
        #     if (e[2]['weight'] == 0):
        #         e[2]['weight'] = 1
        if self._drop_list_len > 1e7:
            import resource

            logger.info(
                "Max RSS after creating the Graph: %.2f GB",
                resource.getrusage(resource.RUSAGE_SELF)[2] / 1024.0 ** 2
            )
        return G

    def _set_metis_log(self, logtext):
        self._metis_logs = logtext.split("\n")

    def get_partition_info(self):
        """
        partition parameter and log entry
        return a string
        """
        return "METIS_LB{0}".format(101 - self._u_factor)

    def _extra_result(self, ret):
        ret["num_parts"] = self._num_parts

    def _parse_metis_output(self, metis_out, jsobj):
        """
        1. parse METIS result, and add group node into the GOJS json
        2. also update edge weight for self._dag
        """
        # key_dict = dict() #k - gojs key, v - gojs group id
        groups = set()
        ogm = self._oid_gid_map
        group_weight = self._group_workloads  # k - gid, v - a tuple of (tw, sz)
        G = self._G
        # start_k = len(self._drop_list) + 1
        start_k = self._drop_list_len + 1
        for gnode, gid in zip(G.nodes(data=True), metis_out):
            groups.add(gid)
            gnode = gnode[1]
            ogm[gnode["oid"]] = gid
            gnode["gid"] = gid

        # house keeping after partitioning
        self._num_parts_done = len(groups)
        if self.dag is not None:
            for e in self.dag.edges(data=True):
                gid0 = metis_out[e[0] - 1]
                gid1 = metis_out[e[1] - 1]
                if gid0 == gid1:
                    e[2]["weight"] = 0

        # the following is for potential partition merging into islands
        if self._merge_parts:
            for gid in groups:
                group_weight[gid] = [0, 0]
            for gnode in G.nodes(data=True):
                tt = group_weight[gnode[1]["gid"]]
                tt[0] += gnode[1]["tw"]
                tt[1] += gnode[1]["sz"]
        # the following is for visualisation using GOJS
        if jsobj is not None:
            node_list = jsobj["nodeDataArray"]
            for node in node_list:
                nid = int(node["key"])
                gid = metis_out[nid - 1]
                node["group"] = gid + start_k

            inner_parts = []
            for gid in groups:
                gn = dict()
                gn["key"] = start_k + gid
                gn["isGroup"] = True
                gn["text"] = "{1}_{0}".format(gid + 1, self._par_label)
                node_list.append(gn)
                inner_parts.append(gn)

            self._inner_parts = inner_parts
            self._node_list = node_list

    def to_gojs_json(self, string_rep=True, outdict=None, visual=False):
        """
        Partition the PGT into a real "PGT with Partitions", thus PGTP, using
        METIS built-in functions

        See METIS usage:
            http://metis.readthedocs.io/en/latest/index.html
        """
        if self._num_parts == 1:
            edgecuts = 0
            metis_parts = [0] * len(self._G.nodes())
        else:
            # prepare METIS parameters
            recursive_param = False if self._ptype == "kway" else True
            if recursive_param and self._obj_type == "vol":
                raise GPGTException(
                    "Recursive partitioning does not support\
                 total volume minimisation."
                )

            if self._drop_list_len > 1e7:
                import resource

                logger.info(
                    "RSS before METIS partitioning: %.2f GB",
                    resource.getrusage(resource.RUSAGE_SELF)[2] / 1024.0 ** 2
                )

            # Call METIS C-lib
            (edgecuts, metis_parts) = self._metis.part_graph(
                self._G,
                nparts=self._num_parts,
                recursive=recursive_param,
                objtype=self._obj_type,
                ufactor=self._u_factor,
            )

        # Output some partitioning result metadata
        if outdict is not None:
            outdict["edgecuts"] = edgecuts
        # self._set_metis_log(" - Data movement: {0}".format(edgecuts))
        self._data_movement = edgecuts

        if visual:
            if self.dag is None:
                self._dag = DAGUtil.build_dag_from_drops(self._drop_list)
            jsobj = super(MetisPGTP, self).to_gojs_json(string_rep=False, visual=visual)
        else:
            jsobj = None
        self._parse_metis_output(metis_parts, jsobj)
        self._metis_out = metis_parts
        self._gojs_json_obj = jsobj  # could be none if not visual
        if string_rep and jsobj is not None:
            return json.dumps(jsobj, indent=2)
        else:
            return jsobj

    def merge_partitions(
        self, new_num_parts, form_island=False, island_type=0, visual=False
    ):
        """
        This is called during resource mapping - deploying partitioned PGT to
        a list of nodes

        form_island:    If True, the merging will form `new_num_parts` logical
                        islands on top of existing partitions (i.e. nodes). this
                        is also known as "reference-based merging"

                        If False, the merging will physically merge current
                        partitions into `new_num_parts` new partitions (i.e. nodes)
                        Thus, there will be no node-island 'hierarchies' created

        island_type:    integer, 0 - data island, 1 - compute island

        """
        # 0. parse the output and get all the partitions
        if not self._can_merge(new_num_parts):
            return

        GG = self._G
        part_edges = defaultdict(int)  # k: from_gid + to_gid, v: sum_of_weight
        for e in GG.edges(data=True):
            from_gid = GG.nodes[e[0]]["gid"]
            to_gid = GG.nodes[e[1]]["gid"]
            k = "{0}**{1}".format(from_gid, to_gid)
            part_edges[k] += e[2]["weight"]

        # 1. build the bi-directional graph again
        # with each partition being a node
        G = nx.Graph()
        G.graph["edge_weight_attr"] = "weight"
        G.graph["node_weight_attr"] = "tw"
        G.graph["node_size_attr"] = "sz"
        for gid, v in self._group_workloads.items():
            # for compute islands, we need to count the # of nodes instead of
            # the actual workload
            twv = 1 if (island_type == 1) else v[0]
            G.add_node(gid, tw=twv, sz=v[1])
        for glinks, v in part_edges.items():
            gl = glinks.split("**")
            G.add_edge(int(gl[0]), int(gl[1]), weight=v)

        if new_num_parts == 1:
            (edgecuts, metis_parts) = (0, [0] * len(G.nodes()))
        else:
            (edgecuts, metis_parts) = self._metis.part_graph(
                G, nparts=new_num_parts, ufactor=1
            )
        tmp_map = self._gid_island_id_map
        islands = set()
        for gid, island_id in zip(G.nodes(), metis_parts):
            tmp_map[gid] = island_id
            islands.add(island_id)
        if not form_island:
            ogm = self._oid_gid_map
            gnodes = GG.nodes(data=True)
            for gnode in gnodes:
                gnode = gnode[1]
                oid = gnode["oid"]
                old_gid = gnode["gid"]
                new_gid = tmp_map[old_gid]
                ogm[oid] = new_gid
                gnode["gid"] = new_gid
            self._num_parts_done = new_num_parts
        else:
            if (
                island_type == 1
                and (self.dag is not None)
                and (self._metis_out is not None)
            ):
                # update intra-comp_island edge weight given it has a different
                # bandwith compared to inter-comp_island
                metis_out = self._metis_out
                for e in self.dag.edges(data=True):
                    gid0 = metis_out[e[0] - 1]
                    gid1 = metis_out[e[1] - 1]
                    if tmp_map[gid0] == tmp_map[gid1]:
                        e[2]["weight"] /= self._bw_ratio
                self._data_movement = None  # force to refresh data_movment
            # add GOJS groups for visualisation
            self._partition_merged = new_num_parts
            if visual:
                island_label = "%s_Island" % (
                    self._island_labels[island_type % len(self._island_labels)]
                )
                start_k = self._drop_list_len + 1
                # start_i = len(node_list) + 1
                start_i = start_k + self._num_parts_done
                node_list = self._node_list
                for island_id in islands:
                    # print('island_id = ', island_id)
                    gn = dict()
                    gn["key"] = island_id + start_i
                    gn["isGroup"] = True
                    gn["text"] = "{1}_{0}".format(island_id + 1, island_label)
                    node_list.append(gn)
                inner_parts = self._inner_parts
                for ip in inner_parts:
                    ip["group"] = tmp_map[ip["key"] - start_k] + start_i


class MySarkarPGTP(PGT):
    """
    use the MySarkarScheduler to produce the PGTP
    """

    def __init__(
        self,
        drop_list,
        num_partitions=0,
        par_label="Partition",
        max_dop=8,
        merge_parts=False,
    ):
        """
        num_partitions: 0 - only do the initial logical partition
                        >1 - does logical partition, partition mergeing and
                        physical mapping
                This parameter will simply ignored
                To control the number of partitions, please call
                def merge_partitions(self, new_num_parts, form_island=False)
        """
        super(MySarkarPGTP, self).__init__(drop_list, build_dag=False)
        self._dag = DAGUtil.build_dag_from_drops(self._drop_list, fake_super_root=False)
        self._num_parts = num_partitions
        self._max_dop = max_dop  # max dop per partition
        self._par_label = par_label
        # self._lpl = None # longest path
        self._ptime = None  # partition time
        self._merge_parts = merge_parts
        # self._edge_cuts = None
        self._partitions = None

        self.init_scheduler()

    def init_scheduler(self):
        self._scheduler = MySarkarScheduler(
            self._drop_list, self._max_dop, dag=self.dag
        )

    def get_partition_info(self):
        """
        partition parameter and log entry
        return a string
        """
        return "Edge Zero"

    def to_partition_input(self, outf):
        pass

    def _extra_result(self, ret):
        ret["num_parts"] = self._num_parts_done

    def merge_partitions(
        self, new_num_parts, form_island=False, island_type=0, visual=False
    ):
        """
        This is called during resource mapping - deploying partitioned PGT to
        a list of nodes

        form_island:    If True, the merging will form `new_num_parts` logical
                        islands on top of existing partitions (i.e. nodes)

                        If False, the merging will physically merge current
                        partitions into `new_num_parts` new partitions (i.e. nodes)
                        Thus, there will be no node-island 'hierarchies' created

        island_type:    integer, 0 - data island, 1 - compute island
        """
        if not self._can_merge(new_num_parts):
            return

        # G = self._scheduler._dag
        G = self.dag
        inner_parts = self._inner_parts
        parts = self._partitions
        groups = self._groups
        key_dict = self._grp_key_dict
        in_out_part_map = dict()
        outer_groups = set()
        if new_num_parts > 1:
            self._scheduler.merge_partitions(new_num_parts, bal_cond=island_type)
        else:
            # all parts share the same outer group (island) when # of island == 1
            ppid = self._drop_list_len + len(groups) + 1
            for part in parts:
                part.parent_id = ppid

        start_k = self._drop_list_len + 1
        start_i = start_k + self._num_parts_done

        for part in parts:
            # parent_id starts from
            # len(self._drop_list) + len(self._parts) + 1, which is the same as
            # start_i
            island_id = part.parent_id - start_i  # make sure island_id starts from 0
            outer_groups.add(island_id)
            in_out_part_map[part.partition_id - start_k] = island_id

        self._gid_island_id_map = in_out_part_map
        if not form_island:
            # update to new gid
            self_oid_gid_map = self._oid_gid_map
            for gnode_g in G.nodes(data=True):
                gnode = gnode_g[1]
                oid = gnode["drop_spec"]["oid"]
                ggid = gnode["gid"] - start_k
                new_ggid = in_out_part_map[ggid]
                self_oid_gid_map[oid] = new_ggid
            # for node in node_list:
            #     ggid = node.get('group', None)
            #     if (ggid is not None):
            #         new_ggid = in_out_part_map[ggid - start_k]
            #         self._oid_gid_map[node['oid']] = new_ggid
            self._num_parts_done = new_num_parts
        else:
            self._partition_merged = new_num_parts
            if island_type == 1:
                for e in self.dag.edges(data=True):
                    # update edege weights within the same compute island
                    if in_out_part_map.get(
                        key_dict[e[0]] - start_k, -0.1
                    ) == in_out_part_map.get(key_dict[e[1]] - start_k, -0.2):
                        # print("e[2]['weight'] =", e[2]['weight'])
                        e[2]["weight"] /= self._bw_ratio
            if visual:
                island_label = "%s_Island" % (
                    self._island_labels[island_type % len(self._island_labels)]
                )
                node_list = self._node_list
                for island_id in outer_groups:
                    gn = dict()
                    gn["key"] = island_id + start_i
                    gn["isGroup"] = True
                    gn["text"] = "{1}_{0}".format(island_id + 1, island_label)
                    node_list.append(gn)

                for ip in inner_parts:
                    ip["group"] = in_out_part_map[ip["key"] - start_k] + start_i

    def to_gojs_json(self, string_rep=True, outdict=None, visual=False):
        """
        Partition the PGT into a real "PGT with Partitions", thus PGTP
        """
        # import inspect
        # print("gojs_json called within MyKarkarPGTP from {0}".format(inspect.stack()[1][3]))

        if self._num_parts_done == 0 and self._partitions is None:
            (
                self._num_parts_done,
                _,
                self._ptime,
                self._partitions,
            ) = self._scheduler.partition_dag()
            # print("%s: _num_parts_done = %d" % (self.__class__.__name__, self._num_parts_done))
            # print("len(self._partitions) = %d" % (len(self._partitions)))
            # for part in self._partitions:
            #     print('Partition: {0}, Actual DoP = {1}, Required DoP = {2}'.\
            #                 format(part._gid, part._max_dop, part._ask_max_dop))
        G = self.dag
        # logger.debug("The same DAG? ", (G == self.dag))
        start_k = self._drop_list_len + 1  # starting gojs group_id
        self_oid_gid_map = self._oid_gid_map
        groups = set()
        key_dict = dict()  # k - gojs key, v - gojs group_id
        root_gid = None
        for gnode_g in G.nodes(data=True):
            gnode = gnode_g[1]
            oid = gnode["drop_spec"]["oid"]
            if "-92" == oid:
                root_gid = gnode["gid"]
                # print("hit fake super root, gid = {0}".format(root_gid))
                continue  # super_fake_root, so skip it
            gid = gnode["gid"]
            key_dict[gnode_g[0]] = gid
            self_oid_gid_map[oid] = gid - start_k
            groups.add(gid)
        if root_gid not in groups:
            # the super root has its own partition, which has no other Drops
            # so ditch this extra partition!
            new_partitions = []
            for part in self._partitions:
                if part._gid != root_gid:
                    new_partitions.append(part)
            self._partitions = new_partitions
            self._num_parts_done = len(new_partitions)
        self._groups = groups
        self._grp_key_dict = key_dict
        # print("group length = %d" % len(groups))
        # print('groups', groups)

        if visual:
            jsobj = super(MySarkarPGTP, self).to_gojs_json(
                string_rep=False, visual=visual
            )

            node_list = jsobj["nodeDataArray"]
            for node in node_list:
                gid = G.nodes[node["key"]]["gid"]  # gojs group_id
                if gid is None:
                    raise GPGTException(
                        "Node {0} does not have a Partition".format(node["key"])
                    )
                node["group"] = gid
                # key_dict[node['key']] = gid
                # self._oid_gid_map[node['oid']] = gid - start_k # real gid starts from 0
            inner_parts = []
            for gid in groups:
                gn = dict()
                gn["key"] = gid
                # logger.debug("group key = {0}".format(gid))
                gn["isGroup"] = True
                # gojs group_id label starts from 1
                # so "gid - leng" instead of "gid - start_k"
                gn["text"] = "{1}_{0}".format((gid - start_k + 1), self._par_label)
                node_list.append(gn)
                inner_parts.append(gn)

            self._node_list = node_list
            self._inner_parts = inner_parts
            self._gojs_json_obj = jsobj
            if string_rep and jsobj is not None:
                return json.dumps(jsobj, indent=2)
            else:
                return jsobj
        else:
            self._gojs_json_obj = None
            return None


class MinNumPartsPGTP(MySarkarPGTP):
    def __init__(
        self,
        drop_list,
        deadline,
        num_partitions=0,
        par_label="Partition",
        max_dop=8,
        merge_parts=False,
        optimistic_factor=0.5,
    ):
        """
        num_partitions: 0 - only do the initial logical partition
                        >1 - does logical partition, partition mergeing and
                        physical mapping
        """
        self._deadline = deadline
        self._opf = optimistic_factor
        super(MinNumPartsPGTP, self).__init__(
            drop_list, num_partitions, par_label, max_dop, merge_parts
        )
        # force it to re-calculate the extra drops due to extra links
        # during linearisation
        self._extra_drops = None

    def get_partition_info(self):
        return "Lookahead"

    def init_scheduler(self):
        self._scheduler = MinNumPartsScheduler(
            self._drop_list,
            self._deadline,
            max_dop=self._max_dop,
            dag=self.dag,
            optimistic_factor=self._opf,
        )


class PSOPGTP(MySarkarPGTP):
    def __init__(
        self,
        drop_list,
        par_label="Partition",
        max_dop=8,
        deadline=None,
        topk=30,
        swarm_size=40,
        merge_parts=False,
    ):
        """
        PSO-based PGTP
        """
        self._deadline = deadline
        self._topk = topk
        self._swarm_size = swarm_size
        super(PSOPGTP, self).__init__(drop_list, 0, par_label, max_dop, merge_parts)
        self._extra_drops = None

    def get_partition_info(self):
        return "Particle Swarm"

    def init_scheduler(self):
        self._scheduler = PSOScheduler(
            self._drop_list,
            max_dop=self._max_dop,
            deadline=self._deadline,
            dag=self.dag,
            topk=self._topk,
            swarm_size=self._swarm_size,
        )
