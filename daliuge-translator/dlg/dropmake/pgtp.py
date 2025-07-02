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

from dlg.dropmake.pgt import PGT, GPGTException
from dlg.dropmake.scheduler import (
    MySarkarScheduler,
    DAGUtil,
    MinNumPartsScheduler,
    PSOScheduler,
)
from dlg.common import CategoryType

logger = logging.getLogger(f"dlg.{__name__}")


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

        G = nx.Graph()
        G.graph["edge_weight_attr"] = "weight"
        G.graph["node_weight_attr"] = "weight"
        G.graph["node_size_attr"] = "size"

        for i, drop in enumerate(self._drop_list):
            try:
                oid = drop["oid"]
            except KeyError:
                logger.debug("Drop does not have oid: %s", drop)
                self._drop_list.pop(i)
                continue
            key_dict[oid] = i + 1  # METIS index starts from 1

        logger.info("Metis partition input progress - dropdict is built")

        if self._drop_list_len > 1e7:
            import resource

            logger.info(
                "self._drop_list, max RSS: %.2f GB",
                resource.getrusage(resource.RUSAGE_SELF)[2] / 1024.0**2,
            )
        tw = 1
        sz = 1
        dst = "outputs"
        ust = "inputs"
        for i, drop in enumerate(self._drop_list):
            oid = drop["oid"]
            myk = i + 1
            tt = drop["categoryType"]
            if tt in [CategoryType.DATA, "data"]:
                dst = "consumers"  # outbound keyword
                ust = "producers"
                tw = 1  # task weight is zero for a Data DROP
                sz = drop.get("weight", 1)  # size
            elif tt in [CategoryType.APPLICATION, "app"]:
                dst = "outputs"
                ust = "inputs"
                tw = drop.get("weight", 1)
                sz = 1
            G.add_node(myk, weight=tw, size=sz, oid=oid)
            adj_drops = []  # adjacent drops (all neighbours)
            if dst in drop:
                adj_drops += drop[dst]
            if ust in drop:
                adj_drops += drop[ust]

            lw = 1
            for inp in adj_drops:
                key = list(inp.keys())[0] if isinstance(inp, dict) else inp
                if tt in [CategoryType.DATA, "data"]:
                    lw = drop["weight"]
                elif tt in [CategoryType.APPLICATION, "app"]:
                    # get the weight of the previous drop
                    lw = self._drop_list[key_dict[key] - 1].get("weight", 1)
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
                resource.getrusage(resource.RUSAGE_SELF)[2] / 1024.0**2,
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
        groups = set()
        # start_k = len(self._drop_list) + 1
        start_k = self._drop_list_len + 1
        for gnode, gid in zip(self._G.nodes(data=True), metis_out):
            groups.add(gid)
            gnode = gnode[1]
            self._oid_gid_map[gnode["oid"]] = gid
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
                self._group_workloads[gid] = [0, 0]
            for gnode in self._G.nodes(data=True):
                tt = self._group_workloads[gnode[1]["gid"]]
                try:
                    tt[0] += int(gnode[1]["weight"])
                    tt[1] += int(gnode[1]["size"])
                except ValueError:
                    tt[0] = 1
                    tt[1] = 1
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
                gn["name"] = "{1}_{0}".format(gid + 1, self._par_label)
                node_list.append(gn)
                inner_parts.append(gn)

            self._inner_parts = inner_parts
            self._node_list = node_list

    def to_gojs_json(self, string_rep=True, visual=False, outdict=None):
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
                    resource.getrusage(resource.RUSAGE_SELF)[2] / 1024.0**2,
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
        self.gojs_json_obj = jsobj  # could be none if not visual
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

        new_num_parts: This refers to the num_islands

        """
        # 0. parse the output and get all the partitions
        if not self._can_merge(new_num_parts):
            return

        part_edges = defaultdict(int)  # k: from_gid + to_gid, v: sum_of_weight
        for e in self._G.edges(data=True):
            from_gid = self._G.nodes[e[0]]["gid"]
            to_gid = self._G.nodes[e[1]]["gid"]
            k = "{0}**{1}".format(from_gid, to_gid)
            part_edges[k] += e[2]["weight"]

        # 1. build the bi-directional graph again
        # with each partition being a node
        G = nx.Graph()
        G.graph["edge_weight_attr"] = "weight"
        G.graph["node_weight_attr"] = "weight"
        G.graph["node_size_attr"] = "size"
        for gid, v in self._group_workloads.items():
            # for compute islands, we need to count the # of nodes instead of
            # the actual workload
            twv = 1 if (island_type == 1) else v[0]
            G.add_node(gid, weight=twv, size=v[1])
        for glinks, v in part_edges.items():
            gl = glinks.split("**")
            G.add_edge(int(gl[0]), int(gl[1]), weight=v)

        if new_num_parts == 1:
            (_, metis_parts) = (0, [0] * len(G.nodes()))
        else:
            (_, metis_parts) = self._metis.part_graph(
                G, nparts=new_num_parts, ufactor=1
            )
        islands = set()
        for gid, island_id in zip(G.nodes(), metis_parts):
            self._gid_island_id_map[gid] = island_id
            islands.add(island_id)
        if not form_island:
            for gnode in self._G.nodes(data=True):
                gnode = gnode[1]
                oid = gnode["oid"]
                old_gid = gnode["gid"]
                new_gid = self._gid_island_id_map[old_gid]
                self._oid_gid_map[oid] = new_gid
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
                for e in self.dag.edges(data=True):
                    gid0 = self._metis_out[e[0] - 1]
                    gid1 = self._metis_out[e[1] - 1]
                    if self._gid_island_id_map[gid0] == self._gid_island_id_map[gid1]:
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
                for island_id in islands:
                    # print('island_id = ', island_id)
                    gn = dict()
                    gn["key"] = island_id + start_i
                    gn["isGroup"] = True
                    gn["name"] = "{1}_{0}".format(island_id + 1, island_label)
                    self._node_list.append(gn)

                for ip in self._inner_parts:
                    ip["group"] = self._gid_island_id_map[ip["key"] - start_k] + start_i


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
            # TODO something about the self._gid_island_id_map
            return

        in_out_part_map = dict()
        outer_groups = set()
        if new_num_parts > 1:
            self._scheduler.merge_partitions(new_num_parts, bal_cond=island_type)
        else:
            ppid = self._drop_list_len + len(self._groups) + 1
            for part in self._partitions:
                part.parent_id = ppid

        start_k = self._drop_list_len + 1
        start_i = start_k + self._num_parts_done

        for part in self._partitions:
            island_id = part.parent_id - start_i  # make sure island_id starts from 0
            outer_groups.add(island_id)
            in_out_part_map[part.partition_id - start_k] = island_id

        self._gid_island_id_map = in_out_part_map
        if not form_island:
            # update to new gid
            for gnode_g in self.dag.nodes(data=True):
                gnode = gnode_g[1]
                oid = gnode["drop_spec"]["oid"]
                ggid = gnode["gid"] - start_k
                new_ggid = in_out_part_map[ggid]
                self._oid_gid_map[oid] = new_ggid
            self._num_parts_done = new_num_parts
        else:
            self._partition_merged = new_num_parts
            if island_type == 1:
                for e in self.dag.edges(data=True):
                    # update edege weights within the same compute island
                    if in_out_part_map.get(
                        self._grp_key_dict[e[0]] - start_k, -0.1
                    ) == in_out_part_map.get(self._grp_key_dict[e[1]] - start_k, -0.2):
                        # print("e[2]['weight'] =", e[2]['weight'])
                        e[2]["weight"] /= self._bw_ratio
            if visual:
                labels = self._island_labels[island_type % len(self._island_labels)]
                island_label = f"{labels}_Island"
                for island_id in outer_groups:
                    gn = dict()
                    gn["key"] = island_id + start_i
                    gn["isGroup"] = True
                    gn["name"] = "{1}_{0}".format(island_id + 1, island_label)
                    self._node_list.append(gn)

                for ip in self._inner_parts:
                    ip["group"] = in_out_part_map[ip["key"] - start_k] + start_i

    def to_gojs_json(self, string_rep=True, visual=False):
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
        # logger.debug("The same DAG? ", (G == self.dag))
        start_k = self._drop_list_len + 1  # starting gojs group_id
        groups = set()
        key_dict = dict()  # k - gojs key, v - gojs group_id
        root_gid = None
        for gnode_g in self.dag.nodes(data=True):
            gnode = gnode_g[1]
            oid = gnode["drop_spec"]["oid"]
            if "-92" == oid:
                root_gid = gnode["gid"]
                # print("hit fake super root, gid = {0}".format(root_gid))
                continue  # super_fake_root, so skip it
            key_dict[gnode_g[0]] = gnode["gid"]
            self._oid_gid_map[oid] = gnode["gid"] - start_k
            groups.add(gnode["gid"])
        if root_gid not in groups:
            # the super root has its own partition, which has no other Drops
            # so ditch this extra partition!
            new_partitions = []
            for part in self._partitions:
                if part.partition_id != root_gid:
                    new_partitions.append(part)
            self._partitions = new_partitions
            self._num_parts_done = len(new_partitions)
        self._groups = groups
        self._grp_key_dict = key_dict

        if visual:
            jsobj = super(MySarkarPGTP, self).to_gojs_json(
                string_rep=False, visual=visual
            )

            node_list = jsobj["nodeDataArray"]
            for node in node_list:
                gid = self.dag.nodes[node["key"]]["gid"]  # gojs group_id
                if gid is None:
                    raise GPGTException(
                        "Node {0} does not have a Partition".format(node["key"])
                    )
                node["group"] = gid
            inner_parts = []
            for gid in groups:
                gn = dict()
                gn["key"] = gid
                gn["isGroup"] = True
                # gojs group_id label starts from 1
                # so "gid - leng" instead of "gid - start_k"
                gn["name"] = "{1}_{0}".format((gid - start_k + 1), self._par_label)
                node_list.append(gn)
                inner_parts.append(gn)

            self._node_list = node_list
            self._inner_parts = inner_parts
            self.gojs_json_obj = jsobj
            if string_rep and jsobj is not None:
                return json.dumps(jsobj, indent=2)
            else:
                return jsobj
        else:
            self.gojs_json_obj = None
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
