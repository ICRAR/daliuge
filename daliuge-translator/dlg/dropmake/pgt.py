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
The DALiuGE resource manager uses the requested logical graphs, the available resources and
the profiling information and turns it into the partitioned physical graph,
which will then be deployed and monitored by the Physical Graph Manager
"""

if __name__ == "__main__":
    __package__ = "dlg.dropmake"


import json
import logging
import math

from dlg.dropmake.lg import GraphException
from dlg.dropmake.scheduler import DAGUtil
from dlg.common import dropdict
from dlg.common import Categories, DropType

logger = logging.getLogger(__name__)


class GPGTNoNeedMergeException(GraphException):
    pass


class PGT(object):
    """
    A DROP representation of Physical Graph Template
    """

    def __init__(self, drop_list, build_dag=True):
        self._drop_list = drop_list
        self._drop_list_len = len(drop_list)
        self._extra_drops = []  # artifacts DROPs produced during L2G mapping
        self._dag = DAGUtil.build_dag_from_drops(self._drop_list) if build_dag else None
        self._json_str = None
        self._oid_gid_map = dict()
        self._gid_island_id_map = dict()
        self._num_parts_done = 0
        self._partition_merged = 0
        self._inner_parts = []  # a list of inner partitions (e.g. nodes)
        # for visualisation only
        self._bw_ratio = 10.0  # bandwidth ratio between instra-comp_island
        # and inter-comp_island
        self._merge_parts = False
        self._island_labels = ["Data", "Compute"]
        self._data_movement = None
        self._reprodata = {}

    def _can_merge(self, new_num_parts):
        if new_num_parts <= 0:
            raise GPGTException("Invalid new_num_parts {0}".format(new_num_parts))
        if not self._merge_parts:
            raise GPGTException(
                "This {0} PGTP is not made for merging".format(self.__class__.__name__)
            )
        if self._num_parts_done <= new_num_parts:
            raise GPGTNoNeedMergeException(
                "No need to merge this {0} PGTP: {1} <= {2}".format(
                    self.__class__.__name__, self._num_parts_done, new_num_parts
                )
            )
        if self._partition_merged == new_num_parts:
            return False
        else:
            return True

    @property
    def drops(self):
        if self._extra_drops is None:
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
        # return "No partitioning. - Completion time: {0} - "\
        # "Data movement: {2} - Min exec time: {1}"\
        # .format(self.pred_exec_time(),
        # self.pred_exec_time(app_drop_only=True),
        # 0)
        return "Raw_unrolling"

    def result(self, lazy=True):
        ret = {}
        ret["algo"] = self.get_partition_info()
        ret["min_exec_time"] = self.pred_exec_time(
            app_drop_only=True, force_answer=(not lazy)
        )
        ret["total_data_movement"] = self.data_movement
        ret["exec_time"] = self.pred_exec_time(force_answer=(not lazy))
        if self._merge_parts and self._partition_merged > 0:
            ret["num_islands"] = self._partition_merged
        self._extra_result(ret)
        return ret

    def _extra_result(self, ret):
        pass

    @property
    def dag(self):
        """
            Return the networkx nx.DiGraph object

        The weight of the same edge (u, v) also depends.
        If it is called after the partitioning, it could have been zeroed
        if both u and v is allocated to the same DropIsland
        """
        return self._dag

    @property
    def data_movement(self):
        """
        Return the TOTAL data movement
        """
        if self._data_movement is not None:
            return self._data_movement
        elif self.dag is not None:
            G = self.dag
            self._data_movement = sum(e[2].get("weight", 0) for e in G.edges(data=True))
        return self._data_movement

    def pred_exec_time(self, app_drop_only=False, wk="weight", force_answer=False):
        """
        Predict execution time using the longest path length
        """
        G = self.dag
        if G is None:
            if force_answer:
                self._dag = DAGUtil.build_dag_from_drops(self._drop_list)
                G = self.dag
            else:
                return None
        if app_drop_only:
            lp = DAGUtil.get_longest_path(G, show_path=True)[0]
            return sum(G.nodes[u].get(wk, 0) for u in lp)
        else:
            return DAGUtil.get_longest_path(G, show_path=False)[1]

    @property
    def json(self):
        """
        Return the JSON string representation of the PGT
        for visualisation
        """
        if self._json_str is None:
            self._json_str = self.to_gojs_json(visual=True)

        return self._json_str
        # return self.to_gojs_json()

    @property
    def reprodata(self):
        return self._reprodata

    @reprodata.setter
    def reprodata(self, value):
        self._reprodata = value

    def merge_partitions(
        self, new_num_parts, form_island=False, island_type=0, visual=False
    ):
        raise Exception("Not implemented. Call sub-class")

    def to_pg_spec(
        self, node_list, ret_str=True, num_islands=1, tpl_nodes_len=0, co_host_dim=True
    ):
        """
        convert pgt to pg specification, and map that to the hardware resources

        node_list:
            A list of nodes (list), whose length == (num_islands + num_node_mgrs)
            if co_locate_islands = False.
            We assume that the MasterDropManager's node is NOT in the node_list

        num_islands:
            - >1 Partitions are "conceptually" clustered into Islands
            - 1 Partitions MAY BE physically merged without generating islands
              depending on the length of node_list
            - num_islands can't be < 1

        tpl_nodes_len: if this is given we generate a pg_spec template
            The pg_spec template is what needs to be send to a deferred deployemnt
            where the daliuge system is started up afer submission (e.g. SLURM)
        """
        logger.debug("tpl_nodes_len: %s, node_list: %s", tpl_nodes_len, node_list)
        if tpl_nodes_len > 0:  # generate pg_spec template
            node_list = range(tpl_nodes_len)  # create a fake list for now

        if 0 == self._num_parts_done:
            raise GPGTException("The graph has not been partitioned yet")

        if node_list is None or 0 == len(node_list):
            raise GPGTException("Node list is empty!")
        nodes_len = len(node_list)

        try:
            num_islands = int(num_islands)
        except:
            raise GPGTException("Invalid num_islands spec: {0}".format(num_islands))
        if num_islands < 1:
            num_islands = 1  # need at least one island manager
        if num_islands > nodes_len:
            raise GPGTException(
                "Number of islands must be <= number of specified nodes!"
            )
        form_island = num_islands > 1
        if nodes_len < 1:  # we allow to run everything on a single node now!
            raise GPGTException("Too few nodes: {0}".format(nodes_len))

        num_parts = self._num_parts_done
        drop_list = self._drop_list + self._extra_drops

        # deal with the co-hosting of DIMs
        if not co_host_dim:
            if form_island and num_parts > nodes_len:
                raise GPGTException(
                    "Insufficient number of nodes: {0}".format(nodes_len)
                )
            is_list = node_list[0:num_islands]
            nm_list = node_list[num_islands:]
        else:
            if form_island and num_islands + num_parts > nodes_len:
                raise GPGTException(
                    "Insufficient number of nodes: {0}".format(nodes_len)
                )
            is_list = node_list
            nm_list = node_list
        nm_len = len(nm_list)
        logger.info(
            "Drops count: %d, partitions count: %d, nodes count: %d, island count: %d",
            len(drop_list), num_parts, nodes_len, len(is_list)
        )

        if form_island:
            self.merge_partitions(num_islands, form_island=True)
            # from Eq.1 we know that num_parts <= nm_len
            # so no need to update its value
        elif nm_len < num_parts:
            self.merge_partitions(nm_len, form_island=False)
            num_parts = nm_len

        lm = self._oid_gid_map
        lm2 = self._gid_island_id_map
        # when #partitions < #nodes the lm values are spread around range(#nodes)
        # which leads to index out of range errors (TODO: find how _oid_gid_map is
        # constructed). The next three lines are attempting to fix this, however
        # then the test_metis_pgtp_gen_pg_island fails. This needs more investigation
        # but is a corner case.
        # values = set(dict(lm).values()) # old unique values
        # values = dict(zip(values,range(len(values)))) # dict with new values
        # lm = {k:values[v] for (k, v) in lm.items()} # replace old values with new

        if tpl_nodes_len:
            nm_list = ["#%s" % x for x in range(nm_len)]  # so that nm_list[i] == '#i'
            is_list = [
                "#%s" % x for x in range(len(is_list))
            ]  # so that is_list[i] == '#i'

        logger.info(
            "nm_list: %s, is_list: %s, lm: %s, lm2: %s", nm_list, is_list, lm, lm2
        )
        for drop in drop_list:
            oid = drop["oid"]
            # For now, simply round robin, but need to consider
            # nodes cross COMPUTE islands which has
            # TODO consider distance between a pair of nodes
            gid = lm[oid]
            drop["node"] = nm_list[gid]
            isid = lm2[gid] % num_islands if form_island else 0
            drop["island"] = is_list[isid]

        if ret_str:
            return json.dumps(drop_list, indent=2)
        else:
            return drop_list

    def to_gojs_json(self, string_rep=True, outdict=None, visual=False):
        """
        Convert PGT (without any partitions) to JSON for visualisation in GOJS

        Sub-class PGTPs will override this function, and replace this with
        actual partitioning, and the visulisation becomes an option
        """
        G = self.dag
        ret = dict()
        ret["class"] = "go.GraphLinksModel"
        nodes = []
        links = []
        key_dict = dict()  # key - oid, value - GOJS key

        for i, drop in enumerate(self._drop_list):
            oid = drop["oid"]
            node = dict()
            node["key"] = i + 1
            key_dict[oid] = i + 1
            node["oid"] = oid
            tt = drop["type"]
            if DropType.PLAIN == tt:
                node["category"] = Categories.DATA
            elif DropType.APP == tt:
                node["category"] = Categories.COMPONENT
            node["text"] = drop["nm"]
            nodes.append(node)

        if self._extra_drops is None:
            extra_drops = []
            remove_edges = []
            add_edges = []  # a list of tuples
            add_nodes = []
            for drop in self._drop_list:
                oid = drop["oid"]
                myk = key_dict[oid]
                for i, oup in enumerate(G.successors(myk)):
                    link = dict()
                    link["from"] = myk
                    from_dt = 0 if drop["type"] == DropType.PLAIN else 1
                    to_dt = G.nodes[oup]["dt"]
                    if from_dt == to_dt:
                        to_drop = G.nodes[oup]["drop_spec"]
                        if from_dt == 0:
                            # add an extra app DROP
                            extra_oid = "{0}_TransApp_{1}".format(oid, i)
                            dropSpec = dropdict(
                                {
                                    "oid": extra_oid,
                                    "type": DropType.APP,
                                    "app": "dlg.drop.BarrierAppDROP",
                                    "nm": "go_app",
                                    "tw": 1,
                                }
                            )
                            # create links
                            drop.addConsumer(dropSpec)
                            dropSpec.addInput(drop)
                            dropSpec.addOutput(to_drop)
                            to_drop.addProducer(dropSpec)
                            mydt = 1
                        else:
                            # add an extra data DROP
                            extra_oid = "{0}_TransData_{1}".format(oid, i)
                            dropSpec = dropdict(
                                {
                                    "oid": extra_oid,
                                    "type": DropType.PLAIN,
                                    "storage": Categories.MEMORY,
                                    "nm": "go_data",
                                    "dw": 1,
                                }
                            )
                            drop.addOutput(dropSpec)
                            dropSpec.addProducer(drop)
                            dropSpec.addConsumer(to_drop)
                            to_drop.addInput(dropSpec)
                            mydt = 0
                        extra_drops.append(dropSpec)
                        lid = len(extra_drops) * -1
                        link["to"] = lid
                        endlink = dict()
                        endlink["from"] = lid
                        endlink["to"] = oup
                        links.append(endlink)
                        # global graph updates
                        # the new drop must have the same gid as the to_drop
                        add_nodes.append(
                            (lid, 1, mydt, dropSpec, G.nodes[oup].get("gid", None))
                        )
                        remove_edges.append((myk, oup))
                        add_edges.append((myk, lid))
                        add_edges.append((lid, oup))
                    else:
                        link["to"] = oup
                    links.append(link)
            for gn in add_nodes:
                # logger.debug("added gid = {0} for new node {1}".format(gn[4], gn[0]))
                G.add_node(gn[0], weight=gn[1], dt=gn[2], drop_spec=gn[3], gid=gn[4])
            G.remove_edges_from(remove_edges)
            G.add_edges_from(add_edges)
            self._extra_drops = extra_drops
        else:
            for drop in self._drop_list:
                oid = drop["oid"]
                myk = key_dict[oid]
                for oup in G.successors(myk):
                    link = dict()
                    link["from"] = myk
                    link["to"] = oup
                    links.append(link)

        # going through the extra_drops
        for i, drop in enumerate(self._extra_drops):
            oid = drop["oid"]
            node = dict()
            node["key"] = (i + 1) * -1
            node["oid"] = oid
            tt = drop["type"]
            if DropType.PLAIN == tt:
                node["category"] = Categories.DATA
            elif DropType.APP == tt:
                node["category"] = Categories.COMPONENT
            node["text"] = drop["nm"]
            nodes.append(node)

        ret["nodeDataArray"] = nodes
        ret["linkDataArray"] = links
        self._gojs_json_obj = ret
        if string_rep:
            return json.dumps(ret, indent=2)
        else:
            return ret
