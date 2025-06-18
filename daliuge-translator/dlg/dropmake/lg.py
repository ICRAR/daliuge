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
import copy

import collections
import datetime
import logging
import time
from itertools import product
import numpy as np

from dlg.common import CategoryType, dropdict

from dlg.dropmake.dm_utils import (
    LG_APPREF,
    get_lg_ver_type,
    convert_construct,
    convert_fields,
    convert_subgraphs,
    LG_VER_EAGLE,
    LG_VER_EAGLE_CONVERTED,
    GraphException,
    GInvalidLink,
    GInvalidNode,
    load_lg,
)
from dlg.dropmake.definition_classes import Categories
from dlg.dropmake.lg_node import LGNode
from dlg.dropmake.graph_config import apply_active_configuration

logger = logging.getLogger(f"dlg.{__name__}")


class LG:
    """
    An object representation of a Logical Graph

    TODO: This is a lot more than just a LG class,
    it is doing all the conversion inside __init__
    """

    def __init__(self, f, ssid=None, apply_config=True):
        """
        parse JSON into LG object graph first
        """
        self._g_var = []
        lg = load_lg(f)
        if ssid is None:
            ts = time.time()
            ssid = datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%dT%H:%M:%S")
        self._session_id = ssid
        self._loop_aware_set = set()

        # key - gather drop oid, value - a tuple with two elements
        # input drops list and output drops list
        self._gather_cache = {}

        lgver = get_lg_ver_type(lg)
        logger.info("Loading graph: %s", lg["modelData"]["filePath"])
        logger.info("Found LG version: %s", lgver)

        if apply_config:
            lg = apply_active_configuration(lg)

        if LG_VER_EAGLE == lgver:
            lg = convert_fields(lg)
            lg = convert_construct(lg)
            lg = convert_subgraphs(lg)
        elif LG_VER_EAGLE_CONVERTED == lgver:
            lg = convert_construct(lg)
        elif LG_APPREF == lgver:
            lg = convert_fields(lg)
        # This ensures that future schema version mods are catched early
        else:
            raise GraphException(
                "Logical graph version '{0}' not supported!".format(lgver)
            )

        self._done_dict = {}
        self._group_q = collections.defaultdict(list)
        self._output_q = collections.defaultdict(list)
        self._start_list = []
        self._lgn_list = []
        stream_output_ports = {}  # key - port_id, value - construct key
        for jd in lg["nodeDataArray"]:
            lgn = LGNode(jd, self._group_q, self._done_dict, ssid)
            self._lgn_list.append(lgn)
            node_ouput_ports = jd.get("outputPorts", {})
            node_ouput_ports.update(jd.get("outputLocalPorts", {}))
            # check all the outports of this node, and store "stream" output
            if len(node_ouput_ports) > 0:
                for name, out_port in node_ouput_ports.items():
                    if name.lower().endswith("stream"):
                        stream_output_ports[out_port["Id"]] = jd["id"]
        # Need to go through the list again, since done_dict is recursive
        for lgn in self._lgn_list:
            if lgn.is_start and lgn.category not in [
                Categories.COMMENT,
                Categories.DESCRIPTION,
            ]:
                if lgn.category == Categories.VARIABLES:
                    self._g_var.append(lgn)
                else:
                    self._start_list.append(lgn)

        self._lg_links = lg["linkDataArray"]

        for lk in self._lg_links:
            src = self._done_dict[lk["from"]]
            srcPort = lk.get("fromPort", None)
            tgt = self._done_dict[lk["to"]]
            tgtPort = lk.get("toPort", None)
            self.validate_link(src, tgt)
            src.add_output(tgt, srcPort)
            tgt.add_input(src, tgtPort)
            # check stream links
            from_port = lk.get("fromPort", "__None__")
            if stream_output_ports.get(from_port, None) == lk["from"]:
                lk["is_stream"] = True
                logger.debug("Found stream from %s to %s", lk["from"], lk["to"])
            else:
                lk["is_stream"] = False
            if "1" == lk.get("loop_aware", "0"):
                self._loop_aware_set.add("%s-%s" % (lk["from"], lk["to"]))

        # key - lgn id, val - a list of pgns associated with this lgn
        self._drop_dict = collections.defaultdict(list)
        self._reprodata = lg.get("reprodata", {})

    def validate_link(self, src, tgt):
        # print("validate_link()", src.id, src.is_scatter(), tgt.id, tgt.is_scatter())
        if src.is_scatter or tgt.is_scatter:
            prompt = "Remember to specify Input App Type for the Scatter construct!"
            raise GInvalidLink(
                "Scatter construct {0} or {1} cannot be linked. {2}".format(
                    src.name, tgt.name, prompt
                )
            )

        if src.is_loop or tgt.is_loop:
            raise GInvalidLink(
                "Loop construct {0} or {1} cannot be linked".format(src.name, tgt.name)
            )

        if src.is_gather:
            if not (
                tgt.jd["categoryType"] in ["app", "application", "Application"]
                and tgt.is_group_start
                and src.inputs[0].h_level == tgt.h_level
            ):
                raise GInvalidLink(
                    "Gather {0}'s output {1} must be a Group-Start Component inside a Group with the same H level as Gather's input".format(
                        src.id, tgt.id
                    )
                )
            # raise GInvalidLink("Gather {0} cannot be the input".format(src.id))
        if tgt.is_groupby:
            if src.is_group:
                raise GInvalidLink(
                    "GroupBy {0} input must not be a group {1}".format(tgt.id, src.id)
                )
            if len(tgt.inputs) > 0:
                raise GInvalidLink(
                    "GroupBy {0} already has input {2} other than {1}".format(
                        tgt.id, src.id, tgt.inputs[0].id
                    )
                )
            if src.gid == 0:
                raise GInvalidLink(
                    "GroupBy {0} requires at least one Scatter around input {1}".format(
                        tgt.id, src.id
                    )
                )
        elif tgt.is_gather:
            if "categoryType" not in src.jd:
                src.jd["categoryType"] = "Data"
            if not src.jd["categoryType"].lower() == "data" and not src.is_groupby:
                raise GInvalidLink(
                    "Gather {0}'s input {1} should be either a GroupBy or Data. {2}".format(
                        tgt.id, src.id, src.jd
                    )
                )

        if src.is_groupby and not tgt.is_gather:
            raise GInvalidLink(
                "Output {1} from GroupBy {0} must be Gather, otherwise embbed {1} inside GroupBy {0}".format(
                    src.id, tgt.id
                )
            )

        if not src.h_related(tgt):
            src_group = src.group
            tgt_group = tgt.group
            if src_group.is_loop and tgt_group.is_loop:
                valid_loop_link = True
                while True:
                    if src_group is None or tgt_group is None:
                        break
                    if src_group.is_loop and tgt_group.is_loop:
                        if src_group.dop != tgt_group.dop:
                            valid_loop_link = False
                            break
                        else:
                            src_group = src_group.group
                            tgt_group = tgt_group.group
                    else:
                        break
                if not valid_loop_link:
                    raise GInvalidLink(
                        "{0} and {1} are not loop synchronised: {2} <> {3}".format(
                            src_group.id, tgt_group.id, src_group.dop, tgt_group.dop
                        )
                    )
            else:
                raise GInvalidLink(
                    "{0} and {1} are not hierarchically related: {2}-({4}) and {3}-({5})".format(
                        src.id,
                        tgt.id,
                        src.group_hierarchy,
                        tgt.group_hierarchy,
                        src.name,
                        tgt.name,
                    )
                )

    def get_child_lp_ctx(self, lgn, lpcxt, idx):
        if lgn.is_loop:
            if lpcxt is None:
                return "{0}".format(idx)
            else:
                return "{0}-{1}".format(lpcxt, idx)
        else:
            return None

    def lgn_to_pgn(self, lgn, iid="0", lpcxt=None, recursive=True):
        """
        convert a logical graph node to physical graph node(s)
        without considering pg links. This is a recursive method, creating also
        all child nodes required by constructs.

        iid:    instance id (string)
        lpcxt:  Loop context
        """
        if lgn.is_group:
            # group nodes are replaced with the input application of the
            # construct
            if not lgn.is_scatter:
                non_inputs = []
                grp_starts = []
                grp_ends = []
                for child in lgn.children:
                    if len(child.inputs) == 0:
                        non_inputs.append(child)
                    if child.is_group_start:
                        grp_starts.append(child)
                    elif child.is_group_end:
                        grp_ends.append(child)
                if len(grp_starts) == 0:
                    gs_list = non_inputs
                else:
                    gs_list = grp_starts
                if lgn.is_loop:
                    if len(grp_starts) == 0 or len(grp_ends) == 0:
                        raise GInvalidNode(
                            f"Loop {lgn.name} should have at least one Start "
                            "Component and one End Data"
                        )
                    for ge in grp_ends:
                        for gs in grp_starts:  # make an artificial circle
                            lk = {}
                            if gs not in ge.outputs:
                                ge.add_output(gs)
                            if ge not in gs.inputs:
                                gs.add_input(ge)
                            lk["from"] = ge.id
                            lk["to"] = gs.id
                            self._lg_links.append(lk)
                            logger.debug("Loop constructed: %s", gs.inputs)
                else:
                    for (
                        gs
                    ) in (
                        gs_list
                    ):  # add artificial logical links to the "first" children
                        lgn.add_input(gs)
                        gs.add_output(lgn)
                        lk = {}
                        lk["from"] = lgn.id
                        lk["to"] = gs.id
                        self._lg_links.append(lk)

            multikey_grpby = False
            lgk = lgn.group_keys
            shape = []
            if lgk is not None and len(lgk) > 1:
                multikey_grpby = True
                # inner most scatter to outer most scatter
                scatters = lgn.group_by_scatter_layers[2]
                # inner most is also the slowest running index
                shape = [x.dop for x in scatters]

            for i in range(lgn.dop):
                # todo - create iid(?)
                miid = f"{iid}-{i}"
                if multikey_grpby:
                    # set up more refined hierarchical context for group by with multiple keys
                    # recover multl-dimension indexes from i
                    grp_h = np.unravel_index(i, shape)
                    grp_h = [str(x) for x in grp_h]
                    miid += "${0}".format("-".join(grp_h))

                if not lgn.is_scatter and not lgn.is_loop:
                    # make GroupBy and Gather drops
                    src_drop = lgn.make_single_drop(miid)
                    self._drop_dict[lgn.id].append(src_drop)
                    if lgn.is_groupby:
                        self._drop_dict["new_added"].append(src_drop["grp-data_drop"])
                    elif lgn.is_gather:
                        pass
                        # self._drop_dict['new_added'].append(src_drop['gather-data_drop'])
                if recursive:
                    for child in lgn.children:
                        self.lgn_to_pgn(
                            child, miid, self.get_child_lp_ctx(lgn, lpcxt, i)
                        )
                else:
                    for child in lgn.children:
                        # Approach next 'set' of children
                        c_copy = copy.deepcopy(child)
                        c_copy.happy = True
                        c_copy.loop_ctx = self.get_child_lp_ctx(lgn, lpcxt, i)
                        c_copy.iid = miid
                        self._start_list.append(c_copy)
        elif lgn.is_mpi:
            for i in range(lgn.dop):
                if lgn.loop_ctx:
                    lpcxt = lgn.loop_ctx
                    iid = lgn.iid
                miid = "{0}-{1}".format(iid, i)
                src_drop = lgn.make_single_drop(miid, loop_ctx=lpcxt, proc_index=i)
                self._drop_dict[lgn.id].append(src_drop)
        elif lgn.is_service:
            # no action required, inputapp node aleady created and marked with "isService"
            pass
        elif lgn.is_subgraph and lgn.jd["isSubGraphApp"]:
            if lgn.loop_ctx:
                iid = lgn.iid
            src_drop = lgn.make_single_drop(iid, loop_ctx=lpcxt)
            if lgn.subgraph:
                kwargs = {"subgraph": lgn.subgraph}
                src_drop.update(kwargs)
            self._drop_dict[lgn.id].append(src_drop)
        else:
            if lgn.loop_ctx or lgn.iid:
                lpcxt = lgn.loop_ctx
                iid = lgn.iid
            src_drop = lgn.make_single_drop(iid, loop_ctx=lpcxt)
            self._drop_dict[lgn.id].append(src_drop)
            if lgn.is_start_listener:
                self._drop_dict["new_added"].append(src_drop["listener_drop"])

    @staticmethod
    def _split_list(l, n):
        """
        Yield successive n-sized chunks from l.
        """
        for i in range(0, len(l), n):
            yield l[i : i + n]

    def _unroll_gather_as_output(self, slgn, tlgn, sdrops, tdrops, chunk_size, llink):
        if slgn.h_level < tlgn.h_level:
            raise GraphException(
                "Gather {0} has higher h-level than its input {1}".format(
                    tlgn.id, slgn.id
                )
            )
        # src must be data
        for i, chunk in enumerate(self._split_list(sdrops, chunk_size)):
            for sdrop in chunk:
                self._link_drops(slgn, tlgn, sdrop, tdrops[i], llink)

    def _get_chunk_size(self, s, t):
        """
        Assumption:
        s or t cannot be Scatter as Scatter does not convert into DROPs
        """
        if t.is_gather:
            ret = t.gather_width
        elif t.is_groupby:
            ret = t.groupby_width
        else:
            ret = s.dop_diff(t)
        return ret

    def _is_stream_link(self, s_type, t_type):
        return s_type in [
            Categories.COMPONENT,
            Categories.DYNLIB_APP,
            Categories.DYNLIB_PROC_APP,
            Categories.PYTHON_APP,
            Categories.DALIUGE_APP
        ] and t_type in [
            Categories.COMPONENT,
            Categories.DYNLIB_APP,
            Categories.DYNLIB_PROC_APP,
            Categories.PYTHON_APP,
            Categories.DALIUGE_APP
        ]

    def _link_drops(
        self,
        slgn: LGNode,
        tlgn: LGNode,
        src_drop: dropdict,
        tgt_drop: dropdict,
        llink: dict,
    ):
        """ """
        sdrop = None
        if slgn.is_gather:
            # sdrop = src_drop['gather-data_drop']
            pass
        elif slgn.is_groupby:
            sdrop = src_drop["grp-data_drop"]
        else:
            sdrop = src_drop

        if tlgn.is_gather:
            gather_oid = tgt_drop["oid"]
            if gather_oid not in self._gather_cache:
                # [self, input_list, output_list]
                self._gather_cache[gather_oid] = [tgt_drop, [], [], llink]
            self._gather_cache[gather_oid][1].append(sdrop)
            logger.debug(
                "Hit gather, link is from %s to %s", llink["from"], llink["to"]
            )
            return

        tdrop = tgt_drop
        s_type = slgn.jd["categoryType"]
        t_type = tlgn.jd["categoryType"]

        if self._is_stream_link(s_type, t_type):
            # 1. create a null_drop in the middle
            # 2. link sdrop to null_drop
            # 3. link tdrop to null_drop as a streamingConsumer

            dropSpec_null = dropdict(
                {
                    "oid": "{0}-{1}-stream".format(
                        sdrop["oid"],
                        tdrop["oid"].replace(self._session_id, ""),
                    ),
                    "categoryType": CategoryType.DATA,
                    "dropclass": "dlg.data.drops.data_base.NullDROP",
                    "name": "StreamNull",
                    "weight": 0,
                }
            )
            sdrop.addOutput(dropSpec_null, name="stream")
            dropSpec_null.addProducer(sdrop, name="stream")
            dropSpec_null.addStreamingConsumer(tdrop, name="stream")
            tdrop.addStreamingInput(dropSpec_null, name="stream")
            self._drop_dict["new_added"].append(dropSpec_null)
        elif s_type in ["Application", "Control"]:
            logger.debug("Getting source and traget port names and IDs of %s and %s", slgn.name, tlgn.name)
            sname = slgn.getPortName("outputPorts", index=-1)
            tname = tlgn.getPortName("inputPorts", index=-1)

            sout_ids = []
            # sname is dictionary of all output ports on the sDROP.
            output_port = sname[llink["fromPort"]]
            input_port = tname[llink["toPort"]]
            # sdrop.addOutput(tdrop, name=output_port)
            # tdrop.addProducer(sdrop, name=input_port)
            if "port_map" not in tdrop:
                tdrop["port_map"] = {input_port:output_port}
            else:
                tdrop["port_map"][input_port] = output_port

            for output_port in sname.keys():
                if tdrop["oid"] not in sout_ids:
                    sdrop.addOutput(tdrop, name=output_port)
                    tdrop.addProducer(sdrop, name=output_port)
                    sout_ids = [list(o.keys())[0] for o in sdrop["outputs"]]
            if Categories.BASH_SHELL_APP == s_type:
                bc = src_drop["command"]
                bc.add_output_param(tlgn.id, tgt_drop["oid"])
        else:
            if slgn.is_gather:  # don't really add them
                gather_oid = src_drop["oid"]
                if gather_oid not in self._gather_cache:
                    # [self, input_list, output_list]
                    self._gather_cache[gather_oid] = [src_drop, [], [], llink]
                self._gather_cache[gather_oid][2].append(tgt_drop)
            else:  # sdrop is a data drop
                # there should be only one port, get the name
                # ^ TODO This comment is no longer true, need to address
                portId = llink["fromPort"] if "fromPort" in llink else None
                sname = slgn.getPortName("outputPorts", portId=portId)
                # could be multiple ports, need to identify
                portId = llink["toPort"] if "toPort" in llink else None
                tname = tlgn.getPortName("inputPorts", portId=portId)
                # logger.debug("Found port names: IN: %s, OUT: %s", sname, tname)
                # logger.debug(
                #     ">>> link from %s to %s (%s) (%s)",
                #     sname,
                #     tname,
                #     llink,
                #     portId,
                # )
                if llink.get("is_stream", False):
                    logger.debug(
                        "link stream connection %s to %s",
                        sdrop["oid"],
                        tdrop["oid"],
                    )
                    sdrop.addStreamingConsumer(tdrop, name=sname)
                    tdrop.addStreamingInput(sdrop, name=sname)
                else:
                    sdrop.addConsumer(tdrop, name=sname)
                    tdrop.addInput(sdrop, name=tname)
            if Categories.BASH_SHELL_APP == t_type:
                bc = tgt_drop["command"]
                bc.add_input_param(slgn.id, src_drop["oid"])

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

        logger.debug(
            "Unroll progress - lgn_to_pgn done %d for session %s",
            len(self._start_list),
            self._session_id,
        )
        for lk in self._lg_links:
            sid = lk["from"]  # source key
            tid = lk["to"]  # target key
            slgn = self._done_dict[sid]
            tlgn = self._done_dict[tid]
            sdrops = self._drop_dict[sid]
            tdrops = self._drop_dict[tid]
            chunk_size = self._get_chunk_size(slgn, tlgn)
            if slgn.is_group and not tlgn.is_group:
                # this link must be artifically added (within group link)
                # since
                # 1. GroupBy's "natural" output must be a Scatter (i.e. group)
                # 2. Scatter "naturally" does not have output
                if (
                    slgn.is_gather and tlgn.gid != sid
                ):  # not the artifical link between gather and its own start child
                    # gather iteration case, tgt must be a Group-Start Component
                    # this is a way to manually sequentialise a Scatter that has a high DoP
                    for i, ga_drop in enumerate(sdrops):
                        if ga_drop["oid"] not in self._gather_cache:
                            logger.warning(
                                "Gather %s Drop not yet in cache, sequentialisation may fail!",
                                slgn.name,
                            )
                            continue
                        j = (i + 1) * slgn.gather_width
                        if j >= tlgn.group.dop and j % tlgn.group.dop == 0:
                            continue
                        while j < (i + 2) * slgn.gather_width and j < tlgn.group.dop * (
                            i + 1
                        ):
                            # TODO merge this code into the function
                            # def _link_drops(self, slgn, tlgn, src_drop, tgt_drop, llink)
                            tname = tlgn.getPortName(port="inputPorts")
                            # Go through gather cache list
                            for gddrop in self._gather_cache[ga_drop["oid"]][1]:
                                gddrop.addConsumer(tdrops[j], name=tname)
                                tdrops[j].addInput(gddrop, name=tname)
                                j += 1

                elif slgn.is_subgraph or tlgn.is_subgraph:
                    pass
                else:
                    if len(sdrops) != len(tdrops):
                        err_info = "For within-group links, # {2} Group Inputs {0} must be the same as # {3} of Component Outputs {1}".format(
                            slgn.id, tlgn.id, len(sdrops), len(tdrops)
                        )
                        raise GraphException(err_info)
                    for i, sdrop in enumerate(sdrops):
                        self._link_drops(slgn, tlgn, sdrop, tdrops[i], lk)
            elif slgn.is_group and tlgn.is_group:
                # slgn must be GroupBy and tlgn must be Gather
                self._unroll_gather_as_output(
                    slgn, tlgn, sdrops, tdrops, chunk_size, lk
                )
            elif not slgn.is_group and (not tlgn.is_group):
                if slgn.is_start_node:
                    continue
                if (
                    (slgn.group is not None)
                    and slgn.group.is_loop
                    and slgn.gid == tlgn.gid
                    and slgn.is_group_end
                    and tlgn.is_group_start
                ):
                    # Re-link to the next iteration's start
                    lsd = len(sdrops)
                    if lsd != len(tdrops):
                        raise GraphException(
                            "# of sdrops '{0}' != # of tdrops '{1}'for Loop '{2}'".format(
                                slgn.name, tlgn.name, slgn.group.name
                            )
                        )
                    # first add the outer construct (scatter, gather, group-by) boundary
                    loop_chunk_size = slgn.group.dop
                    for i, chunk in enumerate(
                        self._split_list(sdrops, loop_chunk_size)
                    ):
                        for j, sdrop in enumerate(chunk):
                            if j < loop_chunk_size - 1:
                                self._link_drops(
                                    slgn,
                                    tlgn,
                                    sdrop,
                                    tdrops[i * loop_chunk_size + j + 1],
                                    lk,
                                )
                elif (
                    slgn.group is not None
                    and slgn.group.is_loop
                    and tlgn.group is not None
                    and tlgn.group.is_loop
                    and (not slgn.h_related(tlgn))
                ):
                    # stepwise locking for links between two Loops
                    for sdrop, tdrop in product(sdrops, tdrops):
                        if sdrop["loop_ctx"] == tdrop["loop_ctx"]:
                            self._link_drops(slgn, tlgn, sdrop, tdrop, lk)
                else:
                    lpaw = ("%s-%s" % (sid, tid)) in self._loop_aware_set
                    if (
                        slgn.group is not None
                        and slgn.group.is_loop
                        and lpaw
                        and slgn.h_level > tlgn.h_level
                    ):
                        loop_iter = slgn.group.dop
                        for i, chunk in enumerate(self._split_list(sdrops, chunk_size)):
                            for j, sdrop in enumerate(chunk):
                                # only link drops in the last loop iteration
                                if j % loop_iter == loop_iter - 1:
                                    self._link_drops(slgn, tlgn, sdrop, tdrops[i], lk)
                    elif (
                        tlgn.group is not None
                        and tlgn.group.is_loop
                        and lpaw
                        and slgn.h_level < tlgn.h_level
                    ):
                        loop_iter = tlgn.group.dop
                        for i, chunk in enumerate(self._split_list(tdrops, chunk_size)):
                            for j, tdrop in enumerate(chunk):
                                # only link drops in the first loop iteration
                                if j % loop_iter == 0:
                                    self._link_drops(slgn, tlgn, sdrops[i], tdrop, lk)

                    elif slgn.h_level >= tlgn.h_level:
                        for i, chunk in enumerate(self._split_list(sdrops, chunk_size)):
                            # distribute slgn evenly to tlgn
                            for sdrop in chunk:
                                self._link_drops(slgn, tlgn, sdrop, tdrops[i], lk)
                    else:
                        for i, chunk in enumerate(self._split_list(tdrops, chunk_size)):
                            # distribute tlgn evenly to slgn
                            for tdrop in chunk:
                                self._link_drops(slgn, tlgn, sdrops[i], tdrop, lk)
            else:  # slgn is not group, but tlgn is group
                if tlgn.is_groupby:
                    grpby_dict = collections.defaultdict(list)
                    layer_index = tlgn.group_by_scatter_layers[1]
                    for gdd in sdrops:
                        src_ctx = gdd["iid"].split("-")
                        if tlgn.group_keys is None:
                            # the last bit of iid (current h id) is the local GrougBy key, i.e. inner most loop context id
                            gby = src_ctx[-1]
                            if (
                                slgn.h_level - 2 == tlgn.h_level and tlgn.h_level > 0
                            ):  # groupby itself is nested inside a scatter
                                # group key consists of group context id + inner most loop context id
                                gctx = "-".join(src_ctx[0:-2])
                                gby = f"{gctx}-{gby}"
                        else:
                            # find the "group by" scatter level
                            gbylist = []
                            if slgn.group.is_groupby:  # a chain of group bys
                                try:
                                    src_ctx = gdd["iid"].split("$")[1].split("-")
                                except IndexError as e:
                                    raise GraphException(
                                        "The group by hiearchy in the multi-key group by '{0}' is not specified for node '{1}'".format(
                                            slgn.group.name, slgn.name
                                        )
                                    ) from e
                            else:
                                src_ctx.reverse()
                            for lid in layer_index:
                                gbylist.append(src_ctx[lid])
                            gby = "-".join(gbylist)
                        grpby_dict[gby].append(gdd)
                    grp_keys = grpby_dict.keys()
                    if len(grp_keys) != len(tdrops):
                        # this happens when groupby itself is nested inside a scatter
                        raise GraphException(
                            "# of Group keys {0} != # of Group Drops {1} for LGN {2}".format(
                                len(grp_keys), len(tdrops), tlgn.id
                            )
                        )
                    grp_keys = sorted(grp_keys)
                    for i, gk in enumerate(grp_keys):
                        grpby_drop = tdrops[i]
                        drop_list = grpby_dict[gk]
                        for drp in drop_list:
                            self._link_drops(slgn, tlgn, drp, grpby_drop, lk)
                            # drp.addOutput(grpby_drop)
                            # grpby_drop.addInput(drp)
                elif tlgn.is_gather:
                    self._unroll_gather_as_output(
                        slgn, tlgn, sdrops, tdrops, chunk_size, lk
                    )
                elif tlgn.is_service:
                    # Only the service node's inputApplication will be translated
                    # to the physical graph as a node of type SERVICE_APP instead of APP
                    # per compute instance
                    tlgn["categoryType"] = "Application"
                    tlgn["category"] = "DALiuGEApp"
                elif tlgn.is_subgraph:
                    pass
                else:
                    raise GraphException(
                        "Unsupported target group {0}".format(tlgn.jd.category)
                    )

        for _, v in self._gather_cache.items():
            input_list = v[1]
            try:
                output_drop = v[2][0]  # "peek" the first element of the output list
            except IndexError:
                continue  # the gather hasn't got output drops, just move on
            llink = v[-1]
            for data_drop in input_list:
                # TODO merge this code into the function
                # def _link_drops(self, slgn, tlgn, src_drop, tgt_drop, llink)
                sname = slgn.getPortName(ports="outputPorts")
                if llink.get("is_stream", False):
                    logger.debug(
                        "link stream connection %s to %s",
                        data_drop["oid"],
                        output_drop["oid"],
                    )
                    data_drop.addStreamingConsumer(output_drop, name=sname)
                    output_drop.addStreamingInput(data_drop, name=sname)
                else:
                    data_drop.addConsumer(output_drop, name=sname)
                    output_drop.addInput(data_drop, name=sname)

        logger.info(
            "Unroll progress - %d links done for session %s",
            len(self._lg_links),
            self._session_id,
        )

        # clean up extra drops
        for lid, lgn in self._done_dict.items():
            if (lgn.is_start_node) and lid in self._drop_dict:
                del self._drop_dict[lid]
            elif lgn.is_start_listener:
                for sl_drop in self._drop_dict[lid]:
                    if "listener_drop" in sl_drop:
                        del sl_drop["listener_drop"]
            elif lgn.is_groupby:
                for sl_drop in self._drop_dict[lid]:
                    if "grp-data_drop" in sl_drop:
                        del sl_drop["grp-data_drop"]
            elif lgn.is_gather:
                del self._drop_dict[lid]
            elif lgn.is_subgraph:
                # Remove the SubGraph construct drop
                if lgn.jd["isSubGraphConstruct"]:
                    del self._drop_dict[lid]
                else:
                    pass

        logger.info(
            "Unroll progress - extra drops done for session %s",
            self._session_id,
        )
        ret = []
        for drop_list in self._drop_dict.values():
            ret += drop_list

        return ret

    @property
    def reprodata(self):
        return self._reprodata
