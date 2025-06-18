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

import json
import logging
import math
import random
import re

from dlg.common import CategoryType
from dlg.common import dropdict
from dlg.dropmake.dm_utils import (
    GraphException,
    GInvalidLink,
    GInvalidNode,
)
from .definition_classes import Categories, DATA_TYPES, APP_TYPES

logger = logging.getLogger(f"dlg.{__name__}")


class LGNode:
    def __init__(self, jd, group_q, done_dict, ssid):
        """
        jd: json_dict (dict)
        group_q: group queue (defaultdict)
        done_dict: LGNode that have been processed (Dict)
        ssid:   session id (string)
        """
        self.id = jd["id"]  # node ID
        self.jd = jd  # JSON TODO: this should be removed
        self.group_q = group_q  # the group hierarchy queue
        self.group = None  # used if node belongs to group
        self._children = []  # list of LGNode objects, children of this node
        self._ssid = ssid  # session ID
        self.is_app = self.jd["categoryType"] == CategoryType.APPLICATION
        self.is_data = self.jd["categoryType"] == CategoryType.DATA
        self.weight = 1  # try to find the weights, else set to 1
        self._converted = False
        self._h_level = None  # hierarcht level
        self._g_h = None
        self._dop = None  # degree of parallelism
        self._gaw = None
        self._grpw = None
        self._inputs = []  # list of LGNode objects connected to this node
        self._outputs = []  # list of LGNode objects connected to this node
        self.dropclass = ""  # e.g. dlg.apps.simple.HelloWorldAPP
        self.reprodata = jd.get("reprodata", {}).copy()
        if "isGroup" in jd and jd["isGroup"] is True:
            self.is_group = True
            for wn in group_q[self.id]:
                wn.group = self
                self.add_child(wn)
            group_q.pop(self.id)  # not thread safe
        else:
            self.is_group = False

        if "parentId" in jd:
            grp_id = jd["parentId"]
            if grp_id in done_dict:
                grp_nd = done_dict[grp_id]
                self.group = grp_nd
                grp_nd.add_child(self)
            else:
                group_q[grp_id].append(self)

        done_dict[self.id] = self
        self.subgraph = jd["subgraph"] if "subgraph" in jd else None
        self.happy = False
        self.loop_ctx = None
        self.iid = None
        self.input_ports = self.getPortName(ports="inputPorts", index=-1)
        self.output_ports = self.getPortName(ports="outputPorts", index=-1)

    def __str__(self):
        return self.name

    @property
    def output_ports(self):
        return self._output_ports

    @output_ports.setter
    def output_ports(self,value):
        """
        Setting the output_ports property.
        """
        self._output_ports = value

    @property
    def input_ports(self):
        return self._input_ports

    @input_ports.setter
    def input_ports(self,value):
        """
        Setting the output_ports property.
        """
        self._input_ports = value


    @property
    def jd(self):
        return self._jd

    @jd.setter
    def jd(self, node_json):
        """
        Setting the jd property to the original data structure directly loaded
        from JSON.
        """
        if "categoryType" not in node_json:
            if node_json["category"] in APP_TYPES:
                node_json["categoryType"] = CategoryType.APPLICATION
            elif node_json["category"] in DATA_TYPES:
                node_json["categoryType"] = CategoryType.DATA
        self._jd = node_json

    @property
    def reprodata(self):
        return self._reprodata

    @reprodata.setter
    def reprodata(self, value):
        self._reprodata = value

    @property
    def is_group(self):
        return self._is_group

    @is_group.setter
    def is_group(self, value):
        self._is_group = value

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        self._id = value

    @property
    def nodetype(self):
        return self._nodetype

    @nodetype.setter
    def nodetype(self, value):
        self._nodetype = value

    @property
    def dropclass(self):
        return self._dropclass

    @dropclass.setter
    def dropclass(self, default_value):
        self.is_data = False
        self.is_app = False
        keys = []
        value = None
        if default_value is None or len(default_value) == 0:
            default_value = "dlg.apps.simple.SleepApp"
        if self.jd["categoryType"] == CategoryType.DATA:
            self.is_data = True
            keys = [
                "dropclass",
                "Data class",
                "dataclass",
            ]
        elif self.jd["categoryType"] == CategoryType.APPLICATION:
            keys = [
                "dropclass",
                "Application Class",
                "Application class",
                "appclass",
            ]
            self.is_app = True
        elif self.jd["categoryType"] in [
            CategoryType.CONSTRUCT,
            CategoryType.CONTROL,
        ]:
            keys = ["inputApplicationName"]
        elif self.jd["categoryType"] in ["Other"]:
            value = "Other"
        else:
            logger.error("Found unknown categoryType: %s", self.jd["categoryType"])
            # raise ValueError
        for key in keys:
            if key in self.jd:
                value = self.jd[key]
                break
            if value is None or value == "":
                value = default_value

        self._dropclass = value

    @property
    def name(self):
        return self.jd.get("name", "")

    @property
    def category(self):
        return self.jd.get("category", "Unknown")

    @property
    def categoryType(self):
        return self.jd.get("categoryType", "Unknown")

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

    def complete_conversion(self):
        self._converted = True

    @property
    def gid(self):
        if self.group is None:
            return 0
        else:
            return self.group.id

    def add_output(self, lg_node, srcPort=None):
        if lg_node not in self._outputs:
            self._outputs.append(lg_node)
        if self.jd.get("outputPorts") and srcPort is not None and srcPort in self.jd["outputPorts"]:
            self.jd["outputPorts"][srcPort]["target_id"] = lg_node.id

    def add_input(self, lg_node, tgtPort=None):
        # only add if not already there
        # this may happen in nested constructs
        if lg_node not in self._inputs:
            self._inputs.append(lg_node)
        if self.jd.get("inputPorts") and tgtPort is not None and tgtPort in self.jd["inputPorts"]:
            self.jd["inputPorts"][tgtPort]["source_id"] = lg_node.id

    def add_child(self, lg_node):
        """
        Add a group member
        """
        if (
            lg_node.is_group
            and not (lg_node.is_scatter)
            and not (lg_node.is_loop)
            and not (lg_node.is_groupby)
        ):
            raise GInvalidNode(
                "Only Scatters, Loops and GroupBys can be nested, but {0} is neither".format(
                    lg_node.id
                )
            )
        self._children.append(lg_node)

    @property
    def children(self):
        return self._children

    @property
    def outputs(self):
        return self._outputs

    @property
    def inputs(self):
        return self._inputs

    @property
    def h_level(self):
        if self._h_level is None:
            _level = 0
            cg = self
            while cg.has_group():
                cg = cg.group
                _level += 1
            if self.is_mpi:
                _level += 1
            self._h_level = _level
        return self._h_level

    @property
    def group_hierarchy(self):
        if self._g_h is None:
            glist = []
            cg = self
            while cg.has_group():
                glist.append(str(cg.gid))
                cg = cg.group
            glist.append("0")
            self._g_h = "-".join(reversed(glist))
        return self._g_h

    @property
    def weight(self):
        return self._weight

    @weight.setter
    def weight(self, default_value):
        """
        The weight of a data drop is its volume.
        The weight of an app drop is the execution time.
        """
        key = []
        if self.is_app:
            key = [k for k in self.jd if re.match(r"execution[\s\_]time", k.lower())]
        elif self.is_data:
            key = [k for k in self.jd if re.match(r"data[\s\_]volume", k.lower())]
        try:
            self._weight = int(self.jd[key[0]])
        except (KeyError, ValueError, IndexError):
            self._weight = int(default_value)

    @property
    def has_child(self):
        return len(self._children) > 0

    @property
    def has_output(self):
        return len(self._outputs) > 0

    @property
    def is_data(self):
        return self._is_data

    @is_data.setter
    def is_data(self, value):
        self._is_data = value

    @property
    def is_app(self):
        return self._is_app
        # return self.jd["categoryType"] == CategoryType.APPLICATION

    @is_app.setter
    def is_app(self, value):
        self._is_app = value

    @property
    def is_start_node(self):
        return self.jd["category"] == Categories.START

    @property
    def is_end_node(self):
        return self.jd["category"] == Categories.END

    @property
    def is_start(self):
        return not self.has_group()

    @property
    def is_dag_root(self):
        leng = len(self.inputs)
        if leng > 1:
            return False
        elif leng == 0:
            if self.is_start_node:
                return False
            else:
                return True
        elif self.is_start:
            return True
        else:
            return False

    @property
    def is_start_listener(self):
        """
        is this a socket listener node
        """
        return len(self.inputs) == 1 and self.is_start_node and self.is_data

    @property
    def is_group_start(self):
        """
        is this a node starting a group (usually inside a loop)
        """
        result = False
        if self.has_group() and (
            "group_start" in self.jd
            or "Group start" in self.jd
            or "Group Start" in self.jd
        ):
            gs = (
                self.jd.get("group_start", False)
                if "group_start" in self.jd
                else self.jd.get("Group start", False)
            )
            if type(gs) == type(True):
                result = gs
            elif type(gs) in [type(1), type(1.0)]:
                result = 1 == gs
            elif type(gs) == type("s"):
                result = gs.lower() in ("true", "1")
        return result

    @property
    def is_group_end(self):
        """
        is this a node ending a group (usually inside a loop)
        """
        result = False
        if self.has_group() and (
            "group_end" in self.jd or "Group end" in self.jd or "Group End" in self.jd
        ):
            ge = (
                self.jd.get("group_end", False)
                if "group_end" in self.jd
                else self.jd.get("Group end", False)
            )
            if type(ge) == type(True):
                result = ge
            elif type(ge) in [type(1), type(1.0)]:
                result = 1 == ge
            elif type(ge) == type("s"):
                result = ge.lower() in ("true", "1")
        return result

    @property
    def is_scatter(self):
        return self.is_group and self._jd["category"] == Categories.SCATTER

    @property
    def is_gather(self):
        return self._jd["category"] == Categories.GATHER

    @property
    def is_loop(self):
        return self.is_group and self._jd["category"] == Categories.LOOP

    @property
    def is_service(self):
        """
        Determines whether a node the parent service node (not the input application)
        """
        return self._jd["category"] == Categories.SERVICE

    @property
    def is_groupby(self):
        return self._jd["category"] == Categories.GROUP_BY

    @property
    def is_branch(self):
        # This is the only special thing required for a branch
        return self._jd["category"] == Categories.BRANCH

    @property
    def is_mpi(self):
        return self._jd["category"] == Categories.MPI

    @property
    def is_subgraph(self):
        if "isSubGraphApp" in self._jd:
            return self._jd["isSubGraphApp"]
        else:
            return self._jd["category"] == Categories.SUBGRAPH

    @property
    def group_keys(self):
        """
        Return:
            None or a list of keys (each key is an integer)
        """
        if not self.is_groupby:
            return None
        val = str(self.jd.get("group_key", "None"))
        if val in ["None", "-1", ""]:
            return None
        else:
            try:
                return [int(x) for x in val.split(",")]
            except ValueError as ve:
                raise GraphException(
                    "group_key must be an integer or comma-separated integers: {0}".format(
                        ve
                    )
                ) from ve

    @property
    def gather_width(self):
        """
        Gather width
        """
        if self.is_gather:
            if self._gaw is None:
                try:
                    self._gaw = int(self.jd["num_of_inputs"])
                except KeyError:
                    self._gaw = 1
            return self._gaw
        else:
            # TODO: use OO style to replace all type-related statements!
            return None

    @property
    def groupby_width(self):
        """
        GroupBy count
        """
        if self.is_groupby:
            if self._grpw is None:
                tlgn = self.inputs[0]
                re_dop = 1
                cg = tlgn.group  # exclude its own group
                while cg.has_group():
                    re_dop *= cg.group.dop
                    cg = cg.group
                self._grpw = re_dop
            return self._grpw
        else:
            return None

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
        if not self.is_groupby:
            return None

        tlgn = self.inputs[0]
        grpks = self.group_keys
        ret_dop = 1
        layer_index = []  # from inner to outer
        layers = []  # from inner to outer
        c = 0
        if tlgn.group.is_groupby:
            # group by followed by another group by
            if grpks is None or len(grpks) < 1:
                raise GInvalidNode(
                    "Must specify group_key for Group By '{0}'".format(self.name)
                )
            # find the "root" groupby and get all of its scatters
            inputgrp = self
            while (inputgrp is not None) and inputgrp.inputs[0].group.is_groupby:
                inputgrp = inputgrp.inputs[0].group
            # inputgrp now is the "root" groupby that follows Scatter immiately
            # move it to Scatter
            inputgrp = inputgrp.inputs[0].group
            # go thru all the scatters
            while (inputgrp is not None) and inputgrp.is_scatter:
                if inputgrp.id in grpks:
                    ret_dop *= inputgrp.dop
                    layer_index.append(c)
                    layers.append(inputgrp)
                inputgrp = inputgrp.group
                c += 1
        else:
            if grpks is None or len(grpks) < 1:
                ret_dop = tlgn.group.dop
                layer_index.append(0)
                layers.append(tlgn.group)
            else:
                if len(grpks) == 1:
                    if grpks[0] == tlgn.group.id:
                        ret_dop = tlgn.group.dop
                        layer_index.append(0)
                        layers.append(tlgn.group)
                    else:
                        raise GInvalidNode(
                            "Wrong single group_key for {0}".format(self.name)
                        )
                else:
                    inputgrp = tlgn.group
                    # find the "groupby column list" from all layers of scatter loops
                    while (inputgrp is not None) and inputgrp.is_scatter:
                        if inputgrp.id in grpks:
                            ret_dop *= inputgrp.dop
                            layer_index.append(c)
                            layers.append(inputgrp)
                        inputgrp = inputgrp.group
                        c += 1

        return ret_dop, layer_index, layers

    @property
    def dop(self):
        """
        Degree of Parallelism:  integer
        default:    1
        """
        if self._dop is None:
            if self.is_group:
                if self.is_scatter:
                    for kw in [
                        "num_of_copies",
                        "num_of_splits",
                        "Number of copies",
                    ]:
                        if kw in self.jd:
                            self._dop = int(self.jd[kw])
                            break
                    if self._dop is None:
                        self._dop = 4  # dummy impl. TODO: Why is this here?
                elif self.is_gather:
                    try:
                        tlgn = self.inputs[0]
                    except IndexError as e:
                        raise GInvalidLink(
                            "Gather '{0}' does not have input!".format(self.id)
                        ) from e
                    if tlgn.is_groupby:
                        tt = tlgn.dop
                    else:
                        tt = self.dop_diff(tlgn)
                    self._dop = int(math.ceil(tt / float(self.gather_width)))
                elif self.is_groupby:
                    self._dop = self.group_by_scatter_layers[0]
                elif self.is_loop:
                    for key in [
                        "num_of_iter",
                        "Number of Iterations",
                        "Number of loops",
                    ]:
                        if key in self.jd:
                            self._dop = int(self.jd.get(key, 1))
                            break
                elif self.is_service:
                    self._dop = 1  # TODO: number of compute nodes
                elif self.is_subgraph:
                    self._dop = 1
                else:
                    raise GInvalidNode(
                        "Unrecognised (Group) Logical Graph Node: '{0}'".format(
                            self._jd["category"]
                        )
                    )
            elif self.is_mpi:
                self._dop = int(self.jd["num_of_procs"])
            else:
                self._dop = 1
        return self._dop

    def dop_diff(self, that_lgn):
        """
        TODO: This does not belong in the LGNode class

        dop difference between inner node/group and outer group
        e.g for each outer group, how many instances of inner nodes/groups
        """
        # if (self.is_group() or that_lgn.is_group()):
        #     raise GraphException("Cannot compute dop diff between groups.")
        # don't check h_related for efficiency since it should have been checked
        # if (self.h_related(that_lgn)):
        il = self.h_level
        al = that_lgn.h_level
        if il == al:
            return 1
        elif il > al:
            oln = that_lgn
            iln = self
        else:
            iln = that_lgn
            oln = self
        re_dop = 1
        cg = iln
        init_cond = cg.gid != oln.gid and cg.has_group()
        while init_cond or cg.is_mpi:
            if cg.is_mpi:
                re_dop *= cg.dop
            # else:
            if init_cond:
                re_dop *= cg.group.dop
            cg = cg.group
            if cg is None:
                break
            init_cond = cg.gid != oln.gid and cg.has_group()
        return re_dop
        # else:
        #     pass
        # raise GInvalidLink("{0} and {1} are not hierarchically related".format(self.id, that_lgn.id))

    def h_related(self, that_lgn):
        """
        TODO: This does not belong in the LGNode class
        """
        that_gh = that_lgn.group_hierarchy
        this_gh = self.group_hierarchy
        if len(that_gh) + len(this_gh) <= 1:
            # at least one is at the root level
            return True

        return that_gh.find(this_gh) > -1 or this_gh.find(that_gh) > -1

    def make_oid(self, iid="0"):
        """
        return:
            ssid_id_iid (string), where
            ssid:   session id
            id:     logical graph node key
            iid:    instance id (for the physical graph node)
        """
        # TODO: This is rather ugly, but a quick and dirty fix. The iid is the rank data we need
        rank = [int(x) for x in iid.split("-")]
        return "{0}_{1}_{2}".format(self._ssid, self.id, iid), rank

    def _update_key_value_attributes(self, kwargs):
        """
        get all the arguments from new fields dictionary in a backwards compatible way
        """
        kwargs["applicationArgs"] = {}
        kwargs["constraintParams"] = {}
        kwargs["componentParams"] = {}
        if "fields" in self.jd:
            kwargs["fields"] = self.jd["fields"]
            for je in self.jd["fields"]:
                # The field to be used is not the text, but the name field
                self.jd[je["name"]] = je["value"]
                kwargs[je["name"]] = je["value"]
                if "parameterType" in je:
                    if je["parameterType"] == "ApplicationArgument":
                        kwargs["applicationArgs"].update({je["name"]: je})
                    elif je["parameterType"] == "ConstraintParameter":
                        kwargs["constraintParams"].update({je["name"]: je})
                    elif je["parameterType"] == "ComponentParameter":
                        kwargs["componentParams"].update({je["name"]: je})

        # NOTE: drop Argxx keywords

    def getPortName(self, ports: str = "outputPorts", index: int = 0, portId=None):
        """
        Return name of port if it exists
        """
        port_selector = {
            "inputPorts": ["InputPort", "InputOutput"],
            "outputPorts": ["OutputPort", "InputOutput"],
        }
        ports_dict = {}
        name = None
        # if portId is None and index >= 0:
        if ports in port_selector:
            for field in self.jd["fields"]:
                if "usage" not in field:  # fixes manual graphs
                    continue
                if field["usage"] in port_selector[ports]:
                    if portId is None or field["id"] == portId:
                        name = field["name"]
                    # can't be sure that name is unique
                    if field["id"] not in ports_dict:
                        ports_dict[field["id"]] = name
        logger.debug("Ports: %s; name: %s; index: %d", ports_dict, name, index)
        return name if index >= 0 else ports_dict

    def _create_groupby_drops(self, drop_spec):
        drop_spec.update(
            {
                "dropclass": "dlg.apps.simple.SleepApp",
                "categoryType": "Application",
            }
        )
        sij = self.inputs[0]
        if not sij.is_data:
            raise GInvalidNode(
                "GroupBy should be connected to a DataDrop, not '%s'" % sij.category
            )
        dw = sij.weight * self.groupby_width

        # additional generated drop
        dropSpec_grp = dropdict(
            {
                "oid": "{0}-grp-data".format(drop_spec["oid"]),
                "categoryType": CategoryType.DATA,
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
                "name": "grpdata",
                "weight": dw,
                "rank": drop_spec["rank"],
                "reprodata": self.jd.get("reprodata", {}),
            }
        )
        kwargs = {}
        kwargs["grp-data_drop"] = dropSpec_grp
        kwargs["weight"] = 1  # barrier literarlly takes no time for its own computation
        kwargs["sleep_time"] = 1
        drop_spec.update(kwargs)
        drop_spec.addOutput(dropSpec_grp, name="grpdata")
        dropSpec_grp.addProducer(drop_spec, name="grpdata")
        return drop_spec

    def _create_gather_drops(self, drop_spec):
        drop_spec.update(
            {
                "dropclass": "dlg.apps.simple.SleepApp",
                "categoryType": "Application",
            }
        )
        gi = self.inputs[0]
        if gi.is_groupby:
            gii = gi.inputs[0]
            dw = int(gii.jd["data_volume"]) * gi.groupby_width * self.gather_width
        else:  # data
            dw = gi.weight * self.gather_width

            # additional generated drop
        dropSpec_gather = dropdict(
            {
                "oid": "{0}-gather-data".format(drop_spec["oid"]),
                "categoryType": CategoryType.DATA,
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
                "name": "gthrdt",
                "weight": dw,
                "rank": drop_spec["rank"],
                "reprodata": self.jd.get("reprodata", {}),
            }
        )
        kwargs = {}
        kwargs["gather-data_drop"] = dropSpec_gather
        kwargs["weight"] = 1
        kwargs["sleep_time"] = 1
        drop_spec.update(kwargs)
        drop_spec.addOutput(dropSpec_gather, name="gthrdata")
        dropSpec_gather.addProducer(drop_spec, name="gthrdata")
        return drop_spec

    def _create_listener_drops(self, drop_spec):
        # create socket listener DROP first
        drop_spec.update(
            {
                "oid": drop_spec["oid"],
                "categoryType": CategoryType.DATA,
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
            }
        )

        # additional generated drop
        dropSpec_socket = dropdict(
            {
                "oid": "{0}-s".format(drop_spec["oid"]),
                "categoryType": CategoryType.APPLICATION,
                "category": "DALiuGEApp",
                "dropclass": "dlg.apps.simple.SleepApp",
                "name": "lstnr",
                "weigth": 5,
                "sleep_time": 1,
                "reprodata": self.jd.get("reprodata", {}),
            }
        )
        # tw -- task weight
        dropSpec_socket["autostart"] = 1
        drop_spec.update({"listener_drop": dropSpec_socket})
        dropSpec_socket.addOutput(
            drop_spec, name=self.getPortName(ports="outputPorts")
        )
        return drop_spec

    def _create_app_drop(self, drop_spec):
        # default generic component becomes "sleep and copy"
        kwargs = {}
        if "appclass" in self.jd:
            app_class = self.jd["appclass"]
        elif self.dropclass is None or self.dropclass == "":
            logger.debug("No dropclass found in: %s", self)
            app_class = "dlg.apps.simple.SleepApp"
        else:
            app_class = self.dropclass
        if self.dropclass == "dlg.apps.simple.SleepApp":
            if self.category == "BashShellApp":
                app_class = "dlg.apps.bash_shell_app.BashShellApp"
            elif self.category == "Docker":
                app_class = "dlg.apps.dockerapp.DockerApp"
                drop_spec["name"] = self.jd["command"]
            else:
                logger.debug(
                    "Might be a problem with this node: %s",
                    json.dumps(self.jd, indent=2),
                )

        self.dropclass = app_class
        self.jd["dropclass"] = app_class
        self.dropclass = app_class
        logger.debug(
            "Creating app drop using class: %s, %s",
            app_class,
            drop_spec["name"],
        )
        if self.dropclass is None or self.dropclass == "":
            logger.warning("Something wrong with this node: %s", self.jd)
        if self.weight is not None:
            if self.weight < 0:
                raise GraphException(
                    f"Execution_time must be greater than 0 for Node {self.name}",
                )
            else:
                kwargs["weight"] = self.weight
        else:
            kwargs["weight"] = random.randint(3, 8)
        if app_class == "dlg.apps.simple.SleepApp":
            kwargs["sleep_time"] = self.weight

        kwargs["dropclass"] = app_class
        kwargs["num_cpus"] = int(self.jd.get("num_cpus", 1))
        if "mkn" in self.jd:
            kwargs["mkn"] = self.jd["mkn"]
        drop_spec.update(kwargs)

        return drop_spec

    def _create_data_drop(self, drop_spec):
        # backwards compatibility
        kwargs = {}
        if "dataclass" in self.jd:
            self.dropclass = self.jd["dataclass"]
        # Backwards compatibility
        if (
            not hasattr(self, "dropclass")
            or self.dropclass == "dlg.apps.simple.SleepApp"
        ):
            if self.category == "File":
                self.dropclass = "dlg.data.drops.file.FileDROP"
            elif self.category == "Memory":
                self.dropclass = "dlg.data.drops.memory.InMemoryDROP"
            elif self.category == "SharedMemory":
                self.dropclass = "dlg.data.drops.memory.SharedMemoryDROP"
            elif self.category == "S3":
                self.dropclass = "dlg.data.drops.s3_drop.S3DROP"
            elif self.category == "NGAS":
                self.dropclass = "dlg.data.drops.ngas.NgasDROP"
            else:
                raise TypeError("Unknown dropclass for drop: {str(self.jd)}")
        logger.debug("Creating data drop using class: %s", self.dropclass)
        kwargs["dropclass"] = self.dropclass
        kwargs["weight"] = self.weight
        if self.is_start_listener:
            drop_spec = self._create_listener_drops(drop_spec)
        drop_spec.update(kwargs)
        return drop_spec

    def make_single_drop(self, iid="0", **kwargs):
        """
        make only one drop from a LG nodes
        one-one mapping

        Dummy implementation as of 09/12/15
        """
        if self.is_loop:
            return {}

        oid, rank = self.make_oid(iid)
        # default spec
        drop_spec = dropdict(
            {
                "oid": oid,
                "name": self.name,
                "categoryType": self.categoryType,
                "category": self.category,
                "dropclass": self.dropclass,
                "storage": self.category,
                "rank": rank,
                "reprodata": self.jd.get("reprodata", {}),
            }
        )
        drop_spec.update(kwargs)
        if self.is_data:
            drop_spec = self._create_data_drop(drop_spec)
        elif self.is_app:
            drop_spec = self._create_app_drop(drop_spec)
        elif self.category == Categories.GROUP_BY:
            drop_spec = self._create_groupby_drops(drop_spec)
        elif self.category == Categories.GATHER:
            drop_spec = self._create_gather_drops(drop_spec)
        elif self.is_service or self.is_branch:
            kwargs["categoryType"] = "Application"
            self.jd["categoryType"] = "Application"
            drop_spec = self._create_app_drop(drop_spec)
        self._update_key_value_attributes(kwargs)
        kwargs["iid"] = iid
        kwargs["lg_key"] = self.id
        if self.is_branch:
            kwargs["categoryType"] = "Application"
        kwargs["name"] = self.name
        # Behaviour is that child-nodes inherit reproducibility data from their parents.
        if self._reprodata is not None:
            kwargs["reprodata"] = self._reprodata.copy()
        kwargs["outputPorts"] = self.jd.get("outputPorts", {})
        kwargs["inputPorts"] = self.jd.get("inputPorts", {})
        drop_spec.update(kwargs)
        return drop_spec

    @staticmethod
    def str_to_bool(value, default_value=False):
        res = True if value in ["1", "true", "True", "yes"] else default_value
        return res

    @staticmethod
    def _mkn_substitution(mkn, value):
        if "%m" in value:
            value = value.replace("%m", str(mkn[0]))
        if "%k" in value:
            value = value.replace("%k", str(mkn[1]))
        if "%n" in value:
            value = value.replace("%n", str(mkn[2]))

        return value
