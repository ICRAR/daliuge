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

import logging
import math
import random

from dlg.common import CategoryType, DropType
from dlg.common import dropdict
from dlg.dropmake.dm_utils import (
    GraphException,
    GInvalidLink,
    GInvalidNode,
)
from dlg.dropmake.utils.bash_parameter import BashCommand
from .definition_classes import Categories, DATA_TYPES, APP_TYPES

logger = logging.getLogger(__name__)


class LGNode:
    def __init__(self, jd, group_q, done_dict, ssid):
        """
        jd: json_dict (dict)
        group_q: group queue (defaultdict)
        done_dict: LGNode that have been processed (Dict)
        ssid:   session id (string)
        """
        self._id = jd["key"]
        self.group_q = group_q
        self.group = None
        self._children = []
        self._outputs = []  # event flow target
        self._inputs = []  # event flow source
        self.jd = jd
        self.inputPorts = jd
        self.outputPorts = jd
        # self.group = None
        self._ssid = ssid
        self._is_app = self.jd["categoryType"] == CategoryType.APPLICATION
        self._is_data = self.jd["categoryType"] == CategoryType.DATA
        self._converted = False
        self._h_level = None
        self._g_h = None
        self._dop = None
        self._gaw = None
        self._grpw = None
        self.inputPorts = "inputPorts"
        self.outputPorts = "outputPorts"
        logger.debug("%s input_ports: %s", self.name, self.inputPorts)
        logger.debug("%s output_ports: %s", self.name, self.outputPorts)
        self._nodetype = ""  # e.g. Data or Application
        self._nodeclass = ""  # e.g. dlg.apps.simple.HelloWorldAPP
        self._reprodata = jd.get("reprodata", {}).copy()
        if "isGroup" in jd and jd["isGroup"] is True:
            self._is_group = True
            for wn in group_q[self.id]:
                wn.group = self
                self.add_child(wn)
            group_q.pop(self.id)  # not thread safe
        else:
            self._is_group = False

        if "group" in jd:
            grp_id = jd["group"]
            if grp_id in done_dict:
                grp_nd = done_dict[grp_id]
                self.group = grp_nd
                grp_nd.add_child(self)
            else:
                group_q[grp_id].append(self)

        done_dict[self.id] = self

    # def __str__(self):
    #     return json.dumps(self.jd)

    @property
    def inputPorts(self):
        return self._inputPorts

    @inputPorts.setter
    def inputPorts(self, value):
        if (
            "categoryType" in value and value["categoryType"] == "Construct"
        ) or ("type" in value and value["type"] == "Construct"):
            self._inputPorts = []
        elif not "inputPorts" in value:
            self._inputPorts = [
                f
                for f in value["fields"]
                if "usage" in f and f["usage"] in ["InputPort", "InOutPort"]
            ]
            # we need this as long as the fields are still using "name"
            if len(self._inputPorts) > 0 and "name" in self._inputPorts[0]:
                for p in self._inputPorts:
                    p["name"] = p["name"]
        else:
            self._inputPorts = value["inputPorts"]

    @property
    def outputPorts(self):
        return self._outputPorts

    @outputPorts.setter
    def outputPorts(self, value):
        if (
            "categoryType" in value and value["categoryType"] == "Construct"
        ) or ("type" in value and value["type"] == "Construct"):
            self._outputPorts = []
        elif not "outputPorts" in value:
            self._outputPorts = [
                f
                for f in value["fields"]
                if f["usage"] in ["OutputPort", "InOutPort"]
            ]
            # we need this as long as the fields are still using "name"
            if len(self._outputPorts) > 0 and "name" in self._outputPorts[0]:
                for p in self._outputPorts:
                    p["name"] = p["name"]
        else:
            self._outputPorts = value["outputPorts"]

    @property
    def jd(self):
        return self._jd

    @jd.setter
    def jd(self, value):
        """
        Setting he jd property to the original data structure directly loaded
        from JSON.
        """
        if "categoryType" not in value:
            if value["category"] in APP_TYPES:
                value["categoryType"] = CategoryType.APPLICATION
            elif value["category"] in DATA_TYPES:
                value["categoryType"] = CategoryType.DATA
        self._jd = value

    @property
    def reprodata(self):
        return self._reprodata

    @property
    def is_group(self):
        return self._is_group

    @property
    def id(self):
        return self._id

    @property
    def nodetype(self):
        return self._nodetype

    @nodetype.setter
    def nodetype(self, value):
        self._nodetype = value

    @property
    def nodeclass(self):
        return self._nodeclass

    @nodeclass.setter
    def nodeclass(self, value):
        if value == CategoryType.DATA:
            self.is_data = True
        if value == CategoryType.APPLICATION:
            self.is_app = True
        self._nodeclass = value

    @property
    def name(self):
        if self.jd.get("name"):
            # backwards compatibility
            # TODO: deprecated
            return self.jd.get("name", "")
        else:
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

    def add_output(self, lg_node):
        if lg_node not in self._outputs:
            self._outputs.append(lg_node)

    def add_input(self, lg_node):
        # only add if not already there
        # this may happen in nested constructs
        if lg_node not in self._inputs:
            self._inputs.append(lg_node)

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
            self._g_h = "/".join(reversed(glist))
        return self._g_h

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
        if self.has_group() and "group_start" in self.jd:
            gs = self.jd["group_start"]
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
        if self.has_group() and "group_end" in self.jd:
            ge = self.jd["group_end"]
            if type(ge) == type(True):
                result = ge
            elif type(ge) in [type(1), type(1.0)]:
                result = 1 == ge
            elif type(ge) == type("s"):
                result = ge.lower() in ("true", "1")
        return result

    @property
    def is_group(self):
        """
        is this a group (aka construct) node
        """
        return self._is_group

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
                )

    @property
    def inputPorts(self):
        return self._inputPorts

    @inputPorts.setter
    def inputPorts(self, port="inputPorts"):
        self._inputPorts = self._getIdText(port="inputPorts", index=-1)

    @property
    def outputPorts(self):
        return self._outputPorts

    @outputPorts.setter
    def outputPorts(self, port="outputPorts"):
        self._outputPorts = self._getIdText(port="outputPorts", index=-1)

    @property
    def gather_width(self):
        """
        Gather width
        """
        if self.is_gather:
            if self._gaw is None:
                try:
                    self._gaw = int(self.jd["num_of_inputs"])
                except:
                    self._gaw = 1
            return self._gaw
        else:
            """
            TODO: use OO style to replace all type-related statements!
            """
            return None
            # raise GraphException(
            #     "Non-Gather LGN {0} does not have gather_width".format(self.id)
            # )

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
            # raise GraphException(
            #     "Non-GroupBy LGN {0} does not have groupby_width".format(
            #         self.id
            #     )
            # )

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
                    "Must specify group_key for Group By '{0}'".format(
                        self.name
                    )
                )
            # find the "root" groupby and get all of its scatters
            inputgrp = self
            while (inputgrp is not None) and inputgrp.inputs[
                0
            ].group.is_groupby:
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
                    for kw in ["num_of_copies", "num_of_splits"]:
                        if kw in self.jd:
                            self._dop = int(self.jd[kw])
                            break
                    if self._dop is None:
                        self._dop = 4  # dummy impl. TODO: Why is this here?
                elif self.is_gather:
                    try:
                        tlgn = self.inputs[0]
                    except IndexError:
                        raise GInvalidLink(
                            "Gather '{0}' does not have input!".format(self.id)
                        )
                    if tlgn.is_groupby:
                        tt = tlgn.dop
                    else:
                        tt = self.dop_diff(tlgn)
                    self._dop = int(math.ceil(tt / float(self.gather_width)))
                elif self.is_groupby:
                    self._dop = self.group_by_scatter_layers[0]
                elif self.is_loop:
                    self._dop = int(self.jd.get("num_of_iter", 1))
                elif self.is_service:
                    self._dop = 1  # TODO: number of compute nodes
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
        rank = [int(x) for x in iid.split("/")]
        return "{0}_{1}_{2}".format(self._ssid, self.id, iid), rank

    def _update_key_value_attributes(self, kwargs):
        # get the arguments from new fields dictionary in a backwards compatible way
        if "fields" in self.jd:
            self.jd.update({"nodeAttributes": {}})
            kwargs.update({"nodeAttributes": {}})
            for je in self.jd["fields"]:
                # The field to be used is not the text, but the name field
                self.jd[je["name"]] = je["value"]
                kwargs[je["name"]] = je["value"]
                self.jd["nodeAttributes"].update({je["name"]: je})
                kwargs["nodeAttributes"].update({je["name"]: je})
        kwargs[
            "applicationArgs"
        ] = {}  # make sure the dict always exists downstream
        if "applicationArgs" in self.jd:  # and fill it if provided
            for je in self.jd["applicationArgs"]:
                j = {je["name"]: {k: je[k] for k in je if k not in ["name"]}}
                self.jd.update(j)
                kwargs["applicationArgs"].update(j)
        if "nodeAttributes" not in kwargs:
            kwargs.update({"nodeAttributes": {}})
        for k, na in kwargs["nodeAttributes"].items():
            if (
                "parameterType" in na
                and na["parameterType"] == "ApplicationArgument"
            ):
                kwargs["applicationArgs"].update({k: na})
        # NOTE: drop Argxx keywords

    def _getIdText(self, port="outputPorts", index=0, portId=None):
        """
        Return IdText of port if it exists

        NOTE: This has now been changed to use the 'name' rather than idText, in anticipation
        of removing idText completely.
        TODO: only returns the first one!!
        """
        port_selector = {
            "inputPorts": ["InputPort", "InputOutput"],
            "outputPorts": ["OutputPort", "InputOutput"],
        }
        ports_dict = {}
        idText = None
        if portId is None and index >= 0:
            if (
                port in self.jd
                and len(self.jd[port]) > index
                and "name" in self.jd[port][index]
            ):
                idText = self.jd[port][index]["name"]
            else:  # everything in 'fields'
                if port in port_selector:
                    for field in self.jd["fields"]:
                        if "usage" not in field:  # fixes manual graphs
                            continue
                        if field["usage"] in port_selector[port]:
                            idText = field["name"]
                            # can't be sure that name is unique
                            if idText not in ports_dict:
                                ports_dict[idText] = [field["id"]]
                            else:
                                ports_dict[idText].append(field["id"])
        else:
            if port in self.jd:
                idText = [
                    p["name"] for p in self.jd[port] if p["Id"] == portId
                ]
                idText = idText[0] if len(idText) > 0 else None
        return idText if index >= 0 else ports_dict

    def create_drop_spec(self, oid, rank, kwargs) -> dropdict:
        """
        New implementation of drop_spec generation method.
        """
        drop_spec = {}
        return drop_spec

    def _create_test_drop_spec(self, oid, rank, kwargs) -> dropdict:
        """
        NOTE: This IS the main function right now, still called 'test' and still should be replaced!!!
        TODO
        This is a test function only
        should be replaced by LGNode class specific methods
        """
        drop_spec = None
        drop_type = self.category
        drop_class = self.category if self.category else self.categoryType

        # backwards compatibility
        if self.categoryType == "Unknown" and "type" in self.jd:
            drop_type = self.jd["type"]
            if drop_type in DATA_TYPES:
                drop_class = "Data"
            elif drop_type in APP_TYPES:
                drop_class = "Application"
            else:
                drop_class = "Unknown"

        self.nodeclass = drop_class
        self.nodetype = drop_type
        if self.is_data:
            if "data_volume" in self.jd:
                kwargs["dw"] = int(self.jd["data_volume"])  # dw -- data weight
            else:
                kwargs["dw"] = 1
            iIdText = self._getIdText(port="inputPorts")
            oIdText = self._getIdText(port="outputPorts")
            logger.debug(
                "Found port names for %s: IN: %s, OUT: %s",
                oid,
                iIdText,
                oIdText,
            )
            if self.is_start_listener:
                # create socket listener DROP first
                drop_spec = dropdict(
                    {
                        "oid": oid,
                        "categoryType": CategoryType.DATA,
                        "category": drop_type,
                        "dataclass": "dlg.data.drops.memory.InMemoryDROP",
                        "storage": drop_type,
                        "rank": rank,
                        "reprodata": self.jd.get("reprodata", {}),
                    }
                )
                dropSpec_socket = dropdict(
                    {
                        "oid": "{0}-s".format(oid),
                        "categoryType": CategoryType.APPLICATION,
                        "category": "PythonApp",
                        "appclass": "dlg.apps.simple.SleepApp",
                        "nm": "lstnr",
                        "name": "lstnr",
                        "tw": 5,
                        "sleepTime": 1,
                        "rank": rank,
                        "reprodata": self.jd.get("reprodata", {}),
                    }
                )
                # tw -- task weight
                dropSpec_socket["autostart"] = 1
                kwargs["listener_drop"] = dropSpec_socket
                dropSpec_socket.addOutput(drop_spec, IdText=oIdText)
            else:
                drop_spec = dropdict(
                    {
                        "oid": oid,
                        "categoryType": CategoryType.DATA,
                        "category": drop_type,
                        "dataclass": "dlg.data.drops.memory.InMemoryDROP",
                        "storage": drop_type,
                        "rank": rank,
                        "reprodata": self.jd.get("reprodata", {}),
                    }
                )
            if drop_type == Categories.FILE:
                dn = self.jd.get("dirname", None)
                if dn:
                    kwargs["dirname"] = dn
                cfe = str(self.jd.get("check_filepath_exists", "0"))
                cfeb = True if cfe in ["1", "true", "yes"] else False
                kwargs["check_filepath_exists"] = cfeb
                fp = self.jd.get("filepath", None)
                if fp:
                    kwargs["filepath"] = fp
                kwargs["dataclass"] = str(
                    self.jd.get("dataclass", "dlg.data.drops.file.FileDROP")
                )
            if drop_type == Categories.MEMORY:
                kwargs["dataclass"] = str(
                    self.jd.get(
                        "dataclass", "dlg.data.drops.memory.InMemoryDROP"
                    )
                )
            self._update_key_value_attributes(kwargs)
            drop_spec.update(kwargs)
        elif self.nodetype in [
            Categories.COMPONENT,
            Categories.PYTHON_APP,
            Categories.BRANCH,
            Categories.PLASMA,
        ]:
            # default generic component becomes "sleep and copy"
            if "appclass" not in self.jd or len(self.jd["appclass"]) == 0:
                app_class = "dlg.apps.simple.SleepApp"
                self.jd[DropType.APPCLASS] = app_class
                self.jd["category"] = Categories.PYTHON_APP
            else:
                app_class = self.jd[DropType.APPCLASS]

            if "execution_time" in self.jd:
                execTime = int(self.jd["execution_time"])
                if execTime < 0:
                    raise GraphException(
                        "Execution_time must be greater"
                        " than 0 for Construct '%s'" % self.name
                    )
            elif app_class != "dlg.apps.simple.SleepApp":
                raise GraphException(
                    "Missing execution_time for Construct '%s'" % self.name
                )
            else:
                execTime = random.randint(3, 8)

            if app_class == "dlg.apps.simple.SleepApp":
                kwargs["sleepTime"] = execTime

            kwargs["tw"] = execTime
            self.jd["execution_time"] = execTime
            drop_spec = dropdict(
                {
                    "oid": oid,
                    "categoryType": CategoryType.APPLICATION,
                    "appclass": app_class,
                    "rank": rank,
                    "reprodata": self.jd.get("reprodata", {}),
                }
            )

            kwargs["num_cpus"] = int(self.jd.get("num_cpus", 1))
            if "mkn" in self.jd:
                kwargs["mkn"] = self.jd["mkn"]
            self._update_key_value_attributes(kwargs)
            drop_spec.update(kwargs)

        elif drop_type in [Categories.DYNLIB_APP, Categories.DYNLIB_PROC_APP]:
            if "libpath" not in self.jd or len(self.jd["libpath"]) == 0:
                raise GraphException(
                    "Missing 'libpath' in Drop {0}".format(self.name)
                )
            drop_spec = dropdict(
                {
                    "oid": oid,
                    "categoryType": CategoryType.APPLICATION,
                    "appclass": "dlg.apps.dynlib.{}".format(drop_type),
                    "rank": rank,
                    "reprodata": self.jd.get("reprodata", {}),
                }
            )
            kwargs["lib"] = self.jd["libpath"]
            kwargs["tw"] = int(self.jd["execution_time"])
            if "mkn" in self.jd:
                kwargs["mkn"] = self.jd["mkn"]

            self._update_key_value_attributes(kwargs)

            drop_spec.update(kwargs)
        elif drop_type in [Categories.BASH_SHELL_APP, Categories.MPI]:
            if drop_type == Categories.MPI:
                app_str = "dlg.apps.mpi.MPIApp"
                kwargs["maxprocs"] = int(self.jd.get("num_of_procs", 4))
            else:
                app_str = "dlg.apps.bash_shell_app.BashShellApp"
            drop_spec = dropdict(
                {
                    "oid": oid,
                    "categoryType": CategoryType.APPLICATION,
                    "appclass": app_str,
                    "rank": rank,
                    "reprodata": self.jd.get("reprodata", {}),
                }
            )
            self._update_key_value_attributes(kwargs)
            if "execution_time" in self.jd:
                try:
                    kwargs["tw"] = int(self.jd["execution_time"])
                except TypeError:
                    kwargs["tw"] = int(self.jd["execution_time"]["value"])
            else:
                # kwargs['tw'] = random.randint(3, 8)
                raise GraphException(
                    "Missing execution_time for Construct '%s'" % self.name
                )
            # add more arguments (support for Arg0x dropped!)
            cmds = []
            for k in [
                "command",
                "input_redirection",
                "output_redirection",
                "command_line_arguments",
            ]:
                if k in self.jd:
                    cmds.append(self.jd[k])
            # kwargs['command'] = ' '.join(cmds)
            if drop_type == Categories.MPI:
                kwargs["command"] = BashCommand(cmds).to_real_command()
            else:
                kwargs["command"] = BashCommand(
                    cmds
                )  # TODO: Check if this actually solves a problem.
            try:
                kwargs["num_cpus"] = int(self.jd.get("num_cpus", 1))
            except TypeError:
                kwargs["num_cpus"] = int(self.jd["num_cpus"]["value"])
            drop_spec.update(kwargs)

        elif drop_type == Categories.DOCKER:
            # Docker application.
            app_class = "dlg.apps.dockerapp.DockerApp"
            typ = CategoryType.APPLICATION
            drop_spec = dropdict(
                {
                    "oid": oid,
                    "categoryType": typ,
                    "appclass": app_class,
                    "rank": rank,
                    "reprodata": self.jd.get("reprodata", {}),
                }
            )

            image = str(self.jd.get("image"))
            if image == "":
                raise GraphException(
                    "Missing image for Construct '%s'" % self.name
                )

            command = str(self.jd.get("command"))
            # There ARE containers which don't need/want a command
            # if command == "":
            #     raise GraphException("Missing command for Construct '%s'" % self.name)

            kwargs["tw"] = int(self.jd.get("execution_time", "5"))
            kwargs["image"] = image
            kwargs["command"] = command
            kwargs["user"] = str(self.jd.get("user", ""))
            kwargs["ensureUserAndSwitch"] = self.str_to_bool(
                str(self.jd.get("ensureUserAndSwitch", "0"))
            )
            kwargs["removeContainer"] = self.str_to_bool(
                str(self.jd.get("removeContainer", "1"))
            )
            kwargs["additionalBindings"] = str(
                self.jd.get("additionalBindings", "")
            )
            kwargs["portMappings"] = str(self.jd.get("portMappings", ""))
            kwargs["shmSize"] = str(self.jd.get("shmSize", ""))
            self._update_key_value_attributes(kwargs)
            drop_spec.update(kwargs)

        elif drop_type == Categories.GROUP_BY:
            drop_spec = dropdict(
                {
                    "oid": oid,
                    "categoryType": CategoryType.APPLICATION,
                    "appclass": "dlg.apps.simple.SleepApp",
                    "rank": rank,
                    "reprodata": self.jd.get("reprodata", {}),
                }
            )
            sij = self.inputs[0].jd
            if not "data_volume" in sij:
                raise GInvalidNode(
                    "GroupBy should be connected to a DataDrop, not '%s'"
                    % sij["category"]
                )
            dw = int(sij["data_volume"]) * self.groupby_width
            dropSpec_grp = dropdict(
                {
                    "oid": "{0}-grp-data".format(oid),
                    "categoryType": CategoryType.DATA,
                    "dataclass": "dlg.data.drops.memory.InMemoryDROP",
                    "nm": "grpdata",
                    "name": "grpdata",
                    "dw": dw,
                    "rank": rank,
                    "reprodata": self.jd.get("reprodata", {}),
                }
            )
            kwargs["grp-data_drop"] = dropSpec_grp
            kwargs[
                "tw"
            ] = 1  # barrier literarlly takes no time for its own computation
            kwargs["sleepTime"] = 1
            drop_spec.addOutput(dropSpec_grp, IdText="grpdata")
            dropSpec_grp.addProducer(drop_spec, IdText="grpdata")
        elif drop_type == Categories.GATHER:
            drop_spec = dropdict(
                {
                    "oid": oid,
                    "categoryType": CategoryType.APPLICATION,
                    "appclass": "dlg.apps.simple.SleepApp",
                    "rank": rank,
                    "reprodata": self.jd.get("reprodata", {}),
                }
            )
            gi = self.inputs[0]
            if gi.is_groupby:
                gii = gi.inputs[0]
                dw = (
                    int(gii.jd["data_volume"])
                    * gi.groupby_width
                    * self.gather_width
                )
            else:  # data
                dw = int(gi.jd["data_volume"]) * self.gather_width
            dropSpec_gather = dropdict(
                {
                    "oid": "{0}-gather-data".format(oid),
                    "categoryType": CategoryType.DATA,
                    "dataclass": "dlg.data.drops.memory.InMemoryDROP",
                    "nm": "gthrdt",
                    "name": "gthrdt",
                    "dw": dw,
                    "rank": rank,
                    "reprodata": self.jd.get("reprodata", {}),
                }
            )
            kwargs["gather-data_drop"] = dropSpec_gather
            kwargs["tw"] = 1
            kwargs["sleepTime"] = 1
            drop_spec.addOutput(dropSpec_gather, IdText="gthrdata")
            dropSpec_gather.addProducer(drop_spec, IdText="gthrdata")
        elif drop_class == Categories.SERVICE:
            raise GraphException(
                f"DROP type: {drop_class} should not appear in physical graph"
            )
        elif drop_type in [Categories.START, Categories.END]:
            # this is at least suspicious in terms of implementation....
            drop_spec = dropdict(
                {
                    "oid": oid,
                    "categoryType": CategoryType.DATA,
                    "dataclass": "dlg.data.drops.data_base.NullDROP",
                    "dw": 0,
                    "rank": rank,
                    "reprodata": self.jd.get("reprodata", {}),
                }
            )
        elif drop_type in Categories.LOOP:
            drop_spec = {}
        else:
            raise GraphException(
                "Unknown DROP type: '{0} {1}'".format(drop_type, drop_class)
            )
        return drop_spec

    def make_single_drop(self, iid="0", **kwargs):
        """
        make only one drop from a LG nodes
        one-one mapping

        Dummy implementation as of 09/12/15
        """
        oid, rank = self.make_oid(iid)
        dropSpec = self._create_test_drop_spec(oid, rank, kwargs)
        if dropSpec is None:
            logger.error(
                ">>>> Unsuccessful creating dropSpec!!%s, %s, %s",
                oid,
                rank,
                kwargs,
            )
            raise ValueError
        kwargs["iid"] = iid
        kwargs["lg_key"] = self.id
        kwargs["dt"] = self.category
        kwargs["category"] = self.category
        if "categoryType" in kwargs:
            kwargs["categoryType"] = self.categoryType
        elif self.jd["category"] in DATA_TYPES:
            kwargs["categoryType"] = "Data"
        else:
            kwargs["categoryType"] = "Application"
        kwargs["nm"] = self.name
        kwargs["name"] = self.name
        # Behaviour is that child-nodes inherit reproducibility data from their parents.
        if self._reprodata is not None:
            kwargs["reprodata"] = self._reprodata.copy()
        if self.is_service:
            kwargs["categoryType"] = DropType.SERVICECLASS
        dropSpec.update(kwargs)
        return dropSpec

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
