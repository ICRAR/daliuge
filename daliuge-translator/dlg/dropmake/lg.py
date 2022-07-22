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

import collections
import datetime
import json
import logging
import math
import os
import random
import time
from itertools import product

import numpy as np
from dlg.common import Categories, DropType
from dlg.common import STORAGE_TYPES, APP_DROP_TYPES
from dlg.common import dropdict
from dlg.dropmake.dm_utils import (
    LG_APPREF,
    getNodesKeyDict,
    get_lg_ver_type,
    convert_construct,
    convert_fields,
    convert_mkn,
    LG_VER_EAGLE,
    LG_VER_EAGLE_CONVERTED,
)
from dlg.dropmake.utils.bash_parameter import BashCommand

logger = logging.getLogger(__name__)


class GraphException(Exception):
    pass


class GInvalidLink(GraphException):
    pass


class GInvalidNode(GraphException):
    pass


class LGNode:
    def __init__(self, jd, group_q, done_dict, ssid):
        """
        jd: json_dict (dict)
        group_q: group queue (defaultdict)
        done_dict: LGNode that have been processed (Dict)
        ssid:   session id (string)
        """
        self._jd = jd
        self._children = []
        self._outs = []  # event flow target
        self._inputs = []  # event flow source
        self.group = None
        self._id = jd["key"]
        self._ssid = ssid
        self._isgrp = False
        self._converted = False
        self._h_level = None
        self._g_h = None
        self._dop = None
        self._gaw = None
        self._grpw = None
        self._reprodata = jd.get("reprodata", {}).copy()
        if "isGroup" in jd and jd["isGroup"] is True:
            self._isgrp = True
            for wn in group_q[self.id]:
                wn.group = self
                self.add_child(wn)
            group_q.pop(self.id)  # not thread safe

        if "group" in jd:
            grp_id = jd["group"]
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
        return self.jd.get("text", "")

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
        self._outs.append(lg_node)

    def add_input(self, lg_node):
        self._inputs.append(lg_node)

    def add_child(self, lg_node):
        """
        Add a group member
        """
        if (
            lg_node.is_group()
            and not (lg_node.is_scatter())
            and not (lg_node.is_loop())
            and not (lg_node.is_groupby())
        ):
            raise GInvalidNode(
                "Only Scatters or Loops can be nested, but {0} is neither".format(
                    lg_node.id
                )
            )
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
        if self._h_level is None:
            _level = 0
            cg = self
            while cg.has_group():
                cg = cg.group
                _level += 1
            if self.is_mpi():
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

    def dop_diff(self, that_lgn):
        """
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
        while init_cond or cg.is_mpi():
            if cg.is_mpi():
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
        that_gh = that_lgn.group_hierarchy
        this_gh = self.group_hierarchy
        if len(that_gh) + len(this_gh) <= 1:
            # at least one is at the root level
            return True

        return that_gh.find(this_gh) > -1 or this_gh.find(that_gh) > -1

    def has_child(self):
        return len(self._children) > 0

    def has_output(self):
        return len(self._outs) > 0

    def is_start_node(self):
        return self.jd["category"] == Categories.START

    def is_end_node(self):
        return self.jd["category"] == Categories.END

    def is_start(self):
        return not self.has_group()

    def is_dag_root(self):
        leng = len(self.inputs)
        if leng > 1:
            return False
        elif leng == 0:
            if self.is_start_node():
                return False
            else:
                return True
        elif self.inputs[0].jd["category"] == Categories.START:
            return True
        else:
            return False

    def is_start_listener(self):
        return (
            len(self.inputs) == 1
            and self.inputs[0].jd["category"] == Categories.START
            and self.jd["category"] in STORAGE_TYPES
        )

    def is_group_start(self):
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

    def is_group_end(self):
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

    def is_group(self):
        return self._isgrp

    def is_scatter(self):
        return self.is_group() and self._jd["category"] == Categories.SCATTER

    def is_gather(self):
        return self._jd["category"] == Categories.GATHER

    def is_loop(self):
        return self.is_group() and self._jd["category"] == Categories.LOOP

    def is_service(self):
        """
        Determines whether a node the parent service node (not the input application)
        """
        return self._jd["category"] == Categories.SERVICE

    def is_groupby(self):
        return self._jd["category"] == Categories.GROUP_BY

    def is_mpi(self):
        return self._jd["category"] == Categories.MPI

    @property
    def group_keys(self):
        """
        Return:
            None or a list of keys (each key is an integer)
        """
        if not self.is_groupby():
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

    def is_branch(self):
        # This is the only special thing required for a branch
        return self._jd["category"] == Categories.BRANCH

    @property
    def gather_width(self):
        """
        Gather width
        """
        if self.is_gather():
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
            raise GraphException(
                "Non-Gather LGN {0} does not have gather_width".format(self.id)
            )

    @property
    def groupby_width(self):
        """
        GroupBy count
        """
        if self.is_groupby():
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
            raise GraphException(
                "Non-GroupBy LGN {0} does not have groupby_width".format(self.id)
            )

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
        if not self.is_groupby():
            return None

        tlgn = self.inputs[0]
        grpks = self.group_keys
        ret_dop = 1
        layer_index = []  # from inner to outer
        layers = []  # from inner to outer
        c = 0
        if tlgn.group.is_groupby():
            # group by followed by another group by
            if grpks is None or len(grpks) < 1:
                raise GInvalidNode(
                    "Must specify group_key for Group By '{0}'".format(self.text)
                )
            # find the "root" groupby and get all of its scatters
            inputgrp = self
            while (inputgrp is not None) and inputgrp.inputs[0].group.is_groupby():
                inputgrp = inputgrp.inputs[0].group
            # inputgrp now is the "root" groupby that follows Scatter immiately
            # move it to Scatter
            inputgrp = inputgrp.inputs[0].group
            # go thru all the scatters
            while (inputgrp is not None) and inputgrp.is_scatter():
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
                            "Wrong single group_key for {0}".format(self.text)
                        )
                else:
                    inputgrp = tlgn.group
                    # find the "groupby column list" from all layers of scatter loops
                    while (inputgrp is not None) and inputgrp.is_scatter():
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
            if self.is_group():
                if self.is_scatter():
                    for kw in ["num_of_copies", "num_of_splits"]:
                        if kw in self.jd:
                            self._dop = int(self.jd[kw])
                            break
                    if self._dop is None:
                        self._dop = 4  # dummy impl. TODO: Why is this here?
                elif self.is_gather():
                    try:
                        tlgn = self.inputs[0]
                    except IndexError:
                        raise GInvalidLink(
                            "Gather '{0}' does not have input!".format(self.id)
                        )
                    if tlgn.is_groupby():
                        tt = tlgn.dop
                    else:
                        tt = self.dop_diff(tlgn)
                    self._dop = int(math.ceil(tt / float(self.gather_width)))
                elif self.is_groupby():
                    self._dop = self.group_by_scatter_layers[0]
                elif self.is_loop():
                    self._dop = int(self.jd.get("num_of_iter", 1))
                elif self.is_service():
                    self._dop = 1  # TODO: number of compute nodes
                else:
                    raise GInvalidNode(
                        "Unrecognised (Group) Logical Graph Node: '{0}'".format(
                            self._jd["category"]
                        )
                    )
            elif self.is_mpi():
                self._dop = int(self.jd["num_of_procs"])
            else:
                self._dop = 1
        return self._dop

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
            for je in self.jd["fields"]:
                # The field to be used is not the text, but the name field
                self.jd[je["name"]] = je["value"]
                kwargs[je["name"]] = je["value"]
        kwargs["applicationArgs"] = {} # make sure the dict always exists downstream
        if "applicationArgs" in self.jd: # and fill it if provided
            for je in self.jd["applicationArgs"]:
                j = {je["name"]:{k:je[k] for k in je if k not in ['name']}}
                self.jd.update(j)
                kwargs["applicationArgs"].update(j)
        for i in range(10):
            k = "Arg%02d" % (i + 1)
            if k not in self.jd:
                continue
            v = self.jd[k]
            if v is not None and len(str(v)) > 0:
                for kv in v.split(","):  # comma separated k-v pairs
                    k_v = kv.replace(" ", "").split("=")
                    if len(k_v) > 1:
                        # Do substitutions for MKN
                        if "mkn" in self.jd:
                            kwargs[k_v[0]] = self._mkn_substitution(
                                self.jd["mkn"], k_v[1]
                            )
                        else:
                            kwargs[k_v[0]] = k_v[1]

    def _getIdText(self, port="inputPorts", index=0, portId=None):
        """
        Return IdText of port if it exists
        """
        idText = None
        if not portId:
            if (
                port in self.jd
                and len(self.jd[port]) > index
                and "IdText" in self.jd[port][index]
            ):
                idText = self.jd[port][index]["IdText"]
        else:
            if (
                port in self.jd
            ):
                idText = [p["IdText"] for p in self.jd["inputPorts"]
                    if p["Id"] == portId][0]
        return idText

    def _create_test_drop_spec(self, oid, rank, kwargs) -> dropdict:
        """
        TODO
        This is a test function only
        should be replaced by LGNode class specific methods
        """
        drop_spec = None
        drop_type = self.jd["category"]
        if drop_type in STORAGE_TYPES:
            if "data_volume" in self.jd:
                kwargs["dw"] = int(self.jd["data_volume"])  # dw -- data weight
            else:
                kwargs["dw"] = 1
            iIdText = self._getIdText(port="inputPorts")
            oIdText = self._getIdText(port="outputPorts")
            if self.is_start_listener():
                # create socket listener DROP first
                drop_spec = dropdict(
                    {
                        "oid": oid,
                        "type": DropType.PLAIN,
                        "storage": drop_type,
                        "rank": rank,
                    }
                )
                dropSpec_socket = dropdict(
                    {
                        "oid": "{0}-s".format(oid),
                        "type": DropType.APP,
                        "app": "dlg.apps.simple.SleepApp",
                        "nm": "lstnr",
                        "tw": 5,
                        "sleepTime": 5,
                        "rank": rank,
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
                        "type": DropType.PLAIN,
                        "storage": drop_type,
                        "rank": rank,
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
            self._update_key_value_attributes(kwargs)
            drop_spec.update(kwargs)
        elif drop_type in [
            Categories.COMPONENT,
            Categories.PYTHON_APP,
            Categories.BRANCH,
        ]:
            # default generic component becomes "sleep and copy"
            if "appclass" not in self.jd or len(self.jd["appclass"]) == 0:
                app_class = "dlg.apps.simple.SleepApp"
            else:
                app_class = self.jd["appclass"]

            if "execution_time" in self.jd:
                execTime = int(self.jd["execution_time"])
                if execTime < 0:
                    raise GraphException(
                        "Execution_time must be greater"
                        " than 0 for Construct '%s'" % self.text
                    )
            elif app_class != "dlg.apps.simple.SleepApp":
                raise GraphException(
                    "Missing execution_time for Construct '%s'" % self.text
                )
            else:
                execTime = random.randint(3, 8)

            if app_class == "dlg.apps.simple.SleepApp":
                kwargs["sleepTime"] = execTime

            kwargs["tw"] = execTime
            drop_spec = dropdict(
                {"oid": oid, "type": DropType.APP, "app": app_class, "rank": rank}
            )

            kwargs["num_cpus"] = int(self.jd.get("num_cpus", 1))
            if "mkn" in self.jd:
                kwargs["mkn"] = self.jd["mkn"]
            self._update_key_value_attributes(kwargs)
            drop_spec.update(kwargs)

        elif drop_type in [Categories.DYNLIB_APP, Categories.DYNLIB_PROC_APP]:
            if "libpath" not in self.jd or len(self.jd["libpath"]) == 0:
                raise GraphException("Missing 'libpath' in Drop {0}".format(self.text))
            drop_spec = dropdict(
                {
                    "oid": oid,
                    "type": DropType.APP,
                    "app": "dlg.apps.dynlib.{}".format(drop_type),
                    "rank": rank,
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
                {"oid": oid, "type": DropType.APP, "app": app_str, "rank": rank}
            )
            self._update_key_value_attributes(kwargs)
            if "execution_time" in self.jd:
                kwargs["tw"] = int(self.jd["execution_time"])
            else:
                # kwargs['tw'] = random.randint(3, 8)
                raise GraphException(
                    "Missing execution_time for Construct '%s'" % self.text
                )
            # add more arguments
            cmds = []
            for i in range(10):
                k = "Arg%02d" % (i + 1,)
                if k not in self.jd:
                    k = "arg%02d" % (i + 1,)
                    if k not in self.jd:
                        continue
                v = self.jd[k]
                if v is not None and len(str(v)) > 0:
                    cmds.append(str(v))
            # add more arguments - this is the new method of adding arguments in EAGLE
            # the method above (Arg**) is retained for compatibility, but eventually should be removed
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
            kwargs["num_cpus"] = int(self.jd.get("num_cpus", 1))
            drop_spec.update(kwargs)

        elif drop_type == Categories.DOCKER:
            # Docker application.
            app_class = "dlg.apps.dockerapp.DockerApp"
            typ = DropType.APP
            drop_spec = dropdict(
                {"oid": oid, "type": typ, "app": app_class, "rank": rank}
            )

            image = str(self.jd.get("image"))
            if image == "":
                raise GraphException("Missing image for Construct '%s'" % self.text)

            command = str(self.jd.get("command"))
            # There ARE containers which don't need/want a command
            # if command == "":
            #     raise GraphException("Missing command for Construct '%s'" % self.text)

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
            kwargs["additionalBindings"] = str(self.jd.get("additionalBindings", ""))
            kwargs["portMappings"] = str(self.jd.get("portMappings", ""))
            kwargs["shmSize"] = str(self.jd.get("shmSize", ""))
            self._update_key_value_attributes(kwargs)
            drop_spec.update(kwargs)

        elif drop_type == Categories.GROUP_BY:
            drop_spec = dropdict(
                {
                    "oid": oid,
                    "type": DropType.APP,
                    "app": "dlg.apps.simple.SleepApp",
                    "rank": rank,
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
                    "type": DropType.PLAIN,
                    "storage": Categories.MEMORY,
                    "nm": "grpdata",
                    "dw": dw,
                    "rank": rank,
                }
            )
            kwargs["grp-data_drop"] = dropSpec_grp
            kwargs["tw"] = 1  # barrier literarlly takes no time for its own computation
            kwargs["sleepTime"] = 1
            drop_spec.addOutput(dropSpec_grp, IdText="grpdata")
            dropSpec_grp.addProducer(drop_spec, IdText="grpdata")
        elif drop_type == Categories.GATHER:
            drop_spec = dropdict(
                {
                    "oid": oid,
                    "type": DropType.APP,
                    "app": "dlg.apps.simple.SleepApp",
                    "rank": rank,
                }
            )
            gi = self.inputs[0]
            if gi.is_groupby():
                gii = gi.inputs[0]
                dw = int(gii.jd["data_volume"]) * gi.groupby_width * self.gather_width
            else:  # data
                dw = int(gi.jd["data_volume"]) * self.gather_width
            dropSpec_gather = dropdict(
                {
                    "oid": "{0}-gather-data".format(oid),
                    "type": DropType.PLAIN,
                    "storage": Categories.MEMORY,
                    "nm": "gthrdt",
                    "dw": dw,
                    "rank": rank,
                }
            )
            kwargs["gather-data_drop"] = dropSpec_gather
            kwargs["tw"] = 1
            kwargs["sleepTime"] = 1
            drop_spec.addOutput(dropSpec_gather, IdText="gthrdata")
            dropSpec_gather.addProducer(drop_spec, IdText="gthrdata")
        elif drop_type == Categories.SERVICE:
            raise GraphException(
                f"DROP type: {drop_type} should not appear in physical graph"
            )
            # drop_spec = dropdict(
            #     {
            #         "oid": oid,
            #         "type": DropType.SERVICE_APP,
            #         "app": "dlg.apps.simple.SleepApp",
            #         "rank": rank
            #     }
            # )
            # kwargs["tw"] = 1

        # elif drop_type == Categories.BRANCH:
        # Branches are now dealt with like any other application and essentially ignored by the translator.

        elif drop_type in [Categories.START, Categories.END]:
            # this is at least suspicious in terms of implementation....
            drop_spec = dropdict(
                {
                    "oid": oid,
                    "type": DropType.PLAIN,
                    "storage": Categories.NULL,
                    "rank": rank,
                }
            )
        elif drop_type == Categories.LOOP:
            pass
        else:
            raise GraphException("Unknown DROP type: '{0}'".format(drop_type))
        return drop_spec

    def make_single_drop(self, iid="0", **kwargs):
        """
        make only one drop from a LG nodes
        one-one mapping

        Dummy implementation as of 09/12/15
        """
        oid, rank = self.make_oid(iid)
        dropSpec = self._create_test_drop_spec(oid, rank, kwargs)
        kwargs["iid"] = iid
        kwargs["lg_key"] = self.id
        kwargs["dt"] = self.jd["category"]
        kwargs["nm"] = self.text
        # Behaviour is that child-nodes inherit reproducibility data from their parents.
        if self._reprodata is not None:
            kwargs["reprodata"] = self._reprodata.copy()
        if "isService" in self.jd and self.jd["isService"]:
            kwargs["type"] = DropType.SERVICE_APP
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


def load_lg(f):
    if isinstance(f, str):
        if not os.path.exists(f):
            raise GraphException("Logical graph {0} not found".format(f))
        with open(f) as f:
            lg = json.load(f)
    elif hasattr(f, "read"):
        lg = json.load(f)
    else:
        lg = f
    return lg


class LG:
    """
    An object representation of Logical Graph
    """

    def __init__(self, f, ssid=None):
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
        self._gather_cache = dict()

        lgver = get_lg_ver_type(lg)

        if LG_VER_EAGLE == lgver:
            lg = convert_mkn(lg)
            lg = convert_fields(lg)
            lg = convert_construct(lg)
        elif LG_VER_EAGLE_CONVERTED == lgver:
            lg = convert_construct(lg)
        elif LG_APPREF == lgver:
            lg = convert_fields(lg)
            lgk = getNodesKeyDict(lg)
        # This ensures that future schema version mods are catched early
        else:
            raise GraphException(
                "Logical graph version '{0}' not supported!".format(lgver)
            )
        self._done_dict = dict()
        self._group_q = collections.defaultdict(list)
        self._output_q = collections.defaultdict(list)
        self._start_list = []
        all_list = []
        stream_output_ports = dict()  # key - port_id, value - construct key
        input_ports = {}
        output_ports = {}
        for jd in lg["nodeDataArray"]:
            if (
                jd["category"] == Categories.COMMENT
                or jd["category"] == Categories.DESCRIPTION
            ):
                continue
            lgn = LGNode(jd, self._group_q, self._done_dict, ssid)
            all_list.append(lgn)
            node_ouput_ports = jd.get("outputPorts", [])
            node_ouput_ports += jd.get("outputLocalPorts", [])
            # check all the outports of this node, and store "stream" output
            if len(node_ouput_ports) > 0:
                for out_port in node_ouput_ports:
                    if out_port.get("IdText", "").lower().endswith("stream"):
                        stream_output_ports[out_port["Id"]] = jd["key"]

        for lgn in all_list:
            if (
                lgn.is_start()
                and lgn.jd["category"] != Categories.COMMENT
                and lgn.jd["category"] != Categories.DESCRIPTION
            ):
                if lgn.jd["category"] == Categories.VARIABLES:
                    self._g_var.append(lgn)
                else:
                    self._start_list.append(lgn)

        self._lg_links = lg["linkDataArray"]

        for lk in self._lg_links:
            src = self._done_dict[lk["from"]]
            tgt = self._done_dict[lk["to"]]
            self.validate_link(src, tgt)
            src.add_output(tgt)
            tgt.add_input(src)
            # duplicate link from referenced APP into Gathers
            # if LG_APPREF == lgver and lgk[tgt]['category'] == 'Gather' and \
            #     'inputApplicationRef' in lgk[tgt].keys():
            #     if lgk[lk]['inputApplicationRef']:
            #         ak = lgk[lk]['inputApplicationRef']
            #         portId = lgk[ak]['inputPorts']

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
        self._lgn_list = all_list
        self._reprodata = lg.get("reprodata", {})

    def validate_link(self, src, tgt):
        # print("validate_link()", src.id, src.is_scatter(), tgt.id, tgt.is_scatter())
        if src.is_scatter() or tgt.is_scatter():
            prompt = "Remember to specify Input App Type for the Scatter construct!"
            raise GInvalidLink(
                "Scatter construct {0} or {1} cannot be linked. {2}".format(
                    src.text, tgt.text, prompt
                )
            )

        if src.is_loop() or tgt.is_loop():
            raise GInvalidLink(
                "Loop construct {0} or {1} cannot be linked".format(src.text, tgt.text)
            )

        if src.is_gather():
            if not (
                tgt.jd["category"] in APP_DROP_TYPES
                and tgt.is_group_start()
                and src.inputs[0].h_level == tgt.h_level
            ):
                raise GInvalidLink(
                    "Gather {0}'s output {1} must be a Group-Start Component inside a Group with the same H level as Gather's input".format(
                        src.id, tgt.id
                    )
                )
            # raise GInvalidLink("Gather {0} cannot be the input".format(src.id))
        if tgt.is_groupby():
            if src.is_group():
                raise GInvalidLink(
                    "GroupBy {0} input must not be a group {1}".format(tgt.id, src.id)
                )
            elif len(tgt.inputs) > 0:
                raise GInvalidLink(
                    "GroupBy {0} already has input {2} other than {1}".format(
                        tgt.id, src.id, tgt.inputs[0].id
                    )
                )
            elif src.gid == 0:
                raise GInvalidLink(
                    "GroupBy {0} requires at least one Scatter around input {1}".format(
                        tgt.id, src.id
                    )
                )
        elif tgt.is_gather():
            if not src.jd["category"] in STORAGE_TYPES and not src.is_groupby():
                raise GInvalidLink(
                    "Gather {0}'s input {1} should be either a GroupBy or Data".format(
                        tgt.id, src.id
                    )
                )

        if src.is_groupby() and not tgt.is_gather():
            raise GInvalidLink(
                "Output {1} from GroupBy {0} must be Gather, otherwise embbed {1} inside GroupBy {0}".format(
                    src.id, tgt.id
                )
            )

        if not src.h_related(tgt):
            ll = src.group
            rl = tgt.group
            if ll.is_loop() and rl.is_loop():
                valid_loop_link = True
                while True:
                    if ll is None or rl is None:
                        break
                    if ll.is_loop() and rl.is_loop():
                        if ll.dop != rl.dop:
                            valid_loop_link = False
                            break
                        else:
                            ll = ll.group
                            rl = rl.group
                    else:
                        break
                if not valid_loop_link:
                    raise GInvalidLink(
                        "{0} and {1} are not loop synchronised: {2} <> {3}".format(
                            ll.id, rl.id, ll.dop, rl.dop
                        )
                    )
            else:
                raise GInvalidLink(
                    "{0} and {1} are not hierarchically related: {2}-({4}) and {3}-({5})".format(
                        src.id,
                        tgt.id,
                        src.group_hierarchy,
                        tgt.group_hierarchy,
                        src.text,
                        tgt.text,
                    )
                )

    def lgn_to_pgn(self, lgn, iid="0", lpcxt=None):
        """
        convert logical graph node to physical graph node
        without considering pg links

        iid:    instance id (string)
        lpcxt:  Loop context
        """
        if lgn.is_group():
            # services nodes are replaced with the input application in the logical graph
            extra_links_drops = not lgn.is_scatter()
            if extra_links_drops:
                non_inputs = []
                grp_starts = []
                grp_ends = []
                for child in lgn.children:
                    if len(child.inputs) == 0:
                        non_inputs.append(child)
                    if child.is_group_start():
                        grp_starts.append(child)
                    elif child.is_group_end():
                        grp_ends.append(child)
                if len(grp_starts) == 0:
                    gs_list = non_inputs
                else:
                    gs_list = grp_starts
                if lgn.is_loop():
                    if len(grp_starts) == 0 or len(grp_ends) == 0:
                        raise GInvalidNode(
                            "Loop '{0}' should have at least one Start Component and one End Data".format(
                                lgn.text
                            )
                        )
                    for ge in grp_ends:
                        for gs in grp_starts:  # make an artificial circle
                            ge.add_output(gs)
                            gs.add_input(ge)
                            lk = dict()
                            lk["from"] = ge.id
                            lk["to"] = gs.id
                            self._lg_links.append(lk)
                else:
                    for (
                        gs
                    ) in (
                        gs_list
                    ):  # add artificial logical links to the "first" children
                        lgn.add_input(gs)
                        gs.add_output(lgn)
                        lk = dict()
                        lk["from"] = lgn.id
                        lk["to"] = gs.id
                        self._lg_links.append(lk)

            multikey_grpby = False
            lgk = lgn.group_keys
            if lgk is not None and len(lgk) > 1:
                multikey_grpby = True
                scatters = lgn.group_by_scatter_layers[
                    2
                ]  # inner most scatter to outer most scatter
                shape = [
                    x.dop for x in scatters
                ]  # inner most is also the slowest running index

            lgn_is_loop = lgn.is_loop()

            def get_child_lp_ctx(idx):
                if lgn_is_loop:
                    if lpcxt is None:
                        return "{0}".format(idx)
                    else:
                        return "{0}/{1}".format(lpcxt, idx)
                else:
                    return None

            for i in range(lgn.dop):
                miid = "{0}/{1}".format(iid, i)
                if multikey_grpby:
                    # set up more refined hierarchical context for group by with multiple keys
                    # recover multl-dimension indexes from i
                    grp_h = np.unravel_index(i, shape)
                    grp_h = [str(x) for x in grp_h]
                    miid += "${0}".format("-".join(grp_h))

                if extra_links_drops and not lgn.is_loop():
                    # make GroupBy and Gather drops
                    src_drop = lgn.make_single_drop(miid)
                    self._drop_dict[lgn.id].append(src_drop)
                    if lgn.is_groupby():
                        self._drop_dict["new_added"].append(src_drop["grp-data_drop"])
                    elif lgn.is_gather():
                        pass
                        # self._drop_dict['new_added'].append(src_drop['gather-data_drop'])
                for child in lgn.children:
                    self.lgn_to_pgn(child, miid, get_child_lp_ctx(i))
        elif lgn.is_mpi():
            for i in range(lgn.dop):
                miid = "{0}/{1}".format(iid, i)
                src_drop = lgn.make_single_drop(miid, loop_cxt=lpcxt, proc_index=i)
                self._drop_dict[lgn.id].append(src_drop)
        elif lgn.is_service():
            # no action required, inputapp node aleady created and marked with "isService"
            pass
        else:
            src_drop = lgn.make_single_drop(iid, loop_cxt=lpcxt)
            self._drop_dict[lgn.id].append(src_drop)
            if lgn.is_start_listener():
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
        if t.is_gather():
            ret = t.gather_width
        elif t.is_groupby():
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
        ] and t_type in [
            Categories.COMPONENT,
            Categories.DYNLIB_APP,
            Categories.DYNLIB_PROC_APP,
            Categories.PYTHON_APP,
        ]

    def _link_drops(self, slgn, tlgn, src_drop, tgt_drop, llink):
        """ """
        sdrop = None
        if slgn.is_gather():
            # sdrop = src_drop['gather-data_drop']
            pass
        elif slgn.is_groupby():
            sdrop = src_drop["grp-data_drop"]
        else:
            sdrop = src_drop

        if tlgn.is_gather():
            gather_oid = tgt_drop["oid"]
            if gather_oid not in self._gather_cache:
                # [self, input_list, output_list]
                self._gather_cache[gather_oid] = [tgt_drop, [], [], llink]
            tup = self._gather_cache[gather_oid]
            tup[1].append(sdrop)
            logger.debug(
                "Hit gather, link is from %s to %s", llink["from"], llink["to"]
            )
            return

        tdrop = tgt_drop
        s_type = slgn.jd["category"]
        t_type = tlgn.jd["category"]

        if self._is_stream_link(s_type, t_type):
            """
            1. create a null_drop in the middle
            2. link sdrop to null_drop
            3. link tdrop to null_drop as a streamingConsumer
            """
            dropSpec_null = dropdict(
                {
                    "oid": "{0}-{1}-stream".format(
                        sdrop["oid"], tdrop["oid"].replace(self._session_id, "")
                    ),
                    "type": DropType.PLAIN,
                    "storage": Categories.NULL,
                    "nm": "StreamNull",
                    "dw": 0,
                }
            )
            sdrop.addOutput(dropSpec_null, IdText="stream")
            dropSpec_null.addProducer(sdrop, IdText="stream")
            dropSpec_null.addStreamingConsumer(tdrop, IdText="stream")
            tdrop.addStreamingInput(dropSpec_null, IdText="stream")
            self._drop_dict["new_added"].append(dropSpec_null)
        elif s_type in APP_DROP_TYPES:
            # use name from source and ID from target
            sIdText = slgn._getIdText("outputPorts")
            tIdText = tlgn._getIdText("inputPorts")
            sdrop.addOutput(tdrop, IdText=sIdText)
            tdrop.addProducer(sdrop, IdText=tIdText)
            if Categories.BASH_SHELL_APP == s_type:
                bc = src_drop["command"]
                bc.add_output_param(tlgn.id, tgt_drop["oid"])
        else:
            if slgn.is_gather():  # don't really add them
                gather_oid = src_drop["oid"]
                if gather_oid not in self._gather_cache:
                    # [self, input_list, output_list]
                    self._gather_cache[gather_oid] = [src_drop, [], [], llink]
                tup = self._gather_cache[gather_oid]
                tup[2].append(tgt_drop)
            else:  # sdrop is a data drop
                # there should be only one port, get the name
                sIdText = slgn._getIdText("outputPorts")
                # could be multiple ports, need to identify
                portId = llink["toPort"] if "toPort" in llink else None
                tIdText = tlgn._getIdText("inputPorts", 
                            portId=portId)
                if llink.get("is_stream", False):
                    logger.debug(
                        "link stream connection %s to %s", sdrop["oid"], tdrop["oid"]
                    )
                    sdrop.addStreamingConsumer(tdrop, IdText=sIdText)
                    tdrop.addStreamingInput(sdrop, IdText=sIdText)
                else:
                    # print("not a stream from %s to %s" % (llink['from'], llink['to']))
                    sdrop.addConsumer(tdrop, IdText=sIdText)
                    tdrop.addInput(sdrop, IdText=tIdText)
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

        logger.info(
            "Unroll progress - lgn_to_pgn done %d for session %s",
                len(self._start_list), self._session_id
        )
        self_loop_aware_set = self._loop_aware_set
        for lk in self._lg_links:
            sid = lk["from"]  # source
            tid = lk["to"]  # target
            slgn = self._done_dict[sid]
            tlgn = self._done_dict[tid]
            sdrops = self._drop_dict[sid]
            tdrops = self._drop_dict[tid]
            chunk_size = self._get_chunk_size(slgn, tlgn)
            if slgn.is_group() and not tlgn.is_group():
                # this link must be artifically added (within group link)
                # since
                # 1. GroupBy's "natural" output must be a Scatter (i.e. group)
                # 2. Scatter "naturally" does not have output
                if (
                    slgn.is_gather() and tlgn.gid != sid
                ):  # not the artifical link between gather and its own start child
                    # gather iteration case, tgt must be a Group-Start Component
                    # this is a way to manually sequentialise a Scatter that has a high DoP
                    for i, ga_drop in enumerate(sdrops):
                        if ga_drop["oid"] not in self._gather_cache:
                            logger.warning(
                                "Gather %s Drop not yet in cache, sequentialisation may fail!",
                                slgn.text
                            )
                            continue
                        j = (i + 1) * slgn.gather_width
                        if j >= tlgn.group.dop and j % tlgn.group.dop == 0:
                            continue
                        while j < (i + 2) * slgn.gather_width and j < tlgn.group.dop * (
                            i + 1
                        ):
                            gather_input_list = self._gather_cache[ga_drop["oid"]][1]
                            # TODO merge this code into the function
                            # def _link_drops(self, slgn, tlgn, src_drop, tgt_drop, llink)
                            tIdText = tlgn._getIdText("inputPorts")
                            for gddrop in gather_input_list:
                                gddrop.addConsumer(tdrops[j], IdText=tIdText)
                                tdrops[j].addInput(gddrop, IdText=tIdText)
                                j += 1

                            # if 'gather-data_drop' in ga_drop:
                            #     gddrop = ga_drop['gather-data_drop'] # this is the "true" target (not source!) drop
                            #     gddrop.addConsumer(tdrops[j])
                            #     tdrops[j].addInput(gddrop)
                            #     j += 1
                else:
                    if len(sdrops) != len(tdrops):
                        err_info = "For within-group links, # {2} Group Inputs {0} must be the same as # {3} of Component Outputs {1}".format(
                            slgn.id, tlgn.id, len(sdrops), len(tdrops)
                        )
                        raise GraphException(err_info)
                    for i, sdrop in enumerate(sdrops):
                        self._link_drops(slgn, tlgn, sdrop, tdrops[i], lk)
            elif slgn.is_group() and tlgn.is_group():
                # slgn must be GroupBy and tlgn must be Gather
                self._unroll_gather_as_output(
                    slgn, tlgn, sdrops, tdrops, chunk_size, lk
                )
            elif not slgn.is_group() and (not tlgn.is_group()):
                if slgn.is_start_node():
                    continue
                elif (
                    (slgn.group is not None)
                    and slgn.group.is_loop()
                    and slgn.gid == tlgn.gid
                    and slgn.is_group_end()
                    and tlgn.is_group_start()
                ):
                    # Re-link to the next iteration's start
                    lsd = len(sdrops)
                    if lsd != len(tdrops):
                        raise GraphException(
                            "# of sdrops '{0}' != # of tdrops '{1}'for Loop '{2}'".format(
                                slgn.text, tlgn.text, slgn.group.text
                            )
                        )
                    # first add the outer construct (scatter, gather, group-by) boundary
                    # oc = slgn.group.group
                    # if (oc is not None and (not oc.is_loop())):
                    #     pass
                    loop_chunk_size = slgn.group.dop
                    for i, chunk in enumerate(
                        self._split_list(sdrops, loop_chunk_size)
                    ):
                        # logger.debug("{0} ** {1}".format(i, loop_chunk_size))
                        for j, sdrop in enumerate(chunk):
                            # logger.debug("{0} -- {1}".format(j, loop_chunk_size))
                            if j < loop_chunk_size - 1:
                                self._link_drops(
                                    slgn,
                                    tlgn,
                                    sdrop,
                                    tdrops[i * loop_chunk_size + j + 1],
                                    lk,
                                )
                                # logger.debug("{0} --> {1}".format(i * loop_chunk_size + j, i * loop_chunk_size + j + 1))

                    # for i, sdrop in enumerate(sdrops):
                    #     if (i < lsd - 1):
                    #         self._link_drops(slgn, tlgn, sdrop, tdrops[i + 1])
                elif (
                    slgn.group is not None
                    and slgn.group.is_loop()
                    and tlgn.group is not None
                    and tlgn.group.is_loop()
                    and (not slgn.h_related(tlgn))
                ):
                    # stepwise locking for links between two Loops
                    for sdrop, tdrop in product(sdrops, tdrops):
                        if sdrop["loop_cxt"] == tdrop["loop_cxt"]:
                            self._link_drops(slgn, tlgn, sdrop, tdrop, lk)
                else:
                    lpaw = ("%s-%s" % (sid, tid)) in self_loop_aware_set
                    if (
                        slgn.group is not None
                        and slgn.group.is_loop()
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
                        and tlgn.group.is_loop()
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
                if tlgn.is_groupby():
                    grpby_dict = collections.defaultdict(list)
                    layer_index = tlgn.group_by_scatter_layers[1]
                    for gdd in sdrops:
                        src_ctx = gdd["iid"].split("/")
                        if tlgn.group_keys is None:
                            # the last bit of iid (current h id) is the local GrougBy key, i.e. inner most loop context id
                            gby = src_ctx[-1]
                            if (
                                slgn.h_level - 2 == tlgn.h_level and tlgn.h_level > 0
                            ):  # groupby itself is nested inside a scatter
                                # group key consists of group context id + inner most loop context id
                                gctx = "/".join(src_ctx[0:-2])
                                gby = gctx + "/" + gby
                        else:
                            # find the "group by" scatter level
                            gbylist = []
                            if slgn.group.is_groupby():  # a chain of group bys
                                try:
                                    src_ctx = gdd["iid"].split("$")[1].split("-")
                                except IndexError:
                                    raise GraphException(
                                        "The group by hiearchy in the multi-key group by '{0}' is not specified for node '{1}'".format(
                                            slgn.group.text, slgn.text
                                        )
                                    )
                            else:
                                src_ctx.reverse()
                            for lid in layer_index:
                                gbylist.append(src_ctx[lid])
                            gby = "/".join(gbylist)
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
                            """
                            drp.addOutput(grpby_drop)
                            grpby_drop.addInput(drp)
                            """
                elif tlgn.is_gather():
                    self._unroll_gather_as_output(
                        slgn, tlgn, sdrops, tdrops, chunk_size, lk
                    )
                elif tlgn.is_service():
                    # Only the service node's inputApplication will be translated
                    # to the physical graph as a node of type SERVICE_APP instead of APP
                    # per compute instance
                    tlgn["type"] = DropType.SERVICE_APP
                else:
                    raise GraphException(
                        "Unsupported target group {0}".format(tlgn.jd["category"])
                    )

        for _, v in self._gather_cache.items():
            input_list = v[1]
            try:
                output_drop = v[2][0]  # "peek" the first element of the output list
            except:
                continue  # the gather hasn't got output drops, just move on
            llink = v[-1]
            for data_drop in input_list:
                # TODO merge this code into the function
                # def _link_drops(self, slgn, tlgn, src_drop, tgt_drop, llink)
                sIdText = slgn._getIdText("outputPorts")
                if llink.get("is_stream", False):
                    logger.debug(
                        "link stream connection %s to %s",
                        data_drop["oid"], output_drop["oid"]
                    )
                    data_drop.addStreamingConsumer(output_drop, IdText=sIdText)
                    output_drop.addStreamingInput(data_drop, IdText=sIdText)
                else:
                    data_drop.addConsumer(output_drop, IdText=sIdText)
                    output_drop.addInput(data_drop, IdText=sIdText)
                # print(data_drop['nm'], data_drop['oid'], '-->', output_drop['nm'], output_drop['oid'])

        logger.info(
            "Unroll progress - links done %d for session %s",
                len(self._lg_links), self._session_id
        )

        # clean up extra drops
        for lid, lgn in self._done_dict.items():
            if (lgn.is_start_node()) and lid in self._drop_dict:
                del self._drop_dict[lid]
            elif lgn.is_start_listener():
                for sl_drop in self._drop_dict[lid]:
                    if "listener_drop" in sl_drop:
                        del sl_drop["listener_drop"]
            elif lgn.is_groupby():
                for sl_drop in self._drop_dict[lid]:
                    if "grp-data_drop" in sl_drop:
                        del sl_drop["grp-data_drop"]
            elif lgn.is_gather():
                # lid_sub = "{0}-gather-data".format(lid)
                del self._drop_dict[lid]
                # for sl_drop in self._drop_dict[lid]:
                #     if 'gather-data_drop' in sl_drop:
                #         del sl_drop['gather-data_drop']

        logger.info(
            "Unroll progress - extra drops done for session %s",
                self._session_id
        )
        ret = []
        for drop_list in self._drop_dict.values():
            ret += drop_list

        for drop in ret:
            if drop["type"] == DropType.APP and drop["app"].endswith(
                Categories.BASH_SHELL_APP
            ):
                bc = drop["command"]
                drop["command"] = bc.to_real_command()

        return ret

    @property
    def reprodata(self):
        return self._reprodata
