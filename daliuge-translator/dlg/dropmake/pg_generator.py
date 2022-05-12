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
import string
import time
from collections import defaultdict
from itertools import product
from typing import ValuesView

import networkx as nx
import numpy as np

from .scheduler import MySarkarScheduler, DAGUtil, MinNumPartsScheduler, PSOScheduler
from .utils.bash_parameter import BashCommand
from ..common import dropdict
from ..common import Categories, DropType
from .dm_utils import (
    LG_APPREF,
    getNodesKeyDict,
    get_lg_ver_type,
    convert_construct,
    convert_fields,
    convert_mkn,
    getAppRefInputs,
    LG_VER_EAGLE,
    LG_VER_EAGLE_CONVERTED,
)
from .utils.bash_parameter import BashCommand
from ..common import Categories
from ..common import STORAGE_TYPES, APP_DROP_TYPES
from ..common import dropdict
from dlg.dropmake.lg import LG, GraphException
from dlg.dropmake.pgt import PGT
from dlg.dropmake.pgtp import MetisPGTP, MySarkarPGTP, MinNumPartsPGTP, PSOPGTP

logger = logging.getLogger(__name__)


class _LGTemplate(string.Template):
    delimiter = "~"
    idpattern = r"[_a-z][_a-z0-9\.]*"


def _flatten_dict(d):
    flat = dict(d)
    for key, value in d.items():
        if isinstance(value, dict):
            flattened = _flatten_dict(value)
            flat.update({"%s.%s" % (key, k): v for k, v in flattened.items()})
        else:
            flat[key] = value
    return flat


def fill(lg, params):
    """Logical Graph + params -> Filled Logical Graph"""
    logger.info("Filling Logical Graph with parameters: %r", params)
    flat_params = _flatten_dict(params)
    if hasattr(lg, "read"):
        lg = lg.read()
    elif not isinstance(lg, str):
        lg = json.dumps(lg)
    lg = _LGTemplate(lg).substitute(flat_params)
    return json.loads(lg)


def unroll(lg, oid_prefix=None, zerorun=False, app=None):
    """Unrolls a logical graph"""
    start = time.time()
    lg = LG(lg, ssid=oid_prefix)
    drop_list = lg.unroll_to_tpl()
    logger.info(
        "Logical Graph unroll completed in %.3f [s]. # of Drops: %d",
        (time.time() - start),
        len(drop_list),
    )
    # Optionally set sleepTimes to 0 and apps to a specific type
    if zerorun:
        for dropspec in drop_list:
            if "sleepTime" in dropspec:
                dropspec["sleepTime"] = 0
    if app:
        for dropspec in drop_list:
            if "app" in dropspec:
                dropspec["app"] = app
    drop_list.append(lg.reprodata)
    return drop_list


ALGO_NONE = 0
ALGO_METIS = 1
ALGO_MY_SARKAR = 2
ALGO_MIN_NUM_PARTS = 3
ALGO_PSO = 4

_known_algos = {
    "none": ALGO_NONE,
    "metis": ALGO_METIS,
    "mysarkar": ALGO_MY_SARKAR,
    "min_num_parts": ALGO_MIN_NUM_PARTS,
    "pso": ALGO_PSO,
    ALGO_NONE: "none",
    ALGO_METIS: "metis",
    ALGO_MY_SARKAR: "mysarkar",
    ALGO_MIN_NUM_PARTS: "min_num_parts",
    ALGO_PSO: "pso",
}


def known_algorithms():
    return [x for x in _known_algos.keys() if isinstance(x, str)]


def partition(
    pgt,
    algo,
    num_partitions=1,
    num_islands=1,
    partition_label="partition",
    show_gojs=False,
    **algo_params,
):
    """Partitions a Physical Graph Template"""

    if isinstance(algo, str):
        if algo not in _known_algos:
            raise ValueError(
                "Unknown partitioning algorithm: %s. Known algorithms are: %r"
                % (algo, _known_algos.keys())
            )
        algo = _known_algos[algo]

    if algo not in _known_algos:
        raise GraphException(
            "Unknown partition algorithm: %d. Known algorithm are: %r"
            % (algo, _known_algos.keys())
        )

    logger.info(
        "Running partitioning with algorithm=%s, %d partitions, "
        "%d islands, and parameters=%r",
        _known_algos[algo],
        num_partitions,
        num_islands,
        algo_params,
    )

    # Read all possible values with defaults
    # Not all algorithms use them, but makes the coding easier
    # do_merge = num_islands > 1
    could_merge = True
    min_goal = algo_params.get("min_goal", 0)
    ptype = algo_params.get("ptype", 0)
    max_load_imb = algo_params.get("max_load_imb", 90)
    max_cpu = algo_params.get("max_cpu", 8)
    max_mem = algo_params.get("max_mem", 1000)
    time_greedy = algo_params.get("time_greedy", 50)
    deadline = algo_params.get("deadline", None)
    topk = algo_params.get("topk", 30)
    swarm_size = algo_params.get("swarm_size", 40)

    max_dop = {"num_cpus": max_cpu, "mem_usage": max_mem}

    if algo == ALGO_NONE:
        pgt = PGT(pgt)

    elif algo == ALGO_METIS:
        ufactor = 100 - max_load_imb + 1
        if ufactor <= 0:
            ufactor = 1
        pgt = MetisPGTP(
            pgt,
            num_partitions,
            min_goal,
            partition_label,
            ptype,
            ufactor,
            merge_parts=could_merge,
        )

    elif algo == ALGO_MY_SARKAR:
        pgt = MySarkarPGTP(
            pgt, num_partitions, partition_label, max_dop, merge_parts=could_merge
        )

    elif algo == ALGO_MIN_NUM_PARTS:
        time_greedy = 1 - time_greedy / 100.0  # assuming between 1 to 100
        pgt = MinNumPartsPGTP(
            pgt,
            deadline,
            num_partitions,
            partition_label,
            max_cpu,
            merge_parts=could_merge,
            optimistic_factor=time_greedy,
        )

    elif algo == ALGO_PSO:
        pgt = PSOPGTP(
            pgt,
            partition_label,
            max_dop,
            deadline=deadline,
            topk=topk,
            swarm_size=swarm_size,
            merge_parts=could_merge,
        )

    else:
        raise GraphException("Unknown partition algorithm: {0}".format(algo))

    pgt.to_gojs_json(string_rep=False, visual=show_gojs)
    if not show_gojs:
        pgt = pgt.to_pg_spec(
            [],
            ret_str=False,
            num_islands=num_islands,
            tpl_nodes_len=num_partitions + num_islands,
        )
    return pgt


def resource_map(pgt, nodes, num_islands=1, co_host_dim=True):
    """Maps a Physical Graph Template `pgt` to `nodes`"""

    logger.info(
        f"Resource mapping called with nodes: {nodes}, islands: {num_islands} and co_host_dim: {co_host_dim}"
    )
    if not nodes:
        err_info = "Empty node_list, cannot map the PG template"
        raise ValueError(err_info)

    # if co_host_dim == True the island nodes appear twice
    dim_list = nodes[0:num_islands]
    nm_list = nodes[num_islands:]
    if type(pgt[0]) is str:
        pgt = pgt[1]  # remove the graph name TODO: we may want to retain that
    for drop_spec in pgt:
        nidx = int(drop_spec["node"][1:])  # skip '#'
        drop_spec["node"] = nm_list[nidx]
        iidx = int(drop_spec["island"][1:])  # skip '#'
        drop_spec["island"] = dim_list[iidx]

    return pgt  # now it's a PG
