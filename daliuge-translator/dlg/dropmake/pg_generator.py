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
import string

from dlg.dropmake.lg import LG, GraphException
from dlg.dropmake.pgt import PGT
from dlg.dropmake.pgtp import MetisPGTP, MySarkarPGTP, MinNumPartsPGTP, PSOPGTP

logger = logging.getLogger(f"dlg.{__name__}")


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
    lg = LG(lg, ssid=oid_prefix)
    drop_list = lg.unroll_to_tpl()
    if zerorun:
        for dropspec in drop_list:
            if "sleep_time" in dropspec:
                dropspec["sleep_time"] = 0
    if app:
        logger.info("Replacing apps with %s", app)
        for dropspec in drop_list:
            if "dropclass" in dropspec and dropspec["categoryType"] == "Application":
                dropspec["dropclass"] = app
                dropspec["sleep_time"] = (
                    dropspec["execution_time"] if "execution_time" in dropspec else 2
                )
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


def _get_algo_param(algo_params, param_name, default):
    """
    Make sure that default is set even if value has been passed as None.
    """
    param = algo_params.get(param_name)
    return param if param is not None else default


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
    min_goal = _get_algo_param(algo_params, "min_goal", 0)
    ptype = _get_algo_param(algo_params, "ptype", 0)
    max_load_imb = _get_algo_param(algo_params, "max_load_imb", 90)
    max_cpu = _get_algo_param(algo_params, "max_cpu", 8)
    max_mem = _get_algo_param(algo_params, "max_mem", 1000)
    time_greedy = _get_algo_param(algo_params, "time_greedy", 50)
    deadline = _get_algo_param(algo_params, "deadline", None)
    topk = _get_algo_param(algo_params, "topk", 30)
    swarm_size = _get_algo_param(algo_params, "swarm_size", 40)

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
            pgt,
            num_partitions,
            partition_label,
            max_dop,
            merge_parts=could_merge,
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
        "Resource mapping called with nodes: %s, islands: %s and co_host_dim: %s",
        nodes, num_islands, co_host_dim
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
        if "node" in drop_spec and "island" in drop_spec:
            nidx = int(drop_spec["node"][1:])  # skip '#'
            drop_spec["node"] = nm_list[nidx]
            iidx = int(drop_spec["island"][1:])  # skip '#'
            drop_spec["island"] = dim_list[iidx]

    return pgt  # now it's a PG
