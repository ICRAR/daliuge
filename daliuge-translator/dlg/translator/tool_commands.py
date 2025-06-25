#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2016
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
"""sub-commands for the dlg command line utility"""

import json
import logging
import optparse # pylint: disable=deprecated-module
import os
import sys

from dlg.common import tool
from dlg.common.reproducibility.reproducibility import (
    init_lgt_repro_data,
    init_lg_repro_data,
    init_pgt_unroll_repro_data,
    init_pgt_partition_repro_data,
    init_pg_repro_data,
)
from dlg.dropmake.pgt import GPGTNoNeedMergeException

logger = logging.getLogger(f"dlg.{__name__}")


def _open_i(path, flags=None):
    if path == "-":
        logger.warning("Waiting for stdin '<' ")
        logger.warning("Please use Ctrl-D to signify end of input.")
        return sys.stdin
    return open(os.path.expanduser(path), flags or "r")


def _open_o(path, flags=None):
    if path == "-":
        logger.warning("Waiting for stdout '>' ")
        logger.warning("Please use Ctrl-D to signify end.")
        return sys.stdout
    return open(os.path.expanduser(path), flags or "w")


def unroll(lg_path, oid_prefix, zerorun=False, app=None):
    """
    Unrolls the Logical Graph in `lg_graph` into a Physical Graph Template
    and return the latter.
    This method prepends `oid_prefix` to all generated Drop OIDs.
    """
    from ..dropmake.pg_generator import unroll as pg_unroll

    logger.info("Start to unroll %s", lg_path)
    return pg_unroll(_open_i(lg_path), oid_prefix=oid_prefix, zerorun=zerorun, app=app)


_param_types = {
    "min_goal": int,
    "ptype": int,
    "max_load_imb": int,
    "max_cpu": int,
    "time_greedy": float,
    "deadline": int,
    "topk": int,
    "swarm_size": int,
    "max_mem": int,
}


def parse_partition_algo_params(algo_params):
    # Double-check that parameters are of shape name=value
    for p in algo_params:
        if len(list(filter(None, p.split("=")))) != 2:
            raise optparse.OptionValueError(
                "Algorithm parameter has no form of name=value: %s" % (p,)
            )
    # Extract algorithm parameters and convert to proper type
    return {
        n: _param_types[n](v)
        for n, v in map(lambda p: p.split("="), algo_params)
        if n in _param_types
    }


def partition(pgt, opts):
    from ..dropmake import pg_generator

    algo_params = parse_partition_algo_params(opts.algo_params or [])
    pg = pg_generator.partition(
        pgt,
        algo=opts.algo,
        num_partitions=opts.partitions,
        num_islands=opts.islands,
        partition_label="partition",
        **algo_params
    )
    logger.info("PG spec is calculated!")
    return pg


def submit(pg, opts):
    from dlg.deploy import common

    session_id = common.submit(
        pg,
        host=opts.host,
        port=opts.port,
        skip_deploy=opts.skip_deploy,
        session_id=opts.session_id,
    )
    if opts.wait:
        common.monitor_sessions(
            session_id,
            host=opts.host,
            port=opts.port,
            poll_interval=opts.poll_interval,
        )

    if opts.reproducibility:
        dump = _setup_output(opts)
        common.monitor_sessions_repro(
            session_id,
            host=opts.host,
            port=opts.port,
            poll_interval=opts.poll_interval,
        )
        repro_data = common.fetch_reproducibility(
            session_id,
            host=opts.host,
            port=opts.port,
            poll_interval=opts.poll_interval,
        )
        dump(repro_data["graph"])


def _add_output_options(parser):
    parser.add_option(
        "-o",
        "--output",
        action="store",
        dest="output",
        type="string",
        help="Where the output should be written to (default: stdout)",
        default="-",
    )
    parser.add_option(
        "-f",
        "--format",
        action="store_true",
        dest="format",
        help="Format JSON output (newline, 2-space indent)",
    )


def _setup_output(opts):
    def dump(obj):
        with _open_o(opts.output) as f:
            json.dump(obj, f, indent=None if opts.format is None else 2)

    return dump


def dlg_fill(parser, args):
    tool.add_logging_options(parser)
    _add_output_options(parser)
    parser.add_option(
        "-L",
        "--logical-graph",
        default="-",
        help="Path to the Logical Graph (default: stdin)",
    )
    parser.add_option(
        "-p",
        "--parameter",
        action="append",
        help="Parameter specification (either 'name=value' or a JSON string)",
        default=[],
    )
    parser.add_option(
        "-R",
        "--reproducibility",
        default="0",
        help="Level of reproducibility. Default 0 (NOTHING). Accepts '0,1,2,4,5,6,7,8'",
    )

    (opts, args) = parser.parse_args(args)
    tool.setup_logging(opts)
    dump = _setup_output(opts)

    def param_spec_type(s):
        if s.startswith("{"):
            return "json"
        elif "=" in s:
            return "kv"
        else:
            return None

    # putting all parameters together in a single dictionary
    for p in opts.parameter:
        if param_spec_type(p) is None:
            parser.error("Parameter %s is neither JSON nor has it key=value form" % p)
    params = [p.split("=") for p in opts.parameter if param_spec_type(p) == "kv"]
    params = dict(params)
    for json_param in (
        json.loads(p) for p in opts.parameter if param_spec_type(p) == "json"
    ):
        params.update(json_param)

    from ..dropmake.pg_generator import fill

    graph = fill(_open_i(opts.logical_graph), params)
    dump(init_lg_repro_data(init_lgt_repro_data(graph, opts.reproducibility)))


def _add_unroll_options(parser):
    parser.add_option(
        "-L",
        "--logical-graph",
        action="store",
        dest="lg_path",
        type="string",
        help="Path to the Logical Graph (default: stdin)",
        default="-",
    )
    parser.add_option(
        "-p",
        "--oid-prefix",
        action="store",
        dest="oid_prefix",
        type="string",
        help="Prefix to use for generated OIDs",
        default="1",
    )
    parser.add_option(
        "-z",
        "--zerorun",
        action="store_true",
        dest="zerorun",
        help="Generate a Physical Graph Template that takes no time to run",
        default=False,
    )
    parser.add_option(
        "--app",
        action="store",
        type="int",
        dest="app",
        help="Force an app to be used in the Physical Graph. 0=Don't force, 1=SleepApp, 2=SleepAndCopy",
        default=0,
    )
    apps = (
        None,
        "dlg.apps.simple.SleepApp",
        "dlg.apps.simple.SleepAndCopyApp",
    )
    return apps


def dlg_unroll(parser, args):
    # Unroll Logical Graph
    tool.add_logging_options(parser)
    _add_output_options(parser)
    apps = _add_unroll_options(parser)
    (opts, args) = parser.parse_args(args)
    tool.setup_logging(opts)
    dump = _setup_output(opts)
    pgt = unroll(
        opts.lg_path, opts.oid_prefix, zerorun=opts.zerorun, app=apps[opts.app]
    )
    # logger.debug(">>> pgt: %s", pgt)
    dump(init_pgt_unroll_repro_data(pgt))


def _add_partition_options(parser):
    from ..dropmake import pg_generator

    parser.add_option(
        "-N",
        "--partitions",
        action="store",
        type="int",
        dest="partitions",
        help="Number of partitions to generate",
        default=1,
    )
    parser.add_option(
        "-i",
        "--islands",
        action="store",
        type="int",
        dest="islands",
        help="Number of islands to use during the partitioning",
        default=1,
    )
    parser.add_option(
        "-a",
        "--algorithm",
        action="store",
        type="choice",
        choices=pg_generator.known_algorithms(),
        dest="algo",
        help=f"Algorithm used to do the partitioning. Select from:\n"
             f"{str(pg_generator.known_algorithms()).strip('[]')}",
        default="metis",
    )
    parser.add_option(
        "-A",
        "--algorithm-param",
        action="append",
        dest="algo_params",
        help="Extra name=value parameters used by the algorithms (algorithm-specific)",
    )


def dlg_partition(parser, args):
    tool.add_logging_options(parser)
    _add_output_options(parser)
    _add_partition_options(parser)
    parser.add_option(
        "-P",
        "--physical-graph-template",
        action="store",
        dest="pgt_path",
        type="string",
        help="Path to the Physical Graph Template (default: stdin)",
        default="-",
    )

    opts, args = parser.parse_args(args)

    tool.setup_logging(opts)
    dump = _setup_output(opts)
    with _open_i(opts.pgt_path) as fi:
        pgt = json.load(fi)
    if not isinstance(pgt, list):
        print("\nOption 'partition' expects an unrolled graph, which is a "
              "JSON-compatible list."
              "\nEither you have passed in the wrong file, "
              "or you need to unroll your Logical Graph first.")
        print("\n\nRun 'dlg unroll -h' for more information.\n")

        sys.exit()

    repro = pgt.pop()  # TODO: Re-integrate
    try:
        pgt = partition(pgt, opts)
    except GPGTNoNeedMergeException:
        print("\nThe combination of -N nodes and -i Islands does not work for "
                "the graph provided. "
              "\nThis is either a result of the parallelism of the graph being too low,"
              " or i >= N."
              "\nConsider reducing the number of islands.\n")
    pgt.append(repro)
    dump(init_pgt_partition_repro_data(pgt))


def dlg_unroll_and_partition(parser, args):
    tool.add_logging_options(parser)
    _add_output_options(parser)
    apps = _add_unroll_options(parser)
    _add_partition_options(parser)
    (opts, args) = parser.parse_args(args)
    tool.setup_logging(opts)
    dump = _setup_output(opts)

    pgt = unroll(
        opts.lg_path, opts.oid_prefix, zerorun=opts.zerorun, app=apps[opts.app]
    )
    init_pgt_unroll_repro_data(pgt)
    repro = pgt.pop()  # TODO: Re-integrate
    pgt = partition(pgt, opts)
    pgt.append(repro)
    dump(init_pgt_partition_repro_data(pgt))


def dlg_map(parser, args):
    import dlg.constants as con

    tool.add_logging_options(parser)
    _add_output_options(parser)
    parser.add_option(
        "-H",
        "--host",
        action="store",
        dest="host",
        help="The host we connect to to deploy the graph",
        default="localhost",
    )
    parser.add_option(
        "-p",
        "--port",
        action="store",
        type="int",
        dest="port",
        help="The port we connect to to deploy the graph",
        default=con.ISLAND_DEFAULT_REST_PORT,
    )
    parser.add_option(
        "-P",
        "--physical-graph-template",
        action="store",
        dest="pgt_path",
        type="string",
        help="Path to the Physical Graph to submit (default: stdin)",
        default="-",
    )
    parser.add_option(
        "-N",
        "--nodes",
        action="store",
        dest="nodes",
        help="The nodes where the Physical Graph will be distributed, comma-separated",
        default=None,
    )
    parser.add_option(
        "-i",
        "--islands",
        action="store",
        type="int",
        dest="islands",
        help="Number of islands to use during the partitioning",
        default=1,
    )
    parser.add_option(
        "--co-host-dim",
        action="store_true",
        dest="co_host_dim",
        help="co-host DIMs with NM during the partitioning",
        default=True,
    )
    (opts, args) = parser.parse_args(args)
    tool.setup_logging(opts)
    dump = _setup_output(opts)

    from ..dropmake import pg_generator
    from dlg.clients import CompositeManagerClient

    if opts.nodes:
        nodes = [n for n in opts.nodes.split(",") if n]
    else:
        client = CompositeManagerClient(opts.host, opts.port, timeout=10)
        nodes = [opts.host] + client.nodes()

    n_nodes = len(nodes)
    if n_nodes <= opts.islands:
        raise Exception(
            "#nodes (%d) should be bigger than number of islands (%d)"
            % (n_nodes, opts.islands)
        )

    with _open_i(opts.pgt_path) as f:
        pgt = json.load(f)

    repro = pgt.pop()  # TODO: Re-include
    pg = pg_generator.resource_map(
        pgt, nodes, opts.islands, co_host_dim=opts.co_host_dim
    )
    pg.append(repro)
    dump(init_pg_repro_data(pg))


def dlg_submit(parser, args):
    import dlg.constants as con

    # Submit Physical Graph
    _add_output_options(parser)
    tool.add_logging_options(parser)
    parser.add_option(
        "-H",
        "--host",
        action="store",
        dest="host",
        help="The host we connect to to deploy the graph",
        default="localhost",
    )
    parser.add_option(
        "-p",
        "--port",
        action="store",
        type="int",
        dest="port",
        help="The port we connect to to deploy the graph",
        default=con.ISLAND_DEFAULT_REST_PORT,
    )
    parser.add_option(
        "-P",
        "--physical-graph",
        action="store",
        dest="pg_path",
        type="string",
        help="Path to the Physical Graph to submit (default: stdin)",
        default="-",
    )
    parser.add_option(
        "-s",
        "--session-id",
        action="store",
        dest="session_id",
        type="string",
        help="Session ID (default: <pg_name>-<current-time>)",
        default=None,
    )
    parser.add_option(
        "-S",
        "--skip-deploy",
        action="store_true",
        dest="skip_deploy",
        help="Skip the deployment step (default: False)",
        default=False,
    )
    parser.add_option(
        "-w",
        "--wait",
        action="store_true",
        help="Wait for the graph execution to finish (default: False)",
        default=False,
    )
    parser.add_option(
        "-i",
        "--poll-interval",
        type="float",
        help="Polling interval used for monitoring the execution (default: 10)",
        default=10,
    )
    parser.add_option(
        "-R",
        "--reproducibility",
        action="store_true",
        dest="reproducibility",
        help="Fetch (and output) reproducibility data for the final execution graph "
        "(default: False)",
    )
    (opts, args) = parser.parse_args(args)

    with _open_i(opts.pg_path) as f:
        pg = json.load(f)
        repro = pg[-1]
        submit(pg, opts)
        logger.warning("Submitted")
        pg.append(repro)


def register_commands():
    translator_group = tool.CommandGroup(
        "translator",
        "Commands for unrolling and partitioning graphs using the dlg translator.")
    tool.cmdwrap(
        "lgweb", translator_group,
        "A Web server for the Logical Graph Editor",
        "dlg.dropmake.web.translator_rest:run",
    )
    tool.cmdwrap(
        "submit", translator_group,
        "Submits a Physical Graph to a Drop Manager", dlg_submit
    )
    tool.cmdwrap(
        "map", translator_group,
        "Maps a Physical Graph Template to resources and produces a Physical Graph",
        dlg_map,
    )
    tool.cmdwrap(
        "unroll", translator_group,
        "Unrolls a Logical Graph into a Physical Graph Template",
        dlg_unroll,
    )
    tool.cmdwrap(
        "partition", translator_group,
        "Divides a Physical Graph Template into N logical partitions",
        dlg_partition,
    )
    tool.cmdwrap("unroll-and-partition", translator_group,
                 "unroll + partition", dlg_unroll_and_partition)
    tool.cmdwrap("fill", translator_group,
                 "Fill a Logical Graph with parameters", dlg_fill)
