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
#    Foundation, Inc., 59 Temple Place, Suite `3`30, Boston,
#    MA 02111-1307  USA
#
"""
Start the DALiuGE cluster on various facilities

Current plan (as of 12-April-2016):
    1. Launch a number of Node Managers (NM) using MPI processes
    2. Having the NM MPI processes to send their IP addresses to the Rank 0
       MPI process
    3. Launch the Island Manager (IM) on the Rank 0 MPI process using those IP
       addresses
"""

import json
import logging
import multiprocessing
import optparse # pylint: disable=deprecated-module
import os
import socket
import subprocess
import sys
import threading
import time
import uuid

from dlg.deploy import dlg_proxy, remotes
from dlg.deploy import common
from dlg import utils
from dlg.common import tool
from dlg.dropmake import pg_generator
from dlg.translator.tool_commands import parse_partition_algo_params
from dlg.manager import cmdline
from dlg.manager.client import NodeManagerClient
from dlg.constants import (
    NODE_DEFAULT_REST_PORT,
    ISLAND_DEFAULT_REST_PORT,
    MASTER_DEFAULT_REST_PORT,
)
from dlg.common.reproducibility.reproducibility import (
    init_pgt_unroll_repro_data,
    init_pgt_partition_repro_data,
    init_pg_repro_data,
)

DIM_WAIT_TIME = 60
MM_WAIT_TIME = DIM_WAIT_TIME
GRAPH_SUBMIT_WAIT_TIME = 10
GRAPH_MONITOR_INTERVAL = 5
VERBOSITY = "5"
LOGGER = logging.getLogger("deploy.dlg.cluster")
APPS = (
    None,
    "test.graphsRepository.SleepApp",
    "test.graphsRepository.SleepAndCopyApp",
)


def check_host(host, port: int, timeout: int = 5, check_with_session=False):
    """
    Checks if a given host/port is up and running (i.e., it is open).
    If ``check_with_session`` is ``True`` then it is assumed that the
    host/port combination corresponds to a Node Manager and the check is
    performed by attempting to create and delete a session.
    """
    if not check_with_session:
        return utils.portIsOpen(host, port, timeout)

    try:
        session_id = str(uuid.uuid4())
        with NodeManagerClient(host, port, timeout=timeout) as client:
            client.create_session(session_id)
            client.destroy_session(session_id)
        return True
    except Exception: #pylint: disable=broad-exception-caught
        return False


def check_hosts(
    ips,
    timeout=None,
    check_with_session=False,
    retry=1,
):
    """
    Check that the given list of IPs are all up in the given port within the
    given timeout, and returns the list of IPs that were found to be up.
    """

    def check_and_add(ip_addr):
        ntries = retry
        if ":" in ip_addr:
            ip, port = ip_addr.split(":")
            port = int(port)
        else:
            ip = ip_addr
            port = NODE_DEFAULT_REST_PORT
        while ntries:
            if check_host(
                ip,
                port,
                timeout=timeout,
                check_with_session=check_with_session,
            ):
                LOGGER.info("Host %s:%d is running", ip, port)
                return f"{ip}:{port}"
            LOGGER.warning("Failed to contact host %s:%d", ip, port)
            ntries -= 1
        return None

    # Don't return None values
    thread_pool = multiprocessing.pool.ThreadPool(min(50, len(ips)))
    result_pool = thread_pool.map(check_and_add, ips)
    thread_pool.close()
    thread_pool.join()

    return [f"{ip}" for ip in result_pool if ip]


def get_ip_via_ifconfig(iface_index):
    out = subprocess.check_output("ifconfig")
    ifaces_info = list(filter(None, out.split(b"\n\n")))
    LOGGER.info("Found %d interfaces, getting %d", len(ifaces_info), iface_index)
    for line in ifaces_info[iface_index].splitlines():
        line = line.strip()
        if line.startswith(b"inet"):
            return utils.b2s(line.split()[1])
    raise ValueError("Interace %d is not an IP interface" % iface_index)


def get_ip_via_netifaces(iface_index):
    return utils.get_local_ip_addr()[iface_index][0]


def start_node_mgr(
    log_dir,
    my_ip,
    logv=1,
    max_threads=0,
    host=None,
    event_listeners="",
    use_tool=True,
):
    """
    Start node manager
    """
    LOGGER.info("Starting node manager on host %s", my_ip)
    host = host or "0.0.0.0"
    log_level = "v" * logv
    args = [
        "-l",
        log_dir,
        "-%s" % log_level,
        "-H",
        host,
        "-m",
        "1024",
        "-t",
        str(max_threads),
    ]
    if event_listeners:
        args += ["--event-listeners", event_listeners]

    if use_tool:
        # This returns immediately
        # proc = tool.start_process("nm", args)
        proc = tool.start_process("nm", args)
        LOGGER.info("Node manager process started with pid %d", proc.pid)
        return proc
    else:
        # This blocks until NM shutdown externally
        return cmdline.dlgNM(optparse.OptionParser(), args)


def start_dim(node_list, log_dir, origin_ip, logv=1):
    """
    Start data island manager
    """
    LOGGER.info(
        "Starting island manager on host %s for node managers %r",
        origin_ip,
        node_list,
    )
    log_level = "v" * logv
    args = [
        "-l",
        log_dir,
        "-%s" % log_level,
        "-N",
        ",".join(node_list),
        "-H",
        "0.0.0.0",
        "-m",
        "2048",
        "--dump_graphs"
    ]
    proc = tool.start_process("dim", args)
    LOGGER.info("Island manager process started with pid %d", proc.pid)
    return proc


def start_mm(node_list, log_dir, logv=1):
    """
    Start master manager

    node_list:  a list of node address that host DIMs
    """
    log_level = "v" * logv
    args = [
        "-l",
        log_dir,
        "-N",
        ",".join(node_list),
        "-%s" % log_level,
        "-H",
        "0.0.0.0",
        "-m",
        "2048",
        "--dump_graphs"
    ]
    proc = tool.start_process("mm", args)
    LOGGER.info("Master manager process started with pid %d", proc.pid)
    return proc


#    cmdline.dlgMM(parser, args)


def _stop(endpoints):
    LOGGER.info("Stopping ThreadPool")

    def _the_stop(endpoint):
        common.BaseDROPManagerClient(endpoint[0], endpoint[1]).stop()

    thread_pool = multiprocessing.pool.ThreadPool(min(50, len(endpoints)))
    thread_pool.map(_the_stop, endpoints)
    thread_pool.close()
    thread_pool.join()


def stop_nms(ips):
    LOGGER.info("Stopping node managers on nodes % s", ips)
    _stop([(ip, NODE_DEFAULT_REST_PORT) for ip in ips])


def stop_dims(ips):
    LOGGER.info("Stopping island managers on nodes %s", ips)
    _stop([(ip, ISLAND_DEFAULT_REST_PORT) for ip in ips])


def stop_mm(ip_addr):
    LOGGER.info("Stopping master managers on node %s", ip_addr)
    _stop([(ip_addr, MASTER_DEFAULT_REST_PORT)])


def submit_and_monitor(physical_graph, opts, host, port, submit=True):
    def _task():
        dump_path = None
        if opts.dump:
            dump_path = os.path.join(opts.log_dir, "status-monitoring.json")
        if submit:
            session_id = common.submit(
                physical_graph, host=host, port=port, session_id=opts.ssid
            )
        else:
            session_id = opts.ssid

        LOGGER.info("Start monitoring session(s) %s on host %s:%s",
                    session_id, host, port)
        while True:
            try:
                common.monitor_sessions(
                    session_id,
                    host=host,
                    port=port,
                    status_dump_path=dump_path,
                )
                break
            except Exception: #pylint: disable=broad-exception-caught
                LOGGER.exception("Monitoring %s:%s failed, restarting it", host, port)
                time.sleep(5)

    threads = threading.Thread(target=_task)
    threads.start()
    return threads


def start_proxy(dlg_host, dlg_port, monitor_host, monitor_port):
    """
    Start the DALiuGE proxy server
    """
    proxy_id = socket.gethostname() + "%.3f" % time.time()
    server = dlg_proxy.ProxyServer(
        proxy_id, dlg_host, monitor_host, dlg_port, monitor_port
    )
    try:
        server.loop()
    except KeyboardInterrupt:
        LOGGER.warning("Ctrl C - Stopping DALiuGE Proxy server")
        sys.exit(1)
    except Exception: # pylint: disable=broad-exception-caught
        LOGGER.exception("DALiuGE proxy terminated unexpectedly")
        sys.exit(1)


def modify_pg(pgt, modifier):
    parts = modifier.split(",")
    func = utils.get_symbol(parts[0])
    args = list(filter(lambda x: "=" not in x, parts[1:]))
    kwargs = dict(map(lambda x: x.split("="), filter(lambda x: "=" in x, parts[1:])))
    return func(pgt, *args, **kwargs)


def get_pg(opts, nms, dims):
    """Gets the Physical Graph that is eventually submitted to the cluster, if any"""

    if not opts.logical_graph and not opts.physical_graph:
        return []

    num_nms = len(nms)
    num_dims = len(dims)
    if opts.logical_graph:
        unrolled = init_pgt_unroll_repro_data(
            pg_generator.unroll(
                opts.logical_graph, opts.ssid, opts.zerorun, APPS[opts.app]
            )
        )
        reprodata = {}
        if not unrolled[-1].get("oid"):
            reprodata = unrolled.pop()
        algo_params = parse_partition_algo_params(opts.algo_params)
        pgt = pg_generator.partition(
            unrolled,
            opts.part_algo,
            num_partitions=num_nms,
            num_islands=num_dims,
            **algo_params,
        )
        pgt.append(reprodata)
        pgt = init_pgt_partition_repro_data(pgt)
        del unrolled  # quickly dispose of potentially big object
    else:
        with open(opts.physical_graph, "rb") as pg_file:
            pgt = json.load(pg_file)

    # modify the PG as necessary
    for modifier in opts.pg_modifiers.split(":"):
        if modifier is not None and modifier != "":
            modify_pg(pgt, modifier)

    # Check which NMs are up and use only those form now on
    nms = check_hosts(
        nms,
        check_with_session=opts.check_with_session,
        timeout=MM_WAIT_TIME,
        retry=3,
    )
    LOGGER.info("Mapping graph to available resources: nms %s, dims %s",
                nms, dims)
    physical_graph = init_pg_repro_data(
        pg_generator.resource_map(
            pgt, dims + nms, num_islands=num_dims, co_host_dim=opts.co_host_dim
        )
    )
    if opts.physical_graph:
        graph_name = f"{opts.physical_graph.split('.pgt.graph')[0]}.pg.graph"
    else:
        graph_name = os.path.basename(opts.log_dir)
        graph_name = f"{graph_name.split('_')[0]}.pg.graph"
    with open(os.path.join(opts.log_dir, graph_name), "wt") as pg_file:
        json.dump(physical_graph, pg_file)
    return physical_graph


def get_ip(opts):
    find_ip = get_ip_via_ifconfig if opts.use_ifconfig else get_ip_via_netifaces
    return find_ip(opts.interface)


def get_remote(opts):
    my_ip = get_ip(opts)
    if opts.remote_mechanism == "mpi":
        return remotes.MPIRemote(opts, my_ip)
    if opts.remote_mechanism == "dlg":
        return remotes.DALiuGERemote(opts, my_ip)
    if opts.remote_mechanism == "dlg-hybrid":
        return remotes.DALiuGEHybridRemote(opts, my_ip)
    else:  # == 'slurm'
        return remotes.SlurmRemote(opts, my_ip)


def main():
    parser = optparse.OptionParser()
    parser.add_option(
        "-l",
        "--log_dir",
        action="store",
        type="string",
        dest="log_dir",
        help="Log directory (required)",
    )
    # if this parameter is present, it means we want to get monitored
    parser.add_option(
        "-m",
        "--monitor_host",
        action="store",
        type="string",
        dest="monitor_host",
        help="Monitor host IP (optional)",
    )
    parser.add_option(
        "-o",
        "--monitor_port",
        action="store",
        type="int",
        dest="monitor_port",
        help="Monitor port",
        default=dlg_proxy.default_dlg_monitor_port,
    )
    parser.add_option(
        "-v",
        "--verbose-level",
        action="store",
        type="int",
        dest="verbose_level",
        help="Verbosity level (1-3) of the DIM/NM logging",
        default=1,
    )
    parser.add_option(
        "-z",
        "--zerorun",
        action="store_true",
        dest="zerorun",
        help="Generate a physical graph that takes no time to run",
        default=False,
    )
    parser.add_option(
        "--app",
        action="store",
        type="int",
        dest="app",
        help="The app to use in the PG. 1=SleepApp (default), 2=SleepAndCopy",
        default=0,
    )

    parser.add_option(
        "-t",
        "--max-threads",
        action="store",
        type="int",
        dest="max_threads",
        help="Max thread pool size used for executing drops. 0 (default) means no pool.",
        default=0,
    )

    parser.add_option(
        "-L",
        "--logical-graph",
        action="store",
        type="string",
        dest="logical_graph",
        help="The filename of the logical graph to deploy",
        default=None,
    )
    parser.add_option(
        "-P",
        "--physical-graph",
        action="store",
        type="string",
        dest="physical_graph",
        help="The filename of the physical graph (template) to deploy",
        default=None,
    )

    parser.add_option(
        "-s",
        "--num_islands",
        action="store",
        type="int",
        dest="num_islands",
        help="The number of Data Islands",
    )

    parser.add_option(
        "-d",
        "--dump",
        action="store_true",
        dest="dump",
        help="dump file base name?",
        default=False,
    )

    parser.add_option(
        "-i",
        "--interface",
        type="int",
        help="Index of network interface to use as the external interface/address for each host",
        default=0,
    )

    parser.add_option(
        "--part-algo",
        type="string",
        dest="part_algo",
        help="Partition algorithms",
        default="metis",
    )
    parser.add_option(
        "-A",
        "--algo-param",
        action="append",
        dest="algo_params",
        help="Extra name=value parameters used by the algorithms (algorithm-specific)",
        default=[],
    )

    parser.add_option(
        "--ssid", type="string", dest="ssid", help="session id", default=""
    )

    parser.add_option(
        "-u",
        "--all_nics",
        action="store_true",
        dest="all_nics",
        help="Listen on all NICs for a node manager",
        default=True,
    )

    parser.add_option(
        "--check-interfaces",
        action="store_true",
        dest="check_interfaces",
        help="Run a small network interfaces test and exit",
        default=False,
    )
    parser.add_option(
        "--collect-interfaces",
        action="store_true",
        dest="collect_interfaces",
        help="Collect all interfaces and exit",
        default=False,
    )
    parser.add_option(
        "--use-ifconfig",
        action="store_true",
        dest="use_ifconfig",
        help="Use ifconfig to find a suitable external interface/address for each host",
        default=False,
    )
    parser.add_option(
        "-S",
        "--check_with_session",
        action="store_true",
        dest="check_with_session",
        help="Check for node managers' availability by creating/destroy a session",
        default=False,
    )

    parser.add_option(
        "--event-listeners",
        action="store",
        type="string",
        dest="event_listeners",
        help="A colon-separated list of event listener classes to be used",
        default="",
    )

    parser.add_option(
        "--sleep-after-execution",
        action="store",
        type="int",
        dest="sleep_after_execution",
        help="Sleep time interval after graph execution finished",
        default=0,
    )

    parser.add_option(
        "--pg-modifiers",
        help=(
            "A colon-separated list of python functions that modify a PG before submission. "
            "Each specification is in the form of <funcname>[,[arg1=]val1][,[arg2=]val2]..."
        ),
        default="",
    )

    parser.add_option(
        "-r",
        "--remote-mechanism",
        help="The mechanism used by this script to coordinate remote processes",
        choices=["mpi", "slurm", "dlg", "dlg-hybrid"],
        default="mpi",
    )

    parser.add_option(
        "--co-host-dim",
        action="store_true",
        dest="co_host_dim",
        help="Start DIM on first NM node",
    )

    (options, _) = parser.parse_args()

    if options.check_interfaces:
        try:
            print("From netifaces: %s" % get_ip_via_netifaces(options.interface))
        except IndexError:
            LOGGER.exception("Failed to get information via netifaces")
        try:
            print("From ifconfig: %s" % get_ip_via_ifconfig(options.interface))
        except ValueError:
            LOGGER.exception("Failed to get information via ifconfig")
        sys.exit(0)
    elif options.collect_interfaces:
        from mpi4py import MPI

        comm = MPI.COMM_WORLD  # @UndefinedVariable
        ips = comm.allgather(get_ip(options))
        if comm.Get_rank() == 0:
            print(" ".join(ips))
        sys.exit(0)

    if bool(options.logical_graph) == bool(options.physical_graph):
        parser.error(
            "Either a logical graph or physical graph filename must be specified"
        )
    for graph_file_name in (options.logical_graph, options.physical_graph):
        if graph_file_name and not os.path.exists(graph_file_name):
            parser.error("Cannot locate graph file at '{0}'".format(graph_file_name))

    if options.monitor_host is not None and options.num_islands > 1:
        parser.error("We do not support proxy monitor multiple islands yet")

    # if options.ssid == "":
    #     options.ssid = time.

    remote = get_remote(options)

    log_dir = "{0}/{1}".format(options.log_dir, remote.my_ip)
    os.makedirs(log_dir)
    logfile = log_dir + "/start_dlg_cluster.log"
    log_format = (
        "%(asctime)-15s [%(levelname)5.5s] [%(threadName)15.15s] "
        "%(name)s#%(funcName)s:%(lineno)s %(message)s"
    )
    logging.basicConfig(filename=logfile, level=logging.DEBUG, format=log_format)

    LOGGER.info("This node has IP address: %s", remote.my_ip)

    envfile_name = os.path.join(log_dir, "env.txt")
    LOGGER.debug("Dumping process' environment to %s", envfile_name)
    with open(envfile_name, "wt") as env_file:
        for name, value in sorted(os.environ.items()):
            env_file.write("%s=%s\n" % (name, value))

    logv = max(min(3, options.verbose_level), 1)

    # need to dump nodes file first
    if remote.is_highest_level_manager:
        LOGGER.info("Node %s is hosting the highest level manager",
                    remote.my_ip)
        nodesfile = os.path.join(log_dir, "nodes.txt")
        LOGGER.debug("Dumping list of nodes to %s", nodesfile)
        with open(nodesfile, "wt") as env_file:
            env_file.write("\n".join(remote.sorted_peers))
    # start the NM
    LOGGER.debug(
        "my_ip:%s; dim_ips:%s, node_ips: %s",
        remote.my_ip, remote.dim_ips, remote.nm_ips
    )
    if options.num_islands == 1:
        REST_PORT = ISLAND_DEFAULT_REST_PORT

        # need to check for NM first and go on if co-hosted
        if remote.is_nm:
            co_hosted = remote.my_ip in remote.dim_ips
            _ = start_node_mgr(
                log_dir,
                remote.my_ip,
                logv=logv,
                max_threads=options.max_threads,
                host=None if options.all_nics else remote.my_ip,
                event_listeners=options.event_listeners,
                use_tool=co_hosted,
            )

        if remote.is_proxy:
            # Wait until the Island Manager is open
            if utils.portIsOpen(remote.hl_mgr_ip, ISLAND_DEFAULT_REST_PORT, 100):
                start_proxy(
                    remote.hl_mgr_ip,
                    ISLAND_DEFAULT_REST_PORT,
                    options.monitor_host,
                    options.monitor_port,
                )
            else:
                LOGGER.warning(
                    "Couldn't connect to the main drop manager, proxy not started"
                )
        elif remote.my_ip in remote.dim_ips:
            co_hosted = True
            nm_uris = [f"{ip}:{NODE_DEFAULT_REST_PORT}" for ip in remote.nm_ips]
            LOGGER.info("Starting island managers on nodes: %s", remote.dim_ips)
            _ = start_dim(nm_uris, log_dir, remote.my_ip, logv=logv)
            # whichever way we came from, now we have to wait until session is finished
            # we always monitor the island, else we will have race conditions
            physical_graph = get_pg(options, nm_uris, remote.dim_ips)
            monitoring_thread = submit_and_monitor(
                physical_graph,
                options,
                remote.dim_ips[0],
                REST_PORT,
                submit=co_hosted,
            )
            monitoring_thread.join()
            # now the session is finished

            # still shutting DIM down first to avoid monitoring conflicts
            stop_dims(remote.dim_ips)
            # now stop all the NMs
            stop_nms(remote.nm_ips)

        # shouldn't need this in addition
        # if dim_proc is not None:
        #     # Stop DALiuGE.
        #     LOGGER.info("Stopping DALiuGE island manager on rank %d", remote.rank)
        #     utils.terminate_or_kill(dim_proc, 5)

    elif remote.is_highest_level_manager:
        # TODO: In the case of more than one island the NMs are not yet started

        physical_graph = get_pg(options, remote.nm_ips, remote.dim_ips)
        remote.send_dim_nodes(physical_graph)

        # 7. make sure all DIMs are up running
        dim_ips_up = check_hosts(
            remote.dim_ips,
            timeout=MM_WAIT_TIME,
            retry=10,
        )
        if len(dim_ips_up) < len(remote.dim_ips):
            LOGGER.warning(
                "Not all DIMs were up and running: %d/%d",
                len(dim_ips_up),
                len(remote.dim_ips),
            )

        monitoring_thread = submit_and_monitor(
            physical_graph, options, remote.my_ip, MASTER_DEFAULT_REST_PORT
        )
        _ = start_mm(remote.dim_ips, log_dir, logv=logv)
        monitoring_thread.join()
        stop_mm(
            remote.my_ip
        )  # TODO: I don't think we need this and least not in the single island case
        stop_dims(remote.dim_ips)
    else:
        nm_ips = remote.recv_dim_nodes()
        proc = start_dim(nm_ips, log_dir, remote.my_ip, logv=logv)
        utils.wait_or_kill(proc, 1e8, period=5)
        stop_nms(remote.nm_ips)


if __name__ == "__main__":
    main()
