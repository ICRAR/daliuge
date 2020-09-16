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
"""
Start the DALiuGE cluster on Magnus / Galaxy at Pawsey

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
import optparse
import os
import socket
import subprocess
import sys
import threading
import time
import uuid

from . import dfms_proxy, remotes
from .. import common
from ... import utils, tool
from ...dropmake import pg_generator
from ...manager import cmdline
from ...manager.client import NodeManagerClient
from ...manager.constants import NODE_DEFAULT_REST_PORT, \
ISLAND_DEFAULT_REST_PORT, MASTER_DEFAULT_REST_PORT


DIM_WAIT_TIME = 60
MM_WAIT_TIME = DIM_WAIT_TIME
GRAPH_SUBMIT_WAIT_TIME = 10
GRAPH_MONITOR_INTERVAL = 5
VERBOSITY = '5'
logger = logging.getLogger('deploy.pawsey.cluster')
apps = (
    None,
    "test.graphsRepository.SleepApp",
    "test.graphsRepository.SleepAndCopyApp",
)

def check_host(host, port, timeout=5, check_with_session=False):
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
        with NodeManagerClient(host, port, timeout=timeout) as c:
            c.create_session(session_id)
            c.destroy_session(session_id)
        return True
    except:
        return False

def check_hosts(ips, port, timeout=None, check_with_session=False, retry=1):
    """
    Check that the given list of IPs are all up in the given port within the
    given timeout, and returns the list of IPs that were found to be up.
    """

    def check_and_add(ip):
        ntries = retry
        while ntries:
            if check_host(ip, port, timeout=timeout, check_with_session=check_with_session):
                logger.info("Host %s:%d is running", ip, port)
                return ip
            logger.warning("Failed to contact host %s:%d", ip, port)
            ntries -= 1
        return None

    # Don't return None values
    tp = multiprocessing.pool.ThreadPool(min(50, len(ips)))
    up = tp.map(check_and_add, ips)
    tp.close()
    tp.join()

    return [ip for ip in up if ip]

def get_ip_via_ifconfig(iface_index):
    out = subprocess.check_output('ifconfig')
    ifaces_info = list(filter(None, out.split(b'\n\n')))
    logger.info('Found %d interfaces, getting %d', len(ifaces_info), iface_index)
    for line in ifaces_info[iface_index].splitlines():
        line = line.strip()
        if line.startswith(b'inet'):
                return utils.b2s(line.split()[1])
    raise ValueError('Interace %d is not an IP interface' % iface_index)

def get_ip_via_netifaces(iface_index):
    return utils.get_local_ip_addr()[iface_index][0]

def start_node_mgr(log_dir, my_ip, logv=1, max_threads=0, host=None, event_listeners=''):
    """
    Start node manager
    """
    logger.info("Starting node manager on host %s", my_ip)
    host = host or '0.0.0.0'
    lv = 'v' * logv
    args = ['-l', log_dir, '-%s' % lv, '-H', host, '-m', '1024', '-t',
            str(max_threads), '--no-dlm',
            '--event-listeners', event_listeners]
    return cmdline.dlgNM(optparse.OptionParser(), args)

def start_dim(node_list, log_dir, origin_ip, logv=1):
    """
    Start data island manager
    """
    logger.info("Starting island manager on host %s for node managers %r", origin_ip, node_list)
    lv = 'v' * logv
    args = ['-l', log_dir, '-%s' % lv, '-N', ','.join(node_list),
            '-H', '0.0.0.0', '-m', '2048']
    proc = tool.start_process('dim', args)
    logger.info('Island manager process started with pid %d', proc.pid)
    return proc

def start_mm(node_list, log_dir, logv=1):
    """
    Start master manager

    node_list:  a list of node address that host DIMs
    """
    lv = 'v' * logv
    parser = optparse.OptionParser()
    args = ['-l', log_dir, '-N', ','.join(node_list), '-%s' % lv,
            '-H', '0.0.0.0', '-m', '2048']
    cmdline.dlgMM(parser, args)


def _stop(endpoints):
    def _the_stop(endpoint):
        common.BaseDROPManagerClient(endpoint[0], endpoint[1]).stop()
    tp = multiprocessing.pool.ThreadPool(min(50, len(endpoints)))
    tp.map(_the_stop, endpoints)
    tp.close()
    tp.join()

def stop_nms(ips):
    _stop([(ip, NODE_DEFAULT_REST_PORT) for ip in ips])

def stop_dims(ips):
    _stop([(ip, ISLAND_DEFAULT_REST_PORT) for ip in ips])

def stop_mm(ip):
    _stop([(ip, MASTER_DEFAULT_REST_PORT)])

def submit_and_monitor(pg, opts, port):
    def _task():
        dump_path = None
        if opts.dump:
            dump_path = os.path.join(opts.log_dir, 'status-monitoring.json')
        session_id = common.submit(pg, host='127.0.0.1', port=port)
        while True:
            try:
                common.monitor_sessions(session_id, host='127.0.0.1', port=port,
                                        status_dump_path=dump_path)
                break
            except:
                logger.exception('Monitoring failed, restarting it')
    t = threading.Thread(target=_task)
    t.start()
    return t


def start_proxy(dlg_host, dlg_port, monitor_host, monitor_port):
    """
    Start the DALiuGE proxy server
    """
    proxy_id = socket.gethostname() + '%.3f' % time.time()
    server = dfms_proxy.ProxyServer(proxy_id, dlg_host, monitor_host, dlg_port, monitor_port)
    try:
        server.loop()
    except KeyboardInterrupt:
        logger.warning("Ctrl C - Stopping DALiuGE Proxy server")
        sys.exit(1)
    except Exception:
        logger.exception("DALiuGE proxy terminated unexpectedly")
        sys.exit(1)

def modify_pg(pgt, modifier):
    parts = modifier.split(',')
    func = utils.get_symbol(parts[0])
    args = list(filter(lambda x: '=' not in x, parts[1:]))
    kwargs = dict(map(lambda x: x.split('='), filter(lambda x: '=' in x, parts[1:])))
    return func(pgt, *args, **kwargs)

def get_pg(opts, nms, dims):
    """Gets the Physical Graph that is eventually submitted to the cluster, if any"""

    if not opts.logical_graph and not opts.physical_graph:
        return

    num_nms = len(nms)
    num_dims = len(dims)
    if opts.logical_graph:
        unrolled = pg_generator.unroll(opts.logical_graph, opts.ssid, opts.zerorun, apps[opts.app])
        algo_params = tool.parse_partition_algo_params(opts.algo_params)
        pgt = pg_generator.partition(unrolled, opts.part_algo,
                                     num_partitions=num_nms,
                                     num_islands=num_dims,
                                     **algo_params)
        del unrolled # quickly dispose of potentially big object
    else:
        with open(opts.physical_graph, 'rb') as f:
            pgt = json.load(f)

    # modify the PG as necessary
    for modifier in opts.pg_modifiers.split(':'):
        modify_pg(pgt, modifier)

    # Check that which NMs are up and use only those form now on
    nms = check_hosts(nms, NODE_DEFAULT_REST_PORT,
                      check_with_session=opts.check_with_session,
                      timeout=MM_WAIT_TIME, retry=3)
    pg = pg_generator.resource_map(pgt, dims + nms, num_islands=num_dims)
    with open(os.path.join(opts.log_dir, 'pg.json'), 'wt') as f:
        json.dump(pg, f)
    return pg

def get_ip(opts):
    find_ip = get_ip_via_ifconfig if opts.use_ifconfig else get_ip_via_netifaces
    return find_ip(opts.interface)

def get_remote(opts):
    my_ip = get_ip(opts)
    if opts.remote_mechanism == 'mpi':
        return remotes.MPIRemote(opts, my_ip)
    elif opts.remote_mechanism == 'dlg':
        return remotes.DALiuGERemote(opts, my_ip)
    elif opts.remote_mechanism == 'dlg-hybrid':
        return remotes.DALiuGEHybridRemote(opts, my_ip)
    else: # == 'slurm'
        return remotes.SlurmRemote(opts, my_ip)

def main():

    parser = optparse.OptionParser()
    parser.add_option("-l", "--log_dir", action="store", type="string",
                    dest="log_dir", help="Log directory (required)")
    # if this parameter is present, it means we want to get monitored
    parser.add_option("-m", "--monitor_host", action="store", type="string",
                    dest="monitor_host", help="Monitor host IP (optional)")
    parser.add_option("-o", "--monitor_port", action="store", type="int",
                    dest="monitor_port", help="Monitor port",
                    default=dfms_proxy.default_dlg_monitor_port)
    parser.add_option("-v", "--verbose-level", action="store", type="int",
                    dest="verbose_level", help="Verbosity level (1-3) of the DIM/NM logging",
                    default=1)
    parser.add_option("-z", "--zerorun", action="store_true",
                      dest="zerorun", help="Generate a physical graph that takes no time to run", default=False)
    parser.add_option("--app", action="store", type="int",
                      dest="app", help="The app to use in the PG. 1=SleepApp (default), 2=SleepAndCopy", default=0)

    parser.add_option("-t", "--max-threads", action="store", type="int",
                      dest="max_threads", help="Max thread pool size used for executing drops. 0 (default) means no pool.", default=0)

    parser.add_option("-L", "--logical-graph", action="store", type="string",
                      dest="logical_graph", help="The filename of the logical graph to deploy", default=None)
    parser.add_option("-P", "--physical-graph", action="store", type="string",
                      dest="physical_graph", help="The filename of the physical graph (template) to deploy", default=None)

    parser.add_option('-s', '--num_islands', action='store', type='int',
                    dest='num_islands', default=1, help='The number of Data Islands')

    parser.add_option('-d', '--dump', action='store_true',
                    dest='dump', help = 'dump file base name?', default=False)

    parser.add_option("-i", "--interface", type="int",
                      help="Index of network interface to use as the external interface/address for each host", default=0)

    parser.add_option('--part-algo', type="string", dest='part_algo', help='Partition algorithms',
                      default='metis')
    parser.add_option("-A", "--algo-param", action="append", dest="algo_params",
                      help="Extra name=value parameters used by the algorithms (algorithm-specific)")

    parser.add_option('--ssid', type="string", dest='ssid', help='session id',
                      default='1')

    parser.add_option("-u", "--all_nics", action="store_true",
                      dest="all_nics", help="Listen on all NICs for a node manager", default=False)

    parser.add_option('--check-interfaces', action='store_true',
                      dest='check_interfaces', help = 'Run a small network interfaces test and exit', default=False)
    parser.add_option('--collect-interfaces', action='store_true',
                      dest='collect_interfaces', help = 'Collect all interfaces and exit', default=False)
    parser.add_option('--use-ifconfig', action='store_true',
                      dest='use_ifconfig', help='Use ifconfig to find a suitable external interface/address for each host', default=False)
    parser.add_option("-S", "--check_with_session", action="store_true",
                      dest="check_with_session", help="Check for node managers' availability by creating/destroy a session", default=False)

    parser.add_option("--event-listeners", action="store", type="string",
                      dest="event_listeners", help="A colon-separated list of event listener classes to be used", default='')

    parser.add_option("--sleep-after-execution", action="store", type="int",
                      dest="sleep_after_execution", help="Sleep time interval after graph execution finished", default=0)

    parser.add_option('--pg-modifiers',
                      help=('A colon-separated list of python functions that modify a PG before submission. '
                            'Each specification is in the form of <funcname>[,[arg1=]val1][,[arg2=]val2]...'),
                      default='')

    parser.add_option('-r', '--remote-mechanism', help='The mechanism used by this script to coordinate remote processes',
                      choices=['mpi', 'slurm', 'dlg', 'dlg-hybrid'], default='mpi')

    (options, _) = parser.parse_args()

    if options.check_interfaces:
        try:
            print("From netifaces: %s" % get_ip_via_netifaces(options.interface))
        except:
            logger.exception("Failed to get information via netifaces")
        try:
            print("From ifconfig: %s" % get_ip_via_ifconfig(options.interface))
        except:
            logger.exception('Failed to get information via ifconfig')
        sys.exit(0)
    elif options.collect_interfaces:
        from mpi4py import MPI
        comm = MPI.COMM_WORLD  # @UndefinedVariable
        ips = comm.allgather(get_ip(options))
        if comm.Get_rank() == 0:
            print(' '.join(ips))
        sys.exit(0)


    if bool(options.logical_graph) == bool(options.physical_graph):
        parser.error("Either a logical graph or physical graph filename must be specified")
    for p in (options.logical_graph, options.physical_graph):
        if p and not os.path.exists(p):
            parser.error("Cannot locate graph file at '{0}'".format(p))

    if (options.monitor_host is not None and options.num_islands > 1):
        parser.error("We do not support proxy monitor multiple islands yet")

    remote = get_remote(options)

    log_dir = "{0}/{1}".format(options.log_dir, remote.rank)
    os.makedirs(log_dir)
    logfile = log_dir + "/start_dlg_cluster.log"
    FORMAT = "%(asctime)-15s [%(levelname)5.5s] [%(threadName)15.15s] %(name)s#%(funcName)s:%(lineno)s %(message)s"
    logging.basicConfig(filename=logfile, level=logging.DEBUG, format=FORMAT)

    logger.info('Starting DALiuGE cluster with %d nodes', remote.size)
    logger.debug('Cluster nodes: %r', remote.sorted_peers)
    logger.debug("Using %s as the local IP where required", remote.my_ip)

    envfile = os.path.join(log_dir, 'env.txt')
    logger.debug("Dumping process' environment to %s", envfile)
    with open(envfile, 'wt') as f:
        for name, value in sorted(os.environ.items()):
            f.write('%s=%s\n' % (name, value))

    logv = max(min(3, options.verbose_level), 1)

    if remote.is_highest_level_manager:
        nodesfile = os.path.join(log_dir, 'nodes.txt')
        logger.debug("Dumping list of nodes to %s", nodesfile)
        with open(nodesfile, 'wt') as f:
            f.write('\n'.join(remote.sorted_peers))

    if remote.is_nm:
        start_node_mgr(log_dir, remote.my_ip, logv=logv,
            max_threads=options.max_threads,
            host=None if options.all_nics else remote.my_ip,
            event_listeners=options.event_listeners)

    elif options.num_islands == 1:
        if remote.is_proxy:
            # Wait until the Island Manager is open
            nm_proc = None
            if utils.portIsOpen(remote.hl_mgr_ip, ISLAND_DEFAULT_REST_PORT, 100):
                start_proxy(remote.hl_mgr_ip, ISLAND_DEFAULT_REST_PORT, options.monitor_host, options.monitor_port)
            else:
                logger.warning("Couldn't connect to the main drop manager, proxy not started")
        else:
            pg = get_pg(options, remote.nm_ips, remote.dim_ips)
            monitoring_thread = submit_and_monitor(pg, options, ISLAND_DEFAULT_REST_PORT)
            nm_proc = start_dim(remote.nm_ips, log_dir, remote.my_ip, logv=logv)
            monitoring_thread.join()
            stop_dims(['127.0.0.1'])
            stop_nms(remote.nm_ips)
        if nm_proc is not None:
            # Stop DALiuGE.
            logger.info("Stopping DALiuGE application on rank %d", remote.rank)
            utils.terminate_or_kill(nm_proc, 5)

    else:

        if remote.is_highest_level_manager:

            pg = get_pg(options, remote.nm_ips, remote.dim_ips)
            remote.send_dim_nodes(pg)

            # 7. make sure all DIMs are up running
            dim_ips_up = check_hosts(remote.dim_ips, ISLAND_DEFAULT_REST_PORT, timeout=MM_WAIT_TIME, retry=10)
            if len(dim_ips_up) < len(remote.dim_ips):
                logger.warning("Not all DIMs were up and running: %d/%d", len(dim_ips_up), len(remote.dim_ips))

            monitoring_thread = submit_and_monitor(pg, options, MASTER_DEFAULT_REST_PORT)
            start_mm(remote.dim_ips, log_dir, logv=logv)
            monitoring_thread.join()
            stop_mm('127.0.0.1')
            stop_dims(remote.dim_ips)
        else:
            nm_ips = remote.recv_dim_nodes()
            proc = start_dim(nm_ips, log_dir, remote.my_ip, logv=logv)
            utils.wait_or_kill(proc, 1e8, period=5)
            stop_nms(remote.nm_ips)

if __name__ == '__main__':
    main()
