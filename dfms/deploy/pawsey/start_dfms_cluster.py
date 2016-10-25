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
Start the DFMS cluster on Magnus / Galaxy at Pawsey

Current plan (as of 12-April-2016):
    1. Launch a number of Node Managers (NM) using MPI processes
    2. Having the NM MPI processes to send their IP addresses to the Rank 0
       MPI process
    3. Launch the Island Manager (IM) on the Rank 0 MPI process using those IP
       addresses
"""

import collections
import json
import logging
import multiprocessing
import optparse
import os
import subprocess
import sys
import threading
import time
import uuid

from dfms import utils
from dfms.deploy.pawsey import dfms_proxy
from dfms.deploy.pawsey.example_client import MonitorClient
from dfms.manager import cmdline as dfms_start
from dfms.manager.client import NodeManagerClient
from dfms.manager.constants import NODE_DEFAULT_REST_PORT, \
ISLAND_DEFAULT_REST_PORT, MASTER_DEFAULT_REST_PORT


DIM_WAIT_TIME = 60
MM_WAIT_TIME = DIM_WAIT_TIME
GRAPH_SUBMIT_WAIT_TIME = 10
GRAPH_MONITOR_INTERVAL = 5
VERBOSITY = '5'
logger = logging.getLogger('deploy.pawsey.cluster')

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
            ntries -= 0
        return None

    # Don't return None values
    tp = multiprocessing.pool.ThreadPool(min(50, len(ips)))
    up = tp.map(check_and_add, ips)
    tp.close()
    tp.join()

    return [ip for ip in up if ip]

def get_ip(loc='Pawsey'):
    """
    This is brittle, but works on Magnus/Galaxy for now
    """
    out = subprocess.check_output('ifconfig')
    if (loc == 'Pawsey'):
        ln = 1 # e.g. 10.128.0.98
    elif (loc == 'Tianhe2'):
        ln = 18 # e.g. 12.6.2.134
    else:
        raise Exception("Unknown deploy location: {0}".format(loc))
    try:
        line = out.split('\n')[ln]
        return line.split()[1].split(':')[1]
    except:
        logger.warning("Fail to obtain IP address from {0}".format(out))
        return 'None'

def start_node_mgr(log_dir, logv=1, max_threads=0, host=None):
    """
    Start node manager
    """
    host = host or '0.0.0.0'
    lv = 'v' * logv
    dfms_start.dfmsNM(args=['cmdline.py', '-l', log_dir,
    '-%s' % lv, '-H', host, '-m', '1024', '-t', str(max_threads), '--no-dlm'])

def start_dim(node_list, log_dir, logv=1):
    """
    Start data island manager
    """
    lv = 'v' * logv
    dfms_start.dfmsDIM(args=['cmdline.py', '-l', log_dir, '-%s' % lv,
    '-N', ','.join(node_list), '-H', '0.0.0.0', '-m', '2048'])

def start_mm(node_list, log_dir, logv=1):
    """
    Start master manager

    node_list:  a list of node address that host DIMs
    """
    lv = 'v' * logv
    dfms_start.dfmsMM(args=['cmdline.py', '-l', log_dir,
    '-N', ','.join(node_list), '-%s' % lv, '-H', '0.0.0.0', '-m', '2048'])

def submit_monitor_graph(dim_ip, lg_path, pg_path, dump_status, zerorun, app, mc, unrolled):
    """
    Submits a graph and then monitors the island manager
    """
    logger.debug("dump_status = {0}".format(dump_status))
    logger.info("Wait for {0} seconds before submitting graph to DIM".format(GRAPH_SUBMIT_WAIT_TIME))
    time.sleep(GRAPH_SUBMIT_WAIT_TIME)

    # use monitorclient to interact with island manager
    dc = mc._dc

    # We are submitting a graph
    if lg_path or pg_path:
        mc._nodes = [dim_ip] + dc.nodes()
        lgn, lg, pg_spec = mc.get_physical_graph(lg_path, pg_path, unrolled=unrolled)
        logger.info("Submitting graph {0}".format(lgn))
        del unrolled

        mc.submit_single_graph(lg_path, pg_path, deploy=True, pg=(lgn, lg, pg_spec))
        logger.info("graph {0} is successfully submitted".format(lgn))

    # We want to monitor the status of the execution
    if (dump_status is not None):
        fp = os.path.dirname(dump_status)
        if (not os.path.exists(fp)):
            return
        gfile = "{0}_g.log".format(dump_status)
        sfile = "{0}_s.log".format(dump_status)
        graph_dict = dict() # k - ssid, v - graph spec json obj
        logger.debug("Ready to check sessions")
        while (True):
            #logger.debug("checking sessions")
            sessions = dc.sessions()
            #logger.debug("len(sessions) = {0}".format(len(sessions)))
            #logger.debug("session0 = {0}".format(sessions[0]))
            for session in sessions: #TODO the interval won't work for multiple sessions
                stt = time.time()
                ssid = session['sessionId']
                #logger.debug("session id = {0}".format(ssid))
                wgs = {}
                wgs['ssid'] = ssid
                wgs['gs'] = dc.graph_status(ssid) #TODO check error
                time_str = '%.3f' % time.time()
                wgs['ts'] = time_str
                if (not graph_dict.has_key(ssid)):
                    graph = dc.graph(ssid)
                    graph_dict[ssid] = graph
                    wg = {}
                    wg['ssid'] = ssid
                    wg['g'] = graph
                    # append to file as a line
                    with open(gfile, 'a') as fg:
                        json.dump(wg, fg)
                        fg.write(os.linesep)
                # append to file as a line
                with open(sfile, 'a') as fs:
                    json.dump(wgs, fs)
                    fs.write(os.linesep)
                dt = time.time() - stt
                if (dt < GRAPH_MONITOR_INTERVAL):
                    try:
                        time.sleep(GRAPH_MONITOR_INTERVAL - dt)
                    except:
                        pass


def start_dfms_proxy(loc, dfms_host, dfms_port, monitor_host, monitor_port):
    """
    Start the DFMS proxy server
    """
    proxy_id = loc + '%.3f' % time.time()
    server = dfms_proxy.DFMSProxy(proxy_id, dfms_host, monitor_host, dfms_port, monitor_port)
    try:
        server.loop()
    except KeyboardInterrupt:
        logger.warning("Ctrl C - Stopping DFMS Proxy server")
        sys.exit(1)
    except Exception as ex:
        logger.error("DFMS proxy terminated unexpectedly: {0}".format(ex))
        sys.exit(1)

def submit_pg(monitor_client, physical_graph):
    logger.info("Wait for {0} seconds before submitting graph to the Master Manager".format(GRAPH_SUBMIT_WAIT_TIME))
    time.sleep(GRAPH_SUBMIT_WAIT_TIME)
    monitor_client.submit_single_graph(None, deploy=True, pg=physical_graph)

def set_env(rank):
    os.environ['PYRO_MAX_RETRIES'] = '10'

def main():

    parser = optparse.OptionParser()
    parser.add_option("-l", "--log_dir", action="store", type="string",
                    dest="log_dir", help="Log directory (required)")
    # if this parameter is present, it means we want to get monitored
    parser.add_option("-m", "--monitor_host", action="store", type="string",
                    dest="monitor_host", help="Monitor host IP (optional)")
    parser.add_option("-o", "--monitor_port", action="store", type="int",
                    dest="monitor_port", help="The port to bind dfms monitor",
                    default=dfms_proxy.default_dfms_monitor_port)
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

    parser.add_option("-c", "--loc", action="store", type="string",
                    dest="loc", help="deployment location (e.g. 'Pawsey' or 'Tianhe2')",
                    default="Pawsey")

    parser.add_option("-u", "--all_nics", action="store_true",
                      dest="all_nics", help="Listen on all NICs for a node manager", default=False)

    parser.add_option('--check-interfaces', action='store_true',
                      dest='check_interfaces', help = 'Run a small network interfaces test and exit', default=False)
    parser.add_option("-S", "--check_with_session", action="store_true",
                      dest="check_with_session", help="Check for node managers' availability by creating/destroy a session", default=False)

    (options, _) = parser.parse_args()

    if options.check_interfaces:
        print("From netifaces: %s" % utils.get_local_ip_addr())
        print("From ifconfig: %s" % get_ip())
        sys.exit(0)

    if options.logical_graph and options.physical_graph:
        parser.error("Either a logical graph or physical graph filename must be specified")
    for p in (options.logical_graph, options.physical_graph):
        if p and not os.path.exists(p):
            parser.error("Cannot locate graph file at '{0}'".format(p))

    if (options.monitor_host is not None and options.num_islands > 1):
        parser.error("We do not support proxy monitor multiple islands yet")

    logv = max(min(3, options.verbose_level), 1)

    from mpi4py import MPI  # @UnresolvedImport
    comm = MPI.COMM_WORLD  # @UndefinedVariable
    num_procs = comm.Get_size()
    rank = comm.Get_rank()

    log_dir = "{0}/{1}".format(options.log_dir, rank)
    os.makedirs(log_dir)
    logfile = log_dir + "/start_dfms_cluster.log"
    FORMAT = "%(asctime)-15s [%(levelname)5.5s] [%(threadName)15.15s] %(name)s#%(funcName)s:%(lineno)s %(message)s"
    logging.basicConfig(filename=logfile, level=logging.DEBUG, format=FORMAT)

    if (num_procs > 1 and options.monitor_host is not None):
        logger.info("Trying to start dfms_cluster with proxy")
        run_proxy = True
        threshold = 2
    else:
        logger.info("Trying to start dfms_cluster without proxy")
        run_proxy = False
        threshold = 1

    if (num_procs == threshold):
        logger.warning("No MPI processes left for running Drop Managers")
        run_node_mgr = False
    else:
        run_node_mgr = True

    # attach rank information at the end of IP address for multi-islands
    rank_str = '' if options.num_islands == 1 else ',%s' % rank
    public_ip = get_ip(options.loc)
    ip_adds = '{0}{1}'.format(public_ip, rank_str)
    origin_ip = ip_adds.split(',')[0]
    ip_adds = comm.gather(ip_adds, root=0)

    proxy_ip = None
    if run_proxy:
        # send island/master manager's IP address to the dfms proxy
        # also let island manager know dfms proxy's IP
        if rank == 0:
            mgr_ip = origin_ip
            comm.send(mgr_ip, dest=1)
            proxy_ip = comm.recv(source=1)
        elif rank == 1:
            mgr_ip = comm.recv(source=0)
            proxy_ip = origin_ip
            comm.send(proxy_ip, dest=0)

    set_env(rank)
    if (options.num_islands == 1):
        if (rank != 0):
            if (run_proxy and rank == 1):
                # Wait until the Island Manager is open
                if utils.portIsOpen(mgr_ip, ISLAND_DEFAULT_REST_PORT, 100):
                    start_dfms_proxy(options.loc, mgr_ip, ISLAND_DEFAULT_REST_PORT, options.monitor_host, options.monitor_port)
                else:
                    logger.warning("Couldn't connect to the main drop manager, proxy not started")
            elif (run_node_mgr):
                logger.info("Starting node manager on host {0}".format(origin_ip))
                start_node_mgr(log_dir, logv=logv,
                max_threads=options.max_threads,
                host=None if options.all_nics else origin_ip)
        else:
            # unroll the graph first while starting node managers on other nodes
            mc = MonitorClient('localhost', ISLAND_DEFAULT_REST_PORT, algo='metis',
                                zerorun=options.zerorun, app=options.app)
            unrolled = None
            if options.logical_graph or options.physical_graph:
                unrolled = mc.unroll_physical_graph(options.logical_graph, options.physical_graph)

            # These are not NMs
            no_nms = [origin_ip, 'None']
            if proxy_ip:
                no_nms += [proxy_ip]

            logger.info("A list of NM IPs: {0}".format(ip_adds))
            logger.info("Starting island manager on host {0} in {1} seconds".format(origin_ip, DIM_WAIT_TIME))
            nm_ips = [ip for ip in ip_adds if ip not in no_nms]
            node_mgrs = check_hosts(nm_ips, NODE_DEFAULT_REST_PORT,
                                    check_with_session=options.check_with_session,
                                    timeout=MM_WAIT_TIME)

            if options.dump:
                arg02 = '{0}/monitor'.format(log_dir)
                logger.info("Local monitor path: {0}".format(arg02))
            else:
                arg02 = None
            threading.Thread(target=submit_monitor_graph,
                            args=(origin_ip, options.logical_graph, options.physical_graph, arg02, options.zerorun, options.app, mc, unrolled)).start()
            start_dim(node_mgrs, log_dir, logv=logv)
    elif (options.num_islands > 1):
        if (rank == 0):
            # master manager
            # 1. use ip_adds to produce the physical graph
            ip_list = []
            ip_rank_dict = dict() # k - ip, v - MPI rank
            for ipr in ip_adds:
                iprs = ipr.split(',')
                ip = iprs[0]
                r = iprs[1]
                if (ip == origin_ip or 'None' == ip):
                    continue
                ip_list.append(ip)
                ip_rank_dict[ip] = int(r)

            if (len(ip_list) <= options.num_islands):
                raise Exception("Insufficient nodes available for node managers")

            # 2 broadcast dim ranks to all nodes to let them know who is the DIM
            dim_ranks = []
            dim_ip_list = ip_list[0:options.num_islands]
            logger.info("A list of DIM IPs: {0}".format(dim_ip_list))
            for dim_ip in dim_ip_list:
                dim_ranks.append(ip_rank_dict[dim_ip])
            dim_ranks = comm.bcast(dim_ranks, root=0)

            # 3 unroll the graph while waiting for node managers to start
            mc_unroll = MonitorClient('localhost', MASTER_DEFAULT_REST_PORT,
                algo='metis', zerorun=options.zerorun, app=options.app)
            unrolled = mc_unroll.unroll_physical_graph(options.logical_graph, options.physical_graph)
            #logger.info("Waiting all node managers to start in %f seconds", MM_WAIT_TIME)
            node_mgrs = check_hosts(ip_list[options.num_islands:], NODE_DEFAULT_REST_PORT,
                                    check_with_session=options.check_with_session,
                                    timeout=MM_WAIT_TIME)

            # 4.  produce the physical graph based on the available node managers
            # that have alraedy been running (we have to assume island manager
            # will run smoothly in the future)
            logger.info("Master Manager producing the physical graph")
            mc = MonitorClient('localhost', MASTER_DEFAULT_REST_PORT,
                algo='metis', zerorun=options.zerorun, app=options.app,
                nodes=(dim_ip_list + node_mgrs), num_islands=options.num_islands)
            lgn, lg, pg_spec = mc.get_physical_graph(options.logical_graph, options.physical_graph, unrolled=unrolled)
            del unrolled

            # 5. parse the pg_spec to get the mapping from islands to node list
            dim_rank_nodes_dict = collections.defaultdict(set)
            for drop in pg_spec:
                dim_ip = drop['island']
                # if (not dim_ip in dim_ip_list):
                #     raise Exception("'{0}' node is not in island list {1}".format(dim_ip, dim_ip_list))
                r = ip_rank_dict[dim_ip]
                n = drop['node']
                dim_rank_nodes_dict[r].add(n)

            # 6 send a node list to each DIM so that it can start
            for dim_ip in dim_ip_list:
                r = ip_rank_dict[dim_ip]
                logger.debug("Sending node list to rank {0}".format(r))
                #TODO this should be in a thread since it is blocking!
                comm.send(list(dim_rank_nodes_dict[r]), dest=r)

            # 7. make sure all DIMs are up running
            dim_ips_up = check_hosts(dim_ip_list, ISLAND_DEFAULT_REST_PORT, timeout=MM_WAIT_TIME, retry=10)
            if len(dim_ips_up) < len(dim_ip_list):
                logger.warning("Not all DIMs were up and running: %d/%d", len(dim_ips_up), len(dim_ip_list))

            # 8. submit the graph in a thread (wait for mm to start)
            pg = (lgn, lg, pg_spec)
            threading.Thread(target=submit_pg, args=(mc, pg)).start()

            # 9. start dfmsMM using islands IP addresses (this will block)
            start_mm(dim_ip_list, log_dir, logv=logv)
        else:
            dim_ranks = None
            dim_ranks = comm.bcast(dim_ranks, root=0)
            logger.debug("Receiving dim_ranks = {0}, my rank is {1}".format(dim_ranks, rank))
            if (rank in dim_ranks):
                logger.debug("Rank {0} is a DIM preparing for receiving".format(rank))
                # island manager
                # get a list of nodes that are its children from rank 0 (dfmsMM)
                nm_list = comm.recv(source=0)
                # no need to wait for node managers since the master manager
                # has already made sure they are up running
                logger.debug("nm_list for DIM {0} is {1}".format(rank, nm_list))
                start_dim(nm_list, log_dir, logv=logv)
            else:
                # node manager
                logger.info("Starting node manager on host {0}".format(origin_ip))
                start_node_mgr(log_dir, logv=logv,
                max_threads=options.max_threads,
                host=None if options.all_nics else origin_ip)

if __name__ == '__main__':
    main()
