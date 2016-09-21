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

import commands, time, sys, os, logging
import json
from optparse import OptionParser
import threading
from collections import defaultdict

import dfms.deploy.pawsey.dfms_proxy as dfms_proxy
from dfms.deploy.pawsey.example_client import MonitorClient
import dfms.deploy.pawsey.example_client as exclient
from dfms.manager.client import DataIslandManagerClient
import dfms.manager.cmdline as dfms_start
from dfms.manager.constants import NODE_DEFAULT_REST_PORT, \
ISLAND_DEFAULT_REST_PORT, MASTER_DEFAULT_REST_PORT

from mpi4py import MPI


DIM_WAIT_TIME = 5
MM_WAIT_TIME = DIM_WAIT_TIME
GRAPH_SUBMIT_WAIT_TIME = 10
GRAPH_MONITOR_INTERVAL = 5
VERBOSITY = '5'
logger = logging.getLogger('deploy.pawsey.cluster')

def ping_host(url, timeout=5, loc='Pawsey'):
    """
    To check if a host is running
    Returns:
        0                Success
        Otherwise        Failure
    """
    if (loc == 'Tianhe2'):
        return 0 # Tianhe2 always return 0 (success)
    cmd = 'curl --connect-timeout %d %s' % (timeout, url)
    try:
        return commands.getstatusoutput(cmd)[0]
    except Exception, err:
        logger.warning("Fail to ping host {0}: {1}".format(url, str(err)))
        return 1

def get_ip(loc='Pawsey'):
    """
    This is brittle, but works on Magnus/Galaxy for now
    """
    re = commands.getstatusoutput('ifconfig')
    if (loc == 'Pawsey'):
        ln = 1 # e.g. 10.128.0.98
    elif (loc == 'Tianhe2'):
        ln = 18 # e.g. 12.6.2.134
    else:
        raise Exception("Unknown deploy location: {0}".format(loc))
    try:
        msg = re[1]
        line = msg.split('\n')[ln]
        return line.split()[1].split(':')[1]
    except:
        logger.warning("Fail to obtain IP address from {0}".format(msg))
        return 'None'

def start_node_mgr(log_dir, logv=1, max_threads=0):
    """
    Start node manager
    """
    lv = 'v' * logv
    dfms_start.dfmsNM(args=['cmdline.py', '-l', log_dir,
    '-%s' % lv, '-H', '0.0.0.0', '-m', '100', '-t', str(max_threads), '--no-dlm'])

def start_dim(node_list, log_dir, logv=1):
    """
    Start data island manager
    """
    lv = 'v' * logv
    dfms_start.dfmsDIM(args=['cmdline.py', '-l', log_dir, '-%s' % lv,
    '-N', ','.join(node_list), '-H', '0.0.0.0', '-m', '1024'])

def start_mm(node_list, log_dir, logv=1):
    """
    Start master manager

    node_list:  a list of node address that host DIMs
    """
    lv = 'v' * logv
    dfms_start.dfmsMM(args=['cmdline.py', '-l', log_dir,
    '-N', ','.join(node_list), '-%s' % lv, '-H', '0.0.0.0', '-m', '1024'])

def submit_monitor_graph(dim_ip, graph_id, dump_status, zerorun, app):
    """
    Submits a graph and then monitors the island manager
    """
    logger.debug("dump_status = {0}".format(dump_status))
    logger.info("Wait for {0} seconds before submitting graph to DIM".format(GRAPH_SUBMIT_WAIT_TIME))
    time.sleep(GRAPH_SUBMIT_WAIT_TIME)
    # use monitorclient to interact with island manager
    if (graph_id is not None):
        dc = mc._dc
        nodes = [dim_ip] + dc.nodes()
        mc = MonitorClient('localhost', ISLAND_DEFAULT_REST_PORT, algo='metis', zerorun=zerorun, app=app, nodes=nodes)
        logger.info("Submitting graph {0}".format(graph_id))
        lgn, lg, pg_spec = mc.get_physical_graph(graph_id)
        mc.submit_single_graph(graph_id, deploy=True, pg=(lgn, lg, pg_spec))
        logger.info("graph {0} is successfully submitted".format(graph_id))
    else:
        dc = DataIslandManagerClient('localhost')
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
    except Exception, ex:
        logger.error("DFMS proxy terminated unexpectedly: {0}".format(ex))
        sys.exit(1)

def submit_pg(monitor_client, physical_graph):
    logger.info("Wait for {0} seconds before submitting graph to the Master Manager".format(GRAPH_SUBMIT_WAIT_TIME))
    time.sleep(GRAPH_SUBMIT_WAIT_TIME)
    monitor_client.submit_single_graph(None, deploy=True, pg=physical_graph)

def set_env(rank):
    os.environ['PYRO_MAX_RETRIES'] = '10'

if __name__ == '__main__':
    """
    """
    parser = OptionParser()
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

    # we add command-line parameter to allow automatic graph submission from file
    parser.add_option('-g', '--gid', action='store', type='int',
                    dest='gid', help = 'Physical graph id')

    parser.add_option('-s', '--num_islands', action='store', type='int',
                    dest='num_islands', default=1, help='The number of Data Islands')

    parser.add_option('-d', '--dump', action='store_true',
                    dest='dump', help = 'dump file base name?', default=False)

    parser.add_option("-c", "--loc", action="store", type="string",
                    dest="loc", help="deployment location (e.g. 'Pawsey' or 'Tianhe2')",
                    default="Pawsey")


    (options, args) = parser.parse_args()

    if (None == options.log_dir):
        parser.print_help()
        sys.exit(1)

    if (options.gid is not None and options.gid >= len(exclient.lgnames)):
        options.gid = 0

    if (options.monitor_host is not None and options.num_islands > 1):
        parser.error("We do not support proxy monitor multiple islands yet")

    logv = max(min(3, options.verbose_level), 1)

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
    ip_adds = '{0}{1}'.format(get_ip(options.loc), rank_str)
    origin_ip = ip_adds.split(',')[0]
    ip_adds = comm.gather(ip_adds, root=0)
    if (run_proxy):
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
                sltime = DIM_WAIT_TIME + 2
                logger.info("Starting dfms proxy on host {0} in {1} seconds".format(origin_ip, sltime))
                time.sleep(sltime)
                start_dfms_proxy(options.loc, mgr_ip, ISLAND_DEFAULT_REST_PORT, options.monitor_host, options.monitor_port)
            elif (run_node_mgr):
                logger.info("Starting node manager on host {0}".format(origin_ip))
                start_node_mgr(log_dir, logv=logv, max_threads=options.max_threads)
        else:
            logger.info("A list of NM IPs: {0}".format(ip_adds))
            logger.info("Starting island manager on host {0} in {1} seconds".format(origin_ip, DIM_WAIT_TIME))
            time.sleep(DIM_WAIT_TIME)
            node_mgrs = []
            for ip in ip_adds:
                if (ip == origin_ip or (run_proxy and ip == proxy_ip) or ('None' == ip)):
                    continue
                url = "http://{0}:{1}".format(ip, NODE_DEFAULT_REST_PORT)
                if (ping_host(url, loc=options.loc) != 0):
                    logger.warning("Fail to ping host {0}".format(url))
                else:
                    logger.info("Host {0} is running".format(url))
                    node_mgrs.append(ip)
            if ((options.gid is not None) or options.dump):
                logger.info("Preparing submitting graph (and monitor it)")
                if (options.dump):
                    arg02 = '{0}/monitor'.format(log_dir)
                    logger.info("Local monitor path: {0}".format(arg02))
                else:
                    arg02 = None
                    logger.info("Local monitor path is not set")
                threading.Thread(target=submit_monitor_graph, args=(origin_ip, options.gid, arg02, options.zerorun, options.app)).start()
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

            # 2 broadcast dim ranks to all nodes to let them know who is the DIM
            dim_ranks = []
            dim_ip_list = ip_list[0:options.num_islands]
            logger.info("A list of DIM IPs: {0}".format(dim_ip_list))
            for dim_ip in dim_ip_list:
                dim_ranks.append(ip_rank_dict[dim_ip])
            dim_ranks = comm.bcast(dim_ranks, root=0)

            # 3 wait for node managers to start
            logger.info("Waiting all node managers to start in {0} seconds".format(MM_WAIT_TIME))
            time.sleep(MM_WAIT_TIME)
            node_mgrs = []
            for ip in ip_list[options.num_islands:]:
                url = "http://{0}:{1}".format(ip, NODE_DEFAULT_REST_PORT)
                if (ping_host(url, loc=options.loc) != 0):
                    logger.warning("Fail to ping node manager {0}".format(url))
                else:
                    logger.info("Node manager {0} is running".format(url))
                    node_mgrs.append(ip)

            # 4.  produce the physical graph based on the available node managers
            # that have alraedy been running (we have to assume island manager
            # will run smoothly in the future)
            logger.info("Master Manager producing the physical graph")
            mc = MonitorClient('localhost', MASTER_DEFAULT_REST_PORT,
                algo='metis', zerorun=options.zerorun, app=options.app,
                nodes=(dim_ip_list + node_mgrs), num_islands=options.num_islands)
            lgn, lg, pg_spec = mc.get_physical_graph(options.gid)

            # 5. parse the pg_spec to get the mapping from islands to node list
            dim_rank_nodes_dict = defaultdict(set)
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
            retry = 10
            for dim_ip in dim_ip_list:
                url = "http://{0}:{1}".format(dim_ip, ISLAND_DEFAULT_REST_PORT)
                for j in range(retry):
                    pr = ping_host(url, loc=options.loc)
                    if (pr == 0):
                        logger.info("Island {0} is running".format(url))
                        break
                    else:
                        logger.warning("Fail to ping island {0}".format(url))
                        if (j < retry - 1):
                            logger.info("Sleep {0} seconds before retry".format(MM_WAIT_TIME))
                            time.sleep(MM_WAIT_TIME)
                        else:
                            logger.info("retry exausted. Give up island {0} for now".format(dim_ip))

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
                start_node_mgr(log_dir, logv=logv, max_threads=options.max_threads)
