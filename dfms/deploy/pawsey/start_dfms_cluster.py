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

from dfms import droputils
import dfms.deploy.pawsey.dfms_proxy as dfms_proxy
from dfms.manager.client import DataIslandManagerClient
import dfms.manager.cmdline as dfms_start
from dfms.manager.constants import NODE_DEFAULT_REST_PORT, \

from mpi4py import MPI


DIM_WAIT_TIME = 5
VERBOSITY = '5'
logger = logging.getLogger('deploy.pawsey.cluster')
DIM_PORT = 8001

def ping_host(url, timeout=5):
    """
    To check if a host is running
    Returns:
        0                Success
        Otherwise        Failure
    """
    cmd = 'curl --connect-timeout %d %s' % (timeout, url)
    try:
        return commands.getstatusoutput(cmd)[0]
    except Exception, err:
        logger.warning("Fail to ping host {0}: {1}".format(url, str(err)))
        return 1

def get_ip():
    """
    This is brittle, but works on Magnus/Galaxy for now
    """
    re = commands.getstatusoutput('ifconfig')
    line = re[1].split('\n')[1]
    return line.split()[1].split(':')[1]

def start_node_mgr(log_dir):
    """
    Start node manager
    """
    dfms_start.dfmsNM(args=['cmdline.py', '-l', log_dir,
    '-vvv', '-H', '0.0.0.0'])

def start_dim(node_list, log_dir):
    """
    Start data island manager
    """
    dfms_start.dfmsDIM(args=['cmdline.py', '-l', log_dir,
    '-N', ','.join(node_list), '-vvv', '-H', '0.0.0.0'])

def submit_graph(filename):
    """
    Submit the graph
    """
    time.sleep(5)
    dc = DataIslandManagerClient('localhost')
    ssid = "{0}-{1}".format(os.path.basename(filename), time.time())

    dc.create_session(ssid)
    with open(filename) as f:
        pg = json.loads(f.read())
        completed_uids = [x['oid'] for x in droputils.get_roots(pg)]
        dc.append_graph(ssid, pg)

    ret = dc.deploy_session(ssid, completed_uids=completed_uids)
    logger.info("session deployed")
    return ret

def start_dfms_proxy(dfms_host, dfms_port, monitor_host, monitor_port):
    """
    Start the DFMS proxy server
    """
    server = dfms_proxy.DFMSProxy(dfms_host, monitor_host, dfms_port, monitor_port)
    try:
        server.loop()
    except KeyboardInterrupt:
        logger.warning("Ctrl C - Stopping DFMS Proxy server")
        sys.exit(1)
    except Exception, ex:
        logger.error("DFMS proxy terminated unexpectedly: {0}".format(ex))
        sys.exit(1)

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
                    dest="monitor_port", help = "The port to bind dfms monitor",
                    default=dfms_proxy.default_dfms_monitor_port)
    parser.add_option('-f', '--filename', action='store', type='string',
                    dest='filename', help = 'Physical graph filename', default=None)
    # we add command-line parameter to allow automatic graph submission from file

    (options, args) = parser.parse_args()

    if (None == options.log_dir):
        parser.print_help()
        sys.exit(1)

    comm = MPI.COMM_WORLD
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

    ip_adds = get_ip()
    origin_ip = ip_adds
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

    if (rank != 0):
        if (run_proxy and rank == 1):
            sltime = DIM_WAIT_TIME + 2
            logger.info("Starting dfms proxy on host {0} in {1} seconds".format(origin_ip, sltime))
            time.sleep(sltime)
            start_dfms_proxy(mgr_ip, DIM_PORT, options.monitor_host, options.monitor_port)
        elif (run_node_mgr):
            logger.info("Starting node manager on host {0}".format(origin_ip))
            start_node_mgr(log_dir)
    else:
        logger.info("A list of NM IPs: {0}".format(ip_adds))
        logger.info("Starting island manager on host {0} in {1} seconds".format(origin_ip, DIM_WAIT_TIME))
        time.sleep(DIM_WAIT_TIME)
        node_mgrs = []
        for ip in ip_adds:
            if (ip == origin_ip or (run_proxy and ip == proxy_ip)):
                continue
            url = "http://{0}:{1}".format(ip, NODE_DEFAULT_REST_PORT)
            if (ping_host(url) != 0):
                logger.warning("Fail to ping host {0}".format(url))
            else:
                logger.info("Host {0} is running".format(url))
                node_mgrs.append(ip)
        if options.filename is not None:
            threading.Thread(target=submit_graph, args=(options.filename,)).start()
        start_dim(node_mgrs, log_dir)
