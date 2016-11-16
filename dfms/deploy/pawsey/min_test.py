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
Pre-test the platform compatibility for deploying the DALiuGE cluster
"""

import logging
import optparse
import subprocess
import sys


logger = logging.getLogger(__name__)

def import_node_mgr():
    from dfms import utils
    from dfms.drop import AppDROP
    from dfms.exceptions import NoSessionException, SessionAlreadyExistsException,\
        DaliugeException
    from dfms.lifecycle.dlm import DataLifecycleManager
    from dfms.manager import constants
    from dfms.manager.drop_manager import DROPManager
    from dfms.manager.session import Session

def import_start_cluster():
    import dfms.deploy.pawsey.dfms_proxy as dfms_proxy
    import dfms.deploy.pawsey.example_client as exclient
    from dfms.manager.client import DataIslandManagerClient
    import dfms.manager.cmdline as dfms_start
    from dfms.manager.constants import NODE_DEFAULT_REST_PORT, \
    ISLAND_DEFAULT_REST_PORT, MASTER_DEFAULT_REST_PORT

def import_composite_mgr():
    import abc
    import collections
    import functools
    import logging
    import multiprocessing.pool
    import threading

    from dfms import remote, graph_loader
    from dfms.ddap_protocol import DROPRel
    from dfms.exceptions import InvalidGraphException, DaliugeException, \
        SubManagerException
    from dfms.manager.client import NodeManagerClient
    from dfms.manager.constants import ISLAND_DEFAULT_REST_PORT, NODE_DEFAULT_REST_PORT
    from dfms.manager.drop_manager import DROPManager
    from dfms.utils import portIsOpen
    from dfms.manager import constants

def import_cmdline():
    from dfms.manager.composite_manager import DataIslandManager, MasterManager
    from dfms.manager.constants import NODE_DEFAULT_REST_PORT, \
        ISLAND_DEFAULT_REST_PORT, MASTER_DEFAULT_REST_PORT, REPLAY_DEFAULT_REST_PORT
    from dfms.manager.node_manager import NodeManager
    from dfms.manager.replay import ReplayManager, ReplayManagerServer
    from dfms.manager.rest import NMRestServer, CompositeManagerRestServer, \
        MasterManagerRestServer
    from dfms.utils import getDfmsPidDir, getDfmsLogsDir, createDirIfMissing

def import_rest():
    from dfms.exceptions import InvalidGraphException, InvalidSessionState, \
        DaliugeException, NoSessionException, SessionAlreadyExistsException, \
        InvalidDropException, InvalidRelationshipException
    from dfms.manager import constants
    from dfms.manager.client import NodeManagerClient
    from dfms.restutils import RestServer, RestClient, DALIUGE_HDR_ERR, \
        RestClientException

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
        logger.warning("Failed to obtain IP address from {0}".format(out))
        return 'None'

if __name__ == '__main__':
    """
    """
    parser = optparse.OptionParser()
    parser.add_option("-i", "--import_dfms", action="store_true",
                      dest="import_dfms", help="Whether to import DFMS libraries", default=False)

    parser.add_option("-l", "--log_to_file", action="store_true",
                      dest="log_to_file", help="Whether to log onto files", default=False)

    (options, args) = parser.parse_args()

    from mpi4py import MPI
    comm = MPI.COMM_WORLD
    num_procs = comm.Get_size()
    rank = comm.Get_rank()

    FORMAT = "%(asctime)-15s [%(levelname)5.5s] [%(threadName)15.15s] %(name)s#%(funcName)s:%(lineno)s %(message)s"
    if (options.log_to_file):
        log_dir = '/group/astronomy856/cwu/dfms/logs/min_test'
        logfile = "{0}/{1}.log".format(log_dir, rank)
        logging.basicConfig(filename=logfile, level=logging.DEBUG, format=FORMAT)
    else:
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format=FORMAT)

    public_ip = get_ip()
    ip_adds = public_ip
    ip_adds = comm.gather(ip_adds, root=0)

    if (rank == 0):
        logger.info("manager ip: {0}".format(public_ip))
        logger.info("got a list of IPs: {0}".format(ip_adds))
    else:
        logger.info("my ip: {0}, my rank: {1}".format(public_ip, rank))

    if (options.import_dfms):
        logger.info("importing a bunch of DFMS libraries")
        import_start_cluster()
        import_cmdline()
        if (rank == 0):
            import_composite_mgr()
        else:
            import_node_mgr()
        import_rest()
        logger.info("Importing successful")
