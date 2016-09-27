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

import commands, time, sys, os, logging
import json
from optparse import OptionParser
import threading
from collections import defaultdict

logger = logging.getLogger('deploy.pawsey.min_test')

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

if __name__ == '__main__':
    """
    """
    parser = OptionParser()
    parser.add_option("-i", "--import_dfms", action="store_true",
                      dest="import_dfms", help="Whether to import DFMS libraries", default=False)

    (options, args) = parser.parse_args()

    comm = MPI.COMM_WORLD  # @UndefinedVariable
    num_procs = comm.Get_size()
    rank = comm.Get_rank()

    public_ip = get_ip()
    ip_adds = public_ip
    ip_adds = comm.gather(ip_adds, root=0)

    if (rank == 0):
        logger.info("manager ip: {0}".format(public_ip))
        logger.info("got a list of IPs: {1}".format(ip_adds))
    else:
        logger.info("my ip: {0}".format(public_ip))

    if (options.import_dfms):
        logger.info("importing a bunch of DFMS libraries")
        if (rank == 0):
            import dfms.deploy.pawsey.dfms_proxy as dfms_proxy
            from dfms.deploy.pawsey.example_client import MonitorClient
            import dfms.deploy.pawsey.example_client as exclient
        else:
            from dfms.manager.client import DataIslandManagerClient
            import dfms.manager.cmdline as dfms_start
            from dfms.manager.constants import NODE_DEFAULT_REST_PORT, \
            ISLAND_DEFAULT_REST_PORT, MASTER_DEFAULT_REST_PORT
        logger.info("Importing successful")
