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
from mpi4py import MPI
import commands, time, sys, os
import dfms.manager.cmdline as dfms_start

def get_ip():
    """
    This is brittle, but works on Magnus/Galaxy for now
    """
    re = commands.getstatusoutput('ifconfig')
    line = re[1].split('\n')[1]
    return line.split()[1].split(':')[1]

def startNM(log_dir):
    """
    Start node manager
    """
    dfms_start.dfmsNM(args=['cmdline.py', '-l', log_dir])

def startDIM(node_list, log_dir, my_ip=None):
    """
    Start data island manager
    """
    if (my_ip is not None):
        try:
            node_list.remove(my_ip)
        except:
            pass
    dfms_start.dfmsDIM(args= ['cmdline.py', '-l', log_dir, '-N', ','.join(node_list)])

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()
log_dir = "{0}/{1}".format(sys.argv[1], rank)

ip_adds = get_ip()
origin_ip = ip_adds
ip_adds = comm.gather(ip_adds, root=0)
if (rank != 0):
    print "Starting node manager on host {0}".format(origin_ip)
    startNM(log_dir)
else:
    print "A list of IP addresses are: ", ip_adds
    print "Starting island manager on host {0}".format(origin_ip)
    startDIM(ip_adds, log_dir, my_ip=origin_ip)
