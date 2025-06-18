#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2019
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
import collections
import logging
import os
import pickle
import time
import re
import socket

from . import deployment_utils

logger = logging.getLogger(f"dlg.{__name__}")


class Remote(object):
    def __init__(self, options, my_ip):
        self.options = options
        self.my_ip = my_ip
        self.num_islands = options.num_islands
        self.run_proxy = options.monitor_host
        self.co_host_dim = options.co_host_dim
        self.rank = None
        self.size = None
        self.sorted_peers = None

    def _get_ip_from_name(self, hostname):
        rx = re.compile(r"^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$")
        if rx.match(hostname):  # already an IP??
            return hostname
        else:
            return socket.gethostbyname(hostname)

    def _set_world(self, rank, size, sorted_peers):
        """Called by subclasses"""
        self.rank = rank
        self.size = size
        if len(set(sorted_peers)) != self.size:
            raise RuntimeError("More than one task started per node, cannot continue")
        # convert nodes to IP addresses if hostnames
        self.sorted_peers = list(map(self._get_ip_from_name(sorted_peers)))
        nm_range = self._nm_range()
        if nm_range[0] == nm_range[1]:
            raise RuntimeError(
                "No nodes left for Node Managers with the requested setup"
            )

    def _dim_range(self):
        first = 0
        if self.num_islands > 1:
            first = 1
        if self.run_proxy:
            first += 1
        return first, first + self.num_islands

    def _nm_range(self):
        if self.co_host_dim:
            first = 0
        else:
            first = 1
        if self.run_proxy:
            first = 2
        if self.num_islands > 1:
            first += self.num_islands
        return first, self.size

    def _dim_nms(self, pg):
        dim_nodes = collections.defaultdict(set)
        ranks = {ip: rank for rank, ip in enumerate(self.sorted_peers)}
        for drop in pg:
            nm, dim = drop["node"], drop["island"]
            dim_nodes[dim].add(nm)
        for dim in self.dim_ips:
            r = ranks[dim]
            nms = list(dim_nodes[dim])
            logger.debug("Nodes for DIM %s (rank %d): %r", dim, r, nms)
            yield r, nms

    @property
    def is_highest_level_manager(self):
        return self.rank == 0

    @property
    def hl_mgr_ip(self):
        return self.sorted_peers[0]

    @property
    def is_dim(self):
        return self.rank in range(*self._dim_range())

    @property
    def dim_ips(self):
        start, end = self._dim_range()
        return self.sorted_peers[start:end]

    @property
    def is_nm(self):
        return self.rank in range(*self._nm_range())

    @property
    def nm_ips(self):
        start, end = self._nm_range()
        return self.sorted_peers[start:end]

    @property
    def is_proxy(self):
        return self.run_proxy and self.rank == 1

    @property
    def proxy_ip(self):
        if not self.run_proxy:
            raise RuntimeError("proxy_ip requested when not running with proxy enabled")
        return self.sorted_peers[1]


class MPIRemote(Remote):
    def __init__(self, options, my_ip):
        super(MPIRemote, self).__init__(options, my_ip)
        from mpi4py import MPI  # @UnresolvedImport

        self.comm = MPI.COMM_WORLD  # @UndefinedVariable
        self._set_world(
            self.comm.Get_rank(), self.comm.Get_size(), self.comm.allgather(self.my_ip)
        )

    def send_dim_nodes(self, pg):
        for dim_rank, nms in self._dim_nms(pg):
            self.comm.send(nms, dest=dim_rank)

    def recv_dim_nodes(self):
        return self.comm.recv(source=0)


class FilesystemBasedRemote(Remote):
    def send_dim_nodes(self, pg):
        basedir = self.options.log_dir
        for dim_rank, nms in self._dim_nms(pg):
            fname = "dim_%d_nodes.pickle" % dim_rank
            stage1_fname = os.path.join(basedir, ".%s" % fname)
            stage2_fname = os.path.join(basedir, "%s" % fname)
            with open(stage1_fname, "wb") as f:
                pickle.dump(nms, f)
            os.rename(stage1_fname, stage2_fname)

    def recv_dim_nodes(self):
        fname = os.path.join(self.options.log_dir, "dim_%d_nodes.pickle" % self.rank)
        while not os.path.exists(fname):
            time.sleep(1)
        with open(fname, "rb") as f:
            return pickle.load(f)


class SlurmRemote(FilesystemBasedRemote):
    def __init__(self, options, my_ip):
        super(SlurmRemote, self).__init__(options, my_ip)
        self._set_world(
            int(os.environ["SLURM_PROCID"]),
            int(os.environ["SLURM_NTASKS"]),
            deployment_utils.list_as_string(os.environ["SLURM_NODELIST"]),
        )


class DALiuGERemote(FilesystemBasedRemote):
    """A remote based on DALIUGE-specific environment variables"""

    def __init__(self, options, my_ip):
        super(DALiuGERemote, self).__init__(options, my_ip)
        ips = os.environ["DALIUGE_CLUSTER_IPS"].split()
        rank = ips.index(my_ip)
        self._set_world(rank, len(ips), ips)


class DALiuGEHybridRemote(DALiuGERemote):
    """Like DALiuGERemote, but initializes MPI as well"""

