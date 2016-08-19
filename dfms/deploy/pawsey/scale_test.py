#!/usr/bin/python
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
Generate the SLURM script, and submit it to the queue based on various paramters

parse the log result, and produce the plot

"""
from datetime import datetime
import sys, os, commands, socket
from string import Template
import optparse

from dfms.deploy.pawsey.example_client import lgnames

sub_tpl_str = """#!/bin/bash --login

#SBATCH --nodes=$NUM_NODES
#SBATCH --ntasks-per-node=1
#SBATCH --job-name=DALiuGE-$PIP_NAME
#SBATCH --time=$JOB_DURATION
#SBATCH --account=$ACCOUNT
#SBATCH --error=err-%j.log

module swap PrgEnv-cray PrgEnv-gnu
module load python/2.7.10
module load mpi4py

aprun -B $PY_BIN -m dfms.deploy.pawsey.start_dfms_cluster -l $LOG_DIR $GID_PAR $PROXY_PAR $GRAPH_VIS_PAR
"""

sub_tpl = Template(sub_tpl_str)

class DefaultConfig(object):
    def __init__(self):
        self._dict = dict()
        l = self.init_list()
        self.setpar('acc', l[0])
        self.setpar('log_root', l[1])

    def init_list(self):
        pass

    def setpar(self, k, v):
        self._dict[k] = v

    def getpar(self, k):
        return self._dict.get(k)

class GalaxyMWAConfig(DefaultConfig):
    def __init__(self):
        super(GalaxyMWAConfig, self).__init__()

    def init_list(self):
        return ['mwaops', '/group/mwaops/cwu/dfms/logs']

class GalaxyASKAPConfig(DefaultConfig):
    def __init__(self):
        super(GalaxyASKAPConfig, self).__init__()

    def init_list(self):
        return ['astronomy856', '/group/astronomy856/cwu/dfms/logs']

class MagnusConfig(DefaultConfig):
    def __init__(self):
        super(MagnusConfig, self).__init__()

    def init_list(self):
        return ['pawsey0129', '/group/pawsey0129/daliuge_logs']

class TianHe2Config(DefaultConfig):
    pass

class ConfigFactory():
    """
    is this really necessary?
    """
    mapping = {'galaxy_mwa':GalaxyMWAConfig(), 'galaxy_askap':GalaxyASKAPConfig(),
    'magnus':MagnusConfig(), 'galaxy':GalaxyASKAPConfig()}

    @staticmethod
    def create_config(facility=None):
        facility = facility.lower() if (facility is not None) else facility
        return ConfigFactory.mapping.get(facility)

class PawseyClient(object):
    """
    parameters we can control:

    1. Pawsey group / account name (Required)
    2. whether to submit a graph, and if so provide graph id (gid)
    3. # of nodes (of Drop Managers)
    4. how long to run
    5. whether to produce offline graph vis
    6. whether to attach proxy for remote monitoring, and if so provide
        DFMS_MON_HOST
        DFMS_MON_PORT
    7. Root directory of the Log files (Required)
    """

    def __init__(self, log_root=None, acc=None,
                job_dur=30,
                mon_host='sdp-dfms.ddns.net',
                mon_port=8098,
                facility=socket.gethostname().split('-')[0]):
        self._config = ConfigFactory.create_config(facility=facility)
        self._acc = self._config.getpar('acc') if (acc is None) else acc
        self._log_root = self._config.getpar('log_root') if (log_root is None) else log_root
        self._num_nodes = 5
        self._job_dur = job_dur
        self._gid = None
        self._graph_vis = False
        self._run_proxy = False
        self._mon_host = mon_host
        self._mon_port = mon_port
        self._pip_name = None


    def set_gid(self, gid):
        if (gid is None):
            return
        if (int(gid) >= len(lgnames)):
            raise Exception("Invalid graph id '{0}'".format(gid))
        self._gid = gid
        self._pip_name = lgnames[gid].split('.')[0]

    @property
    def num_daliuge_nodes(self):
        if (self._run_proxy):
            return self._num_nodes - 2 # exclude the proxy node
        else:
            return self._num_nodes - 1 # exclude the data island node

    def get_log_dirname(self):
        """
        (pipeline name_)[Nnum_of_daliuge_nodes]_[time_stamp]
        """
        dtstr = datetime.now().strftime("%Y-%m-%dT%H-%M-%S") #.%f
        if (self._pip_name is None):
            return "N{0}_{1}".format(self.num_daliuge_nodes, dtstr)
        else:
            return "{0}_N{1}_{2}".format(self._pip_name, self.num_daliuge_nodes, dtstr)

    def label_job_dur(self):
        """
        e.g. 135 min --> 02:15:00
        """
        seconds = self._job_dur * 60
        m, s = divmod(seconds, 60)
        h, m = divmod(m, 60)
        return "%02d:%02d:%02d" % (h, m, s)

    def submit_job(self):
        lgdir = '{0}/{1}'.format(self._log_root, self.get_log_dirname())
        if (not os.path.exists(lgdir)):
            os.makedirs(lgdir)

        pardict = dict()
        pardict['NUM_NODES'] = str(self._num_nodes)
        pardict['PIP_NAME'] = self._pip_name
        pardict['JOB_DURATION'] = self.label_job_dur()
        pardict['ACCOUNT'] = self._acc
        pardict['PY_BIN'] = sys.executable
        pardict['LOG_DIR'] = lgdir
        pardict['GID_PAR'] = '-g %d' % self._gid if (self._gid is not None) else ''
        pardict['PROXY_PAR'] = '-m %s -o %d' % (_mon_host, _mon_port) if self._run_proxy else ''
        pardict['GRAPH_VIS_PAR'] = '-d' if self._graph_vis else ''

        job_desc = sub_tpl.safe_substitute(pardict)
        job_file = '{0}/jobsub.sh'.format(lgdir)
        with open(job_file, 'w') as jf:
            jf.write(job_desc)

        os.chdir(lgdir) # so that slurm logs will be dumped here
        cmd = 'sbatch %s' % job_file
        rc, rs = commands.getstatusoutput(cmd)
        print rs

if __name__ == '__main__':
    parser = optparse.OptionParser()

    parser.add_option("-a", "--action", action="store", type="int",
                      dest="action", help="1 - submit job, 2 - analyse log", default=1)

    parser.add_option("-l", "--log-root", action="store",
                      dest="log_root", help="The root directory of the log file", default="localhost")

    parser.add_option("-g", "--graph-id", action="store", type="int",
                      dest="graph_id", help="The graph to deploy (0 - %d)" % (len(lgnames) - 1), default=None)

    parser.add_option("-t", "--job-dur", action="store", type="int",
                      dest="job_dur", help="job duration in minutes", default=30)

    (opts, args) = parser.parse_args(sys.argv)
    pc = PawseyClient(job_dur=opts.job_dur)
    pc.set_gid(opts.graph_id)
    pc.submit_job()
