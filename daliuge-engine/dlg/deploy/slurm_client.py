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
Contains a slurm client which generates slurm scripts from daliuge graphs.
"""

import datetime
import sys
import os
import subprocess
from dlg.runtime import __git_version__ as git_commit

from dlg.deploy.configs import ConfigFactory, init_tpl
from dlg.deploy.deployment_constants import DEFAULT_AWS_MON_PORT, DEFAULT_AWS_MON_HOST
from dlg.deploy.deployment_utils import find_numislands, label_job_dur


class SlurmClient:
    """
    parameters we can control:

    1. user group / account name (Required)
    2. whether to submit a graph, and if so provide graph path
    3. # of nodes (of Drop Managers)
    4. how long to run
    5. whether to produce offline graph vis
    6. whether to attach proxy for remote monitoring, and if so provide
        DLG_MON_HOST
        DLG_MON_PORT
    7. Root directory of the Log files (Required)
    """

    def __init__(
        self,
        log_root=None,
        acc=None,
        physical_graph_template_data=None,  # JSON formatted physical graph template
        logical_graph=None,
        job_dur=30,
        num_nodes=None,
        run_proxy=False,
        mon_host=DEFAULT_AWS_MON_HOST,
        mon_port=DEFAULT_AWS_MON_PORT,
        logv=1,
        facility=None,
        zerorun=False,
        max_threads=0,
        sleepncopy=False,
        num_islands=None,
        all_nics=False,
        check_with_session=False,
        submit=True,
        pip_name=None,
    ):
        self._config = ConfigFactory.create_config(facility=facility)
        self._acc = self._config.getpar("acc") if (acc is None) else acc
        self._log_root = (
            self._config.getpar("log_root") if (log_root is None) else log_root
        )
        self.modules = self._config.getpar("modules")
        self.venv = self._config.getpar("venv")
        self._num_nodes = num_nodes
        self._job_dur = job_dur
        self._logical_graph = logical_graph
        self._physical_graph_template_data = physical_graph_template_data
        self._visualise_graph = False
        self._run_proxy = run_proxy
        self._mon_host = mon_host
        self._mon_port = mon_port
        self._pip_name = pip_name
        self._logv = logv
        self._zerorun = zerorun
        self._max_threads = max_threads
        self._sleepncopy = sleepncopy
        self._num_islands = num_islands
        self._all_nics = all_nics
        self._check_with_session = check_with_session
        self._submit = submit
        self._dtstr = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")  # .%f
        self._num_islands, self._num_nodes, self._pip_name = find_numislands(
            self._physical_graph_template_data
        )

    def get_log_dirname(self):
        """
        (pipeline name_)[Nnum_of_daliuge_nodes]_[time_stamp]
        """
        # Moved setting of dtstr to init
        # to ensure it doesn't change for this instance of SlurmClient()
        # dtstr = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")  # .%f
        graph_name = self._pip_name.split("_")[0]  # use only the part of the graph name
        return "{0}_{1}".format(graph_name, self._dtstr)

    def create_job_desc(self, physical_graph_file):
        """
        Creates the slurm script from a physical graph
        """
        log_dir = "{0}/{1}".format(self._log_root, self.get_log_dirname())
        pardict = dict()
        pardict["VENV"] = self.venv
        pardict["NUM_NODES"] = str(self._num_nodes)
        pardict["PIP_NAME"] = self._pip_name
        pardict["SESSION_ID"] = os.path.split(log_dir)[-1]
        pardict["JOB_DURATION"] = label_job_dur(self._job_dur)
        pardict["ACCOUNT"] = self._acc
        pardict["PY_BIN"] = "python3" if pardict["VENV"] else sys.executable
        pardict["LOG_DIR"] = log_dir
        pardict["GRAPH_PAR"] = (
            '-L "{0}"'.format(self._logical_graph)
            if self._logical_graph
            else '-P "{0}"'.format(physical_graph_file)
            if physical_graph_file
            else ""
        )
        pardict["PROXY_PAR"] = (
            "-m %s -o %d" % (self._mon_host, self._mon_port) if self._run_proxy else ""
        )
        pardict["GRAPH_VIS_PAR"] = "-d" if self._visualise_graph else ""
        pardict["LOGV_PAR"] = "-v %d" % self._logv
        pardict["ZERORUN_PAR"] = "-z" if self._zerorun else ""
        pardict["MAXTHREADS_PAR"] = "-t %d" % self._max_threads
        pardict["SNC_PAR"] = "--app 1" if self._sleepncopy else "--app 0"
        pardict["NUM_ISLANDS_PAR"] = "-s %d" % self._num_islands
        pardict["ALL_NICS"] = "-u" if self._all_nics else ""
        pardict["CHECK_WITH_SESSION"] = "-S" if self._check_with_session else ""
        pardict["MODULES"] = self.modules

        job_desc = init_tpl.safe_substitute(pardict)
        return job_desc

    def submit_job(self):
        """
        Submits the slurm script to the cluster
        """
        log_dir = "{0}/{1}".format(self._log_root, self.get_log_dirname())
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        physical_graph_file_name = "{0}/{1}".format(log_dir, self._pip_name)
        with open(physical_graph_file_name, "w") as physical_graph_file:
            physical_graph_file.write(self._physical_graph_template_data)
            physical_graph_file.close()

        job_file_name = "{0}/jobsub.sh".format(log_dir)
        job_desc = self.create_job_desc(physical_graph_file_name)
        with open(job_file_name, "w") as job_file:
            job_file.write(job_desc)

        with open(os.path.join(log_dir, "git_commit.txt"), "w") as git_file:
            git_file.write(git_commit)
        if self._submit:
            os.chdir(log_dir)  # so that slurm logs will be dumped here
            print(subprocess.check_output(["sbatch", job_file_name]))
        else:
            print(f"Created job submission script {job_file_name}")
