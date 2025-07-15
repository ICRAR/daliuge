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
import shutil
import tempfile
import string
import dlg.remote as dlg_remote
from dlg.runtime import __git_version__ as git_commit

from dlg.deploy.configs import ConfigFactory, init_tpl, dlg_exec_str
from dlg.deploy.configs import DEFAULT_MON_PORT, DEFAULT_MON_HOST
from dlg.deploy.deployment_utils import find_numislands, label_job_dur
from paramiko.ssh_exception import SSHException


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
        dlg_root=None,
        host=None,
        acc=None,
        physical_graph_template_file=None,  # filename of physical graph template
        logical_graph=None,
        job_dur=30,
        num_nodes=None,
        run_proxy=False,
        mon_host=DEFAULT_MON_HOST,
        mon_port=DEFAULT_MON_PORT,
        logv=1,
        facility=None,
        zerorun=False,
        max_threads=0,
        sleepncopy=False,
        num_islands=None,
        all_nics=False,
        check_with_session=False,
        submit=False,
        remote=True,
        pip_name=None,
        username=None,
        ssh_key="",
        config=None,
        slurm_template=None,
        suffix=None
    ):
        ## Here, we want to separate out the following
        ## Config derived from CONFIG Factory - we replace with ini file
        ## Config derived from CLI, intended for replacement in the SLURM job script
        ##    - We want to replace these directives with the SLURM template
        ## Config derived from CLI that is used in the final script call
        ## Any leftover config - we keep as normal

        if config:
            # Do the config from the config file
            self.host = config['login_node']
            self._acc = config['account'] # superceded by slurm_template if present
            self.dlg_root = config['dlg_root']
            self.modules = config['modules']
            self.venv = config['venv'] # superceded by slurm_template if present
            self.exec_prefix = config["exec_prefix"]
            self.username = config['user'] if 'user' in config else username
            if not self.username:
                print("Username not configured in INI file, using local username...")
        else:
            # Setup SLURM environment variables using config
            config = ConfigFactory.create_config(facility=facility, user=username)
            self.host = config.getpar("host") if host is None else host
            self._acc = config.getpar("account") if (acc is None) else acc
            # self._user = config.getpar("user") if (username is None) else username

            # environment & sbatch
            self.dlg_root = config.getpar("dlg_root") if not dlg_root else dlg_root
            self.modules = config.getpar("modules")
            self.venv = config.getpar("venv")
            self.exec_prefix = config.getpar("exec_prefix")
            self.username = username
        # sbatch 
        if slurm_template:
            self._slurm_template = slurm_template
            self._num_nodes = 1 # placeholder
            self._job_dur = 1 # placeholder
        else:
            self._slurm_template = None
            if num_nodes is None:
                self._num_nodes = 1
            else:
                self._num_nodes = num_nodes
            self._job_dur = job_dur

        # self._log_root = (
        #     self._config.getpar("log_root") if (log_root is None) else log_root
        # )
        # 
        # start_dlg_cluster arguments
        self.visualise_graph = False
        self._logical_graph = logical_graph
        self._physical_graph_template_file = physical_graph_template_file
        self._run_proxy = run_proxy
        self._mon_host = mon_host
        self._mon_port = mon_port
        self._pip_name = pip_name
        self._logv = logv
        self._zerorun = zerorun
        self._max_threads = max_threads
        self._sleepncopy = sleepncopy
        if num_islands is None:
            self._num_islands = 1
        else:
            self._num_islands = num_islands
        self._all_nics = all_nics
        self._check_with_session = check_with_session
        self._submit = submit
        self._suffix = self.create_session_suffix(suffix)
        ni, nn, self._pip_name = find_numislands(self._physical_graph_template_file)
        if isinstance(ni, int) and ni >= self._num_islands:
            self._num_islands = ni
        if nn and nn >= self._num_nodes:
            self._num_nodes = nn

        # used for remote login/directory management.
        self._remote = remote
        self.ssh_key = ssh_key 
    
    def create_session_suffix(self, suffix=None):
        """
        Create a suffix to identify the session. If no suffix is specified, use the 
        current datetime setting. 

        :param: suffix, used to specify a non-datetime suffix. 
        :return: the final suffix 
        """
        if not suffix:
            return datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
        else:
            return suffix

    def get_session_dirname(self):
        """
        (pipeline name_)[Nnum_of_daliuge_nodes]_[time_stamp]
        """
        graph_name = self._pip_name.split("_")[0]  # use only the part of the graph name
        graph_name = graph_name.rsplit(".pgt.graph")[0]
        return "{0}_{1}".format(graph_name, self._suffix)
    

    def apply_slurm_template(self, template_str, session_id, dlg_root):
        """
        Given a string from a template file, use a string.Template object to perform
        safe substution on the string and replace $VALUES with the correct value 
        specified. 
        """
        intermed_slurm = string.Template(template_str) 
        ims = intermed_slurm.safe_substitute(session_id=session_id, dlg_root=dlg_root)
        print("Creating job description")
        return ims + "\n\n" + dlg_exec_str

    def create_paramater_mapping(self, session_dir, physical_graph_file):
        """
        Map the runtime or configured parameters to the session environment and SLURM 
        script paramteres, in anticipation of using substition. 
        """ 
        pardict = {}
        pardict["SESSION_ID"] = os.path.split(session_dir)[-1]
        pardict["MODULES"] = self.modules
        pardict["DLG_ROOT"] = self.dlg_root
        pardict["EXEC_PREFIX"] = self.exec_prefix
        pardict["NUM_NODES"] = str(self._num_nodes)
        pardict["JOB_DURATION"] = label_job_dur(self._job_dur)

        pardict["VENV"] = self.venv
        pardict["PIP_NAME"] = self._pip_name
        pardict["ACCOUNT"] = self._acc
        pardict["PY_BIN"] = "python3" if pardict["VENV"] else sys.executable
        pardict["LOG_DIR"] = session_dir
        pardict["GRAPH_PAR"] = (
            '--logical-graph "{0}"'.format(self._logical_graph)
            if self._logical_graph
            else (
                '--physical-graph "{0}"'.format(physical_graph_file)
                if physical_graph_file
                else ""
            )
        )
        pardict["PROXY_PAR"] = (
            "--monitor_host %s --monitor_port %d" % (self._mon_host, self._mon_port)
            if self._run_proxy
            else ""
        )
        pardict["GRAPH_VIS_PAR"] = "--dump" if self.visualise_graph else ""
        pardict["LOGV_PAR"] = "--verbose-level %d" % self._logv
        pardict["ZERORUN_PAR"] = "--zerorun" if self._zerorun else ""
        pardict["MAXTHREADS_PAR"] = "--max-threads %d" % self._max_threads
        pardict["SNC_PAR"] = "--app 1" if self._sleepncopy else "--app 0"
        pardict["NUM_ISLANDS_PAR"] = "--num_islands %d" % self._num_islands
        pardict["ALL_NICS"] = "--all_nics" if self._all_nics else ""
        pardict["CHECK_WITH_SESSION"] = (
            "--check_with_session" if self._check_with_session else ""
        )
        return pardict

    def create_job_desc(self, physical_graph_file):
        """
        Creates the slurm script from a physical graph

        This uses string.Template to apply substitutions that are linked to the 
        parameters defined at runtime. These parameters map to $VALUEs in a pre-defined
        execution command that contains the necessary parameters to run DALiuGE through
        SLURM. 
        """

        session_dir = "{0}/workspace/{1}".format(
            self.dlg_root, self.get_session_dirname()
        )
        pardict = self.create_paramater_mapping(session_dir, physical_graph_file)

        if self._slurm_template:
            slurm_str = self.apply_slurm_template(self._slurm_template, 
                                                  pardict['SESSION_ID'],
                                                  pardict['DLG_ROOT'])
            return string.Template(slurm_str).safe_substitute(pardict)

        return init_tpl.safe_substitute(pardict)

    @property
    def session_dir(self):
        return "{0}/workspace/{1}".format(
            self.dlg_root, self.get_session_dirname()
        )

    def mk_session_dir(self, dlg_root: str = ""):
        """
        Create the session directory. If dlg_root is provided it is used,
        else env var DLG_ROOT is used.
        """

        if dlg_root:  # has always preference
            self.dlg_root = dlg_root
        if self._remote and not self.dlg_root:
            print("Deploying on a remote cluster requires specifying DLG_ROOT!")
            print("Unable to create session directory!")
            return ""
        elif not self._remote:
            # locally fallback to env var
            if os.environ["DLG_ROOT"]:
                self.dlg_root = os.environ["DLG_ROOT"]
            else:
                self.dlg_root = f"{os.environ['HOME']}.dlg"
        session_dir =  self.session_dir
        if not self._remote and not os.path.exists(session_dir):
            os.makedirs(session_dir)
        if self._remote:
            command = f"mkdir -p {session_dir}"
            print(
                f"Creating remote session directory on {self.username}@{self.host}: {command}"
            )
            try:
                dlg_remote.execRemote(
                    self.host, command, username=self.username, pkeyPath=self.ssh_key
                )
            except (TypeError, SSHException) as e:
                print(
                    f"ERROR: Unable to create {session_dir} on {self.username}@{self.host}, {str(e)}"
                )
                return None

        return session_dir

    def submit_job(self):
        """
        Submits the slurm script to the requested facility

        :returns: jobId, the id of the SLURM job create on the facility. 
                  None if a remote directory could not be created or if an error occurs
                  during connection. 
        """
        jobId = None
        session_dir = self.mk_session_dir()
        if not session_dir:
            print("No session_dir created.")
            return jobId

        physical_graph_file_name = "{0}/{1}".format(session_dir, self._pip_name)
        if self._physical_graph_template_file:
            if self._remote:
                print(f"Copying PGT to: {physical_graph_file_name}")
                dlg_remote.copyTo(
                    self.host,
                    self._physical_graph_template_file,
                    physical_graph_file_name,
                    username=self.username,
                    pkeyPath=self.ssh_key,
                )
            else:
                shutil.copyfile(
                    self._physical_graph_template_file, physical_graph_file_name
                )

        job_file_name = "{0}/jobsub.sh".format(session_dir)
        job_desc = self.create_job_desc(physical_graph_file_name)

        if self._remote:
            print(f"Creating SLURM script remotely: {job_file_name}")
            tjob = tempfile.mktemp()
            with open(tjob, "w+t") as t:
                t.write(job_desc)
            dlg_remote.copyTo(
                self.host,
                tjob,
                job_file_name,
                username=self.username,
                pkeyPath=self.ssh_key,
            )
            os.remove(tjob)
        else:
            with open(job_file_name, "w") as job_file:
                job_file.write(job_desc)
            with open(os.path.join(session_dir, "git_commit.txt"), "w") as git_file:
                git_file.write(git_commit)
        if self._submit:
            if not self._remote:
                os.chdir(session_dir)  # so that slurm logs will be dumped here
                print(subprocess.check_output(["sbatch", job_file_name]))
            else:
                command = f"cd {session_dir} && sbatch --parsable {job_file_name}"
                print(f"Submitting sbatch job: {command}")
                stdout, stderr, exitStatus = dlg_remote.execRemote(
                    self.host, command, username=self.username, pkeyPath=self.ssh_key
                )
                if exitStatus != 0:
                    print(
                        f"Job submission unsuccessful: {exitStatus}, {stderr.decode()}"
                    )
                else:
                    jobId = stdout.decode()
                    print(f"Job with ID {jobId} submitted successfully.")
        else:
            print(f"Created job submission script {job_file_name}")
        return jobId
