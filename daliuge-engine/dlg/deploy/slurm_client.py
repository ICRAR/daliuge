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
import logging
import os
import shutil
import string
import subprocess
import sys
import tempfile
import time

import dlg.remote as dlg_remote

from configparser import ConfigParser, ExtendedInterpolation
from pathlib import Path
from paramiko.ssh_exception import SSHException

from dlg.exceptions import RemoteSessionRuntimeException
from dlg.runtime import __git_version__ as git_commit

from dlg.deploy.configs import ConfigFactory, init_tpl, dlg_exec_str
from dlg.deploy.configs import DEFAULT_MON_PORT, DEFAULT_MON_HOST
from dlg.deploy.deployment_utils import find_numislands, label_job_dur

LOGGER = logging.getLogger(f"dlg.{__name__}")

def process_config(config_file: str):
    """
    Use configparser to process INI file

    Current functionality: 
        - Returns remote environment config (e.g. DLG_ROOT, HOME etc.)

    Future Functionality: 
        - Graph translation parameters
        - Engine parameters

    :returns: dict, confiport information
    """
    parser = ConfigParser(interpolation=ExtendedInterpolation())
    all_opts = {}
    parser.read(config_file)
    for s in parser.sections():
        all_opts |= (dict(parser[s]))
    return {key: value for key, value in all_opts.items() if value}

def process_slurm_template(template_file: str):
    if not template_file:
        return None
    template = Path(template_file)
    with template.open('r') as fp:
        return fp.read()

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
        dlg_root: str = "",
        host: str = "",
        acc: str = "",
        physical_graph_template_file: str = "",  # filename of physical graph template
        logical_graph: str="",
        job_dur: int = 30,
        num_nodes: int = None,
        run_proxy: bool = False,
        mon_host: int = DEFAULT_MON_HOST,
        mon_port: int = DEFAULT_MON_PORT,
        logv: int = 1,
        facility: str = "",
        zerorun: bool =False,
        max_threads: int = 0,
        sleepncopy: bool = False,
        num_islands: int = None,
        all_nics: bool = False,
        check_with_session: bool =False,
        submit: bool = False,
        remote: bool = True,
        username: str = "",
        ssh_key="",
        config=None,
        slurm_template=None,
        suffix=None,
        wait: bool=False,
    ):
        """
        Initialize a SLURM client for deploying DALiuGE graphs

        :param dlg_root: Root directory for DALiuGE installation
        :param host: Hostname of the SLURM cluster
        :param acc: SLURM account name
        :param physical_graph_template_file: Path to physical graph template file
        :param logical_graph: Path to logical graph file
        :param job_dur: Duration of job in minutes
        :param num_nodes: Number of compute nodes to request
        :param run_proxy: Whether to enable remote monitoring proxy
        :param mon_host: Monitoring host address
        :param mon_port: Monitoring port number  
        :param logv: Logging verbosity level (0-3)
        :param facility: Target facility name
        :param zerorun: Run without executing drops
        :param max_threads: Maximum number of threads per process
        :param sleepncopy: Use sleep-and-copy application
        :param num_islands: Number of islands in deployment
        :param all_nics: Use all network interfaces
        :param check_with_session: Check graph status with session
        :param submit: Submit job to SLURM queue
        :param remote: Whether deploying to remote cluster
        :param pip_name: Name of pipeline
        :param username: Remote username
        :param ssh_key: Path to SSH key file
        :param config: Configuration dictionary or file path
        :param slurm_template: Custom SLURM script template
        :param suffix: Custom suffix for session directory
        :param wait: Whether to wait for job completion
        """

        if config:
            if isinstance(config, dict):
                parsed_config = config
            elif os.path.isfile(config):
                parsed_config = process_config(config)
            else:
                raise ValueError(f"Invalid config: {config}")

            try:
                self.host = parsed_config.get('login_node')
                # superceded by slurm_template if that is present
                self._acc = parsed_config.get('account')
                self.dlg_root = parsed_config.get('dlg_root')
                self.modules = parsed_config.get('modules')
                # superceded by slurm_template if that is present
                self.venv = parsed_config.get('venv')
                self.exec_prefix = parsed_config.get("exec_prefix")
                self.username = parsed_config.get('user', username)
                if not self.username:
                    print("Username not configured in INI file, using local username...")
            except KeyError as e:
                print(f"Missing {str(e)} entry in .ini, please review your configuration "
                      f"setup.")
                sys.exit(1)
        else:
            # Setup SLURM environment variables using Factory
            parsed_config = ConfigFactory.create_config(facility=facility, user=username)
            self.host = parsed_config.getpar("host") if host is None else host
            self._acc = parsed_config.getpar("account") if (acc is None) else acc
            # self._user = config.getpar("user") if (username is None) else username

            # environment & sbatch
            self.dlg_root = parsed_config.getpar("dlg_root") if not dlg_root else dlg_root
            self.modules = parsed_config.getpar("modules")
            self.venv = parsed_config.getpar("venv")
            self.exec_prefix = parsed_config.getpar("exec_prefix")
            self.username = username
        # sbatch 
        self._slurm_template = process_slurm_template(slurm_template)
        self._job_dur = job_dur
        self._num_nodes = num_nodes if num_nodes else 1  # placeholder

        # start_dlg_cluster arguments
        self.visualise_graph = False
        self._logical_graph = logical_graph
        self._physical_graph_template_file = physical_graph_template_file
        self._run_proxy = run_proxy
        self._mon_host = mon_host
        self._mon_port = mon_port
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
        if self._physical_graph_template_file:
            self._pip_name = Path(self._physical_graph_template_file).name
            ni, nn = find_numislands(self._physical_graph_template_file)
            if isinstance(ni, int) and ni >= self._num_islands:
                self._num_islands = ni
            if nn and nn >= self._num_nodes:
                self._num_nodes = nn

        # self._logical_graph = ""

        # used for remote login/directory management.
        self._remote = remote
        self.ssh_key = ssh_key if ssh_key else None

        self.wait = wait
    
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

    def create_paramater_mapping(self, session_dir, remote_graph_file):
        """
        Map the runtime or configured parameters to the session environment and SLURM 
        script paramteres, in anticipation of using substition. 
        """ 
        pardict = {}
        pardict["SESSION_ID"] = os.path.split(session_dir)[-1]
        pardict["MODULES"] = f"\n{self.modules}\n"
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
            '--logical-graph "{0}"'.format(remote_graph_file)
            if self._logical_graph
            else (
                '--physical-graph "{0}"'.format(remote_graph_file)
                if remote_graph_file
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

    @property
    def session_id(self):
        return os.path.split(self.session_dir)[-1]

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
        # TODO look into if we have permissions to create directory
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

        # subgraph_dir = Path("/tmp/")
        # subgraph_name = "subgraph.graph"
        # self._logical_graph = subgraph_dir / subgraph_name
        # with open(self._logical_graph, 'w+') as fp:
        #     json.dump(logical_graph, fp)

        session_dir = self.mk_session_dir()
        if not session_dir:
            print("No session_dir created.")
            return jobId

        if self._logical_graph:
            print("Changing to logical graph submission.")
            self._pip_name = Path(self._logical_graph).name

        remote_graph_file_name = "{0}/{1}".format(session_dir, self._pip_name)
        print(f"{remote_graph_file_name=}")
        if self._physical_graph_template_file:
            if self._remote:
                print(f"Copying PGT to: {remote_graph_file_name}")
                dlg_remote.copyTo(
                    self.host,
                    self._physical_graph_template_file,
                    remote_graph_file_name,
                    username=self.username,
                    pkeyPath=self.ssh_key,
                )
            else:
                shutil.copyfile(
                    self._physical_graph_template_file, remote_graph_file_name
                )
        elif self._logical_graph:
            if self._remote:
                print(f"Copying LGT to: {remote_graph_file_name}")
                dlg_remote.copyTo(
                    self.host,
                    self._logical_graph,
                    remote_graph_file_name,
                    username=self.username,
                )

        job_file_name = "{0}/jobsub.sh".format(session_dir)
        job_desc = self.create_job_desc(remote_graph_file_name)

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

        if self.wait and self._submit:
            jobId = self.fetch_remote_status(jobId)
            return jobId
        elif self.wait and not self._submit:
            raise RemoteSessionRuntimeException(
                self,
                "Cannot wait for remote job submission without "
                        "submitting. Make sure to set 'submit' to True if "
                        "wanting to wait for SlurmClient (e.g. for a SubGraph.")
        else:
            return None


    def fetch_remote_status(self, jobId: str, timeout: int = 15):
        #Use sacct --jobs=jobID --format=state --noheader
        running = True
        if not jobId:
            return None
        job_id = jobId.strip()
        while running:
            command = f"sacct --jobs={job_id} --format=state --noheader"
            stdout, stderr, _ = dlg_remote.execRemote(
                self.host, command, username=self.username
            )
            print(f"{stdout=},{stderr=}")
            # Process result here
            curr_jobs = stdout.decode().lower().split()
            if "failed" in curr_jobs or "cancelled" in curr_jobs:
                raise RuntimeError(
                    f"Slurm job did not finish successfully: stdout is: {stdout}\n.")
            if "pending" in curr_jobs or "running" in curr_jobs:
                time.sleep(timeout)
                continue
            if curr_jobs.count("completed") == len(curr_jobs):
                # Success!
                running = False

        return jobId


    def collect_job_data(self):
        """
        1. Zip remote session directory
        2. Copy it to this machine
        """
        command = f"tar -cvfz {self.session_id}.tar.gz {self.session_id}"
        if self._remote:
            dlg_remote.execRemote(self.host, command, username=self.username)
            output_path = f"{self.session_id}.tar.gz"
            dlg_remote.copyFrom(self.host,
                            remotePath=f"{self.session_dir}.tar.xz",
                            localPath=f"{self.session_dir}.tar.xz",
                            username=self.username)
        else:
            return None
        return output_path
