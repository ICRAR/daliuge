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

import datetime
import json
import optparse # pylint: disable=deprecated-module
import pwd
import re
import socket
import sys
import time
import os

from pathlib import Path

from dlg.deploy.configs import (
    ConfigFactory,
)  # get all available configurations

from dlg.deploy.configs import (
    DEFAULT_MON_PORT,
    DEFAULT_MON_HOST,
)
from dlg.deploy.slurm_client import SlurmClient
from dlg.common.reproducibility.reproducibility import (
    init_pgt_unroll_repro_data,
    init_pgt_partition_repro_data,
)

from dlg.deploy.configs.config_manager import ConfigManager, ConfigType

from dlg.dropmake import pg_generator

FACILITIES = ConfigFactory.available()


def get_timestamp(line):
    """
    microsecond precision
    """
    split = line.split()
    date_time = "{0}T{1}".format(split[0], split[1])
    pattern = "%Y-%m-%dT%H:%M:%S,%f"
    epoch = time.mktime(time.strptime(date_time, pattern))
    parsed_date = datetime.datetime.strptime(date_time, pattern)
    microseconds = parsed_date.microsecond / 1e6
    return microseconds + epoch


class LogEntryPair:
    """
    Generates log entries
    """

    def __init__(self, name, gstart, gend):
        self._name = name
        self._gstart = (
            gstart + 2
        )  # group 0 is the whole matching line, group 1 is the catchall
        self._gend = gend + 2
        self._start_time = None
        self._end_time = None
        self._other = {}

    def check_start(self, match, line):
        if self._start_time is None and match.group(self._gstart):
            self._start_time = get_timestamp(line)

    def check_end(self, match, line):
        if self._end_time is None and match.group(self._gend):
            self._end_time = get_timestamp(line)
            if self._name == "unroll":
                self._other["num_drops"] = int(line.split()[-1])
            elif self._name == "node managers":
                self._other["num_node_mgrs"] = int(
                    line.split("Got a node list with")[1].split()[0]
                )
            elif self._name == "build drop connections":
                self._other["num_edges"] = int(line.split()[-4][1:-1])

    def get_duration(self):
        if (self._start_time is None) or (self._end_time is None):
            # print "Cannot calc duration for
            # '{0}': start_time:{1}, end_time:{2}".format(self._name,
            # self._start_time, self._end_time)
            return None
        return self._end_time - self._start_time

    def reset(self):
        self._start_time = None
        self._end_time = None

    @property
    def name(self):
        return self._name

    @property
    def other(self):
        return self._other


def build_dim_log_entry_pairs():
    return [
        LogEntryPair(name, g1, g2)
        for name, g1, g2 in (
            ("unroll", 0, 1),
            ("translate", 2, 3),
            ("gen pg spec", 3, 4),
            ("create session", 5, 6),
            ("separate graph", 7, 8),
            ("add session to all", 9, 10),
            ("deploy session to all", 11, 12),
            ("build drop connections", 13, 14),
            ("trigger drops", 15, 16),
            ("node managers", 17, 17),
        )
    ]


def build_nm_log_entry_pairs():
    return [
        LogEntryPair(name, g1, g2)
        for name, g1, g2 in (
            ("completion_time_old", 0, 3),  # Old master branch
            ("completion_time", 2, 3),
            ("node_deploy_time", 1, 2),
        )
    ]


def construct_catchall_pattern(node_type):
    pattern_strs = LogParser.kwords.get(node_type)
    patterns = [
        x.format(".*").replace("(", r"\(").replace(")", r"\)") for x in pattern_strs
    ]
    catchall = "|".join(["(%s)" % (s,) for s in patterns])
    catchall = ".*(%s).*" % (catchall,)
    return re.compile(catchall)


class LogParser:
    """
    TODO: This needs adjustment to new log directory names!!

    Given the log dir, analyse all DIM and NMs logs, and store the resuls
    in CSV, which has the following fields:
    ====================================
    0.  user name (e.g. cwu)
    1.  facility (e.g. galaxy)
    2.  pipeline (e.g. lofar_std)
    3.  time (e.g. 2016-08-22T11-52-11/)
    4.  # of nodes
    5.  # of drops
    6.  Git commit number
    7.  unroll_time
    8.  translation_time
    9.  pg_spec_gen_time
    10. created_session_at_all_nodes_time
    11. graph_separation_time
    12. push_sub_graphs_to_all_nodes_time
    13. created_drops_at_all_nodes_time
    14. Num_pyro_connections_at_all_nodes
    15. created_pyro_conn_at_all_nodes_time
    16. triggered_drops_at_all_nodes_time
    17. Total completion time


    Detailed description of each field:
    https://confluence.icrar.uwa.edu.au/display/DALIUGE/Scalability+test#Scalabilitytest-Datacollection
    """

    dim_kl = [
        "Start to unroll",
        "Unroll completed for {0} with # of Drops",
        "Start to translate",
        "Translation completed for",
        "PG spec is calculated",
        "Creating Session {0} in all hosts",
        "Successfully created session {0} in all hosts",
        "Separating graph",
        "Removed (and sanitized) {0} inter-dm relationships",
        "Adding individual graphSpec of session {0} to each DM",
        "Successfully added individual graphSpec of session {0} to each DM",
        "Deploying Session {0} in all hosts",
        "Successfully deployed session {0} in all hosts",
        "Establishing {0} drop relationships",
        "Established all drop relationships {0} in",
        "Moving Drops to COMPLETED right away",
        "Successfully triggered drops",
        "Got a node list with {0} node managers",
    ]

    nm_kl = [
        "Starting Pyro4 Daemon for session",  # Logged by the old master branch
        "Creating DROPs for session",  # Drops are being created
        "Session {0} is now RUNNING",  # All drops created and ready
        "Session {0} finished",  # All drops executed
    ]

    kwords = dict()
    kwords["dim"] = dim_kl
    kwords["name"] = nm_kl

    def __init__(self, log_dir):
        self._dim_log_f = None
        if not self.check_log_dir(log_dir):
            raise Exception("No DIM log found at: {0}".format(log_dir))
        self._log_dir = log_dir
        self._dim_catchall_pattern = construct_catchall_pattern(node_type="dim")
        # self._nm_catchall_pattern = construct_catchall_pattern(node_type="nm")
        self._nm_catchall_pattern = construct_catchall_pattern(node_type="name")

    def parse(self, out_csv=None):
        """
        e.g. lofar_std_N4_2016-08-22T11-52-11
        """
        logb_name = os.path.basename(self._log_dir)
        search_string = re.search("_N[0-9]+_", logb_name)
        if search_string is None:
            raise Exception("Invalid log directory: {0}".format(self._log_dir))
        delimit = search_string.group(0)
        split = logb_name.split(delimit)
        pip_name = split[0]
        do_date = split[1]
        num_nodes = int(delimit.split("_")[1][1:])
        user_name = pwd.getpwuid(os.stat(self._dim_log_f[0]).st_uid).pw_name
        gitf = os.path.join(self._log_dir, "git_commit.txt")
        if os.path.exists(gitf):
            with open(gitf, "r") as git_file:
                git_commit = git_file.readline().strip()
        else:
            git_commit = "None"

        # parse DIM log
        dim_log_pairs = build_dim_log_entry_pairs()
        for lff in self._dim_log_f:
            with open(lff, "r") as dimlog:
                for line in dimlog:
                    matches = self._dim_catchall_pattern.match(line)
                    if not matches:
                        continue
                    for lep in dim_log_pairs:
                        lep.check_start(matches, line)
                        lep.check_end(matches, line)

        num_drops = -1
        temp_dim = []
        num_node_mgrs = 0
        for lep in dim_log_pairs:
            add_dur = True
            if lep.name == "unroll":
                num_drops = lep.other.get("num_drops", -1)
            elif lep.name == "node managers":
                num_node_mgrs = lep.other.get("num_node_mgrs", 0)
                add_dur = False
            elif lep.name == "build drop connections":
                num_edges = lep.other.get("num_edges", -1)
                temp_dim.append(str(num_edges))
            if add_dur:
                temp_dim.append(str(lep.get_duration()))

        # parse NM logs
        nm_logs = []
        max_node_deploy_time = 0
        num_finished_sess = 0

        num_dims = 0
        for log_directory_file_name in os.listdir(self._log_dir):
            # Check this is a dir and contains the NM log
            if not os.path.isdir(os.path.join(self._log_dir, log_directory_file_name)):
                continue
            nm_logf = os.path.join(self._log_dir, log_directory_file_name, "dlgNM.log")
            nm_dim_logf = os.path.join(
                self._log_dir, log_directory_file_name, "dlgDIM.log"
            )
            nm_mm_logf = os.path.join(
                self._log_dir, log_directory_file_name, "dlgMM.log"
            )
            if not os.path.exists(nm_logf):
                if os.path.exists(nm_dim_logf) or os.path.exists(nm_mm_logf):
                    num_dims += 1
                continue

            # Start anew every time
            nm_log_pairs = build_nm_log_entry_pairs()
            nm_logs.append(nm_log_pairs)

            # Read NM log and fill all LogPair objects
            with open(nm_logf, "r") as nmlog:
                for line in nmlog:
                    matches = self._nm_catchall_pattern.match(line)
                    if not matches:
                        continue
                    for lep in nm_log_pairs:
                        lep.check_start(matches, line)
                        lep.check_end(matches, line)

            # Looking for the deployment times and counting for finished sessions
            for lep in nm_log_pairs:
                # Consider only valid durations
                dur = lep.get_duration()
                if dur is None:
                    continue

                if lep.name in ("completion_time", "completion_time_old"):
                    num_finished_sess += 1
                elif lep.name == "node_deploy_time":
                    if dur > max_node_deploy_time:
                        max_node_deploy_time = dur

        theory_num_nm = num_nodes - num_dims
        actual_num_nm = num_node_mgrs or theory_num_nm
        if actual_num_nm != num_finished_sess:
            print(
                "Pipeline %s is not complete: %d != %d."
                % (pip_name, actual_num_nm, num_finished_sess)
            )
            # return
        else:
            failed_nodes = theory_num_nm - actual_num_nm
            num_nodes -= failed_nodes
            if failed_nodes > 0:
                print(
                    "Pipeline %s has %d node managers that failed to start!"
                    % (pip_name, failed_nodes)
                )

        # The DIM waits for all NMs to setup before triggering the first drops.
        # This has the effect that the slowest to setup will make the others
        # idle while already in RUNNING state, effectively increasing their
        # "exec_time". We subtract the maximum deploy time to account for this
        # effect
        max_exec_time = 0
        for log_entry_pairs in nm_logs:
            indexed_leps = {lep.name: lep for lep in log_entry_pairs}
            deploy_time = indexed_leps["node_deploy_time"].get_duration()
            if deploy_time is None:  # since some node managers failed to start
                continue
            exec_time = (
                indexed_leps["completion_time"].get_duration()
                or indexed_leps["completion_time_old"].get_duration()
            )
            if exec_time is None:
                continue
            real_exec_time = exec_time - (max_node_deploy_time - deploy_time)
            if real_exec_time > max_exec_time:
                max_exec_time = real_exec_time

        temp_nm = [str(max_exec_time)]

        ret = [
            user_name,
            socket.gethostname().split("-")[0],
            pip_name,
            do_date,
            num_nodes,
            num_drops,
            git_commit,
        ]
        ret = [str(x) for x in ret]
        num_dims = num_dims if num_dims == 1 else num_dims - 1  # exclude master manager
        add_line = ",".join(ret + temp_dim + temp_nm + [str(int(num_dims))])
        if out_csv is not None:
            with open(out_csv, "a") as out_file:
                out_file.write(add_line)
                out_file.write(os.linesep)
        else:
            print(add_line)

    def check_log_dir(self, log_dir):
        possible_logs = [
            os.path.join(log_dir, "0", "dlgDIM.log"),
            os.path.join(log_dir, "0", "dlgMM.log"),
        ]
        for dim_log_f in possible_logs:
            if os.path.exists(dim_log_f):
                self._dim_log_f = [dim_log_f]
                if dim_log_f == possible_logs[0]:
                    cluster_log = os.path.join(log_dir, "0", "start_dlg_cluster.log")
                    if os.path.exists(cluster_log):
                        self._dim_log_f.append(cluster_log)
                return True
        return False

def process_config(config_file: str):
    """
    Use configparser to process INI file

    Current functionality: 
        - Returns remote environment config (e.g. DLG_ROOT, HOME etc.)

    Future Functionality: 
        - Graph translation parameters
        - Engine parameters

    :returns: dict, config information
    """
    from configparser import ConfigParser, ExtendedInterpolation
    parser = ConfigParser(interpolation=ExtendedInterpolation())
    parser.read(config_file)
    return (dict(parser["ENVIRONMENT"]))

def process_slurm_template(template_file: str):
    template = Path(template_file)
    with template.open('r') as fp:
        return fp.read()

def create_experiment_group(parser: optparse.OptionParser):
    """
    Establish experiment group to separate out experimenatal options

    :param parser: parser that we are updating
    :return: the group for experiments
    """
    group=optparse.OptionGroup(parser, "Experimental Options",
                      "Caution: These are not properly tested and likely to"
                      "be rough around the edges.")

    group.add_option(
        "--config_file",
        dest="config_file",
        type="string",
        action="store",
        help="Use INI configuration file.",
        default=None
    )
    group.add_option(
        "--slurm_template",
        dest="slurm_template",
        type="string",
        action="store",
        help="Use SLURM template file for job submission. WARNING: Using this command will over-write other job-parameters passed here.",
        default=None
    )

    return group

def create_job_group():
    """
    TODO: LIU-424
    """

def create_graph_group():
    """
    TODO: LIU-424
    """

def run(_, args):
    parser = optparse.OptionParser(
        usage="\n%prog --action [submit|analyse] -f <facility> [options]\n\n%prog -h for further help"
    )

    parser.add_option(
        "-a",
        "--action",
        action="store",
        type="choice",
        choices=["submit", "analyse"],
        dest="action",
        help="**submit** job or **analyse** log",
        default=None,
    )
    parser.add_option(
        "-l",
        "--log-root",
        action="store",
        dest="log_root",
        help="The root directory of the log file",
    )
    parser.add_option(
        "-d",
        "--log-dir",
        action="store",
        dest="log_dir",
        help="The directory of the log file for parsing",
    )
    parser.add_option(
        "-L",
        "--logical-graph",
        action="store",
        type="string",
        dest="logical_graph",
        help="The filename of the logical graph to deploy",
        default=None,
    )
    parser.add_option(
        "-A",
        "--algorithm",
        action="store",
        type="string",
        dest="algorithm",
        help="The algorithm to be used for the translation",
        default="metis",
    )
    parser.add_option(
        "-O",
        "--algorithm-parameters",
        action="store",
        type="string",
        dest="algorithm_params",
        help="Parameters for the translation algorithm",
        default=None,
    )
    parser.add_option(
        "-P",
        "--physical-graph",
        action="store",
        type="string",
        dest="physical_graph",
        help="The filename of the physical graph (template) to deploy",
        default=None,
    )
    parser.add_option(
        "-t",
        "--job-dur",
        action="store",
        type="int",
        dest="job_dur",
        help="job duration in minutes",
        default=30,
    )
    parser.add_option(
        # TODO: num_nodes needs to be >= #partitions in PGT, if provided
        "-n",
        "--num_nodes",
        action="store",
        type="int",
        dest="num_nodes",
        help="Number of compute nodes requested",
    )
    parser.add_option(
        "-i",
        "--visualise_graph",
        action="store_true",
        dest="visualise_graph",
        help="Whether to visualise graph (poll status)",
        default=False,
    )
    parser.add_option(
        "-p",
        "--run_proxy",
        action="store_true",
        dest="run_proxy",
        help="Whether to attach proxy server for real-time monitoring",
        default=False,
    )
    parser.add_option(
        "-m",
        "--monitor_host",
        action="store",
        type="string",
        dest="mon_host",
        help="Monitor host IP (optional)",
        default=DEFAULT_MON_HOST,
    )
    parser.add_option(
        "-o",
        "--monitor_port",
        action="store",
        type="int",
        dest="mon_port",
        help="The port to bind DALiuGE monitor",
        default=DEFAULT_MON_PORT,
    )
    parser.add_option(
        "-v",
        "--verbose-level",
        action="store",
        type="int",
        dest="verbose_level",
        help="Verbosity level (1-3) of the DIM/NM logging",
        default=1,
    )
    parser.add_option(
        "-c",
        "--csvoutput",
        action="store",
        dest="csv_output",
        help="CSV output file to keep the log analysis result",
    )
    parser.add_option(
        "-z",
        "--zerorun",
        action="store_true",
        dest="zerorun",
        help="Generate a physical graph that takes no time to run",
        default=False,
    )
    parser.add_option(
        "-y",
        "--sleepncopy",
        action="store_true",
        dest="sleepncopy",
        help="Whether include COPY in the default Component drop",
        default=False,
    )
    parser.add_option(
        "-T",
        "--max-threads",
        action="store",
        type="int",
        dest="max_threads",
        help="Max thread pool size used for executing drops. 0 (default) means no pool.",
        default=0,
    )
    parser.add_option(
        "-s",
        "--num_islands",
        action="store",
        type="int",
        dest="num_islands",
        help="The number of Data Islands",
    )
    parser.add_option(
        "-u",
        "--all_nics",
        action="store_true",
        dest="all_nics",
        help="Listen on all NICs for a node manager",
        default=False,
    )
    parser.add_option(
        "-S",
        "--check_with_session",
        action="store_true",
        dest="check_with_session",
        help="Check for node managers' availability by creating/destroy a session",
        default=False,
    )
    parser.add_option(
        "-f",
        "--facility",
        dest="facility",
        choices=FACILITIES,
        action="store",
        help=f"The facility for which to create a submission job\nValid options: {FACILITIES}",
        default=None,
    )
    parser.add_option(
        "--submit",
        dest="submit",
        action="store_true",
        help="If set to False, the job is not submitted, but the script is generated",
        default=False,
    )
    parser.add_option(
        "--remote",
        dest="remote",
        action="store_true",
        help="If set to True, the job is submitted/created for a remote submission",
        default=False,
    )
    parser.add_option(
        "-D",
        "--dlg_root",
        dest="dlg_root",
        action="store",
        type="string",
        help="Overwrite the DLG_ROOT directory provided by the config",
    )
    parser.add_option(
        "-C",
        "--configs",
        dest="configs",
        action="store_true",
        help="Display the available configurations and exit",
        default=False,
    )

    parser.add_option(
        "-U",
        "--username",
        dest="username",
        type="string",
        action="store",
        help="Remote username, if different from local",
        default=None,
    )

    parser.add_option(
        "--ssh_key",
        action="store",
        help="Path to ssh private key",
        default=None
    )

    parser.add_option_group(create_experiment_group(parser))
    (opts, _) = parser.parse_args(sys.argv)

    cfg_manager = ConfigManager(FACILITIES)

    if opts.configs:
        print(f"Available facilities: {FACILITIES}")
    if not (opts.action and opts.facility):
        parser.error("Missing required parameters!")
        parser.print_help()
    if opts.facility not in FACILITIES:
        parser.error(f"Unknown facility provided. Please choose from {FACILITIES}")

    if opts.action == "analyse":
        if opts.log_dir is None:
            # you can specify:
            # either a single directory
            if opts.log_root is None:
                config = ConfigFactory.create_config(
                    facility=opts.facility, user=opts.username
                )
                log_root = config.getpar("log_root")
            else:
                log_root = opts.log_root
            if log_root is None or (not os.path.exists(log_root)):
                parser.error(
                    "Missing or invalid log directory/facility for log analysis"
                )
            # or a root log directory
            else:
                for log_dir in os.listdir(log_root):
                    log_dir = os.path.join(log_root, log_dir)
                    if os.path.isdir(log_dir):
                        try:
                            log_parser = LogParser(log_dir)
                            log_parser.parse(out_csv=opts.csv_output)
                        except Exception as exp: # pylint: disable=broad-exception-caught
                            print("Fail to parse {0}: {1}".format(log_dir, exp))
        else:
            log_parser = LogParser(opts.log_dir)
            log_parser.parse(out_csv=opts.csv_output)
    elif opts.action == "submit":
        path_to_graph_file = None
        if opts.logical_graph and opts.physical_graph:
            parser.error(
                "Either a logical graph XOR physical graph filename must be specified"
            )
        elif opts.logical_graph:
            path_to_graph_file = opts.logical_graph
        elif opts.physical_graph:
            path_to_graph_file = opts.physical_graph
        if path_to_graph_file and not os.path.exists(path_to_graph_file):
            parser.error(f"Cannot locate graph file at '{path_to_graph_file}'")
        else:
            graph_file = os.path.basename(path_to_graph_file)
            pre, ext = os.path.splitext(graph_file)
            if os.path.splitext(pre)[-1] != ".pre":
                pgt_file = ".".join([pre, "pgt", ext[1:]])
            else:
                pgt_file = graph_file
            if opts.logical_graph:
                with open(path_to_graph_file) as f:
                    # logical graph provided, translate first
                    lg_graph = json.loads(f.read())
                    pgt = pg_generator.unroll(lg_graph, zerorun=opts.zerorun)
                    pgt = init_pgt_unroll_repro_data(pgt)
                    reprodata = pgt.pop()
                    pgt = pg_generator.partition(
                        pgt=pgt,
                        algo=opts.algorithm,
                        algo_params=opts.algorithm_params,
                        num_islands=opts.num_islands,
                        num_partitions=opts.num_nodes,
                    )
                    pgt.append(reprodata)
                    pgt = init_pgt_partition_repro_data(pgt)
                pgt_name = pgt_file
                pgt_file = f"/tmp/{pgt_file}"
                with open(pgt_file, "w") as o:
                    json.dump((pgt_name, pgt), o)
            else:
                pgt_file = path_to_graph_file

        if opts.config_file:
            config_path = cfg_manager.load_user_config(ConfigType.ENV, opts.config_file)
            if not config_path:
                print("Provided --config_file option that does not exist!")
                sys.exit(1)
            config = process_config(config_path) if config_path else None
        else:
            config = None
        if opts.slurm_template:
            template_path = cfg_manager.load_user_config(ConfigType.SLURM, opts.slurm_template)
            if not template_path:
                print("Provided --slurm_template option that does not exist!")
                sys.exit(1)
            template = process_slurm_template(template_path)  if template_path else None
        else:
            template = None

        client = SlurmClient(
            dlg_root=opts.dlg_root,
            facility=opts.facility,
            job_dur=opts.job_dur,
            num_nodes=opts.num_nodes,
            logv=opts.verbose_level,
            zerorun=opts.zerorun,
            max_threads=opts.max_threads,
            run_proxy=opts.run_proxy,
            mon_host=opts.mon_host,
            mon_port=opts.mon_port,
            num_islands=opts.num_islands,
            all_nics=opts.all_nics,
            check_with_session=opts.check_with_session,
            physical_graph_template_file=pgt_file,
            submit=opts.submit,
            remote=opts.remote,
            username=opts.username,
            ssh_key=opts.ssh_key,
            config=config,
            slurm_template=template
        )

        client.visualise_graph = opts.visualise_graph
        client.submit_job()
    else:
        parser.print_help()
        parser.error(f"Invalid input from args: {args}!")


if __name__ == "__main__":
    run(None, sys.argv[1:])
