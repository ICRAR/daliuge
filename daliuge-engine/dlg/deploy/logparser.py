#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2025
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

import datetime
import os
import pwd
import re
import socket
import time

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

    def __init__(self, log_dir):
        self._dim_log_f = None
        if not self.check_log_dir(log_dir):
            raise RuntimeError("No DIM log found at: {0}".format(log_dir))
        self._log_dir = log_dir
        self._dim_catchall_pattern = self._construct_catchall_pattern(node_type="dim")
        # self._nm_catchall_pattern = construct_catchall_pattern(node_type="nm")
        self._nm_catchall_pattern = self._construct_catchall_pattern(node_type="name")

    def _construct_catchall_pattern(self, node_type):
        pattern_strs = dim_kl if node_type == "dim" else nm_kl
        patterns = [
            x.format(".*").replace("(", r"\(").replace(")", r"\)") for x in pattern_strs
        ]
        catchall = "|".join([f"{s}" for s in patterns])
        catchall = f".*{catchall}.*"
        return re.compile(catchall)

    def parse(self, out_csv=None):
        """
        e.g. lofar_std_N4_2016-08-22T11-52-11
        """
        logb_name = os.path.basename(self._log_dir)
        search_string = re.search("_N[0-9]+_", logb_name)
        if search_string is None:
            raise RuntimeError("Invalid log directory: {0}".format(self._log_dir))
        delimit = search_string[0]
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
