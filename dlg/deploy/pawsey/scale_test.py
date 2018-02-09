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
import optparse
import os
import pwd
import re
import socket
import string
import subprocess
import sys
import time

from ... import __git_version__ as git_commit, utils


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

aprun -b -n $NUM_NODES -N 1 $PY_BIN -m dlg.deploy.pawsey.start_dfms_cluster -l $LOG_DIR $GRAPH_PAR $PROXY_PAR $GRAPH_VIS_PAR $LOGV_PAR $ZERORUN_PAR $MAXTHREADS_PAR $SNC_PAR $NUM_ISLANDS_PAR $ALL_NICS $CHECK_WITH_SESSION
"""

sub_tpl = string.Template(sub_tpl_str)

default_aws_mon_host = 'sdp-dfms.ddns.net'
default_aws_mon_port = 8898

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
    def __init__(self):
        super(TianHe2Config, self).__init__()

    def init_list(self): #TODO please fill in
        return ['SHAO', '/group/shao/daliuge_logs']

class ConfigFactory():
    mapping = {'galaxy_mwa':GalaxyMWAConfig, 'galaxy_askap':GalaxyASKAPConfig,
    'magnus':MagnusConfig, 'galaxy':GalaxyASKAPConfig}

    @staticmethod
    def create_config(facility=None):
        facility = facility.lower() if (facility is not None) else facility
        return ConfigFactory.mapping.get(facility)()

class PawseyClient(object):
    """
    parameters we can control:

    1. Pawsey group / account name (Required)
    2. whether to submit a graph, and if so provide graph path
    3. # of nodes (of Drop Managers)
    4. how long to run
    5. whether to produce offline graph vis
    6. whether to attach proxy for remote monitoring, and if so provide
        DLG_MON_HOST
        DLG_MON_PORT
    7. Root directory of the Log files (Required)
    """

    def __init__(self, log_root=None, acc=None,
                 pg=None, lg=None,
                 job_dur=30,
                 num_nodes=5,
                 run_proxy=False,
                 mon_host=default_aws_mon_host,
                 mon_port=default_aws_mon_port,
                 logv=1,
                 facility=socket.gethostname().split('-')[0],
                 zerorun=False,
                 max_threads=0,
                 sleepncopy=False,
                 num_islands=1,
                 all_nics=False,
                 check_with_session=False):
        self._config = ConfigFactory.create_config(facility=facility)
        self._acc = self._config.getpar('acc') if (acc is None) else acc
        self._log_root = self._config.getpar('log_root') if (log_root is None) else log_root
        self._num_nodes = num_nodes
        self._job_dur = job_dur
        self._lg = lg
        self._pg = pg
        self._graph_vis = False
        self._run_proxy = run_proxy
        self._mon_host = mon_host
        self._mon_port = mon_port
        self._pip_name = utils.fname_to_pipname(lg or pg) if lg or pg else 'None'
        self._logv = logv
        self._zerorun = zerorun
        self._max_threads = max_threads
        self._sleepncopy = sleepncopy
        self._num_islands = num_islands
        self._all_nics = all_nics
        self._check_with_session = check_with_session

    @property
    def num_daliuge_nodes(self):
        if (self._run_proxy):
            ret = self._num_nodes - 1 # exclude the proxy node
        else:
            ret = self._num_nodes - 0 # exclude the data island node?
        if (ret <= 0):
            raise Exception("Not enough nodes {0} to run DALiuGE.".format(self._num_nodes))
        return ret

    def get_log_dirname(self):
        """
        (pipeline name_)[Nnum_of_daliuge_nodes]_[time_stamp]
        """
        dtstr = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S") #.%f
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
        pardict['GRAPH_PAR'] = '-L "{0}"'.format(self._lg) if self._lg else '-P "{0}"'.format(self._pg) if self._pg else ''
        pardict['PROXY_PAR'] = '-m %s -o %d' % (self._mon_host, self._mon_port) if self._run_proxy else ''
        pardict['GRAPH_VIS_PAR'] = '-d' if self._graph_vis else ''
        pardict['LOGV_PAR'] = '-v %d' % self._logv
        pardict['ZERORUN_PAR'] = '-z' if self._zerorun else ''
        pardict['MAXTHREADS_PAR'] = '-t %d' % (self._max_threads)
        pardict['SNC_PAR'] = '--app 1' if self._sleepncopy else '--app 0'
        pardict['NUM_ISLANDS_PAR'] = '-s %d' % (self._num_islands)
        pardict['ALL_NICS'] = '-u' if self._all_nics else ''
        pardict['CHECK_WITH_SESSION'] = '-S' if self._check_with_session else ''

        job_desc = sub_tpl.safe_substitute(pardict)
        job_file = '{0}/jobsub.sh'.format(lgdir)
        with open(job_file, 'w') as jf:
            jf.write(job_desc)

        with open(os.path.join(lgdir, 'git_commit.txt'), 'w') as gf:
            gf.write(git_commit)

        os.chdir(lgdir) # so that slurm logs will be dumped here
        print(subprocess.check_output(['sbatch', job_file]))

class LogEntryPair(object):
    """
    """
    def __init__(self, name, gstart, gend):
        self._name = name
        self._gstart = gstart + 2 # group 0 is the whole matching line, group 1 is the catchall
        self._gend = gend + 2
        self._start_time = None
        self._end_time = None
        self._other = dict() # hack

    def get_timestamp(self, line):
        """
        microsecond precision
        """
        sp = line.split()
        date_time = '{0}T{1}'.format(sp[0], sp[1])
        pattern = '%Y-%m-%dT%H:%M:%S,%f'
        epoch = time.mktime(time.strptime(date_time, pattern))
        return datetime.datetime.strptime(date_time, pattern).microsecond / 1e6 + epoch

    def check_start(self, match, line):
        if self._start_time is None and match.group(self._gstart):
            self._start_time = self.get_timestamp(line)

    def check_end(self, match, line):
        if self._end_time is None and match.group(self._gend):
            self._end_time = self.get_timestamp(line)
            if (self._name == 'unroll'):
                self._other['num_drops'] = int(line.split()[-1])
            elif (self._name == 'node managers'):
                self._other['num_node_mgrs'] = int(line.split('Got a node list with')[1].split()[0])
            elif (self._name == 'build drop connections'):
                self._other['num_edges'] = int(line.split()[-4][1:-1])

    def get_duration(self):
        if ((self._start_time is None) or (self._end_time is None)):
            #print "Cannot calc duration for '{0}': start_time:{1}, end_time:{2}".format(self._name,
            #self._start_time, self._end_time)
            return None
        return (self._end_time - self._start_time)

    def reset(self):
        self._start_time = None
        self._end_time = None


class LogParser(object):
    """
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

    dim_kl = ['Start to unroll',
    'Unroll completed for {0} with # of Drops',
    'Start to translate',
    'Translation completed for',
    'PG spec is calculated',
    'Creating Session {0} in all hosts',
    'Successfully created session {0} in all hosts',
    'Separating graph',
    'Removed (and sanitized) {0} inter-dm relationships',
    'Adding individual graphSpec of session {0} to each DM',
    'Successfully added individual graphSpec of session {0} to each DM',
    'Deploying Session {0} in all hosts',
    'Successfully deployed session {0} in all hosts',
    'Establishing {0} drop relationships',
    'Established all drop relationships {0} in',
    'Moving Drops to COMPLETED right away',
    'Successfully triggered drops',
    'Got a node list with {0} node managers']

    nm_kl = [
        'Starting Pyro4 Daemon for session', # Logged by the old master branch
        'Creating DROPs for session',        # Drops are being created
        'Session {0} is now RUNNING',        # All drops created and ready
        'Session {0} finished'               # All drops executed
    ]

    kwords = dict()
    kwords['dim'] = dim_kl
    kwords['nm'] = nm_kl

    def __init__(self, log_dir):
        self._dim_log_f = None
        if (not self.check_log_dir(log_dir)):
            raise Exception("No DIM log found at: {0}".format(log_dir))
        self._log_dir = log_dir
        self._dim_catchall_pattern = self.construct_catchall_pattern(node_type='dim')
        self._nm_catchall_pattern = self.construct_catchall_pattern(node_type='nm')

    def build_dim_log_entry_pairs(self):
        return [LogEntryPair(name, g1, g2) for name, g1, g2 in (
            ('unroll', 0, 1),
            ('translate', 2, 3),
            ('gen pg spec', 3, 4),
            ('create session', 5, 6),
            ('separate graph', 7, 8),
            ('add session to all', 9, 10),
            ('deploy session to all', 11, 12),
            ('build drop connections', 13, 14),
            ('trigger drops', 15, 16),
            ('node managers', 17, 17),
        )]

    def build_nm_log_entry_pairs(self):
        return [LogEntryPair(name, g1, g2) for name, g1, g2 in (
            ('completion_time_old', 0, 3), # Old master branch
            ('completion_time', 2, 3),
            ('node_deploy_time', 1, 2),
        )]

    def construct_catchall_pattern(self, node_type):
        pattern_strs = LogParser.kwords.get(node_type)
        patterns = [x.format('.*').replace('(', r'\(').replace(')', r'\)') for x in pattern_strs]
        catchall = '|'.join(['(%s)' % (s,) for s in patterns])
        catchall = ".*(%s).*" % (catchall,)
        return re.compile(catchall)

    def parse(self, out_csv=None):
        """
        e.g. lofar_std_N4_2016-08-22T11-52-11
        """
        logb_name = os.path.basename(self._log_dir)
        ss = re.search('_N[0-9]+_', logb_name)
        if (ss is None):
            raise Exception("Invalid log directory: {0}".format(self._log_dir))
        delimit = ss.group(0)
        sp = logb_name.split(delimit)
        pip_name = sp[0]
        do_date = sp[1]
        num_nodes = int(delimit.split('_')[1][1:])
        user_name = pwd.getpwuid(os.stat(self._dim_log_f[0]).st_uid).pw_name
        gitf = os.path.join(self._log_dir, 'git_commit.txt')
        if (os.path.exists(gitf)):
            with open(gitf, 'r') as gf:
                git_commit = gf.readline().strip()
        else:
            git_commit = 'None'

        # parse DIM log
        dim_log_pairs = self.build_dim_log_entry_pairs()
        for lff in self._dim_log_f:
            with open(lff, "r") as dimlog:
                for line in dimlog:
                    m = self._dim_catchall_pattern.match(line)
                    if not m:
                        continue
                    for lep in dim_log_pairs:
                        lep.check_start(m, line)
                        lep.check_end(m, line)

        num_drops = -1
        temp_dim = []
        num_node_mgrs = 0
        for lep in dim_log_pairs:
            add_dur = True
            if ('unroll' == lep._name):
                num_drops = lep._other.get('num_drops', -1)
            elif ('node managers' == lep._name):
                num_node_mgrs = lep._other.get('num_node_mgrs', 0)
                add_dur = False
            elif ('build drop connections' == lep._name):
                num_edges = lep._other.get('num_edges', -1)
                temp_dim.append(str(num_edges))
            if (add_dur):
                temp_dim.append(str(lep.get_duration()))

        # parse NM logs
        nm_logs = []
        max_node_deploy_time = 0
        num_finished_sess = 0

        num_dims = 0
        for df in os.listdir(self._log_dir):

            # Check this is a dir and contains the NM log
            if not os.path.isdir(os.path.join(self._log_dir, df)):
                continue
            nm_logf = os.path.join(self._log_dir, df, 'dlgNM.log')
            nm_dim_logf = os.path.join(self._log_dir, df, 'dlgDIM.log')
            nm_mm_logf = os.path.join(self._log_dir, df, 'dlgMM.log')
            if not os.path.exists(nm_logf):
                if (os.path.exists(nm_dim_logf) or os.path.exists(nm_mm_logf)):
                    num_dims += 1
                continue

            # Start anew every time
            nm_log_pairs = self.build_nm_log_entry_pairs()
            nm_logs.append(nm_log_pairs)

            # Read NM log and fill all LogPair objects
            with open(nm_logf, "r") as nmlog:
                for line in nmlog:
                    m = self._nm_catchall_pattern.match(line)
                    if not m:
                        continue
                    for lep in nm_log_pairs:
                        lep.check_start(m, line)
                        lep.check_end(m, line)

            # Looking for the deployment times and counting for finished sessions
            for lep in nm_log_pairs:

                # Consider only valid durations
                dur = lep.get_duration()
                if dur is None:
                    continue

                if lep._name in ('completion_time', 'completion_time_old'):
                    num_finished_sess += 1
                elif lep._name == 'node_deploy_time':
                    if dur > max_node_deploy_time:
                        max_node_deploy_time = dur

        theory_num_nm = num_nodes - num_dims
        actual_num_nm = num_node_mgrs or theory_num_nm
        if actual_num_nm != num_finished_sess:
            print("Pipeline %s is not complete: %d != %d." % (pip_name, actual_num_nm, num_finished_sess))
            return
        else:
            failed_nodes = theory_num_nm - actual_num_nm
            num_nodes -= failed_nodes
            if (failed_nodes > 0):
                print("Pipeline %s has %d node managers that failed to start!" % (pip_name, failed_nodes))

        # The DIM waits for all NMs to setup before triggering the first drops.
        # This has the effect that the slowest to setup will make the others
        # idle while already in RUNNING state, effectively increasing their
        # "exec_time". We subtract the maximum deploy time to account for this
        # effect
        max_exec_time = 0
        for log_entry_pairs in nm_logs:

            indexed_leps = {lep._name: lep for lep in log_entry_pairs}
            deploy_time = indexed_leps['node_deploy_time'].get_duration()
            if (deploy_time is None): # since some node managers failed to start
                continue
            exec_time = indexed_leps['completion_time'].get_duration() or indexed_leps['completion_time_old'].get_duration()
            real_exec_time = exec_time - (max_node_deploy_time - deploy_time)
            if real_exec_time > max_exec_time:
                max_exec_time = real_exec_time

        temp_nm = [str(max_exec_time)]

        ret = [user_name, socket.gethostname().split('-')[0], pip_name, do_date,
        num_nodes, num_drops, git_commit]
        ret = [str(x) for x in ret]
        num_dims = num_dims if num_dims == 1 else num_dims - 1 #exclude master manager
        add_line = ','.join(ret + temp_dim + temp_nm + [str(int(num_dims))])
        if (out_csv is not None):
            with open(out_csv, 'a') as of:
                of.write(add_line)
                of.write(os.linesep)
        else:
            print(add_line)

    def check_log_dir(self, log_dir):
        possible_logs = [
        os.path.join(log_dir, '0', 'dlgDIM.log'),
        os.path.join(log_dir, '0', 'dlgMM.log')
        ]
        for dim_log_f in possible_logs:
            if (os.path.exists(dim_log_f)):
                self._dim_log_f = [dim_log_f]
                if (dim_log_f == possible_logs[0]):
                    cluster_log = os.path.join(log_dir, '0', 'start_dlg_cluster.log')
                    if (os.path.exists(cluster_log)):
                        self._dim_log_f.append(cluster_log)
                return True
        return False

if __name__ == '__main__':
    parser = optparse.OptionParser()

    parser.add_option("-a", "--action", action="store", type="int",
                      dest="action", help="1 - submit job, 2 - analyse log", default=1)
    parser.add_option("-l", "--log-root", action="store",
                      dest="log_root", help="The root directory of the log file")
    parser.add_option("-d", "--log-dir", action="store",
                      dest="log_dir", help="The directory of the log file for parsing")
    parser.add_option("-L", "--logical-graph", action="store", type="string",
                      dest="logical_graph", help="The filename of the logical graph to deploy", default=None)
    parser.add_option("-P", "--physical-graph", action="store", type="string",
                      dest="physical_graph", help="The filename of the physical graph (template) to deploy", default=None)
    parser.add_option("-t", "--job-dur", action="store", type="int",
                      dest="job_dur", help="job duration in minutes", default=30)
    parser.add_option("-n", "--num_nodes", action="store", type="int",
                      dest="num_nodes", help="number of compute nodes requested", default=5)
    parser.add_option('-i', '--graph_vis', action='store_true',
                    dest='graph_vis', help='Whether to visualise graph (poll status)', default=False)
    parser.add_option('-p', '--run_proxy', action='store_true',
                    dest='run_proxy', help='Whether to attach proxy server for real-time monitoring', default=False)
    parser.add_option("-m", "--monitor_host", action="store", type="string",
                    dest="mon_host", help="Monitor host IP (optional)", default=default_aws_mon_host)
    parser.add_option("-o", "--monitor_port", action="store", type="int",
                    dest="mon_port", help="The port to bind DALiuGE monitor", default=default_aws_mon_port)
    parser.add_option("-v", "--verbose-level", action="store", type="int",
                    dest="verbose_level", help="Verbosity level (1-3) of the DIM/NM logging",
                    default=1)
    parser.add_option("-c", "--csvoutput", action="store",
                      dest="csv_output", help="CSV output file to keep the log analysis result")
    parser.add_option("-z", "--zerorun", action="store_true",
                      dest="zerorun", help="Generate a physical graph that takes no time to run", default=False)
    parser.add_option("-y", "--sleepncopy", action="store_true",
                      dest="sleepncopy", help="Whether include COPY in the default Component drop", default=False)
    parser.add_option("-T", "--max-threads", action="store", type="int",
                      dest="max_threads", help="Max thread pool size used for executing drops. 0 (default) means no pool.", default=0)
    parser.add_option('-s', '--num_islands', action='store', type='int',
                    dest='num_islands', default=1, help='The number of Data Islands')
    parser.add_option("-u", "--all_nics", action="store_true",
                      dest="all_nics", help="Listen on all NICs for a node manager", default=False)
    parser.add_option("-S", "--check_with_session", action="store_true",
                      dest="check_with_session", help="Check for node managers' availability by creating/destroy a session", default=False)

    (opts, args) = parser.parse_args(sys.argv)
    if (opts.action == 2):
        if (opts.log_dir is None):
            # you can specify:
            # either a single directory
            if (opts.log_root is None):
                facility = socket.gethostname().split('-')[0]
                config = ConfigFactory.create_config(facility=facility)
                log_root = config.getpar('log_root')
            else:
                log_root = opts.log_root
            if (log_root is None or (not os.path.exists(log_root))):
                parser.error("Missing or invalid log directory/facility for log analysis")
            # or a root log directory
            else:
                for df in os.listdir(log_root):
                    df = os.path.join(log_root, df)
                    if (os.path.isdir(df)):
                        try:
                            lg = LogParser(df)
                            lg.parse(out_csv=opts.csv_output)
                        except Exception as exp:
                            print("Fail to parse {0}: {1}".format(df, exp))
        else:
            lg = LogParser(opts.log_dir)
            lg.parse(out_csv=opts.csv_output)
    elif (opts.action == 1):

        if opts.logical_graph and opts.physical_graph:
            parser.error("Either a logical graph or physical graph filename must be specified")
        for p in (opts.logical_graph, opts.physical_graph):
            if p and not os.path.exists(p):
                parser.error("Cannot locate graph file at '{0}'".format(p))

        pc = PawseyClient(job_dur=opts.job_dur, num_nodes=opts.num_nodes, logv=opts.verbose_level,
                          zerorun=opts.zerorun, max_threads=opts.max_threads,
                          run_proxy=opts.run_proxy, mon_host=opts.mon_host, mon_port=opts.mon_port,
                          num_islands=opts.num_islands, all_nics=opts.all_nics,
                          check_with_session=opts.check_with_session,
                          lg=opts.logical_graph, pg=opts.physical_graph)
        pc._graph_vis = opts.graph_vis
        pc.submit_job()
    else:
        parser.error("Invalid action -a")
