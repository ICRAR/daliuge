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
import sys, os, commands, socket, re, commands, time
from string import Template
import optparse
from os import stat
from pwd import getpwuid


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
    pass

class ConfigFactory():
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
                num_nodes=5,
                mon_host=default_aws_mon_host,
                mon_port=default_aws_mon_port,
                facility=socket.gethostname().split('-')[0]):
        self._config = ConfigFactory.create_config(facility=facility)
        self._acc = self._config.getpar('acc') if (acc is None) else acc
        self._log_root = self._config.getpar('log_root') if (log_root is None) else log_root
        self._num_nodes = num_nodes
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
            ret = self._num_nodes - 2 # exclude the proxy node
        else:
            ret = self._num_nodes - 1 # exclude the data island node
        if (ret <= 0):
            raise Exception("Not enough nodes {0} to run DALiuGE.".format(self._num_nodes))
        return ret

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

class LogEntryPair(object):
    """
    """
    def __init__(self, name, start, end):
        self._name = name
        self._start = start
        self._end = end
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
        return datetime.strptime(date_time, pattern).microsecond / 1e6 + epoch

    def check_start(self, line):
        if ((self._start_time is None) and (re.search(self._start, line) is not None)):
            self._start_time = self.get_timestamp(line)

    def check_end(self, line):
        if ((self._end_time is None) and (re.search(self._end, line) is not None)):
            self._end_time = self.get_timestamp(line)
            if (self._name == 'unroll'):
                self._other['num_drops'] = int(line.split()[-1])

    # def sd(self):
    #     """
    #     start done?
    #     """
    #     return self._start_time is not None
    #
    # def ed(self):
    #     """
    #     end done?
    #     """
    #     return self._end_time is not None

    def get_duration(self):
        if ((self._start_time is None) or (self._end_time is None)):
            print "Cannot calc duration for '{0}' due to lack of start/end time".format(self._name)
            return None
        return (self._end_time - self._start_time)


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
    6.  unroll_time
    7.  translation_time
    8.  pg_spec_gen_time
    9.  created_session_at_all_nodes_time
    10. graph_separation_time
    11. push_sub_graphs_to_all_nodes_time
    12. created_drops_at_all_nodes_time
    13. created_pyro_conn_at_all_nodes_time
    14. triggered_drops_at_all_nodes_time

    Detailed description of each field:
    https://confluence.icrar.uwa.edu.au/display/DALIUGE/Scalability+test#Scalabilitytest-Datacollection
    """

    def __init__(self, log_dir):
        self._dim_log_f = None
        if (not self.check_log_dir(log_dir)):
            raise Exception("No DIM log found at: {0}".format(log_dir))
        self._log_dir = log_dir
        self._grep_cmd, self._py_pattern = self.construct_patterns()
        self._entry_pairs = self.build_log_entry_pairs()

    def build_log_entry_pairs(self):
        pp = self._py_pattern
        rl = []
        rl.append(LogEntryPair('unroll', pp[0], pp[1]))
        rl.append(LogEntryPair('translate', pp[2], pp[3]))
        rl.append(LogEntryPair('gen pg spec', pp[3], pp[4]))
        rl.append(LogEntryPair('create session', pp[5], pp[6]))
        rl.append(LogEntryPair('separate graph', pp[7], pp[8]))
        rl.append(LogEntryPair('add session to all', pp[9], pp[10]))
        rl.append(LogEntryPair('deploy session to all', pp[11], pp[12]))
        rl.append(LogEntryPair('build drop connections', pp[13], pp[14]))
        rl.append(LogEntryPair('trigger drops', pp[15], pp[16]))
        return rl

    def construct_patterns(self):
        key_line = ['Start to unroll',
        'Unroll completed for {0} with # of Drops',
        'Start to translate',
        'Translation completed for',
        'PG spec is calculated',
        'Creating Session {0} in all hosts',
        'Successfully created session {0} in all hosts',
        'Separating graph with {0} dropSpecs',
        'Removed (and sanitized) {0} inter-dm relationships',
        'Adding individual graphSpec of session {0} to each DM',
        'Successfully added individual graphSpec of session {0} to each DM',
        'Deploying Session {0} in all hosts',
        'Successfully deployed session {0} in all hosts',
        'Establishing {0} drop relationships',
        'Established all drop relationships {0} in',
        'Moving following DROPs to COMPLETED right away',
        'Successfully triggered drops']
        wildcards = '.*'
        grep_start = '"\\<'
        grep_end = '\\>"'
        grep_keys = [grep_start + x.format(wildcards) + grep_end for x in key_line]
        grep_cmd = ' -e '.join(grep_keys)
        python_keys = [x.format(wildcards) for x in key_line]
        return ("grep -e %s" % grep_cmd, python_keys)

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
        user_name = getpwuid(stat(self._dim_log_f).st_uid).pw_name

        cmd = "%s %s" % (self._grep_cmd, self._dim_log_f)
        ret, lines = commands.getstatusoutput(cmd)
        if (0 != ret):
            raise Exception("Fail to run: %s" % (cmd))
        ll = lines.split(os.linesep)
        for line in ll:
            for lep in self._entry_pairs:
                lep.check_start(line)
                lep.check_end(line)

        num_drops = -1
        temp = []
        for lep in self._entry_pairs:
            if ('unroll' == lep._name):
                num_drops = lep._other.get('num_drops', -1)
            temp.append(str(lep.get_duration()))

        ret = []
        def add_to_re(item):
            ret.append(str(item))

        add_to_re(user_name)
        add_to_re(socket.gethostname().split('-')[0])
        add_to_re(pip_name)
        add_to_re(do_date)
        add_to_re(num_nodes)
        add_to_re(num_drops)
        add_line = ','.join(ret + temp)
        if (out_csv is not None):
            with open(out_csv, 'a') as of:
                of.write(add_line)
                of.write(os.linesep)
        else:
            print add_line

    def check_log_dir(self, log_dir):
        dim_log_f = os.path.join(log_dir, '0', 'dfmsDIM.log')
        if (os.path.exists(dim_log_f)):
            self._dim_log_f = dim_log_f
            return True
        else:
            return False

if __name__ == '__main__':
    parser = optparse.OptionParser()

    parser.add_option("-a", "--action", action="store", type="int",
                      dest="action", help="1 - submit job, 2 - analyse log", default=1)
    parser.add_option("-l", "--log-root", action="store",
                      dest="log_root", help="The root directory of the log file")
    parser.add_option("-d", "--log-dir", action="store",
                      dest="log_dir", help="The directory of the log file for parsing")
    parser.add_option("-g", "--graph-id", action="store", type="int",
                      dest="graph_id", help="The graph to deploy (0 - %d)" % (len(lgnames) - 1), default=None)
    parser.add_option("-t", "--job-dur", action="store", type="int",
                      dest="job_dur", help="job duration in minutes", default=30)
    parser.add_option("-n", "--num_nodes", action="store", type="int",
                      dest="num_nodes", help="number of compute nodes requested", default=5)
    parser.add_option('-i', '--graph_vis', action='store_true',
                    dest='graph_vis', help='Whether to visualise graph (poll status)', default=False)
    parser.add_option('-p', '--use_proxy', action='store_true',
                    dest='use_proxy', help='Whether to attach proxy server for real-time monitoring', default=False)
    parser.add_option("-m", "--monitor_host", action="store", type="string",
                    dest="monitor_host", help="Monitor host IP (optional)")
    parser.add_option("-o", "--monitor_port", action="store", type="int",
                    dest="monitor_port", help="The port to bind dfms monitor",
                    default=default_aws_mon_port)

    (opts, args) = parser.parse_args(sys.argv)
    if (opts.action == 2):
        if (opts.log_dir is None):
            print("Missing log dir for log analysis")
            sys.exit(1)
        else:
            lg = LogParser(opts.log_dir)
            lg.parse()
    elif (opts.action == 1):
        pc = PawseyClient(job_dur=opts.job_dur, num_nodes=opts.num_nodes)
        if (opts.graph_id is not None):
            pc.set_gid(opts.graph_id)
        pc._graph_vis = opts.graph_vis
        pc._run_proxy = opts.use_proxy
        if (opts.use_proxy):
            if (opts.monitor_host is None):
                print("Use default proxy host '%s'" % default_aws_mon_host)
            else:
                self._mon_host = opts.monitor_host
            self._mon_port = opts.monitor_port
        elif (opts.monitor_host is not None):
            print("Please enable proxy by switch on the '-p' option")
            sys.exit(1)
        pc.submit_job()
    else:
        print("Invalid action")
