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
import functools
import json
import logging
import optparse
import os
import subprocess
import sys
import time

from dfms import droputils
from dfms.deploy.pawsey import dfms_proxy
from dfms.dropmake.pg_generator import LG, MySarkarPGTP, MetisPGTP
from dfms.exceptions import DaliugeException
from dfms.manager import constants, proc_daemon, cmdline
from dfms.manager.client import CompositeManagerClient


logger = logging.getLogger(__name__)

def _open_i(path, flags=None):
    if path == '-':
        return sys.stdin
    return open(os.path.expanduser(path), flags or 'r')

def _open_o(path, flags=None):
    if path == '-':
        return sys.stdout
    return open(os.path.expanduser(path), flags or 'w')

def _fname_to_pipname(fname):
    fname = fname.split('/')[-1]
    if fname.endswith('.json'):
        fname = fname[:-5]
    return fname

def unroll(lg_path, oid_prefix, zerorun=False, app=None):
    '''
    Unrolls the Logical Graph in `lg_graph` into a Physical Graph Template
    and return the latter.
    This method prepends `oid_prefix` to all generated Drop OIDs.
    '''
    lg = LG(_open_i(lg_path), ssid=oid_prefix)
    logger.info("Start to unroll %s", lg_path)
    drop_list = lg.unroll_to_tpl()
    logger.info("Unroll completed for %s with # of Drops: %d", lg_path, len(drop_list))

    # Optionally set sleepTimes to 0 and apps to a specific type
    if zerorun:
        for dropspec in drop_list:
            if 'sleepTime' in dropspec:
                dropspec['sleepTime'] = 0
    if app:
        for dropspec in drop_list:
            if 'app' in dropspec:
                dropspec['app'] = app

    return drop_list

def partition(pgt, pip_name, num_partitions, num_islands, algo='metis'):
    '''
    Partitions the Physical Graph Template `pgt` with the algorithm `algo`
    using `num_partitions` partitions.
    '''

    logger.info("Initialising PGTP %s", algo)
    if algo == 'sarkar':
        pgtp = MySarkarPGTP(pgt, num_partitions, merge_parts=True)
    else:
        pgtp = MetisPGTP(pgt, num_partitions)
    del pgt
    logger.info("PGTP initialised %s", algo)

    logger.info("Starting to partition %s", pip_name)
    pgtp.to_gojs_json(string_rep=False, visual=True)
    pgt = pgtp.to_pg_spec([], ret_str=False,
                          num_islands=num_islands,
                          tpl_nodes_len=num_partitions+num_islands)
    logger.info("Partitioning completed for %s", pip_name)

    return pgt

def submit(host, port, pg,
           skip_deploy=False, session_id=None, completed_uids=None):

    session_id = session_id or "%f" % (time.time())
    completed_uids = completed_uids or droputils.get_roots(pg)

    with CompositeManagerClient(host, port, timeout=10) as client:
        client.create_session(session_id)
        logger.info("Session %s created", session_id)
        client.append_graph(session_id, pg)
        logger.info("Graph for session %s appended", session_id)
        if not skip_deploy:
            client.deploy_session(session_id, completed_uids=completed_uids)
            logger.info("Session %s deployed", session_id)

def _add_logging_options(parser):
    parser.add_option("-v", "--verbose", action="count",
                      dest="verbose", help="Become more verbose. The more flags, the more verbose")
    parser.add_option("-q", "--quiet", action="count",
                      dest="quiet", help="Be less verbose. The more flags, the quieter")

def _setup_logging(opts):

    levels = [
        logging.NOTSET,
        logging.DEBUG,
        logging.INFO,
        logging.WARNING,
        logging.ERROR,
        logging.CRITICAL
    ]

    # Default is WARNING
    lidx = 3
    if opts.verbose:
        lidx -= min((opts.verbose, 3))
    elif opts.quiet:
        lidx += min((opts.quiet, 2))
    level = levels[lidx]

    # Let's configure logging now
    # We use stderr for loggin because stdout is the default output file
    # for several operations
    fmt = logging.Formatter("%(asctime)-15s [%(levelname)5.5s] [%(threadName)15.15s] %(name)s#%(funcName)s:%(lineno)s %(message)s")
    fmt.converter = time.gmtime
    streamHdlr = logging.StreamHandler(sys.stderr)
    streamHdlr.setFormatter(fmt)
    logging.root.addHandler(streamHdlr)
    logging.root.setLevel(level)




commands = {}
def cmdwrap(cmdname, desc):
    def decorated(f):
        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            parser = optparse.OptionParser(description=desc)
            f(parser, *args, **kwargs)
        commands[cmdname] = (desc, wrapped)
        return wrapped
    return decorated

# Commands existing in other modules
cmdwrap('nm', 'Starts a Node Manager')(cmdline.dlgNM)
cmdwrap('dim', 'Starts a Drop Island Manager')(cmdline.dlgDIM)
cmdwrap('mm', 'Starts a Master Manager')(cmdline.dlgMM)
cmdwrap('replay', 'Starts a Replay Manager')(cmdline.dlgReplay)
cmdwrap('daemon', 'Starts a DALiuGE Daemon process')(proc_daemon.run_with_cmdline)
cmdwrap('proxy', 'A reverse proxy to be used in restricted environments to contact the Drop Managers')(dfms_proxy.run)

def _add_unroll_options(parser):
    parser.add_option('-L', '--logical-graph', action="store", dest='lg_path', type="string",
                      help='Path to the Logical Graph (default: stdin)', default='-')
    parser.add_option('-p', '--oid-prefix', action="store", dest='oid_prefix', type="string",
                      help='Prefix to use for generated OIDs', default='1')
    parser.add_option("-z", "--zerorun", action="store_true",
                      dest="zerorun", help="Generate a Physical Graph Template that takes no time to run", default=False)
    parser.add_option("--app", action="store", type="int",
                      dest="app", help="Force an app to be used in the Physical Graph. 0=SleepApp, 1=SleepAndCopy", default=0)

@cmdwrap('unroll', 'Unrolls a Logical Graph into a Physical Graph Template')
def dlg_unroll(parser, args):

    # Unroll Logical Graph
    _add_logging_options(parser)
    _add_unroll_options(parser)
    parser.add_option('-o', '--output', action="store", dest='output', type="string",
                      help='Where the Physical Graph Template should be written to (default: stdout)', default='-')
    (opts, args) = parser.parse_args(args)
    _setup_logging(opts)

    apps = (
        "test.graphsRepository.SleepApp",
        "test.graphsRepository.SleepAndCopyApp",
    )
    with _open_o(opts.output) as f:
        json.dump(unroll(opts.lg_path, opts.oid_prefix, zerorun=opts.zerorun, app=apps[opts.app]), f)

def _add_partition_options(parser):
    parser.add_option("-N", "--partitions", action="store", type="int",
                      dest="partitions", help="Number of partitions to generate", default=1)
    parser.add_option("-i", "--islands", action="store", type="int",
                      dest="islands", help="Number of islands to use during the partitioning", default=1)
    parser.add_option("-A", "--algorithm", action="store", type="choice", choices=['metis', 'sarkar'],
                      dest="algo", help="algorithm used to do the partitioning", default="metis")

@cmdwrap('partition', 'Divides a Physical Graph Template into N logical partitions')
def dlg_partition(parser, args):

    _add_logging_options(parser)
    _add_partition_options(parser)
    parser.add_option('-P', '--physical-graph-template', action='store', dest='pgt_path', type='string',
                      help='Path to the Physical Graph Template (default: stdin)', default='-')
    parser.add_option('-o', '--output', action="store", dest='output', type="string",
                      help='Where the partitioned Physical Graph Template should be written to (default: stdout)', default='-')
    (opts, args) = parser.parse_args(args)
    _setup_logging(opts)

    pip_name = _fname_to_pipname(opts.pgt_path)
    with _open_i(opts.pgt_path) as fi:
        pgt = json.load(fi)
    with _open_o(opts.output) as fo:
        json.dump(partition(pgt, pip_name, opts.partitions, opts.islands, opts.algo), fo)

@cmdwrap('unroll-and-partition', 'unroll + partition')
def dlg_unroll_and_partition(parser, args):

    _add_logging_options(parser)
    _add_unroll_options(parser)
    _add_partition_options(parser)
    parser.add_option('-o', '--output', action="store", dest='output', type="string",
                      help='Where the partitioned Physical Graph Template should be written to (default: stdout)', default='-')
    (opts, args) = parser.parse_args(args)
    _setup_logging(opts)

    apps = (
        "test.graphsRepository.SleepApp",
        "test.graphsRepository.SleepAndCopyApp",
    )
    with _open_o(opts.output) as f:
        pip_name = _fname_to_pipname(opts.lg_path)
        pgt = unroll(opts.lg_path, opts.oid_prefix, zerorun=opts.zerorun, app=apps[opts.app])
        json.dump(partition(pgt, pip_name, opts.partitions, opts.islands, opts.algo), f)


@cmdwrap('map', 'Translates a Logical Graph or Physical Graph Template into a Physical Graph')
def dlg_map(parser, args):

    parser.add_option('-H', '--host', action='store',
                      dest='host', help='The host we connect to to deploy the graph', default='localhost')
    parser.add_option("-p", "--port", action="store", type="int",
                      dest='port', help='The port we connect to to deploy the graph', default=constants.ISLAND_DEFAULT_REST_PORT)
    parser.add_option('-P', '--physical-graph-template', action='store', dest='pgt_path', type='string',
                      help='Path to the Physical Graph to submit (default: stdin)', default='-')
    parser.add_option('-o', '--output', action="store", dest='output', type="string",
                      help='Where the Physical Graph should be written to (default: stdout)', default='-')
    parser.add_option("-N", "--nodes", action="store",
                      dest="nodes", help="The nodes where the Physical Graph will be distributed, comma-separated", default=None)
    parser.add_option("-i", "--islands", action="store", type="int",
                      dest="islands", help="Number of drop islands", default=1)
    parser.add_option("-A", "--algorithm", action="store", type="choice", choices=['metis', 'sarkar'],
                      dest="algo", help="algorithm used to do the LG --> PG conversion", default="metis")
    parser.add_option("-z", "--zerorun", action="store_true",
                      dest="zerorun", help="Generate a Physical Graph that takes no time to run", default=False)
    parser.add_option("--app", action="store", type="int",
                      dest="app", help="Force an app to be used in the Physical Graph. 0=SleepApp, 1=SleepAndCopy", default=None)
    (opts, args) = parser.parse_args(args)

    if opts.nodes:
        node_list = [n for n in opts.nodes.split(',') if n]
    else:
        client = CompositeManagerClient(opts.host, opts.port, timeout=10)
        node_list = client.nodes()

    n_nodes = len(node_list)
    if n_nodes <= opts.islands:
        raise Exception("#nodes (%d) should be bigger than number of islands (%d)" % (n_nodes, opts.islands))
    lnl = n_nodes - opts.islands

    with _open_i(opts.pgt_path) as f:
        drop_list = json.load(f)
    pip_name = _fname_to_pipname(opts.pgt_path)



@cmdwrap('submit', 'Submits a Physical Graph to a Drop Manager')
def dlg_submit(parser, args):

    # Submit Physical Graph
    parser.add_option('-H', '--host', action='store',
                      dest='host', help='The host we connect to to deploy the graph', default='localhost')
    parser.add_option("-p", "--port", action="store", type="int",
                      dest='port', help='The port we connect to to deploy the graph', default=constants.ISLAND_DEFAULT_REST_PORT)
    parser.add_option('-P', '--physical-graph', action='store', dest='pg_path', type='string',
                      help='Path to the Physical Graph to submit (default: stdin)', default='-')
    parser.add_option('-s', '--session-id', action='store', dest='session_id', type='string',
                      help='Session ID (default: <pg_name>-<current-time>)', default=None)
    parser.add_option('-S', '--skip-deploy', action='store_true', dest='skip_deploy',
                      help='Skip the deployment step (default: False)', default=False)
    (opts, args) = parser.parse_args(args)

    with _open_i(opts.pg_path) as f:
        submit(opts.host, opts.port, json.load(f),
               skip_deploy=opts.skip_deploy, session_id=opts.session_id)


def print_usage(prgname):
    print('Usage: %s [command] [options]' % (prgname))
    print('')
    print('\n'.join(['Commands are:'] + ['\t%-25.25s%s' % (cmdname,desc_and_f[0]) for cmdname,desc_and_f in sorted(commands.items())]))
    print('')
    print('Try %s [command] --help for more details' % (prgname))

def run(args=sys.argv):

    # Manually parse the first argument, which will be
    # either -h/--help or a dlg command
    # In the future we should probably use the argparse module
    prgname = sys.argv[0]
    if len(sys.argv) == 1:
        print_usage(prgname)
        sys.exit(1)

    cmd = sys.argv[1]
    sys.argv.pop(0)

    if cmd in ['-h', '--help', 'help']:
        print_usage(prgname)
        sys.exit(0)

    if cmd not in commands:
        print("Unknown command: %s" % (cmd,))
        print_usage(prgname)
        sys.exit(1)

    commands[cmd][1](sys.argv[1:])

def start_process(cmd, args, **subproc_args):
    """
    Start 'dlg cmd <args>' in a different process.
    If `cmd` is not a known command an exception is raised.
    `subproc_args` are passed down to the process creation via `Popen`.

    This method returns the new process.
    """
    if cmd not in commands:
        raise DaliugeException("Unknown command: %s" % (cmd,))

    cmdline = [sys.executable, '-m', __name__, cmd] + args
    return subprocess.Popen(cmdline, **subproc_args)

# We can also be executed as a module
if __name__ == '__main__':
    run()