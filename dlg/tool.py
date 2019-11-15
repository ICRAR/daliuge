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
import importlib
import json
import logging
import optparse
import os
import subprocess
import sys
import time


logger = logging.getLogger(__name__)


def _open_i(path, flags=None):
    if path == '-':
        return sys.stdin
    return open(os.path.expanduser(path), flags or 'r')


def _open_o(path, flags=None):
    if path == '-':
        return sys.stdout
    return open(os.path.expanduser(path), flags or 'w')


def unroll(lg_path, oid_prefix, zerorun=False, app=None):
    """
    Unrolls the Logical Graph in `lg_graph` into a Physical Graph Template
    and return the latter.
    This method prepends `oid_prefix` to all generated Drop OIDs.
    """
    from .dropmake.pg_generator import unroll
    logger.info("Start to unroll %s", lg_path)
    return unroll(_open_i(lg_path), oid_prefix=oid_prefix, zerorun=zerorun, app=app)


_param_types = {'min_goal': int, 'ptype': int, 'max_load_imb': int,
               'max_cpu': int, 'time_greedy': float, 'deadline': int,
               'topk': int, 'swarm_size': int, 'max_mem': int}


def parse_partition_algo_params(algo_params):
    # Double-check that parameters are of shape name=value
    for p in algo_params:
        if len(list(filter(None, p.split('=')))) != 2:
            raise optparse.OptionValueError('Algorithm parameter has no form of name=value: %s' % (p,))
    # Extract algorithm parameters and convert to proper type
    return {n: _param_types[n](v)
            for n, v in map(lambda p: p.split('='), algo_params)
            if n in _param_types}


def partition(pgt, opts):

    from .dropmake import pg_generator

    algo_params = parse_partition_algo_params(opts.algo_params or [])
    pg = pg_generator.partition(pgt, algo=opts.algo, num_partitions=opts.partitions,
                                 num_islands=opts.islands, partition_label='partition',
                                 **algo_params)
    logger.info("PG spec is calculated!")
    return pg


def submit(pg, opts):
    from .deploy import common
    session_id = common.submit(pg, host=opts.host, port=opts.port,
                               skip_deploy=opts.skip_deploy,
                               session_id=opts.session_id)
    if opts.wait:
        common.monitor_sessions(session_id, host=opts.host, port=opts.port,
                                poll_interval=opts.poll_interval)


def _add_logging_options(parser):
    parser.add_option("-v", "--verbose", action="count",
                      dest="verbose", help="Become more verbose. The more flags, the more verbose")
    parser.add_option("-q", "--quiet", action="count",
                      dest="quiet", help="Be less verbose. The more flags, the quieter")


def _add_output_options(parser):
    parser.add_option('-o', '--output', action="store", dest='output', type="string",
                      help='Where the output should be written to (default: stdout)', default='-')
    parser.add_option('-f', '--format', action="store_true",
                      dest='format', help="Format JSON output (newline, 2-space indent)")


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


def _setup_output(opts):
    def dump(obj):
        with _open_o(opts.output) as f:
            json.dump(obj, f, indent=None if opts.format is None else 2)
    return dump


commands = {}


def cmdwrap(cmdname, desc):
    def decorated(f):

        # If it's not a callable we assume it's a string
        # in which case we lazy-load the module:function when it gets called
        if not callable(f):
            orig_f = f
            class importer(object):
                def __call__(self, *args, **kwargs):
                    modname, fname = orig_f.split(':')
                    module = importlib.import_module(modname)
                    return getattr(module, fname)(*args, **kwargs)
            f = importer()

        def wrapped(*args, **kwargs):
            parser = optparse.OptionParser(description=desc)
            f(parser, *args, **kwargs)
        commands[cmdname] = (desc, wrapped)
        return wrapped
    return decorated

# Commands existing in other modules
cmdwrap('nm', 'Starts a Node Manager')('dlg.manager.cmdline:dlgNM')
cmdwrap('dim', 'Starts a Drop Island Manager')('dlg.manager.cmdline:dlgDIM')
cmdwrap('mm', 'Starts a Master Manager')('dlg.manager.cmdline:dlgMM')
cmdwrap('replay', 'Starts a Replay Manager')('dlg.manager.cmdline:dlgReplay')
cmdwrap('daemon', 'Starts a DALiuGE Daemon process')('dlg.manager.proc_daemon:run_with_cmdline')
cmdwrap('proxy', 'A reverse proxy to be used in restricted environments to contact the Drop Managers')('dlg.deploy.pawsey.dfms_proxy:run')
cmdwrap('monitor', 'A proxy to be used in conjunction with the dlg proxy in restricted environments')('dlg.deploy.pawsey.dfms_monitor:run')
cmdwrap('lgweb', 'A Web server for the Logical Graph Editor')('dlg.dropmake.web.lg_web:run')


@cmdwrap('version', 'Reports the DALiuGE version and exits')
def version(parser, args):
    from . import __version__, __git_version__
    print("Version: %s" % __version__)
    print("Git version: %s" % __git_version__)


@cmdwrap('fill', 'Fill a Logical Graph with parameters')
def fill(parser, args):
    _add_logging_options(parser)
    _add_output_options(parser)
    parser.add_option(
        '-L', '--logical-graph', default='-',
        help="Path to the Logical Graph (default: stdin)")
    parser.add_option(
        '-p', '--parameter', action='append',
        help="Parameter specification (either 'name=value' or a JSON string)",
        default=())

    (opts, args) = parser.parse_args(args)
    _setup_logging(opts)
    dump = _setup_output(opts)

    def param_spec_type(s):
        if s.startswith('{'):
            return 'json'
        elif '=' in s:
            return 'kv'
        else:
            return None

    # putting all parameters together in a single dictionary
    for p in opts.parameter:
        if param_spec_type(p) is None:
            parser.error('Parameter %s is neither JSON nor has it key=value form' % p)
    params = [p.split('=')
              for p in opts.parameter
              if param_spec_type(p) == 'kv']
    params = dict(params)
    for json_param in (json.loads(p) for p in opts.parameter if param_spec_type(p) == 'json'):
        params.update(json_param)

    from .dropmake.pg_generator import fill
    dump(fill(_open_i(opts.logical_graph), params))


def _add_unroll_options(parser):
    parser.add_option('-L', '--logical-graph', action="store", dest='lg_path', type="string",
                      help='Path to the Logical Graph (default: stdin)', default='-')
    parser.add_option('-p', '--oid-prefix', action="store", dest='oid_prefix', type="string",
                      help='Prefix to use for generated OIDs', default='1')
    parser.add_option("-z", "--zerorun", action="store_true",
                      dest="zerorun", help="Generate a Physical Graph Template that takes no time to run", default=False)
    parser.add_option("--app", action="store", type="int",
                      dest="app", help="Force an app to be used in the Physical Graph. 0=Don't force, 1=SleepApp, 2=SleepAndCopy", default=0)
    apps = (
        None,
        'dlg.apps.simple.SleepApp',
        'dlg.apps.simple.SleepAndCopyApp'
    )
    return apps


@cmdwrap('unroll', 'Unrolls a Logical Graph into a Physical Graph Template')
def dlg_unroll(parser, args):

    # Unroll Logical Graph
    _add_logging_options(parser)
    _add_output_options(parser)
    apps = _add_unroll_options(parser)
    (opts, args) = parser.parse_args(args)
    _setup_logging(opts)
    dump = _setup_output(opts)

    dump(unroll(opts.lg_path, opts.oid_prefix, zerorun=opts.zerorun, app=apps[opts.app]))


def _add_partition_options(parser):

    from .dropmake import pg_generator
    parser.add_option("-N", "--partitions", action="store", type="int",
                      dest="partitions", help="Number of partitions to generate", default=1)
    parser.add_option("-i", "--islands", action="store", type="int",
                      dest="islands", help="Number of islands to use during the partitioning", default=1)
    parser.add_option("-a", "--algorithm", action="store", type="choice", choices=pg_generator.known_algorithms(),
                      dest="algo", help="algorithm used to do the partitioning", default="metis")
    parser.add_option("-A", "--algorithm-param", action="append", dest="algo_params",
                      help="Extra name=value parameters used by the algorithms (algorithm-specific)")


@cmdwrap('partition', 'Divides a Physical Graph Template into N logical partitions')
def dlg_partition(parser, args):

    _add_logging_options(parser)
    _add_output_options(parser)
    _add_partition_options(parser)
    parser.add_option('-P', '--physical-graph-template', action='store', dest='pgt_path', type='string',
                      help='Path to the Physical Graph Template (default: stdin)', default='-')
    (opts, args) = parser.parse_args(args)
    _setup_logging(opts)
    dump = _setup_output(opts)

    with _open_i(opts.pgt_path) as fi:
        pgt = json.load(fi)

    dump(partition(pgt, opts))


@cmdwrap('unroll-and-partition', 'unroll + partition')
def dlg_unroll_and_partition(parser, args):

    _add_logging_options(parser)
    _add_output_options(parser)
    apps = _add_unroll_options(parser)
    _add_partition_options(parser)
    (opts, args) = parser.parse_args(args)
    _setup_logging(opts)
    dump = _setup_output(opts)

    pgt = unroll(opts.lg_path, opts.oid_prefix, zerorun=opts.zerorun, app=apps[opts.app])
    dump(partition(pgt, opts))


@cmdwrap('map', 'Maps a Physical Graph Template to resources and produces a Physical Graph')
def dlg_map(parser, args):

    from .manager import constants

    _add_logging_options(parser)
    _add_output_options(parser)
    parser.add_option('-H', '--host', action='store',
                      dest='host', help='The host we connect to to deploy the graph', default='localhost')
    parser.add_option("-p", "--port", action="store", type="int",
                      dest='port', help='The port we connect to to deploy the graph', default=constants.ISLAND_DEFAULT_REST_PORT)
    parser.add_option('-P', '--physical-graph-template', action='store', dest='pgt_path', type='string',
                      help='Path to the Physical Graph to submit (default: stdin)', default='-')
    parser.add_option("-N", "--nodes", action="store",
                      dest="nodes", help="The nodes where the Physical Graph will be distributed, comma-separated", default=None)
    parser.add_option("-i", "--islands", action="store", type="int",
                      dest="islands", help="Number of islands to use during the partitioning", default=1)
    (opts, args) = parser.parse_args(args)
    _setup_logging(opts)
    dump = _setup_output(opts)

    from .dropmake import pg_generator
    from .manager.client import CompositeManagerClient

    if opts.nodes:
        nodes = [n for n in opts.nodes.split(',') if n]
    else:
        client = CompositeManagerClient(opts.host, opts.port, timeout=10)
        nodes = [opts.host] + client.nodes()

    n_nodes = len(nodes)
    if n_nodes <= opts.islands:
        raise Exception("#nodes (%d) should be bigger than number of islands (%d)" % (n_nodes, opts.islands))

    with _open_i(opts.pgt_path) as f:
        pgt = json.load(f)

    dump(pg_generator.resource_map(pgt, nodes, opts.islands))


@cmdwrap('submit', 'Submits a Physical Graph to a Drop Manager')
def dlg_submit(parser, args):

    from .manager import constants

    # Submit Physical Graph
    _add_logging_options(parser)
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
    parser.add_option('-w', '--wait', action='store_true',
                      help='Wait for the graph execution to finish (default: False)', default=False)
    parser.add_option('-i', '--poll-interval', type='float',
                      help='Polling interval used for monitoring the execution (default: 10)',
                      default=10)
    (opts, args) = parser.parse_args(args)

    with _open_i(opts.pg_path) as f:
        submit(json.load(f), opts)


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


def start_process(cmd, args=(), **subproc_args):
    """
    Start 'dlg cmd <args>' in a different process.
    If `cmd` is not a known command an exception is raised.
    `subproc_args` are passed down to the process creation via `Popen`.

    This method returns the new process.
    """

    from .exceptions import DaliugeException
    if cmd not in commands:
        raise DaliugeException("Unknown command: %s" % (cmd,))

    cmdline = [sys.executable, '-m', __name__, cmd]
    if args:
        cmdline.extend(args)
    logger.debug("Launching %s", cmdline)
    return subprocess.Popen(cmdline, **subproc_args)


# We can also be executed as a module
if __name__ == '__main__':
    run()
