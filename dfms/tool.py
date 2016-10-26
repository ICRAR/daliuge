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
import sys
import time

from dfms import droputils
from dfms.dropmake.pg_generator import LG, MySarkarPGTP, MetisPGTP
from dfms.manager import constants, proc_daemon, cmdline
from dfms.manager.client import CompositeManagerClient
from dfms.deploy.pawsey import dfms_proxy


logger = logging.getLogger(__name__)

def open_i(path, flags=None):
    if path == '-':
        return sys.stdin
    return open(os.path.expanduser(path), flags or 'r')

def open_o(path, flags=None):
    if path == '-':
        return sys.stdout
    return open(os.path.expanduser(path), flags or 'w')

def fname_to_pipname(fname):
    fname = fname.split('/')[-1]
    if fname.endswith('.json'):
        fname = fname[:-5]
    return fname

def _unroll(lg_path, oid_prefix):
    lg = LG(open_i(lg_path), ssid=oid_prefix)
    logger.info("Start to unroll %s", lg_path)
    drop_list = lg.unroll_to_tpl()
    logger.info("Unroll completed for %s with # of Drops: %d", lg_path, len(drop_list))
    return drop_list


commands = {}
def cmdwrap(cmdname):
    def decorated(f):
        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            f(*args, **kwargs)
        commands[cmdname] = f
        return wrapped
    return decorated

# Commands existing in other modules
cmdwrap('nm')(cmdline.dlgNM)
cmdwrap('dim')(cmdline.dlgDIM)
cmdwrap('mm')(cmdline.dlgMM)
cmdwrap('replay')(cmdline.dlgReplay)
cmdwrap('daemon')(proc_daemon.run_with_cmdline)
cmdwrap('proxy')(dfms_proxy.run)

@cmdwrap('unroll')
def dlgUnroll(args):

    # Unroll Logical Graph
    parser = optparse.OptionParser(description='Unrolls a Logical Graph into a Physical Graph Template')
    parser.add_option('-L', '--logical-graph', action="store", dest='lg_path', type="string",
                      help='Path to the Logical Graph (default: stdin)', default='-')
    parser.add_option('-p', '--oid-prefix', action="store", dest='oid_prefix', type="string",
                      help='Prefix to use for generated OIDs', default='1')
    parser.add_option('-o', '--output', action="store", dest='output', type="string",
                      help='Where the Physical Graph Template should be written to (default: stdout)', default='-')
    (opts, args) = parser.parse_args(args)

    with open_o(opts.output) as f:
        json.dump(_unroll(opts.lg_path, opts.oid_prefix), f)

@cmdwrap('translate')
def dlgTranslate(args):

    parser = optparse.OptionParser(description='Translates a Logical Graph or Physical Graph Template into a Physical Graph')
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

    with open_i(opts.pgt_path) as f:
        drop_list = json.load(f)
    pip_name = fname_to_pipname(opts.pgt_path)

    logger.info("Initialising PGTP %s", opts.algo)
    if opts.algo == 'sarkar':
        pgtp = MySarkarPGTP(drop_list, lnl, merge_parts=True)
    else:
        pgtp = MetisPGTP(drop_list, lnl)
    del drop_list
    logger.info("PGTP initialised %s", opts.algo)

    logger.info("Start to translate %s", pip_name)
    pgtp.to_gojs_json(string_rep=False)
    logger.info("Translation completed for %s", pip_name)

    pg_spec = pgtp.to_pg_spec(node_list, ret_str=False, num_islands=opts.islands, tpl_nodes_len=0)

#     else:
#         # pg_spec template, fill it with real IP addresses directly
#         if (len(node_list) > 0):
#             logger.info("Starting to translate %s", pip_name)
#             dim_list = node_list[0:self._num_islands]
#             nm_list = node_list[self._num_islands:]
#             for drop_spec in drop_list:
#                 nidx = int(drop_spec['node'][1:]) # skip '#'
#                 drop_spec['node'] = nm_list[nidx]
#                 iidx = int(drop_spec['island'][1:]) # skip '#'
#                 drop_spec['island'] = dim_list[iidx]
#             logger.info("Translation completed for %s", pip_name)
#         else:
#             raise DaliugeException("Empty node_list, cannot translate the pg_spec template")
#         pg_spec = drop_list


    # Optionally set sleepTimes to 0 and apps to a specific type
    if opts.zerorun:
        for dropspec in pg_spec:
            if 'sleepTime' in dropspec:
                dropspec['sleepTime'] = 0
    if opts.app is not None:
        apps = (
            'test.graphsRepository.SleepApp',
            'test.graphsRepository.SleepAndCopyApp',
        )
        for dropspec in pg_spec:
            if 'app' in dropspec:
                dropspec['app'] = apps(opts.app)

    logger.info("Physical Graph calculated")

    with open_o(opts.output) as f:
        json.dump(pg_spec, f)

@cmdwrap('submit')
def dlgSubmit(args):

    # Submit Physical Graph
    parser = optparse.OptionParser(description='Submits a Physical Graph to a Drop Manager')
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

    client = CompositeManagerClient(opts.host, opts.port, timeout=10)

    with open_i(opts.pg_path) as f:
        pg_spec = json.load(f)

    logger.info("About to compute roots")
    completed_uids = droputils.get_roots(pg_spec)
    logger.info("Len of completed_uids is {0}".format(len(completed_uids)))

    # forget about the objects in memory and work with a json dump form now on
    pg_asjson = json.dumps(pg_spec)
    del pg_spec

    session_id = opts.session_id
    if session_id is None:
        session_id = "{0}_%f".format(fname_to_pipname(opts.pg_path) if opts.pg_path != '-' else 'unknown', time.time())

    client.create_session(session_id)
    logger.info("Session %s created", session_id)
    client.append_graph(session_id, pg_asjson)
    logger.info("Graph for session %s appended", session_id)

    if not opts.skip_deploy:
        client.deploy_session(session_id, completed_uids=completed_uids)
        logger.info("Session %s deployed", session_id)


def print_usage(prgname):
    print('Usage: %s [command] [options]' % (prgname))
    print('')
    print('\n'.join(['Commands are:'] + ['\t%s' % (cmdname) for cmdname in sorted(commands)]))
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

    commands[cmd](sys.argv[1:])

# We can also be executed as a module
if __name__ == '__main__':
    run()