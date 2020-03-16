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
"""dlg command line utility"""

import importlib
import logging
import optparse
import subprocess
import sys
import time


logger = logging.getLogger(__name__)


def add_logging_options(parser):
    parser.add_option("-v", "--verbose", action="count",
                      dest="verbose", help="Become more verbose. The more flags, the more verbose")
    parser.add_option("-q", "--quiet", action="count",
                      dest="quiet", help="Be less verbose. The more flags, the quieter")


def setup_logging(opts):

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

        # If it's not a callable we assume it's a string
        # in which case we lazy-load the module:function when it gets called
        if not callable(f):
            orig_f = f

            class Importer(object):
                def __call__(self, *args, **kwargs):
                    modname, fname = orig_f.split(':')
                    module = importlib.import_module(modname)
                    return getattr(module, fname)(*args, **kwargs)
            f = Importer()

        def wrapped(*args, **kwargs):
            parser = optparse.OptionParser(description=desc)
            f(parser, *args, **kwargs)
        commands[cmdname] = (desc, wrapped)
        return wrapped
    return decorated

# Commands existing in other packages/distributions
# One day we should think how to extend this at runtime automatically
try:
    importlib.import_module('dlg.runtime.version')
    cmdwrap('nm', 'Starts a Node Manager')('dlg.manager.cmdline:dlgNM')
    cmdwrap('dim', 'Starts a Drop Island Manager')('dlg.manager.cmdline:dlgDIM')
    cmdwrap('mm', 'Starts a Master Manager')('dlg.manager.cmdline:dlgMM')
    cmdwrap('replay', 'Starts a Replay Manager')('dlg.manager.cmdline:dlgReplay')
    cmdwrap('daemon', 'Starts a DALiuGE Daemon process')('dlg.manager.proc_daemon:run_with_cmdline')
    cmdwrap('proxy', 'A reverse proxy to be used in restricted environments to contact the Drop Managers')('dlg.deploy.pawsey.dfms_proxy:run')
    cmdwrap('monitor', 'A proxy to be used in conjunction with the dlg proxy in restricted environments')('dlg.deploy.pawsey.dfms_monitor:run')
except ImportError:
    pass
try:
    importlib.import_module('dlg.translator.version')
    cmdwrap('lgweb', 'A Web server for the Logical Graph Editor')('dlg.dropmake.web.lg_web:run')
    cmdwrap('fill', 'Fill a Logical Graph with parameters')('dlg.translator.tool_ext:fill')
    cmdwrap('unroll', 'Unrolls a Logical Graph into a Physical Graph Template')('dlg.translator.tool_ext:dlg_unroll')
    cmdwrap('partition', 'Divides a Physical Graph Template into N logical partitions')('dlg.translator.tool_ext:dlg_partition')
    cmdwrap('unroll-and-partition', 'unroll + partition')('dlg.translator.tool_ext:dlg_unroll_and_partition')
    cmdwrap('map', 'Maps a Physical Graph Template to resources and produces a Physical Graph')('dlg.translator.tool_ext:dlg_map')
    cmdwrap('submit', 'Submits a Physical Graph to a Drop Manager')('dlg.translator.tool_ext:dlg_submit')
except ImportError:
    pass


@cmdwrap('version', 'Reports the DALiuGE version and exits')
def version(parser, args):
    from .version import version, git_version
    print("Version: %s" % version)
    print("Git version: %s" % git_version)


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

    from ..exceptions import DaliugeException
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
