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

# pylint: disable=unused-argument

"""dlg command line utility"""
import importlib
import logging
import optparse # pylint: disable=deprecated-module
import subprocess
import sys
import time

from dataclasses import dataclass
from importlib.metadata import entry_points

logger = logging.getLogger("dlg")

@dataclass
class CommandGroup:
    """
    Collection for information about a particular CLI 'Group'.

    The Group will ensure relevant commands are collected and displayed together for
    easier understanding.
    """
    name: str
    description: str


def add_logging_options(parser):
    parser.add_option(
        "-v",
        "--verbose",
        action="count",
        dest="verbose",
        help="Become more verbose. The more flags, the more verbose",
    )
    parser.add_option(
        "-q",
        "--quiet",
        action="count",
        dest="quiet",
        help="Be less verbose. The more flags, the quieter",
    )


def setup_logging(opts):

    levels = [
        logging.NOTSET,
        logging.DEBUG,
        logging.INFO,
        logging.WARNING,
        logging.ERROR,
        logging.CRITICAL,
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
    fmt = logging.Formatter(
        "%(asctime)-15s [%(levelname)5.5s] [%(threadName)15.15s] %(name)s#%(funcName)s:%(lineno)s %(message)s"
    )
    fmt.converter = time.gmtime
    streamHdlr = logging.StreamHandler(sys.stderr)
    streamHdlr.setFormatter(fmt)
    logging.root.addHandler(streamHdlr)
    logging.root.setLevel(level)


commands = {}


def cmdwrap(cmdname, group, desc, f):

    # If it's not a callable we assume it's a string
    # in which case we lazy-load the module:function when it gets called
    if not callable(f):
        orig_f = f

        class Importer(object):
            def __call__(self, *args, **kwargs):
                modname, fname = orig_f.split(":")
                module = importlib.import_module(modname)
                try:
                    return getattr(module, fname)(*args, **kwargs)
                except TypeError:
                    return getattr(module, fname)()

        f = Importer()

    def wrapped(*args, **kwargs):
        parser = optparse.OptionParser(description=desc)
        f(parser, *args, **kwargs)

    if group.name in commands:
        commands[group.name]["commands"][cmdname] = (desc, wrapped)
    else:
        commands[group.name] = {"desc": group.description, "commands":{}}
        commands[group.name]["commands"][cmdname] = (desc, wrapped)

def version(parser, args):
    from .version import version as vversion
    from .version import git_version

    print("Version: %s" % vversion)
    print("Git version: %s" % git_version)

common_group = CommandGroup("common", "Base commands for dlg CLI")
cmdwrap("version", common_group, "Reports the DALiuGE version and exits", version)

try:
    import dlg_paletteGen # pylint: disable=unused-import
    palette_group = CommandGroup("zpalette", "Wrapper for dlg_paletteGen")
    cmdwrap("palette", palette_group, "Generate palettes for EAGLE",
            "dlg_paletteGen.__main__:main")
except ImportError:
    pass

def _load_commands():
    if sys.version_info.minor < 10:
        all_entry_points = entry_points()
        for entry_point in all_entry_points["dlg.tool_commands"]:
            entry_point.load().register_commands()
    else:
        for entry_point in entry_points(  # pylint: disable=E1123
            group="dlg.tool_commands"
        ):  # pylint: disable=unexpected-keyword-arg
            entry_point.load().register_commands()

def format_cmd(cmd, data):
    desc, _ = data
    return f"\t {cmd:25}{desc:25}"

def print_usage(prgname):
    print("Usage: %s [command] [options]\n" % (prgname))
    for _, grouped_commands in sorted(commands.items()):
        print(f"\n{grouped_commands['desc']}")
        print("\n".join([format_cmd(cmd, data)
                         for cmd, data in sorted(grouped_commands['commands'].items())]))
    print("")
    print("Try %s [command] --help for more details" % (prgname))

def _get_cmd(cmd):
    _group = None
    for group, grouped_commands in sorted(commands.items()):
        if cmd in grouped_commands['commands']:
            return group, cmd
    return _group, cmd


def run(args=sys.argv):

    _load_commands()

    # Manually parse the first argument, which will be
    # either -h/--help or a dlg command
    # In the future we should probably use the argparse module
    prgname = sys.argv[0]
    if len(sys.argv) == 1:
        print_usage(prgname)
        sys.exit(1)

    cmd = sys.argv[1]
    sys.argv.pop(0)

    if cmd in ["-h", "--help", "help"]:
        print_usage(prgname)
        sys.exit(0)

    group, _  = _get_cmd(cmd)
    if not group:
        print("Unknown command: %s" % (cmd,))
        print_usage(prgname)
        sys.exit(1)

    commands[group]["commands"][cmd][1](sys.argv[1:])


def start_process(cmd, args=(), **subproc_args):
    """
    Start 'dlg cmd <args>' in a different process.
    If `cmd` is not a known command an exception is raised.
    `subproc_args` are passed down to the process creation via `Popen`.

    This method returns the new process.
    """

    _load_commands()
    group, _  = _get_cmd(cmd)
    from ..exceptions import DaliugeException

    if cmd not in commands[group]["commands"]:
        raise DaliugeException("Unknown command: %s" % (cmd,))

    cmdline = ["dlg", cmd]
    if args:
        cmdline.extend(args)
    logger.debug("Launching %s", cmdline)
    return subprocess.Popen(cmdline, **subproc_args)


# We can also be executed as a module
if __name__ == "__main__":
    run()
