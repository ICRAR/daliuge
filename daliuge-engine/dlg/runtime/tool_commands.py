#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2020
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

from dlg.common import tool


def include_dir(_parser, _args):
    from . import get_include_dir

    print(get_include_dir())


def register_commands():
    tool.cmdwrap("nm", "Starts a Node Manager", "dlg.manager.cmdline:dlgNM")
    tool.cmdwrap("dim", "Starts a Drop Island Manager", "dlg.manager.cmdline:dlgDIM")
    tool.cmdwrap("mm", "Starts a Master Manager", "dlg.manager.cmdline:dlgMM")
    tool.cmdwrap("replay", "Starts a Replay Manager", "dlg.manager.cmdline:dlgReplay")
    tool.cmdwrap(
        "daemon",
        "Starts a DALiuGE Daemon process",
        "dlg.manager.proc_daemon:run_with_cmdline",
    )
    tool.cmdwrap(
        "proxy",
        "A reverse proxy to be used in restricted environments to contact the Drop Managers",
        "dlg.deploy.dlg_proxy:run",
    )
    tool.cmdwrap(
        "monitor",
        "A proxy to be used in conjunction with the dlg proxy in restricted environments",
        "dlg.deploy.dlg_monitor:run",
    )
    tool.cmdwrap(
        "include_dir",
        "Print the directory where C header files can be found",
        include_dir,
    )
