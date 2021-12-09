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
"""Extract help from DALiuGE CLI into RST format"""

import subprocess
from typing import OrderedDict
from dlg.common import tool

# unfortunately this does not preserve the same order
# tool._load_commands()


def load_commands(allOut):
    o = subprocess.run(["dlg", "-h"], capture_output=True)
    main_help = o.stdout.decode()
    allOut["dlg"] = main_help
    commands = list(
        map(lambda x: x[1:].split(" ", 1)[0], o.stdout.decode().split("\n")[3:-3])
    )
    return commands


def main(allOut):
    for c in load_commands(allOut):
        key = "dlg %s" % c
        head = "Command: %s" % key
        print(head)
        print("-" * len(head))
        print("Help output::\n")
        o = subprocess.run(["dlg", c, "-h"], capture_output=True)
        allOut[key] = o.stdout.decode()
        print("   " + allOut[key].replace("\n", "\n   "))  # indent
        print()


if __name__ == "__main__":
    allOut = OrderedDict()
    main(allOut)
