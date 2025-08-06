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
import pytest
import subprocess
import unittest

from dlg import common
from dlg.common import tool
from dlg.testutils import ManagerStarter

# Note this test will only run with a full installation of DALiuGE.
pexpect = pytest.importorskip("dlg.dropmake")

class TestTool(ManagerStarter, unittest.TestCase):
    def test_cmdhelp(self):
        """Checks that all dlg commands have a help"""
        tool._load_commands()
        for group, commands in tool.commands.items():
            for cmd in commands['commands']:
                p = tool.start_process(
                    cmd, ["-h"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
                )
                out, err = p.communicate()
                common.wait_or_kill(p, timeout=10)
                self.assertEqual(
                    0,
                    p.returncode,
                    "cmd: %s, out: %s" % (cmd + " -h", common.b2s(out + err)),
                )
