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
import os
import subprocess
import unittest

import pkg_resources

from dlg import tool, utils
from dlg.deploy import common
from dlg.manager.session import SessionStates
from dlg.testutils import ManagerStarter


class TestTool(ManagerStarter, unittest.TestCase):

    def test_cmdhelp(self):
        """Checks that all dlg commands have a help"""
        for cmd in tool.commands:
            with open(os.devnull, 'wb') as devnull:
                p = tool.start_process(cmd, ['-h'], stdout=devnull, stderr=devnull)
                utils.wait_or_kill(p, timeout=10)
                self.assertEqual(0, p.returncode)

    def test_pipeline(self):
        """A pipeline from an LG all the way to a finished graph execution"""
        with self.start_nm_in_thread(), self.start_dim_in_thread():
            lg = pkg_resources.resource_filename( # @UndefinedVariable
                'test.dropmake', 'logical_graphs/lofar_std.json')

            fill = tool.start_process('fill', ['-L', lg], stdout=subprocess.PIPE)
            unroll = tool.start_process('unroll', ['-z', '--app', '1'], stdin=fill.stdout, stdout=subprocess.PIPE)
            partition = tool.start_process('partition', stdin=unroll.stdout, stdout=subprocess.PIPE)
            map_ = tool.start_process('map', stdin=partition.stdout, stdout=subprocess.PIPE)
            submit = tool.start_process('submit', ['-w', '-i', '0.2'], stdin=map_.stdout)

            for proc in fill, unroll, partition, map_, submit:
                self.assertEquals(proc.wait(), 0)

            # It actually finished
            sessions_status = common.monitor_sessions().values()
            self.assertEquals(next(iter(next(iter(sessions_status)))), SessionStates.FINISHED)
