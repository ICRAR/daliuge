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
import logging
import time
import unittest

from dlg.common import dropdict, tool
from dlg.manager import client
from dlg.utils import terminate_or_kill

from test.dlg_engine_testutils import DROPManagerUtils, TerminatingTestHelper

logger = logging.getLogger(f"dlg.{__name__}")
hostname = "localhost"


def memory_drop(uid):
    return dropdict(
        {
            "node": hostname,
            "oid": uid,
            "uid": uid,
            "categoryType": "Data",
            "dropclass": "dlg.data.drops.memory.InMemoryDROP",
        }
    )


def create_graph(branches, drops_per_branch):
    graph = []
    completed_uids = []
    final_apps = []
    for branch in range(branches):
        for i in range(drops_per_branch):
            data_uid = "data_%d_branch_%d" % (i, branch)
            app_uid = "app_%d_branch_%d" % (i, branch)
            data_drop = memory_drop(data_uid)
            app_drop = dropdict(
                {
                    "node": hostname,
                    "oid": app_uid,
                    "uid": app_uid,
                    "categoryType": "Application",
                    "dropclass": "dlg.apps.simple.SleepAndCopyApp",
                    "sleep_time": 0,
                }
            )
            data_drop.addConsumer(app_drop)
            graph.append(data_drop)
            graph.append(app_drop)
            if i == 0:
                completed_uids.append(data_uid)
                prev_app = data_drop
            elif i == drops_per_branch - 1:
                final_apps.append(app_drop)
            else:
                data_drop.addProducer(prev_app)

    final_drop = memory_drop("final")
    for final_app in final_apps:
        final_drop.addProducer(final_app)

    graph.append(final_drop)
    DROPManagerUtils.add_test_reprodata(graph)
    return graph, completed_uids


class TestBigGraph(unittest.TestCase):
    """
    A small class that simply checks that the deployment of a considerable-sized
    graph takes no longer than really expected.
    """

    def setUp(self):
        unittest.TestCase.setUp(self)

        args = ["-H", hostname, "-qq"]
        self.dmProcess = tool.start_process("nm", args)

    def tearDown(self):
        terminate_or_kill(self.dmProcess, 5)
        unittest.TestCase.tearDown(self)

    def test_submit_hugegraph(self):
        # Each branch contains a data drop and an app drop
        # All branches connect to a final data drop
        drops_per_branch = 5000
        branches = 5
        n_drops = drops_per_branch * branches * 2 + 1
        graph, completed_uids = create_graph(
            branches=branches, drops_per_branch=drops_per_branch
        )
        self.assertEqual(n_drops, len(graph) - 1)  # -1 for reprodata at end
        self._run_graph(graph, completed_uids, timeout=5)

    def _run_graph(self, graph, completed_uids, timeout=5):
        sessionId = "lala"
        restPort = 8989
        args = ["--port", str(restPort), "-N", hostname, "-qq"]

        logger.debug("Starting NM on port %d", restPort)
        c = client.NodeManagerClient(port=restPort)
        dimProcess = tool.start_process("dim", args)

        with TerminatingTestHelper(dimProcess, timeout=timeout):
            c.create_session(sessionId)
            logger.info("Appending graph")
            c.append_graph(sessionId, graph)

            # What we are actually trying to measure with all this stuff
            start = time.time()
            c.deploy_session(sessionId, completed_uids)
            delta = time.time() - start

            # A minute is more than enough, in my PC it takes around 4 or 5 [s]
            # A minute is also way less than the ~2 [h] we observed in AWS
            self.assertLessEqual(
                delta, 90, "It took way too much time to create all drops"
            )
