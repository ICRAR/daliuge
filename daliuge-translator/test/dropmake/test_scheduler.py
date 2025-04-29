#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2015
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

import os
import unittest

import psutil
from dlg.dropmake.lg import LG
from dlg.dropmake.scheduler import (
    Scheduler,
    MySarkarScheduler,
    DAGUtil,
    Partition,
    MinNumPartsScheduler,
    PSOScheduler,
)

from dlg.common.path_utils import get_lg_fpath

if "DALIUGE_TESTS_RUNLONGTESTS" in os.environ:
    skip_long_tests = not bool(os.environ["DALIUGE_TESTS_RUNLONGTESTS"])
else:
    if (psutil.Process().username().lower() in ("chen", "cwu")) and bool(
        int(os.environ.get("TEST_PSO_SCHEDULER", 0))
    ):
        skip_long_tests = False
    else:
        skip_long_tests = True


class TestScheduler(unittest.TestCase):
    def test_incremental_antichain(self):
        part = Partition(100, 8)
        G = part._dag
        assert part.probe_max_dop(1, 2, True, True, True) == DAGUtil.get_max_dop(
            part._dag
        )
        G.add_edge(2, 3)
        assert part.probe_max_dop(2, 3, False, True, True) == DAGUtil.get_max_dop(
            part._dag
        )
        # G.add_edge(1, 4)
        # assert(part.probe_max_dop(1, 4, False, True, True) == DAGUtil.get_max_dop(part._dag))
        # G.add_edge(2, 5)
        l = part.probe_max_dop(2, 5, False, True, True)
        r = DAGUtil.get_max_dop(part._dag)
        assert l == r, "l = {0}, r = {1}".format(l, r)

    def test_basic_scheduler(self):
        fp = get_lg_fpath("logical_graphs", "cont_img_mvp.graph")
        lg = LG(fp)
        drop_list = lg.unroll_to_tpl()
        Scheduler(drop_list)

    def test_minnumparts_scheduler(self):
        lgs = {
            "cont_img_mvp.graph": 500,
            "cont_img_mvp.graph": 200,
            "test_grpby_gather.graph": 90,
            "chiles_simple.graph": 160,
        }
        mdp = 8
        ofa = 0.5
        for lgn, deadline in lgs.items():
            fp = get_lg_fpath("logical_graphs", lgn)
            lg = LG(fp)
            drop_list = lg.unroll_to_tpl()
            mps = MinNumPartsScheduler(
                drop_list, deadline, max_dop=mdp, optimistic_factor=ofa
            )
            mps.partition_dag()

    def test_mysarkar_scheduler(self):
        lgs = {
            "cont_img_mvp.graph": 20,
            "cont_img_mvp.graph": 15,
            "test_grpby_gather.graph": 10,
            "chiles_simple.graph": 5,
        }
        mdp = 8
        for lgn, numparts in lgs.items():
            fp = get_lg_fpath("logical_graphs", lgn)
            lg = LG(fp)
            drop_list = lg.unroll_to_tpl()
            mys = MySarkarScheduler(drop_list, max_dop=mdp)
            _, _, _, parts = mys.partition_dag()
            for part in parts:
                pass
                """
                if (part.cardinality > 0):
                    part.schedule.schedule_matrix
                    DAGUtil.ganttchart_matrix(part.schedule._dag, part.schedule._topo_sort)
                """
            # mys.merge_partitions(numparts)

    @unittest.skipIf(
        skip_long_tests,
        "Skipping because they take too long. Chen to eventually shorten them",
    )
    def test_pso_scheduler(self):
        lgs = {
            "cont_img_mvp.graph": 540,
            "cont_img_mvp.graph": 450,
            "test_grpby_gather.graph": 70,
            "chiles_simple.graph": 160,
        }
        mdp = 2
        for lgn, deadline in lgs.items():
            fp = get_lg_fpath("logical_graphs", lgn)
            lg = LG(fp)
            drop_list = lg.unroll_to_tpl()
            psps01 = PSOScheduler(drop_list, max_dop=mdp)
            psps01.partition_dag()
            psps02 = PSOScheduler(drop_list, max_dop=mdp, deadline=deadline)
            psps02.partition_dag()
