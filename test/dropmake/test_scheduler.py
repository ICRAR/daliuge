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

import unittest

import pkg_resources
import psutil

from dfms.dropmake.pg_generator import LG
from dfms.dropmake.scheduler import (Scheduler, MySarkarScheduler, DAGUtil,
Partition, MinNumPartsScheduler, PSOScheduler, SAScheduler, MCTSScheduler)


not_chen = psutil.Process().username() not in ('chen', 'cwu')

class TestScheduler(unittest.TestCase):

    def test_incremental_antichain(self):
        part = Partition(100, 8)
        G = part._dag
        #G.add_edge(1, 2)
        assert(part.probe_max_dop(1, 2, True, True, True) == DAGUtil.get_max_dop(part._dag))
        #print "-----"
        G.add_edge(2, 3)
        assert(part.probe_max_dop(2, 3, False, True, True) == DAGUtil.get_max_dop(part._dag))
        #print "-----"
        G.add_edge(1, 4)
        assert(part.probe_max_dop(1, 4, False, True, True) == DAGUtil.get_max_dop(part._dag))
        #print "-----"
        G.add_edge(2, 5)
        l = part.probe_max_dop(2, 5, False, True, True)
        r = DAGUtil.get_max_dop(part._dag)
        #print l, r
        assert l == r, "l = {0}, r = {1}".format(l, r)

    def test_basic_scheduler(self):
        fp = pkg_resources.resource_filename('dfms.dropmake', 'web/lofar_std.json')
        lg = LG(fp)
        drop_list = lg.unroll_to_tpl()
        mys = Scheduler(drop_list)
        #print mys._dag.edges(data=True)

    def test_minnumparts_scheduler(self):
        lgnames = ['cont_img.json', 'lofar_std.json', 'chiles_two.json', 'test_grpby_gather.json', 'chiles_two_dev1.json', 'chiles_simple.json']
        tgt_deadline = [500, 200, 300, 90, 80, 160] #250
        mdp = 8
        ofa = 0.5
        for j, lgn in enumerate(lgnames):
            fp = pkg_resources.resource_filename('dfms.dropmake', 'web/{0}'.format(lgn))
            lg = LG(fp)
            drop_list = lg.unroll_to_tpl()
            #logger.info("MinNumPartsScheduler Partitioning ", lgn)
            lll = len(lgn) + len("Partitioning ") + 1
            #logger.info("=" * lll)
            mps = MinNumPartsScheduler(drop_list, tgt_deadline[j], max_dop=mdp, optimistic_factor=ofa)
            num_parts_done, lpl, ptime, parts = mps.partition_dag()
            #logger.info("{3} partitioned: parts = {0}, lpl = {1}, ptime = {2:.2f}".format(num_parts_done, lpl, ptime, lgn))
            #logger.info("-" * lll)

    def test_mysarkar_scheduler(self):
        lgnames = ['cont_img.json', 'lofar_std.json', 'chiles_two.json', 'test_grpby_gather.json', 'chiles_two_dev1.json', 'chiles_simple.json']
        #lgnames = [lgnames[1]]
        tgt_partnum = [20, 15, 15, 10, 10, 5]
        mdp = 8
        s_matrix = True
        for j, lgn in enumerate(lgnames):
            fp = pkg_resources.resource_filename('dfms.dropmake', 'web/{0}'.format(lgn))
            lg = LG(fp)
            drop_list = lg.unroll_to_tpl()
            #logger.info( "MySarkarScheduler Partitioning ", lgn)
            lll = len(lgn) + len("Partitioning ") + 1
            #logger.info( "=" * lll)
            mys = MySarkarScheduler(drop_list, max_dop=mdp)
            num_parts_done, lpl, ptime, parts = mys.partition_dag()
            #logger.info( "{3} partitioned: parts = {0}, lpl = {1}, ptime = {2:.2f}".format(num_parts_done, lpl, ptime, lgn))
            if (s_matrix):
                for i, part in enumerate(parts):
                    if (part.cardinality > 0):
                        ma = part.schedule.schedule_matrix
                        ga = DAGUtil.ganttchart_matrix(part.schedule._dag, part.schedule._topo_sort)
                        # print "Partition ", i
                        # print "scheduling matrix: ", ma.shape
                        # print ma
                        # print "ganttchart matrix: ", ga.shape
                        # print ga
                        # print "Workload: ", part.schedule.workload
                        # print
            mys.merge_partitions(tgt_partnum[j])
            #logger.info( "-" * lll)

    @unittest.skipIf(not_chen, "Skipping because they take too long. Chen to eventually shorten them")
    def test_pso_scheduler(self):
        lgnames = ['cont_img.json', 'lofar_std.json', 'chiles_two.json',
        'test_grpby_gather.json', 'chiles_two_dev1.json', 'chiles_simple.json',
        'test_seq_gather.json']
        #lgnames = ['test_seq_gather.json']
        tgt_deadline = [540, 450, 60, 70, 60, 160, 150] #250
        #tgt_deadline = [150]
        mdp = 2
        for j, lgn in enumerate(lgnames):
            fp = pkg_resources.resource_filename('dfms.dropmake', 'web/{0}'.format(lgn))
            lg = LG(fp)
            drop_list = lg.unroll_to_tpl()
            psps01 = PSOScheduler(drop_list, max_dop=mdp)
            num_parts_done, lpl, ptime, parts = psps01.partition_dag()
            #print "PSO (no deadline): {3} partitioned: parts = {0}, lpl = {1}, ptime = {2:.2f}".format(num_parts_done, lpl, ptime, lgn)
            psps02 = PSOScheduler(drop_list, max_dop=mdp, deadline=tgt_deadline[j])
            num_parts_done, lpl, ptime, parts = psps02.partition_dag()
            #print "PSO (deadline): {3} partitioned: parts = {0}, lpl = {1}, deadline = {4}, ptime = {2:.2f}".format(num_parts_done, lpl, ptime, lgn, tgt_deadline[j])

    @unittest.skipIf(not_chen, "Skipping because they take too long. Chen to eventually shorten them")
    def test_sa_scheduler(self):
        lgnames = ['lofar_std.json']
        tgt_deadline = [450]
        mdp = 4
        for j, lgn in enumerate(lgnames):
            fp = pkg_resources.resource_filename('dfms.dropmake', 'web/{0}'.format(lgn))
            lg = LG(fp)
            drop_list = lg.unroll_to_tpl()
            pssa01 = SAScheduler(drop_list, max_dop=mdp)
            #pssa01 = PSOScheduler(drop_list, max_dop=mdp)
            num_parts_done, lpl, ptime, parts = pssa01.partition_dag()
            #print "SA (no deadline): {3} partitioned: parts = {0}, lpl = {1}, ptime = {2:.2f}".format(num_parts_done, lpl, ptime, lgn)
            pssa02 = SAScheduler(drop_list, max_dop=mdp, deadline=tgt_deadline[j])
            num_parts_done, lpl, ptime, parts = pssa02.partition_dag()
            #print "SA (deadline): {3} partitioned: parts = {0}, lpl = {1}, deadline = {4}, ptime = {2:.2f}".format(num_parts_done, lpl, ptime, lgn, tgt_deadline[j])

    @unittest.skipIf(not_chen, "Skipping because they take too long. Chen to eventually shorten them")
    def test_mcts_scheduler(self):
        lgnames = ['lofar_std.json']
        tgt_deadline = [450]
        mdp = 4
        for j, lgn in enumerate(lgnames):
            fp = pkg_resources.resource_filename('dfms.dropmake', 'web/{0}'.format(lgn))
            lg = LG(fp)
            drop_list = lg.unroll_to_tpl()
            pssa01 = MCTSScheduler(drop_list, max_dop=mdp, max_calc_time=0.25)
            num_parts_done, lpl, ptime, parts = pssa01.partition_dag()
            #logger.info("MCTS (no deadline): {3} partitioned: parts = {0}, lpl = {1}, ptime = {2:.2f}".format(num_parts_done, lpl, ptime, lgn))
    #         # pssa02 = SAScheduler(drop_list, max_dop=mdp, deadline=tgt_deadline[j])
    #         # num_parts_done, lpl, ptime, parts = pssa02.partition_dag()
    #         #print "SA (deadline): {3} partitioned: parts = {0}, lpl = {1}, deadline = {4}, ptime = {2:.2f}".format(num_parts_done, lpl, ptime, lgn, tgt_deadline[j])
