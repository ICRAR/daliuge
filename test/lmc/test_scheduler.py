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
from dfms.lmc.scheduler import Scheduler, MySarkarScheduler, DAGUtil, Partition

class TestScheduler(unittest.TestCase):

    def test_incremental_antichain(self):
        part = Partition(100, 8)
        G = part._dag
        G.add_edge(1, 2)
        assert(part.probe_max_dop(1, 2, True, True, True) == DAGUtil.get_max_dop(part._dag))
        print "-----"
        G.add_edge(2, 3)
        assert(part.probe_max_dop(2, 3, False, True, True) == DAGUtil.get_max_dop(part._dag))
        print "-----"
        G.add_edge(1, 4)
        assert(part.probe_max_dop(1, 4, False, True, True) == DAGUtil.get_max_dop(part._dag))
        print "-----"
        G.add_edge(2, 5)
        l = part.probe_max_dop(2, 5, False, True, True)
        r = DAGUtil.get_max_dop(part._dag)
        assert l == r, "l = {0}, r = {1}".format(l, r)
