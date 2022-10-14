#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2017
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
"""
This module tests the top-level reproducibility repro methods for missing entries.
"""
import unittest
from dlg.common.reproducibility.reproducibility import (
    init_lgt_repro_data,
    init_lg_repro_data,
    init_pgt_unroll_repro_data,
    init_pgt_partition_repro_data,
    init_pg_repro_data,
)


class InitReproTest(unittest.TestCase):
    """
    Tests top-level repro methods for missing reprodata
    """

    def test_lgt_init(self):
        _graph = {}
        sig = init_lgt_repro_data(_graph, "0")
        self.assertIn("reprodata", sig)

    def test_lg_init(self):
        _graph = {}
        sig = init_lg_repro_data(_graph)
        self.assertNotIn("reprodata", sig)

    def test_pgt_unroll_init(self):
        _graph = [{}]
        sig = init_pgt_unroll_repro_data(_graph)
        self.assertNotIn("signature", sig[0])

    def test_pgt_partition_init(self):
        _graph = [{}]
        sig = init_pgt_partition_repro_data(_graph)
        self.assertNotIn("signature", sig[0])

    def test_pg_init(self):
        _graph = [{}]
        sig = init_pg_repro_data(_graph)
        self.assertNotIn("signature", sig[0])
