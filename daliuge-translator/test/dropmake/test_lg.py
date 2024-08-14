#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2024
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

import json
import copy
import unittest
from dlg.dropmake.lg import LG

try:
    from importlib.resources import files, as_file
except (ImportError, ModuleNotFoundError):
    from importlib_resources import files

NODES = 'nodeDataArray'
LINKS = 'linkDataArray'

def get_lg_fname(lg_name):
    return str(files(__package__) / f"logical_graphs/{lg_name}")

class TestLGNodeLoading(unittest.TestCase):

    def test_LGNode_SubgraphData(self):
        """
        Test that when we initially construct the logical graph, the SubGraph data node
        that is added to the graph stores the sub-graph nodes and links.
        """
        fname = get_lg_fname("ExampleSubgraphSimple.graph")
        lg = LG(fname)
        subgraph_data_node_key = -1
        self.assertIsNotNone(lg._done_dict[subgraph_data_node_key].subgraph)
