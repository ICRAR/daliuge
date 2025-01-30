#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2014
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
Unit/regression testing for the application of named_port_utils.py

Use examples derived from the following DROP classes: 
- pyfunc
- s3_drop
- bash_shell_app
- dockerapp 
- mpi
"""
import dill
import logging
import unittest
import pytest

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Note this test will only run with a full installation of DALiuGE.
pexpect = pytest.importorskip("dlg.dropmake")

import json
from pathlib import Path

import dlg.droputils as droputils
import dlg.graph_loader as graph_loader

from dlg.ddap_protocol import DROPStates
from dlg.dropmake import path_utils

class TestPortsEncoding(unittest.TestCase):
    """
    Given a dropspec, make sure the ports are loaded correctly. 
    """ 
    def _create_and_run_graph_spec_from_lgt(self, logical_graph_name: str):
        """
        Boilerplate graph running code which takes a Logical Graph Template (LGT) and 
        performs the translation and submission through the graph loader. 
        
        Returns drop leaf nodes on graph completion. 
        """
        spec = Path(path_utils.get_lg_fpath("drop_spec", logical_graph_name))
        with Path(spec).open('r', encoding="utf-8") as f: 
            appDropSpec = json.load(f)
        
        roots = graph_loader.createGraphFromDropSpecList(appDropSpec)
        # drops = [v for d,v in drops.items()]
        leafs = droputils.getLeafNodes(roots)  
        with  droputils.DROPWaiterCtx(self, leafs, timeout=3):
            for drop in roots: 
                fut = drop.async_execute()
                fut.result()
    
        return leafs


    def test_pyfunc_ports_encoding(self):
        """
        
        This test evaluates the use of per-port encoding using the test_ports.graph, 
        described below. 
        
        The CreateMultiA drop has inputs of 2,8 (non-default), which are replicated
        with the chosen encoding (numpy and pickle, respectively). These are passed to 
        CreateMultiB, which again replicates the output with selected encoding. These are
        passed to FileDrops, from which we can confirm the encoding has worked as 
        expected. 


                    ------> numpy(2) ---              ------> "numpy(2)"
                    |                   |             |
                    (npy)              (npy)        (UTF-8) 
                    |                   |             |
        <CreateMultiA(2,8)>                  <CreateMultiB> 
                    |                   |             |
                    (pickle)          (pickle)       (pickle)
                    |                   |             |
                    ------> pickle(8) ---             -----> pickle(8)
        """

        leafs = self._create_and_run_graph_spec_from_lgt("test_ports.graph")               
        for l in leafs:
            self.assertEqual(DROPStates.COMPLETED, l.status) 

        # Leaf Node 1 has been encoded first as numpy, second as UTF-8. 
        leaf = leafs.pop()
        desc = leaf.open()
        self.assertEqual(8, dill.loads(leaf.read(desc)))
        leaf = leafs.pop()
        desc = leaf.open()
        self.assertEqual("array(2)", leaf.read(desc).decode())
        

    def test_bash_shell_ports(self): 
        """
        "drop_spec", "pyfunc_glob_shell_test.graph"
        """
        leafs = self._create_and_run_graph_spec_from_lgt("pyfunc_glob_shell_test.graph")
        
        for l in leafs:
            self.assertEqual(DROPStates.COMPLETED, l.status) 
        self.assertEqual(0, leafs.pop().proc.returncode)
