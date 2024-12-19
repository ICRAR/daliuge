#a
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
import logging
import unittest

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

import json
import dlg.droputils as droputils

import dlg.graph_loader as graph_loader

from pathlib import Path
from dlg.dropmake import path_utils

from dlg.ddap_protocol import DROPStates

from test.apps.test_pyfunc import func_with_defaults

class TestPortsLoaded(unittest.TestCase):
    """
    Given a dropspec, make sure the ports are loaded correctly. 
    """ 

    def test_extract_attributes(self):
        """
        Given a drop, make sure the attributes are accurately loaded.

        Rules for applicationArgs: 
            - If it is an applicationArgument and it has a port value, we don't
              set this as a param

        """
        spec = Path(path_utils.get_lg_fpath("drop_spec", "test_ports.graph"))
        with Path(spec).open('r') as f: 
            appDropSpec = json.load(f)
        
        roots = graph_loader.createGraphFromDropSpecList(appDropSpec)
        # drops = [v for d,v in drops.items()]
        leafs = droputils.getLeafNodes(roots)  
        with  droputils.DROPWaiterCtx(self, leafs, timeout=3000):
            for drop in roots: 
                fut = drop.async_execute()
                fut.result()
                
        for l in leafs:
            self.assertEqual(DROPStates.COMPLETED, l.status) 
