#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2015, 2024
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
Utility classes and functions designed to facilitate end-to-end testing of DALiuGE.

This requires both the daliuge-engine and daliuge-translator to be installed.
"""
import json
import pytest
import unittest

import dlg.droputils as droputils
import dlg.graph_loader as graph_loader

from pathlib import Path
from dlg.common.path_utils import get_lg_fpath
# Note this test will only run with a full installation of DALiuGE
pexpect = pytest.importorskip("dlg.dropmake")

def create_and_run_graph_spec_from_graph_file(test_case: unittest.TestCase,
                                              graph_name: str,
                                              resolve_path = True,
                                              app_root=True):
    """
    Boilerplate graph running code which takes a Logical Graph Template (LGT) and
    performs the translation and submission through the graph loader.

    Returns drop leaf nodes on graph completion.
    """
    if resolve_path:
        spec = Path(get_lg_fpath("drop_spec", graph_name))
    else:
        spec = Path(graph_name)
    with spec.open('r', encoding="utf-8") as f:
            appDropSpec = json.load(f)

    roots = graph_loader.createGraphFromDropSpecList(appDropSpec)

    # drops = [v for d,v in drops.items()]
    leafs = droputils.getLeafNodes(roots)
    with droputils.DROPWaiterCtx(test_case, leafs, timeout=300):
        for drop in roots:
            if app_root:
                fut = drop.async_execute()
                fut.result()
            else:
                drop.setCompleted()

    return leafs
