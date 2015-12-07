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
#
"""
https://confluence.ska-sdp.org/display/PRODUCTTREE/C.1.2.4.4.2+DFM+Resource+Manager

DFM resource managr uses the requested logical graphs, the available resources and
the profiling information and turns it into the partitioned physical graph,
which will then be deployed and monitored by the Physical Graph Manager
"""
import json, os
from dfms.drop import dropdict
from UserDict import UserDict

class GraphException(Exception):
    pass

class LGNode(UserDict):
    def __init__(self, *arg, **kw):
        UserDict.__init__(self, *arg, **kw)
        self._outs = []

    def get_id(self):
        return self['key']

    def add_box(self, lg_node):
        pass

    def add_output(self, lg_node):
        pass

class LG():
    """
    An object representation of Logical Graph
    """

    def __init__(self, json_path):
        """
        parse JSON into LG object
        """
        if (not os.path.exists(json_path)):
            raise GraphException("Logical graph {0} not found".format(json_path))
        with open(json_path) as df:
            lg = json.load(df)
            sd = self._find_start(lg):
            if (sd is None):
                raise GraphException("No start DROP!")
            else:
                self._start = sd
            """
            parse the graph
            """
            self._parse_local(None, lg)

    def _find_start(self, lg):
        """
        Assuming there is only one "Start"
        """
        for n in lg['nodeDataArray']:
            if (n['category'] == 'Start'):
                return n
        return None

    def get_nodes_from(self, from_key):
        pass

    def _make_oid(self, k):
        """
        k:  key in the logical grph (int)
        dummy implentation now
        """
        return "00000_{0}".format(k)

    def _make_drop(self, jd):
        """
        Return the DROP spec instance

        jd: json_dict, two examples (data and component respectively):

        { u'category': u'Data',
          u'data_volume': 25,
          u'group': -58,
          u'key': -59,
          u'loc': u'40.96484375000006 -250.53115793863992',
          u'text': u'Channel @ \nAll Day'},

        { u'Arg01': u'',
          u'Arg02': u'',
          u'Arg03': u'',
          u'Arg04': u'',
          u'category': u'Component',
          u'execution_time': 20,
          u'group': -60,
          u'key': -56,
          u'loc': u'571.6718750000005 268.0000000000004',
          u'text': u'DD Calibration'}
        """
        dropSpec = dropdict({'oid':self._make_oid(jd['key']), 'type':'plain', 'storage':'file'})
        dropSpec.update(kwargs)
        return dropSpec

    def _parse_local(self, in_dict, lg):
        """
        Parse out all DROPs at the local level (each level represents a box)
        Four types of boxes:
        1. "Diagram" root level
        2. "Group By" box
        3. "Gather" box
        4. "Scatter" box

        Levels can be nested and hierarchical
        """
        if (in_dict is None):
            """
            Entire diagram level
            """
            pass
        else:
            pass

    def get_ds_list(self):
        """
        get a list of DSS
        """
        pass

def para_lg(lg):
    """
    Parallelise the logical graph
    1. Parse lg into LG_node_graph,
    2 list all Data Scatters (DS),
    3. for each DS, determine DoP
    4. fork out new DROPs based on DoP for each DS
    5. produce pg (DROP definitions)
    """
    pass

def l2g(lg, num_procs):
    """
    A simple implementation to do the mapping with the following features:

    1. parallelise lg into lg_para
    2. convert lg_para into pyrros input matrix and pg (without partitions)
    3. run pyrros
    4. convert pyrros output matrix into pgp directly (physical graph partition)
    5. see chilesdospec.py examples for producing the final PG specification
    """
    pass
