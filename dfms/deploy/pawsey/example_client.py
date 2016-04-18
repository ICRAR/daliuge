#!/usr/bin/python
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

"""
Examples on how to interact with the DFMS remote monitor (on AWS),
which represents a DIM or MM inside the Pawsey firewall,
in order to convert a logical graph into a physical graph, and then
create sessions, append graphs, and deploy sessions to execute the converted
physical graph in Pawsey
"""

import pkg_resources, json, sys

from dfms.dropmake.pg_generator import LG, PGT, MetisPGTP, MySarkarPGTP, MinNumPartsPGTP
from dfms.restutils import RestClient
from dfms.manager.client import DataIslandManagerClient

lgnames = ['lofar_std.json', 'chiles_two.json', 'test_grpby_gather.json',
'chiles_two_dev1.json', 'chiles_simple.json']


class MonitorClient(object):
    def __init__(self, mhost, mport, timeout=10, sch_algo='sarkar'):
        self._host = mhost
        self._port = mport
        self._rc = RestClient(mhost, mport, timeout)
        self._dc = DataIslandManagerClient(mhost, mport)
        self._sch_algo = sch_algo

    def get_avail_hosts(self):
        return self._dc.nodes()
        """
        ret = self._rc._request('/api', 'get')
        ret_dict = json.loads(ret)
        return ret_dict['hosts']
        """

    def submit_single_graph(self, graph_id, algo='sarkar'):
        lgn = lgnames[graph_id]
        fp = pkg_resources.resource_filename('dfms.dropmake', 'web/{0}'.format(lgn))
        lg = LG(fp)
        drop_list = lg.unroll_to_tpl()
        #node_list = self.get_avail_hosts()
        node_list = self._dc.nodes()
        pgtp = MySarkarPGTP(drop_list, len(node_list), merge_parts=True)
        pgtp.json
        pg_spec = pgtp.to_pg_spec(node_list)
        ssid = "{0}-{1}".format(lgn.split('.')[0], lg._session_id)
        self._dc.create_session(ssid)
        print "session created"
        self._dc.append_graph(ssid, pg_spec)
        print "graph appended"

        #ret = self._dc.deploy_session(ssid, completed_uids=[])
        ret = self._dc.deploy_session(ssid)
        print "session deployed"
        return ret

    def produce_physical_graphs(self, graph_id, algo='sarkar', tgt="/tmp"):
        lgn = lgnames[graph_id]
        fp = pkg_resources.resource_filename('dfms.dropmake', 'web/{0}'.format(lgn))
        lg = LG(fp)
        drop_list = lg.unroll_to_tpl()
        node_list = self._dc.nodes()
        #node_list = ['10.128.0.11', '10.128.0.14', '10.128.0.15', '10.128.0.16']
        pgtp = MySarkarPGTP(drop_list, len(node_list), merge_parts=True)
        pgtp.json
        pg_spec = pgtp.to_pg_spec(node_list)
        with open('/{1}/sar_{0}_pgspec.json'.format(lgn.split('.')[0], tgt), 'w') as f:
            f.write(pg_spec)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Please provide graph id: (integer, 0 - 4)"
        sys.exit(2)
    gid = int(sys.argv[1])
    if (gid > len(lgnames) - 1):
        print "graph id is too large"
        sys.exit(1)
    #mc = MonitorClient('sdp-dfms.ddns.net', 8097)
    mc = MonitorClient('localhost', 8097)
    #mc.submit_single_graph(gid)
    mc.produce_physical_graphs(gid)
