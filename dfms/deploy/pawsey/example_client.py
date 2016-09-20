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
Examples on how to interact with the Drop Island Manager.

This program first converts a logical graph into a physical graph, and then
create a session, appends the physical graph, and deploys the session to execute
the converted physical graph in the Drop Island.
"""

import json
import optparse
import sys

import pkg_resources, logging

from dfms import droputils
from dfms.dropmake.pg_generator import LG, MySarkarPGTP, MetisPGTP
from dfms.manager.client import CompositeManagerClient


logger = logging.getLogger(__name__)

lgnames = ['lofar_std.json', 'chiles_two.json', 'test_grpby_gather.json', #2
'chiles_two_dev1.json', 'chiles_simple.json', 'mwa_gleam.json', #5
'mwa_gleam_simple.json', 'lofar_std_large.json', 'chiles_two_dev2.json',#8
'lofar_test_2x4.json', 'lofar_test_4x4.json', 'lofar_test_4x8.json',#11
'lofar_test_8x8.json', 'lofar_test_8x16.json', 'lofar_test_16x16.json',#14
'lofar_test_16x32.json', 'lofar_test_32x32.json', 'lofar_test_32x64.json',#17
'lofar_test_64x64.json', 'lofar_test_64x128.json', 'lofar_test_128x128.json',#20
'lofar_test_128x256.json', 'lofar_test_256x256.json', 'lofar_test_256x512.json',#23
'simple_test_500.json', 'simple_test_1000.json', 'simple_test_2000.json']#26


class MonitorClient(object):

    apps = (
        "test.graphsRepository.SleepApp",
        "test.graphsRepository.SleepAndCopyApp",
    )

    def __init__(self,
                 mhost, mport, timeout=10,
                 algo='sarkar',
                 output=None,
                 zerorun=False,
                 app=None,
                 nodes=[],
                 num_islands=1):

        self._host = mhost
        self._port = mport
        self._dc = CompositeManagerClient(mhost, mport, timeout=10)
        self._algo = algo
        self._zerorun = zerorun
        self._output = output
        self._app = MonitorClient.apps[app] if app else None
        self._nodes = nodes
        self._num_islands = num_islands

    def get_physical_graph(self, graph_id):
        """
        nodes:  If non-empty, is a list of node (e.g. IP addresses, string type),
                    which shoud NOT include the MasterManager's node address

        We will first try finding node list from the `nodes` parameter. If it
        is empty, we will try the DIM or MM manager. If that is also empty,
        we will bail out.
        """
        node_list = self._nodes or self._dc.nodes()
        lnl = len(node_list) - self._num_islands
        if (lnl == 0):
            raise Exception("Cannot find node list from either managers or external parameters")
        logger.info("Got a node list with {0} nodes".format(lnl))

        lgn = lgnames[graph_id]
        fp = pkg_resources.resource_filename('dfms.dropmake', 'web/{0}'.format(lgn))  # @UndefinedVariable
        lg = LG(fp)
        logger.info("Start to unroll {0}".format(lgn))
        drop_list = lg.unroll_to_tpl()
        logger.info("Unroll completed for {0} with # of Drops: {1}".format(lgn, len(drop_list)))

        if 'sarkar' == self._algo:
            pgtp = MySarkarPGTP(drop_list, lnl, merge_parts=True)
        else:
            pgtp = MetisPGTP(drop_list, lnl)

        # Trigering something...
        logger.info("Start to translate {0}".format(lgn))
        pgtp.to_gojs_json(string_rep=False)
        logger.info("Translation completed for {0}".format(lgn))

        pg_spec = pgtp.to_pg_spec(node_list, ret_str=False, num_islands=self._num_islands)
        logger.info("PG spec is calculated!")

        if self._zerorun:
            for dropspec in pg_spec:
                if 'sleepTime' in dropspec:
                    dropspec['sleepTime'] = 0
        app = self._app
        if app:
            for dropspec in pg_spec:
                if 'app' in dropspec:
                    dropspec['app'] = app

        return lgn, lg, pg_spec

    def submit_single_graph(self, graph_id, deploy=False, pg=None):
        """
        pg: (if not None) a tuple of (lgn, lg, pg_spec)
        """
        if (pg is None):
            lgn, lg, pg_spec = self.get_physical_graph(graph_id)
        else:
            lgn, lg, pg_spec = pg

        if self._output:
            with open(self._output, 'w') as f:
                json.dump(pg_spec, f, indent=2)

        def uid_for_drop(dropSpec):
            if 'uid' in dropSpec:
                return dropSpec['uid']
            return dropSpec['oid']

        logger.info("About to compute roots")
        completed_uids = [uid_for_drop(x) for x in droputils.get_roots(pg_spec)]
        logger.info("Len of completed_uids is {0}".format(len(completed_uids)))
        ssid = "{0}-{1}".format(lgn.split('.')[0], lg._session_id)
        self._dc.create_session(ssid)
        logger.info("session {0} created".format(ssid))
        self._dc.append_graph(ssid, pg_spec)
        logger.info("graph {0} appended".format(ssid))

        if (deploy):
            self._dc.deploy_session(ssid, completed_uids=completed_uids)
            logger.info("session {0} deployed".format(ssid))

    def write_physical_graph(self, graph_id):
        lgn, _, pg_spec = self.get_physical_graph(graph_id)
        fname = self._output or '{1}_pgspec.json'.format(lgn.split('.')[0])
        with open(fname, 'w') as f:
            json.dump(pg_spec, f, indent=1)

if __name__ == '__main__':

    parser = optparse.OptionParser()
    parser.add_option("-l", "--list", action="store_true",
                      dest="list_graphs", help="List available graphs and exit", default=False)
    parser.add_option("-H", "--host", action="store",
                      dest="host", help="The host where the graph will be deployed", default="localhost")
    parser.add_option("-N", "--nodes", action="store",
                      dest="nodes", help="The nodes where the physical graph will be distributed, comma-separated", default="localhost")
    parser.add_option("-i", "--islands", action="store", type="int",
                      dest="islands", help="Number of drop islands", default=1)
    parser.add_option("-a", "--action", action="store",
                      dest="act", help="action, either 'submit' or 'print'", default="submit")
    parser.add_option("-A", "--algorithm", action="store",
                      dest="algo", help="algorithm used to do the LG --> PG conversion, either 'metis' or 'sarkar'", default="sarkar")
    parser.add_option("-p", "--port", action="store", type="int",
                      dest="port", help="The port we connect to to deploy the graph", default=8001)
    parser.add_option("-g", "--graph-id", action="store", type="int",
                      dest="graph_id", help="The graph to deploy (0 - 7)", default=None)
    parser.add_option("-o", "--output", action="store", type="string",
                      dest="output", help="Where to dump the general physical graph", default=None)
    parser.add_option("-z", "--zerorun", action="store_true",
                      dest="zerorun", help="Generate a physical graph that takes no time to run", default=False)
    parser.add_option("--app", action="store", type="int",
                      dest="app", help="The app to use in the PG. 0=SleepApp (default), 1=SleepAndCopy", default=0)

    (opts, args) = parser.parse_args(sys.argv)

    if opts.list_graphs:
        print '\n'.join(["%2d: %s" % (i,g) for i,g in enumerate(lgnames)])
        sys.exit(0)

    fmt = "%(asctime)-15s [%(levelname)5.5s] [%(threadName)15.15s] %(name)s#%(funcName)s:%(lineno)s %(message)s"
    logging.basicConfig(level=logging.INFO, format=fmt)

    if opts.graph_id is None:
        parser.error("Missing -g")
    if opts.graph_id >= len(lgnames):
        parser.error("-g must be between 0 and %d" % (len(lgnames),))

    nodes = opts.nodes.split(',')
    mc = MonitorClient(opts.host, opts.port, output=opts.output, algo=opts.algo,
                       zerorun=opts.zerorun, app=opts.app, nodes=nodes,
                       num_islands=opts.islands)

    if 'submit' == opts.act:
        mc.submit_single_graph(opts.graph_id, deploy=True)
    elif 'print' == opts.act:
        mc.write_physical_graph(opts.graph_id)
    else:
        raise Exception('Unknown action: {0}'.format(opts.act))
