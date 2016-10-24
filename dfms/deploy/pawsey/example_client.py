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
import logging
import optparse
import os
import sys

from dfms import droputils
from dfms.dropmake.pg_generator import LG, MySarkarPGTP, MetisPGTP
from dfms.exceptions import InvalidGraphException, DaliugeException
from dfms.manager.client import CompositeManagerClient


logger = logging.getLogger(__name__)


def fname_to_lgname(fname):
    fname = fname.split('/')[-1]
    if fname.endswith('.json'):
        fname = fname[:-5]
    return fname

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
                 num_islands=1,
                 skip_tpl_check=False):

        self._host = mhost
        self._port = mport
        self._dc = CompositeManagerClient(mhost, mport, timeout=10)
        self._algo = algo
        self._zerorun = zerorun
        self._output = output
        self._app = MonitorClient.apps[app] if app else None
        self._nodes = nodes
        self._num_islands = num_islands
        self._skip_tpl_check = skip_tpl_check

    def unroll_physical_graph(self, lg_path, pg_path):
        """
        Unroll the PGT from the original logical graph or
        from reading an existing unrolled PGT
        """

        # Unroll Logical Graph
        if lg_path:
            lg = LG(lg_path, ssid='1')#TODO make a more meaningful session id
            logger.info("Start to unroll {0}".format(lg_path))
            drop_list = lg.unroll_to_tpl()
            logger.info("Unroll completed for {0} with # of Drops: {1}".format(lg_path, len(drop_list)))
            return fname_to_lgname(lg_path), lg, drop_list

        # Load Physical Graph from JSON file
        try:
            with open(pg_path, 'r') as pgf:
                # use the same log info so that log parser can pick up
                logger.info("Start to unroll {0}".format(pg_path))
                drop_list = json.load(pgf)
                logger.info("Unroll completed for {0} with # of Drops: {1}".format(pg_path, len(drop_list)))
        except Exception as exp:
            err_info = 'Fail to load physical graph: {0}'.format(exp)
            logger.error(err_info)
            raise DaliugeException(err_info)

        if (len(drop_list) < 1):
            err_info = "Invalid graph_id {0}".format(pg_path)
            logger.error(err_info)
            raise InvalidGraphException(err_info)

        return fname_to_lgname(pg_path), None, drop_list

    def get_physical_graph(self, lg_path, pg_path, tpl_nodes_len=0, unrolled=None):
        """
        nodes:  If non-empty, is a list of node (e.g. IP addresses, string type),
                    which shoud NOT include the MasterManager's node address

        We will first try finding node list from the `nodes` parameter. If it
        is empty, we will try the DIM or MM manager. If that is also empty,
        we will bail out.
        """
        if (tpl_nodes_len > 0):
            node_list = [] # fake list
            lnl = tpl_nodes_len - self._num_islands
        else:
            node_list = self._nodes or self._dc.nodes()
            lnl = len(node_list) - self._num_islands
        if (lnl == 0):
            raise Exception("Cannot find node list from either managers or external parameters")
        logger.info("Got a node list with %d node managers", lnl)

        if unrolled:
            lgn, lg, drop_list = unrolled
            del unrolled # get ready for garbage collection
        else:
            lgn, lg, drop_list = self.unroll_physical_graph(lg_path, pg_path)

        perform_partition = True
        if (pg_path
            and (not self._skip_tpl_check)
            and ('node' in drop_list[0])
            and drop_list[0]['node'].startswith('#')):
            # template
            perform_partition = False

        if perform_partition:
            logger.info("Initialising PGTP {0}".format(self._algo))
            if 'sarkar' == self._algo:
                pgtp = MySarkarPGTP(drop_list, lnl, merge_parts=True)
            else:
                pgtp = MetisPGTP(drop_list, lnl)
            del drop_list
            logger.info("PGTP initialised {0}".format(self._algo))

            logger.info("Start to translate {0}".format(lgn))
            pgtp.to_gojs_json(string_rep=False)
            logger.info("Translation completed for {0}".format(lgn))

            pg_spec = pgtp.to_pg_spec(node_list, ret_str=False,
                    num_islands=self._num_islands,
                    tpl_nodes_len=tpl_nodes_len)
        else:
            # pg_spec template, fill it with real IP addresses directly
            if (len(node_list) > 0):
                logger.info("Start to translate {0}".format(lgn))
                dim_list = node_list[0:self._num_islands]
                nm_list = node_list[self._num_islands:]
                for drop_spec in drop_list:
                    nidx = int(drop_spec['node'][1:]) # skip '#'
                    drop_spec['node'] = nm_list[nidx]
                    iidx = int(drop_spec['island'][1:]) # skip '#'
                    drop_spec['island'] = dim_list[iidx]
                logger.info("Translation completed for {0}".format(lgn))
            else:
                err_info = "Empty node_list, cannot translate the pg_spec template!"
                logger.error(err_info)
                raise DaliugeException(err_info)
            pg_spec = drop_list

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

    def submit_single_graph(self, lg_path, pg_path, deploy=False, pg=None, tpl_nodes_len=0):
        """
        pg: (if not None) a tuple of (lgn, lg, pg_spec)
        """
        if (pg is None):
            lgn, lg, pg_spec = self.get_physical_graph(lg_path, pg_path, tpl_nodes_len=tpl_nodes_len)
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
        completed_uids = droputils.get_roots(pg_spec)
        logger.info("Len of completed_uids is {0}".format(len(completed_uids)))

        # forget about the objects in memory and work with a json dump form now on
        pg_asjson = json.dumps(pg_spec)
        del pg_spec

        ssid = "{0}-{1}".format(lgn.split('.')[0], '' if (lg is None) else lg._session_id)
        ssid = ssid.replace('/', '-') # slash is not allowed in graph session id
        self._dc.create_session(ssid)
        logger.info("session {0} created".format(ssid))
        self._dc.append_graph(ssid, pg_asjson)
        logger.info("graph {0} appended".format(ssid))

        if (deploy):
            self._dc.deploy_session(ssid, completed_uids=completed_uids)
            logger.info("session {0} deployed".format(ssid))

    def write_physical_graph(self, lg_path, pg_path, tpl_nodes_len=0):
        lgn, _, pg_spec = self.get_physical_graph(lg_path, pg_path, tpl_nodes_len=tpl_nodes_len)
        fname = self._output or '{1}_pgspec.json'.format(lgn.split('.')[0])
        with open(fname, 'w') as f:
            json.dump(pg_spec, f, indent=1)

if __name__ == '__main__':
    """
    Some examples on unrolling / partitioning from existing PGT (full or template):

    python -m dfms.deploy.pawsey.example_client -g 13  -o /tmp/pg_spec_g13_full.json -N 1.2.3.0,1.2.3.1,1.2.3.2,1.2.3.3,1.2.3.4
    python -m dfms.deploy.pawsey.example_client -g /tmp/pg_spec_g13_full.json -N 0.2.3.0,0.2.3.1,0.2.3.2,0.2.3.3 -o /tmp/pg_spec_g13_full_4.json
    python -m dfms.deploy.pawsey.example_client -g /tmp/pg_spec_g13_full_4.json -t 3 -o /tmp/pg_spec_g13_full_3.tpl
    python -m dfms.deploy.pawsey.example_client -o /tmp/pg_spec_g13_full_3.json  -g /tmp/pg_spec_g13_full_3.tpl -N qq,ee,tt
    grep "\"node\": \"tt\"" /tmp/pg_spec_g13_full_3.json | wc -l
    grep "\"node\": \"ee\"" /tmp/pg_spec_g13_full_3.json | wc -l
    grep "\"island\": \"qq\"" /tmp/pg_spec_g13_full_3.json | wc -l
    #TODO write test cases to assert the above (wc1 + wc2 == wc3 == 2226)
    python -m dfms.deploy.pawsey.example_client -o /tmp/pg_spec_g13_7.tpl  -g /tmp/pg_spec_g13_full_3.tpl -t 7 -s
    python -m dfms.deploy.pawsey.example_client -o /tmp/pg_spec_g13_7.json  -g /tmp/pg_spec_g13_full_3.tpl -s -N aa,bb,cc,dd,ee,ff,gg
    """

    parser = optparse.OptionParser()
    parser.add_option("-H", "--host", action="store",
                      dest="host", help="The host where the graph will be deployed", default="localhost")
    parser.add_option("-N", "--nodes", action="store",
                      dest="nodes", help="The nodes where the physical graph will be distributed, comma-separated", default=None)
    parser.add_option("-i", "--islands", action="store", type="int",
                      dest="islands", help="Number of drop islands", default=1)
    parser.add_option("-a", "--action", action="store", type="choice", choices=['submit', 'print'],
                      dest="act", help="The action to perform", default="submit")
    parser.add_option("-A", "--algorithm", action="store", type="choice", choices=['metis', 'sarkar'],
                      dest="algo", help="algorithm used to do the LG --> PG conversion", default="metis")
    parser.add_option("-p", "--port", action="store", type="int",
                      dest="port", help="The port we connect to to deploy the graph", default=8001)
    parser.add_option("-l", "--logical-graph", action="store", type="string",
                      dest="logical_graph", help="The filename of the logical graph to deploy", default=None)
    parser.add_option("-P", "--physical-graph", action="store", type="string",
                      dest="physical_graph", help="The filename of the physical graph (template) to deploy", default=None)
    parser.add_option("-o", "--output", action="store", type="string",
                      dest="output", help="Where to dump the general physical graph", default=None)
    parser.add_option("-z", "--zerorun", action="store_true",
                      dest="zerorun", help="Generate a physical graph that takes no time to run", default=False)
    parser.add_option("--app", action="store", type="int",
                      dest="app", help="The app to use in the PG. 0=SleepApp (default), 1=SleepAndCopy", default=0)
    parser.add_option("-t", "--tpl-nodes-len", action="store", type="int",
                      dest="tpl_nodes_len", help="node list length for generating pg_spec template", default=0)
    parser.add_option("-s", "--skip-tpl-check", action="store_true",
                      dest="skip_tpl_check", help="Whether to skip checking if a pg_spec is a template", default=False)

    (opts, args) = parser.parse_args(sys.argv)

    fmt = "%(asctime)-15s [%(levelname)5.5s] [%(threadName)15.15s] %(name)s#%(funcName)s:%(lineno)s %(message)s"
    logging.basicConfig(level=logging.INFO, format=fmt)

    if not opts.logical_graph and not opts.physical_graph:
        parser.error("Missing logical graph or physical graph template filename")
    if opts.logical_graph and opts.physical_graph:
        parser.error("Either a logical graph or physical graph filename must be specified")
    for p in (opts.logical_graph, opts.physical_graph):
        if p and not os.path.exists(p):
            parser.error("Cannot locate graph file at '{0}'".format(p))
    if opts.act == 'print' and not opts.output:
        parser.error('Action "print" requires a valid -o value')

    nodes = [n for n in opts.nodes.split(',') if n] if opts.nodes else []
    mc = MonitorClient(opts.host, opts.port, output=opts.output, algo=opts.algo,
                       zerorun=opts.zerorun, app=opts.app, nodes=nodes,
                       num_islands=opts.islands, skip_tpl_check=opts.skip_tpl_check)

    if 'submit' == opts.act:
        mc.submit_single_graph(opts.logical_graph, opts.physical_graph, deploy=True, tpl_nodes_len=opts.tpl_nodes_len)
    elif 'print' == opts.act:
        mc.write_physical_graph(opts.logical_graph, opts.physical_graph, tpl_nodes_len=opts.tpl_nodes_len)
    else:
        raise Exception('Unknown action: {0}'.format(opts.act))