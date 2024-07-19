#
#    ICRAR - International Centre for Radio Astronomy Research 2024
#    (c) UWA - The University of Western Australia
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

import threading
import time

from dlg.common import tool, terminate_or_kill, get_roots
from dlg.apps.app_base import BarrierAppDROP
from dlg.data.drops.data_base import DataDROP, logger
from dlg.data.io import DataIO
from dlg.manager.composite_manager import DataIslandManager, MasterManager
from dlg.manager.node_manager import NodeManager
from dlg.manager.rest import NMRestServer, CompositeManagerRestServer
from dlg.dropmake.pg_generator import unroll, partition

from dlg.ddap_protocol import (
    AppDROPStates,
    DROPStates,
)


##
# @brief SubGraphApp
# @par EAGLE_START
# @param category PythonApp
# @param tag daliuge
# @param manager_port_number 8080/Integer/ApplicationArgument/NoPort/ReadWrite//False/False/The port number of the SubGraph Node Manager
# @param hostname localhost/string/ApplicationArgument/NoPort/ReadWrite//False/False/The host name of the SubGraph NodeManager
# @param dropclass dlg.apps.simple.SleepAndCopyApp/String/ComponentParameter/NoPort/ReadOnly//False/False/
# @param execution_time 5/Float/ConstraintParameter/NoPort/ReadOnly//False/False/Estimated execution time
# @param num_cpus 1/Integer/ConstraintParameter/NoPort/ReadOnly//False/False/Number of cores used
# @param group_start False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Is this node the start of a group?
# @param input_parser pickle/Select/ComponentParameter/NoPort/ReadWrite/raw,pickle,eval,npy,path,dataurl/False/False/Input port parsing technique
# @param output_parser pickle/Select/ComponentParameter/NoPort/ReadWrite/raw,pickle,eval,npy,path,dataurl/False/False/Output port parsing technique
# @par EAGLE_END
class SubGraphApp(BarrierAppDROP):

    def initialize(self, **kwargs):
        super(SubGraphApp, self).initialize(**kwargs)
        self._nm = self._popArg(kwargs, "nodeManagerPort", 8080)
        self._dim = self._popArg(kwargs, "islandManagerPort", 8081)
        self._subgraph = self._popArg(kwargs, "subgraph", {})
        # TODO use the node manage and island manager ports
        self._nodes = self._popArg(kwargs, "nodes", ['localhost:8080'])
        self._islands = self._popArg(kwargs, "nodes", ['localhost:8081'])

    def _pollRESTInterface(self, manager, pollFrequencyInSeconds: int = 30):
        """
        :param pollFrequencyInSeconds:
        :return:
        """
        finished = True
        for host, status in manager.getGraphStatus("subgraphsession").items():
            if (status['status'] != DROPStates.ERROR
                    and status['status'] != DROPStates.COMPLETED):
                finished = False
        time.sleep(pollFrequencyInSeconds)
        return finished

    def _translateSubGraph(self, node_list=None):
        algo = "metis"
        # c = RestClient("localhost", 8090, timeout=10)
        pgt = unroll(self._subgraph)
        pg = partition(
            pgt,
            algo=algo,
            num_partitions=1,
            num_islands=1,
            partition_label="Partition",
            show_gojs=True,
            max_cpu=8,
            max_load_imb=100
        )
        return pg.to_pg_spec(node_list, ret_str=False)

    def run(self):
        """
        Use the subgraph data application to get the subgraph data, then use that to
        launch a new session for the engine.
        1. need the 'subgraph' named port
        2. Need to duplicate the input data for the subgraph

        :return:
        """

        node_manager = NodeManager('localhost', rpc_port=6667, events_port=5556)
        nm_server = NMRestServer(node_manager)
        nm_thread = threading.Thread(target=nm_server.start, args=("localhost", self._nm))
        nm_thread.start()
        dim = DataIslandManager(self._nodes)
        dim_server = CompositeManagerRestServer(dim)
        dim_thread = threading.Thread(target=dim_server.start,
                                      args=("localhost", self._dim))
        dim_thread.start()

        graphspec = {}
        nodes = [i for i in self._islands]
        nodes.extend(self._nodes)
        try:
            graphspec = self._translateSubGraph(nodes)
        except Exception as e:
            logger.debug("Exception when translating subgraph: %s", e)
            node_manager.shutdown()
            nm_server.stop()
            dim.shutdown()
            self.execStatus = AppDROPStates.CANCELLED
            self.status = DROPStates.CANCELLED
            return
        dim.createSession("subgraphsession")
        dim.addGraphSpec("subgraphsession", graphspec)
        # Get roots
        roots = get_roots(graphspec)
        dim.deploySession("subgraphsession", completedDrops=roots)
        while not self._pollRESTInterface(manager=dim):
            logger.info("Running subgraph externally")

        logger.info("Subgraph finished executing")
        node_manager.shutdown()
        nm_server.stop()
        dim.shutdown()