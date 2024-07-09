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
from dlg.apps.app_base import BarrierAppDROP
from dlg.data.drops.data_base import DataDROP, logger
from dlg.data.io import DataIO
from dlg.manager.composite_manager import DataIslandManager, MasterManager
from dlg.manager.node_manager import NodeManager
from dlg.manager.rest import NMRestServer, CompositeManagerRestServer


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
        self._managerPort = self._popArg(kwargs, "managerPort", 8080)
        self._subgraph = self._popArg(kwargs, "subgraph", {})

    def run(self):
        """
        Use the subgraph data application to get the subgraph data, then use that to
        launch a new session for the engine.
        1. need the 'subgraph' named port
        2. Need to duplicate the input data for the subgraph

        :return:
        """

        port = self._managerPort
        manager = NodeManager('localhost', rpc_port=6667, events_port=5556)
        nm_server = NMRestServer(manager)
        nm_thread = threading.Thread(target=nm_server.start, args=("localhost", port))
        nm_thread.start()
        dim = DataIslandManager([f"localhost:{port}"])
        # dim_server = CompositeManagerRestServer(dim)
        dim.createSession("SubGraphSession")
        dim.addGraphSpec("SubGraphSession", self._subgraph)
        dim.deploySession("SubGraphSession")
