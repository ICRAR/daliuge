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
'''
Created on 20 Jul 2015

@author: rtobar
'''

import codecs
import json
import os
import threading
import unittest

from luigi import scheduler, worker
import pkg_resources
import six

from dlg import droputils
from dlg import graph_loader, utils
from dlg.apps.socket_listener import SocketListenerApp
from dlg.luigi_int import FinishGraphExecution


test_data = os.urandom(16*1024)

class LuigiTests(unittest.TestCase):
    """
    A class with one testing method for each of the graphs created by the
    graphsRepository module. Although I could have written a single method that
    executes automatically all graphs contained in the graphsRepository module
    I preferred to have explicit separated methods for each graph to be able to
    pinpoint failures more easily.
    """
    def test_mwa_fornax_pg(self):
        self._test_graph('mwa_fornax_pg')

    def test_testGraphLuigiDriven(self):
        self._test_graph('testGraphLuigiDriven')

    def test_chiles_pg(self):
        self._test_graph('chiles_pg', 8)

    def test_complex_graph(self):
        self._test_graph('complex_graph', 5)

    def test_container_pg(self):
        self._test_graph('container_pg')

    def test_testGraphMixed(self):
        self._test_graph('testGraphMixed')

    def test_testGraphDropDriven(self):
        self._test_graph('testGraphDropDriven')

    def test_test_pg(self):
        self._test_graph('test_pg')

    def test_complexFromFile(self):
        self._test_graphFromFile("complex.js", 5)

    def _test_graphFromFile(self, f, socketListeners=1):
        with pkg_resources.resource_stream("test", "graphs/%s" % (f)) as f:  # @UndefinedVariable
            self._test_graph(graph_loader.createGraphFromDropSpecList(json.load(codecs.getreader('utf-8')(f))), socketListeners)

    def _test_graph(self, pgCreator, socketListeners=1):
        if isinstance(pgCreator, six.string_types):
            pgCreator = "test.graphsRepository.%s" % (pgCreator)
        task = FinishGraphExecution(pgCreator=pgCreator)
        sch = scheduler.CentralPlannerScheduler()
        w = worker.Worker(scheduler=sch)
        w.add(task)

        # Start executing the SocketListenerApps so they open their ports
        for drop,_ in droputils.breadFirstTraverse(task.roots):
            if isinstance(drop, SocketListenerApp):
                threading.Thread(target=lambda drop: drop.execute(), args=(drop,)).start()

        # Write to the initial nodes of the graph to trigger the graph execution
        for i in range(socketListeners):
            threading.Thread(target=utils.write_to, name='socketWriter', args=("localhost", 1111+i, test_data, 2)).start()

        # Run the graph! Luigi will either monitor or execute the DROPs
        w.run()
        w.stop()

        # ... but at the end all the nodes of the graph should be completed
        # and should exist
        for drop,_ in droputils.breadFirstTraverse(task.roots):
            self.assertTrue(drop.isCompleted() and drop.exists(), "%s is not COMPLETED or doesn't exist" % (drop.uid))

if __name__ == '__main__':
    unittest.main()