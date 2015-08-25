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

import os
import threading
import unittest

from luigi import scheduler, worker
import pkg_resources

from dfms import doutils, graph_loader
from dfms.luigi_int import FinishGraphExecution
import graphsRepository


test_data = str(bytearray(os.urandom(16*1024)))

class LuigiTests(unittest.TestCase):
    """
    A class with one testing method for each of the graphs created by the
    graphsRepository module. Although I could have written a single method that
    executes automatically all graphs contained in the graphsRepository module
    I preferred to have explicit separated methods for each graph to be able to
    pinpoint failures more easily.
    """

    def setUp(self):
        super(LuigiTests, self).setUp()
        self.prevDef = graphsRepository.defaultSleepTime
        graphsRepository.defaultSleepTime = 0

    def tearDown(self):
        graphsRepository.defaultSleepTime = self.prevDef
        super(LuigiTests, self).tearDown()

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

    def test_testGraphDODriven(self):
        self._test_graph('testGraphDODriven')

    def test_test_pg(self):
        self._test_graph('test_pg')

    def test_complexFromFile(self):
        self._test_graphFromFile("complex.js", 5)

    def _test_graphFromFile(self, f, socketListeners=1):
        f = pkg_resources.resource_stream("test", "graphs/%s" % (f))  # @UndefinedVariable
        self._test_graph(graph_loader.readObjectGraph(f), socketListeners)

    def _test_graph(self, pgCreator, socketListeners=1):
        if isinstance(pgCreator, basestring):
            pgCreator = "test.graphsRepository.%s" % (pgCreator)
        task = FinishGraphExecution(pgCreator=pgCreator)
        sch = scheduler.CentralPlannerScheduler()
        w = worker.Worker(scheduler=sch)
        w.add(task)

        # Write to the initial nodes of the graph to trigger the graph execution
        for i in xrange(socketListeners):
            port = 1111 + i
            t = threading.Thread(None, self.writeToLocalhostSocket, 'socketWriter', [port])
            t.daemon = 1
            t.start()

        # Run the graph! Luigi will either monitor or execute the DOs
        w.run()

        # ... but at the end all the nodes of the graph should be completed
        # and should exist
        doutils.breadFirstTraverse(task.roots,\
                                   lambda do: self.assertTrue(do.isCompleted() and do.exists(), "%s is not COMPLETED or doesn't exist" % (do.uid)))

    def writeToLocalhostSocket(self, port):
        import socket
        socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        socket.connect(("localhost", port))
        socket.send(test_data)
        socket.close()

if __name__ == '__main__':
    unittest.main()