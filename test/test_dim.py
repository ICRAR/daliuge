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
import random
import socket
import string
import threading
import time
import unittest

import Pyro4
from Pyro4.naming import NameServerDaemon

from dfms import doutils
from dfms.dim.data_island_manager import DataIslandManager
from dfms.dom.data_object_mgr import DataObjectMgr
from dfms.utils import portIsOpen


domId = 'lala'
hostname = socket.gethostname()

class TestDIM(unittest.TestCase):

    def setUp(self):

        # Start the NS
        self._nsDaemon = NameServerDaemon()
        threading.Thread(target=lambda:self._nsDaemon.requestLoop()).start()

        # Start a DOM. This is the DOM which the DIM connects to.
        #
        # We start it here to avoid the DIM connecting via SSH to the localhost
        # and spawning a dfmsDOM process; both things need proper setup which we
        # cannot do here (ssh publick key installation, ssh service up, proper
        # environment available, etc)
        #
        # Anyway, this is also useful because we can check that things have
        # occurred at the DOM level in the test cases
        domId = 'dom_' + hostname
        self._dom = DataObjectMgr(domId, False)
        self._domDaemon = Pyro4.Daemon(host='0.0.0.0', port=4000)
        self._nsDaemon.nameserver.register(domId, self._domDaemon.register(self._dom))
        threading.Thread(target=lambda: self._domDaemon.requestLoop()).start()

        # The DIM we're testing
        self._dim = DataIslandManager(domId, [hostname])

    def tearDown(self):
        self._domDaemon.shutdown()
        self._nsDaemon.shutdown()
        while portIsOpen('localhost', Pyro4.config.NS_PORT):
            time.sleep(0.01)

    def test_createSession(self):

        sessionId = 'lalo'
        dim = self._dim

        dim.createSession(sessionId)
        self.assertEquals(1, len(self._dom.getSessionIds()))
        self.assertEquals(sessionId, self._dom.getSessionIds()[0])

    def test_addGraphSpec(self):

        sessionId = 'lalo'
        dim = self._dim

        # No location specified
        graphSpec = [{'oid':'A', 'type':'plain', 'storage':'memory'}]
        self.assertRaises(Exception, dim.addGraphSpec, sessionId, graphSpec)

        # Wrong location specified
        graphSpec = [{'oid':'A', 'type':'plain', 'storage':'memory', 'location':'unknown_host'}]
        self.assertRaises(Exception, dim.addGraphSpec, sessionId, graphSpec)

        # OK
        graphSpec = [{'oid':'A', 'type':'plain', 'storage':'memory', 'location':hostname}]
        dim.createSession(sessionId)
        dim.addGraphSpec(sessionId, graphSpec)
        graphFromDOM = self._dom.getGraph(sessionId)
        self.assertEquals(1, len(graphFromDOM))
        doSpec = graphFromDOM.values()[0]
        self.assertEquals('A', doSpec['oid'])
        self.assertEquals('plain', doSpec['type'])
        self.assertEquals('memory', doSpec['storage'])

    def test_deployGraph(self):

        sessionId = 'lalo'
        graphSpec = [{'oid':'A', 'type':'plain', 'storage':'memory', 'location':hostname, 'consumers':['B']},
                     {'oid':'B', 'type':'app', 'app':'test.graphsRepository.SleepAndCopyApp', 'sleepTime':0, 'outputs':['C'], 'location':hostname},
                     {'oid':'C', 'type':'plain', 'storage':'memory', 'location':hostname}]
        dim = self._dim

        dim.createSession(sessionId)
        dim.addGraphSpec(sessionId, graphSpec)

        # Deploy now and get the uris. With that we get then A's and C's proxies
        uris = dim.deploySession(sessionId)
        a = Pyro4.Proxy(uris['A'])
        c = Pyro4.Proxy(uris['C'])

        data = ''.join([random.choice(string.ascii_letters + string.digits) for _ in xrange(10)])

        with doutils.EvtConsumerProxyCtx(self, c, 3):
            a.write(data)
            a.setCompleted()

        self.assertEquals(data, doutils.allDataObjectContents(c))