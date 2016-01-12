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

import os
import random
import shutil
import string
import unittest
import warnings

from docker.client import AutoVersionClient
from docker.errors import DockerException

from dfms import droputils
from dfms.apps.dockerapp import DockerApp
from dfms.drop import FileDROP, NgasDROP
from dfms.droputils import DROPWaiterCtx

class DockerTests(unittest.TestCase):

    def tearDown(self):
        shutil.rmtree("/tmp/sdp_dfms", True)

    def test_simpleCopy(self):
        """
        Simple test for a dockerized application. It copies the contents of one
        file into another via the command-line cp utility. It then checks that
        the contents of the target DROP are correct, and that the target file is
        actually owned by our process.

        The test will not run if a docker daemon cannot be contacted though;
        this is to avoid failures in machines that don't have a docker service
        running.
        """

        try:
            AutoVersionClient()
        except DockerException:
            warnings.warn("Cannot contact the Docker daemon, skipping docker tests")
            return

        a = FileDROP('a', 'a')
        b = DockerApp('b', 'b', image='ubuntu:14.04', command='cp %i0 %o0')
        c = FileDROP('c', 'c')

        b.addInput(a)
        b.addOutput(c)

        # Random data so we always check different contents
        data = ''.join([random.choice(string.ascii_letters + string.digits) for _ in xrange(10)])
        with DROPWaiterCtx(self, c, 100):
            a.write(data)
            a.setCompleted()

        self.assertEquals(data, droputils.allDropContents(c))

        # We own the file, not root
        uid = os.getuid()
        self.assertEquals(uid, os.stat(c.path).st_uid)

        # Now remove the container
        AutoVersionClient().remove_container(b.containerId)

    def test_clientServer(self):
        """
        A client-server duo. The server outputs the data it receives to its
        output DROP, which in turn is the data held in its input DROP. The graph
        looks like this:

        A --|--> B(client) --|--> D
            |--> C(server) --|

        C is a server application which B connects to. Therefore C must be
        started before B, so B knows C's IP address and connects successfully.
        Although the real writing is done by C, B in this example is also
        treated as a publisher of D. This way D waits for both applications to
        finish before proceeding.
        """
        try:
            AutoVersionClient()
        except DockerException:
            warnings.warn("Cannot contact the Docker daemon, skipping docker tests")
            return

        a = FileDROP('a', 'a')
        b = DockerApp('b', 'b', image='ubuntu:14.04', command='cat %i0 > /dev/tcp/%containerIp[c]%/8000')
        c = DockerApp('c', 'c', image='ubuntu:14.04', command='nc -l 8000 > %o0')
        d = FileDROP('d', 'd')

        b.addInput(a)
        b.addOutput(d)
        c.addInput(a)
        c.addOutput(d)

        # Let 'b' handle its interest in c
        b.handleInterest(c)

        data = ''.join([random.choice(string.ascii_letters + string.digits) for _ in xrange(10)])
        with DROPWaiterCtx(self, d, 100):
            a.write(data)
            a.setCompleted()

        self.assertEquals(data, droputils.allDropContents(d))

        # Now remove the containers
        client = AutoVersionClient()
        for dockerApp in b, c:
            client.remove_container(dockerApp.containerId)

    def test_quotedCommands(self):
        """
        A test to check that commands using quotes are correctly executed, which
        means that their quotes were correctly escaped when the final docker
        command was executed
        """

        def assertMsgIsCorrect(msg, command):
            a = DockerApp('a', 'a', image='ubuntu:14.04', command=command)
            b = FileDROP('b','b')
            a.addOutput(b)
            with DROPWaiterCtx(self, b, 1):
                a.execute()
            self.assertEquals(msg, droputils.allDropContents(b))

        msg = "This is a message with a single quote: '"
        assertMsgIsCorrect(msg, 'echo -n "{0}" > %o0'.format(msg))
        msg = 'This is a message with a double quotes: "'
        assertMsgIsCorrect(msg, "echo -n '{0}' > %o0".format(msg))

    def test_dataURLReference(self):
        """
        A test to check that DROPs other than FileDROPs and DirectoryContainers
        can pass their dataURLs into docker containers
        """
        a = NgasDROP('a', 'a') # not a filesystem-related DROP, we can reference its URL in the command-line
        b = DockerApp('b', 'b', image="ubuntu:14.04", command="echo -n '%iDataURL0' > %o0")
        c = FileDROP('c', 'c')
        b.addInput(a)
        b.addOutput(c)
        with DROPWaiterCtx(self, b, 1):
            a.setCompleted()
        self.assertEquals(a.dataURL, droputils.allDropContents(c))
        print a.dataURL