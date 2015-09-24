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

from dfms import doutils
from dfms.apps.dockerapp import DockerApp
from dfms.data_object import FileDataObject
from dfms.doutils import DOWaiterCtx


class DockerTests(unittest.TestCase):

    def tearDown(self):
        shutil.rmtree("/tmp/sdp_dfms", True)

    def test_docker(self):
        """
        Simple test for a dockerized application. It copies the contents of one
        file into another via the command-line cp utility. It then checks that
        the contents of the target DO are correct, and that the target file is
        actually owned by our process.
        """

        a = FileDataObject('a', 'a')
        b = DockerApp('b', 'b', image='ubuntu:14.04', command='cp %i0 %o0')
        c = FileDataObject('c', 'c')

        b.addInput(a)
        b.addOutput(c)

        # Random data so we always check different contents
        data = ''.join([random.SystemRandom().choice(string.ascii_letters + string.digits) for _ in xrange(10)])
        with DOWaiterCtx(self, c, 100):
            a.write(data)
            a.setCompleted()

        self.assertEquals(data, doutils.allDataObjectContents(c))

        # We own the file, not root
        uid = os.getuid()
        self.assertEquals(uid, os.stat(c.path).st_uid)