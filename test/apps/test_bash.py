#
#    ICRAR - International Centre for Radio Astronomy Research
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
#
"""
Test the Bash App
"""
import os
import random
import shutil
import string
import unittest

from dfms import droputils
from dfms.apps.bash_shell_app import BashShellApp
from dfms.drop import FileDROP
from dfms.droputils import DROPWaiterCtx


class BashAppTests(unittest.TestCase):

    def tearDown(self):
        shutil.rmtree("/tmp/sdp_dfms", True)

    def test_echo(self):
        a = FileDROP('a', 'a')
        b = BashShellApp('b', 'b', command='cp %i0 %o0')
        c = FileDROP('c', 'c')

        b.addInput(a)
        b.addOutput(c)

        # Random data so we always check different contents
        data = ''.join([random.choice(string.ascii_letters + string.digits) for _ in xrange(10)])
        with DROPWaiterCtx(self, c, 100):
            a.write(data)
            a.setCompleted()

        self.assertEqual(data, droputils.allDropContents(c))

        # We own the file, not root
        uid = os.getuid()
        self.assertEqual(uid, os.stat(c.path).st_uid)

    def test_quoted_commands(self):
        """
        A test to check that commands using quotes are correctly executed, which
        means that their quotes were correctly escaped when the final docker
        command was executed
        """

        def assert_message_is_correct(message, command):
            a = BashShellApp('a', 'a', command=command)
            b = FileDROP('b', 'b')
            a.addOutput(b)
            with DROPWaiterCtx(self, b, 100):
                a.execute()
            self.assertEqual(message, droputils.allDropContents(b))

        msg = "This is a message with a single quote: '"
        assert_message_is_correct(msg, 'echo -n "{0}" > %o0'.format(msg))
        msg = 'This is a message with a double quotes: "'
        assert_message_is_correct(msg, "echo -n '{0}' > %o0".format(msg))
