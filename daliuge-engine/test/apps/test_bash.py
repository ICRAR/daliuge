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
Test the different bash-related applications
"""

import os
import shutil
import tempfile
import unittest

from dlg import droputils
from dlg.apps.bash_shell_app import (
    BashShellApp,
    StreamingInputBashApp,
    StreamingOutputBashApp,
    StreamingInputOutputBashApp,
)
from dlg.ddap_protocol import DROPStates
from dlg.drop import FileDROP, InMemoryDROP
from dlg.droputils import DROPWaiterCtx


class BashAppTests(unittest.TestCase):
    def tearDown(self):
        shutil.rmtree("/tmp/daliuge_tfiles", True)

    def test_echo(self):
        a = FileDROP("a", "a")
        b = BashShellApp("b", "b", command="cp %i0 %o0")
        c = FileDROP("c", "c")

        b.addInput(a)
        b.addOutput(c)

        # Random data so we always check different contents
        data = os.urandom(10)
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
            a = BashShellApp("a", "a", command=command)
            b = FileDROP("b", "b")
            a.addOutput(b)
            with DROPWaiterCtx(self, b, 100):
                a.async_execute()
            self.assertEqual(message.encode("utf8"), droputils.allDropContents(b))

        msg = "This is a message with a single quote: '"
        assert_message_is_correct(msg, 'echo -n "{0}" > %o0'.format(msg))
        msg = 'This is a message with a double quotes: "'
        assert_message_is_correct(msg, "echo -n '{0}' > %o0".format(msg))
    
    def test_envvars(self):
        """Checks that the DLG_* environment variables are available to bash programs"""

        app_uid = "a"
        session_id = "session-id"

        # a fake session that has an ID
        class dummy(object):
            pass

        session = dummy()
        session.sessionId = session_id

        def assert_envvar_is_there(varname, value):
            command = "echo -n $%s > %%o0" % (varname)
            a = BashShellApp(app_uid, app_uid, dlg_session=session, command=command)
            b = FileDROP("b", "b")
            a.addOutput(b)
            with DROPWaiterCtx(self, b, 100):
                a.async_execute()
            self.assertEqual(value.encode("utf8"), droputils.allDropContents(b))

        assert_envvar_is_there("DLG_UID", app_uid)
        assert_envvar_is_there("DLG_SESSION_ID", session_id)


class StreamingBashAppTests(unittest.TestCase):
    def test_single_pipe(self):
        """
        A simple test where two bash apps are connected to each other in a
        streaming fashion. The data flows through a pipe which is created by
        the framework. The data drop in between acts only as a intermediator
        to establish the underlying communication channel.

        -------------     --------------     -------------     ----------
        | BashApp A | --> | InMemory B | --> | BashApp C | --> | File D |
        |   echo    |     | "/a/pipe"  |     |    dc     |     |        |
        -----*-------     --------------     ------*------     ----------
             |                                     |
             +-------------|named-pipe|------------+

        BashApp A writes "5 4 3 2 1" (each on a new line), which is read by cat
        and redirected to D.
        """

        output_fname = tempfile.mktemp()

        a = StreamingOutputBashApp("a", "a", command=r"echo -en '5\n4\n3\n2\n1'")
        b = InMemoryDROP("b", "b")
        c = StreamingInputBashApp("c", "c", command="cat > %o0")
        d = FileDROP("d", "d", filepath=output_fname)

        a.addOutput(b)
        c.addStreamingInput(b)
        c.addOutput(d)

        # Let's fire the app
        with DROPWaiterCtx(self, d, 2):
            a.async_execute()

        # The application executed, finished, and its output was recorded
        for drop in (a, b, c, d):
            self.assertEqual(
                DROPStates.COMPLETED,
                drop.status,
                "Drop %r not COMPLETED: %d" % (drop, drop.status),
            )
        self.assertEqual(
            [5, 4, 3, 2, 1], [int(x) for x in droputils.allDropContents(d).split(b"\n")]
        )

        # Clean up and go
        os.remove(output_fname)

    def test_two_simultaneous_pipes(self):
        """
        A more complicated test where three bash applications run at the same
        time. The first streams its output to the second one, while the second
        one streams *its* output to the third one.

        -------------     --------------     -------------     --------------     -------------     ----------
        | BashApp A | --> | InMemory B | --> | BashApp C | --> | InMemory D | --> | BashApp E | --> | File F |
        |   echo    |     | "/pipe1"   |     |    dc     |     | "/pipe2"   |     |   sort    |     |        |
        -----*-------     --------------     ----*--*-----     --------------     -----*-------     ----------
             |                                   |  |                                  |
             +-------------|named-pipe|----------+  +-----------|named-pipe|-----------+

        BashApp A writes "5 4 3 2 1" (each on a new line), which is read
        by "cat" (BashApp C). The printed results (a copy of the original) are
        streamed through D and read by "sort" (BashApp E), which writes the
        output to F.
        """

        output_fname = tempfile.mktemp()

        a = StreamingOutputBashApp("a", "a", command=r"echo -en '5\n4\n3\n2\n1'")
        b = InMemoryDROP("b", "b")
        c = StreamingInputOutputBashApp("c", "c", command="cat")
        d = InMemoryDROP("d", "d")
        e = StreamingInputBashApp("e", "e", command="sort -n > %o0")
        f = FileDROP("f", "f", filepath=output_fname)

        a.addOutput(b)
        b.addStreamingConsumer(c)
        c.addOutput(d)
        d.addStreamingConsumer(e)
        e.addOutput(f)

        # Let's fire the app
        with DROPWaiterCtx(self, f, 2):
            a.async_execute()

        # The application executed, finished, and its output was recorded
        for drop in (a, b, c, d, e, f):
            self.assertEqual(DROPStates.COMPLETED, drop.status)
        self.assertEqual(
            [1, 2, 3, 4, 5],
            [int(x) for x in droputils.allDropContents(f).strip().split(b"\n")],
        )

        # Clean up and go
        os.remove(output_fname)
