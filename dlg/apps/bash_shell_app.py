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
Module containing bash-related AppDrops

The module contains four classes that offer running bash commands in different
execution modes; that is, in fully batch mode, or with its input and/or output
as a stream of data to the previous/next application.
"""

import contextlib
import logging
import os
import socket
import struct
import subprocess
import tempfile
import threading
import time
import types

import six

from .. import droputils, utils
from ..ddap_protocol import AppDROPStates, DROPStates
from ..drop import BarrierAppDROP, AppDROP
from ..exceptions import InvalidDropException


logger = logging.getLogger(__name__)

def mesage_stdouts(prefix, stdout, stderr, enc='utf8'):
    msg = prefix
    if not stdout and not stderr:
        return msg
    msg += ", output follows:"
    if stdout:
        msg += "\n==STDOUT==\n" + utils.b2s(stdout, enc)
    if stderr:
        msg += "\n==STDERR==\n" + utils.b2s(stderr, enc)
    return msg

def run_bash(cmd, inputs, outputs, stdin=None, stdout=subprocess.PIPE):
    """
    Runs the given `cmd`. If any `inputs` and/or `outputs` are given
    (dictionaries of uid:drop elements) they are used to replace any placeholder
    value in `cmd` with either drop paths or dataURLs.

    `stdin` indicates at file descriptor or file object to use as the standard
    input of the bash process. If not given no stdin is given to the process.

    Similarly, `stdout` is a file descriptor or file object where the standard
    output of the process is piped to. If not given it is consumed by this
    method and potentially logged.
    """

    # Replace inputs/outputs in command line with paths or data URLs
    fsInputs = {uid: i for uid,i in inputs.items() if droputils.has_path(i)}
    fsOutputs = {uid: o for uid,o in outputs.items() if droputils.has_path(o)}
    cmd = droputils.replace_path_placeholders(cmd, fsInputs, fsOutputs)

    dataURLInputs = {uid: i for uid,i in inputs.items() if not droputils.has_path(i)}
    dataURLOutputs = {uid: o for uid,o in outputs.items() if not droputils.has_path(o)}
    cmd = droputils.replace_dataurl_placeholders(cmd, dataURLInputs, dataURLOutputs)

    # Wrap everything inside bash
    cmd = ('/bin/bash', '-c', cmd)
    logger.debug("Command after user creation and wrapping is: %s", cmd)

    start = time.time()

    # Run and wait until it finishes
    process = subprocess.Popen(cmd,
                               close_fds=True,
                               stdin=stdin,
                               stdout=stdout,
                               stderr=subprocess.PIPE,
                               env=os.environ.copy())

    logger.debug("Process launched, waiting now...")

    pstdout, pstderr = process.communicate()
    if stdout != subprocess.PIPE:
        pstdout = b"<piped-out>"
    pcode = process.returncode

    end = time.time()
    logger.info("Finished in %.3f [s] with exit code %d", (end-start), pcode)

    if pcode == 0 and logger.isEnabledFor(logging.DEBUG):
        logger.debug(mesage_stdouts("Command finished successfully", pstdout, pstderr))
    elif pcode != 0:
        message = "Command didn't finish successfully (exit code %d)" % (pcode,)
        logger.error(mesage_stdouts(message, pstdout, pstderr))
        raise Exception(message)

def close_and_remove(fo, fname):
    fo.close()
    try:
        os.remove(fname)
        logger.debug("Removed %s", fname)
    except OSError:
        logger.exception("Error while removing %s", fname)

def prepare_output_channel(this_node, out_drop):
    """
    Prepares an output channel that will serve as the stdout of a bash command.
    Depending on the values of ``this_node`` and ``out_drop`` the channel will
    be a named pipe or a socket.
    """

    # If the output drop is local then we set up a named pipe
    # otherwise we set up a socket server on our side,
    # which will result in a socket client on the other side
    pipe_name = None
    if out_drop.node == this_node:
        pipe_name = tempfile.mktemp()
        os.mkfifo(pipe_name)
        logger.debug("Created named pipe %s", pipe_name)

        # the pipe needs to be opened after the data is sent to the other
        # application because open() blocks until the other end is also
        # opened
        data = six.b("pipe://%s" % (pipe_name,))
        out_drop.write(data)
        return open(pipe_name, 'wb')

    else:
        host = this_node or socket.gethostname()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((host, 0))
        sock.listen(1)
        port = sock.getsockname()[1]
        logger.debug("Created TCP socket server at %s:%d", host, port)

        # to get a connection from the other side we have to write the data
        # into the output drop first so the other side connects to us
        out_drop.write(six.b("tcp://%s:%d" % (host, port)))
        with contextlib.closing(sock):
            csock, csockaddr = sock.accept()
            csock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 1000))
            logger.debug("Received connection from %r", csockaddr)
            return csock

def prepare_input_channel(data):
    """
    Prepares an input channel that will serve as the stdin of a bash command.
    Depending on the contents of ``data`` the channel will be a named pipe or a
    socket.
    """

    # We don't even look at "data", we simply set up a communication channel
    if data.startswith(b'pipe://'):
        pipe_name = data[7:]
        pipe = open(pipe_name, 'rb')
        logger.debug("Opened pipe %s for reading", pipe_name)

        # Return an object that runs pipe.read on read()
        # and close_and_remove on close()
        chan = lambda: None # a simple object where attributes can be set!
        chan.read = pipe.read
        chan.fileno = pipe.fileno
        chan.close = types.MethodType(lambda s: close_and_remove(pipe, pipe_name), chan)
        return chan

    elif data.startswith(b'tcp://'):
        host, port = data[6:].split(b':')
        host = host.decode('utf-8')
        port = int(port)
        sock = utils.connect_to(host, port, 10)
        sock.settimeout(None)
        logger.debug("Connected to TCP socket %s:%d for reading", host, port)
        return sock

    raise Exception("Unsupported streaming channel: %s", data)

class BashShellBase(object):
    """
    Common class for BashShell apps. It simply requires a command to be
    specified.
    """

    def initialize(self, **kwargs):
        super(BashShellBase, self).initialize(**kwargs)

        self._command = self._getArg(kwargs, 'command', None)
        if not self._command:
            raise InvalidDropException(self, 'No command specified, cannot create BashShellApp')

    def dataURL(self):
        return type(self).__name__

class StreamingInputBashAppBase(BashShellBase, AppDROP):
    """
    Base class for bash command applications that consume a stream of incoming
    data.
    """

    def initialize(self, **kwargs):
        BashShellBase.initialize(self, **kwargs)
        self._this_finished = False
        self._prev_finished = False
        self._notified = False

    def notify_if_finished(self):
        if not self._notified and self._prev_finished and self._this_finished:
            self._notified = True
            self._notifyAppIsFinished()

    def dropCompleted(self, uid, drop_state):
        self._prev_finished = True
        self.notify_if_finished()

    def dataWritten(self, uid, data):
        threading.Thread(target=self.execute, args=(data,)).start()

    def execute(self, data):
        logger.debug("Received incoming data connection info: %s", data)
        self.execStatus = AppDROPStates.RUNNING
        try:
            self.run(data)
            drop_state = DROPStates.COMPLETED
            execStatus = AppDROPStates.FINISHED
        except:
            logger.exception('Error while executing %r' % (self,))
            drop_state = DROPStates.ERROR
            execStatus = AppDROPStates.ERROR
        finally:
            self.execStatus = execStatus
            self.status = drop_state
            self._this_finished = True
            self.notify_if_finished()

#
# Now the actual 4 classes:
# * batch
# * output-only stream
# * input-only stream
# * full-stream
#
class BashShellApp(BashShellBase, BarrierAppDROP):
    """
    An app that runs a bash command in batch mode; that is, it waits until all
    its inputs are COMPLETED. It also *doesn't* output a stream of data; see
    StreamingOutputBashApp for those cases.
    """
    def run(self):
        run_bash(self._command, self._inputs, self._outputs)

class StreamingOutputBashApp(BashShellBase, BarrierAppDROP):
    """
    Like BashShellApp, but its stdout is a stream of data that is fed into the
    next application.
    """
    def run(self):
        with contextlib.closing(prepare_output_channel(self.node, self.outputs[0])) as outchan:
            run_bash(self._command, self._inputs, {}, stdout=outchan)
        logger.debug("Closed output channel")

class StreamingInputBashApp(StreamingInputBashAppBase):
    """
    An app that runs a bash command that consumes data from stdin.

    The streaming of data that appears on stdin takes place outside the
    framework; what is streamed through the framework is the information needed
    to establish the streaming channel. This information is also used to kick
    this application off.
    """
    def run(self, data):
        with contextlib.closing(prepare_input_channel(data)) as inchan:
            run_bash(self._command, {}, self._outputs, stdin=inchan)
        logger.debug("Closed input channel")

class StreamingInputOutputBashApp(StreamingInputBashAppBase):
    """
    Like StreamingInputBashApp, but its stdout is also a stream of data that is
    fed into the next application.
    """
    def run(self, data):
        with contextlib.closing(prepare_input_channel(data)) as inchan:
            with contextlib.closing(prepare_output_channel(self.node, self.outputs[0])) as outchan:
                run_bash(self._command, {}, {}, stdout=outchan, stdin=inchan)
            logger.debug("Closed output channel")
        logger.debug("Closed input channel")