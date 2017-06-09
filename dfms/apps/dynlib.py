#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2017
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

import ctypes
import functools

import six

from ..ddap_protocol import AppDROPStates
from ..drop import AppDROP, BarrierAppDROP
from ..exceptions import InvalidDropException


_read_cb_type = ctypes.CFUNCTYPE(ctypes.c_size_t,
                                 ctypes.POINTER(ctypes.c_char),
                                 ctypes.c_size_t)

_write_cb_type = ctypes.CFUNCTYPE(ctypes.c_size_t,
                                  ctypes.POINTER(ctypes.c_char),
                                  ctypes.c_size_t)

_app_running_cb_type = ctypes.CFUNCTYPE(None)

_app_done_cb_type = ctypes.CFUNCTYPE(None, ctypes.c_int)

class CDlgInput(ctypes.Structure):
    _fields_ = [('uid', ctypes.c_char_p),
                ('oid', ctypes.c_char_p),
                ('status', ctypes.c_int),
                ('read', _read_cb_type)]

class CDlgStreamingInput(ctypes.Structure):
    _fields_ = [('uid', ctypes.c_char_p),
                ('oid', ctypes.c_char_p)]

class CDlgOutput(ctypes.Structure):
    _fields_ = [('uid', ctypes.c_char_p),
                ('oid', ctypes.c_char_p),
                ('write', _write_cb_type)]

class CDlgApp(ctypes.Structure):
    _fields_ = [('uid', ctypes.c_char_p),
                ('oid', ctypes.c_char_p),
                ('inputs', ctypes.POINTER(CDlgInput)),
                ('n_inputs', ctypes.c_uint),
                ('streaming_inputs', ctypes.POINTER(CDlgStreamingInput)),
                ('n_streaming_inputs', ctypes.c_uint),
                ('outputs', ctypes.POINTER(CDlgOutput)),
                ('n_outputs', ctypes.c_uint),
                ('running', _app_running_cb_type),
                ('done', _app_done_cb_type),
                ('data', ctypes.c_void_p),]

class DynlibAppBase(object):

    def initialize(self, **kwargs):
        super(DynlibAppBase, self).initialize(**kwargs)

        if 'lib' not in kwargs:
            raise InvalidDropException("library not specified")

        # Try with a simple name, or as full path
        from ctypes.util import find_library
        libname = kwargs.pop('lib')
        lib = find_library(libname) or libname

        self.lib = ctypes.cdll.LoadLibrary(lib)
        expected_functions = ('init_app_drop', 'run')
        for fname in expected_functions:
            if hasattr(self.lib, fname):
                continue
            raise InvalidDropException("%s doesn't have function %s" % (lib, fname))

        # Create the initial contents of the C dlg_app_info structure
        # We pass no inputs because we don't know them (and don't need them)
        # at this point yet.
        # The running and done callbacks are also NULLs
        self._c_app = CDlgApp(six.b(self.uid), six.b(self.oid),
                                   None, 0, None, 0, None, 0,
                                   ctypes.cast(None, _app_running_cb_type),
                                   ctypes.cast(None, _app_done_cb_type),
                                   None)

        # Let the shared library initialize this app
        if self.lib.init_app_drop(ctypes.pointer(self._c_app)):
            raise InvalidDropException("%s app failed during initialization" % (lib,))

    def addOutput(self, outputDrop, back=True):
        super(DynlibAppBase, self).addOutput(outputDrop, back)

        # Update the list of outputs on our app structure
        app = self._c_app

        output = self._to_c_output(outputDrop)
        print("New output: %r" % output)
        outputs = [output]
        if app.n_outputs:
            prev_outputs = (CDlgOutput * app.n_outputs).from_address(ctypes.addressof(app.outputs))
            print("Previous outputs: %r" % (prev_outputs[:],))
            outputs = prev_outputs[:] + outputs
        print("Outputs: %r" % (outputs,))
        app.outputs = (CDlgOutput * (app.n_outputs + 1))(*outputs)
        app.n_outputs += 1

    def _to_c_output(self, o):
        w = _write_cb_type(functools.partial(self._write_to_output, o.write))
        return CDlgOutput(six.b(o.uid), six.b(o.oid), w)

    def _write_to_output(self, output_write, buf, n):
        return output_write(buf[:n])


class DynlibStreamApp(DynlibAppBase, AppDROP):

    def initialize(self, **kwargs):
        super(DynlibStreamApp, self).initialize(**kwargs)

        # Set up callbacks for the library to signal they the application
        # is running, and that it has ended
        def _running():
            self.execStatus = AppDROPStates.RUNNING
        def _done(status):
            self.execStatus = status
            self._notifyAppIsFinished()

        self._c_app.running = _app_running_cb_type(_running)
        self._c_app.done = _app_done_cb_type(_done)

    def dataWritten(self, uid, data):
        app_p = ctypes.pointer(self._c_app)
        self.lib.data_written(app_p, six.b(uid), data, len(data))

    def dropCompleted(self, uid, drop_state):
        app_p = ctypes.pointer(self._c_app)
        self.lib.drop_completed(app_p, six.b(uid), drop_state)

    def addInput(self, inputDrop, back=True):
        super(DynlibStreamApp, self).addInput(inputDrop, back)
        self._c_app.n_inputs += 1

    def addStreamingInput(self, streamingInputDrop, back=True):
        super(DynlibStreamApp, self).addStreamingInput(streamingInputDrop, back)
        self._c_app.n_streaming_inputs += 1


class DynlibApp(DynlibAppBase, BarrierAppDROP):

    def run(self):

        # read / write callbacks
        def _read(input_read, desc, buf, n):
            x = input_read(desc, n)
            ctypes.memmove(ctypes.addressof(buf.contents), x, len(x))
            return len(x)

        # Update our C structure to include inputs, which we open for reading
        inputs = []
        opened_info = []
        for i in self.inputs:
            desc = i.open()
            opened_info.append((i, desc))
            r = _read_cb_type(functools.partial(_read, i.read, desc))
            inputs.append(CDlgInput(six.b(i.uid), six.b(i.oid), i.status, r))
        self._c_app.inputs = (CDlgInput * len(inputs))(*inputs)
        self._c_app.n_inputs = len(inputs)

        try:
            self.lib.run(ctypes.pointer(self._c_app))
        finally:
            for x, desc  in opened_info:
                x.close(desc);