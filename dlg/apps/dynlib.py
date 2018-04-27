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
import logging
import multiprocessing
import threading

import six

from .. import rpc, utils
from ..ddap_protocol import AppDROPStates
from ..drop import AppDROP, BarrierAppDROP
from ..exceptions import InvalidDropException


logger = logging.getLogger(__name__)

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
                ('name', ctypes.c_char_p),
                ('status', ctypes.c_int),
                ('read', _read_cb_type)]

class CDlgStreamingInput(ctypes.Structure):
    _fields_ = [('uid', ctypes.c_char_p),
                ('oid', ctypes.c_char_p),
                ('name', ctypes.c_char_p)]

class CDlgOutput(ctypes.Structure):
    _fields_ = [('uid', ctypes.c_char_p),
                ('oid', ctypes.c_char_p),
                ('name', ctypes.c_char_p),
                ('write', _write_cb_type)]

class CDlgApp(ctypes.Structure):
    _fields_ = [('appname', ctypes.c_char_p),
                ('uid', ctypes.c_char_p),
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

def _to_c_input(i):
    """
    Convert an input drop into its corresponding C structure
    """

    def _read(_i, desc, buf, n):
        x = _i.read(desc, n)
        ctypes.memmove(buf, x, len(x))
        return len(x)

    desc = i.open()
    r = _read_cb_type(functools.partial(_read, i, desc))
    c_input = CDlgInput(six.b(i.uid), six.b(i.oid), six.b(i.name), i.status, r)
    return desc, c_input

def _to_c_output(o):
    """
    Convert an output drop into its corresponding C structure
    """

    def _write(_o, buf, n):
        return _o.write(buf[:n])

    w = _write_cb_type(functools.partial(_write, o))
    return CDlgOutput(six.b(o.uid), six.b(o.oid), six.b(o.name), w)

def prepare_c_inputs(c_app, inputs):
    """
    Converts all inputs to its C equivalents and sets them into `c_app`
    """

    c_inputs = []
    opened_info = []
    for i in inputs:
        desc, c_input = _to_c_input(i)
        opened_info.append((i, desc))
        c_inputs.append(c_input)
    c_app.inputs = (CDlgInput * len(c_inputs))(*c_inputs)
    c_app.n_inputs = len(c_inputs)
    return opened_info

def prepare_c_outputs(c_app, outputs):
    """
    Converts all outputs to its C equivalents and sets them into `c_app`
    """

    c_outputs = [_to_c_output(o) for o in outputs]
    c_app.outputs = (CDlgOutput * len(c_outputs))(*c_outputs)
    c_app.n_outputs = len(c_outputs)

def run(lib, c_app, opened_info):
    """
    Invokes the `run` method on `lib` with the given `c_app`. After completion,
    all opened file descriptors are closed.
    """
    try:
        if lib.run(ctypes.pointer(c_app)):
            raise Exception("Invocation of %r:run returned with status != 0" % lib)
    finally:
        for x, desc  in opened_info:
            x.close(desc);

class InvalidLibrary(Exception):
    pass

def load_and_init(libname, oid, uid, params):
    """
    Loads and initializes `libname` with the given parameters, prepares the
    corresponding C application structure, and returns both objects
    """

    # Try with a simple name, or as full path
    from ctypes.util import find_library
    libname = find_library(libname) or libname

    lib = ctypes.cdll.LoadLibrary(libname)
    logger.info("Loaded %s as %r", libname, lib)
    expected_functions = ('init', 'run')
    for fname in expected_functions:
        if hasattr(lib, fname):
            continue
        raise InvalidLibrary("%s doesn't have function %s" % (libname, fname))

    # Create the initial contents of the C dlg_app_info structure
    # We pass no inputs because we don't know them (and don't need them)
    # at this point yet.
    # The running and done callbacks are also NULLs
    c_app = CDlgApp(None, six.b(uid), six.b(oid),
                    None, 0, None, 0, None, 0,
                    ctypes.cast(None, _app_running_cb_type),
                    ctypes.cast(None, _app_done_cb_type),
                    None)


    # Collect the rest of the parameters to pass them down to the library
    # We need to keep them in a local variable so when we expose them to
    # the app later on via pointers we still have their contents
    local_params = [(six.b(str(k)), six.b(str(v))) for k, v in params.items()]
    logger.debug("Extra parameters passed to application: %r", local_params)

    # Wrap in ctypes
    str_ptr_type = ctypes.POINTER(ctypes.c_char_p)
    two_str_type = (ctypes.c_char_p * 2)
    app_params = [two_str_type(k, v) for k, v in local_params]
    app_params.append(None)
    params = (str_ptr_type * len(app_params))(*app_params)

    # Let the shared library initialize this app
    if lib.init(ctypes.pointer(c_app), params):
        raise InvalidLibrary("%s failed during initialization" % (libname,))

    return lib, c_app

class DynlibAppBase(object):

    def initialize(self, **kwargs):
        super(DynlibAppBase, self).initialize(**kwargs)

        if 'lib' not in kwargs:
            raise InvalidDropException(self, "library not specified")

        try:
            self.lib, self._c_app = load_and_init(kwargs.pop('lib'), self.oid, self.uid, kwargs)
        except InvalidLibrary as e:
            raise InvalidDropException(self, e.args[0])

        # Have we properly set the outputs in the C application structure yet?
        self._c_outputs_set = False
        self._c_outputs_setting_lock = threading.Lock()

    def _ensure_c_outputs_are_set(self):

        with self._c_outputs_setting_lock:
            if self._c_outputs_set:
                return
            prepare_c_outputs(self._c_app, self.outputs)

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
        self._ensure_c_outputs_are_set()
        app_p = ctypes.pointer(self._c_app)
        self.lib.data_written(app_p, six.b(uid), data, len(data))

    def dropCompleted(self, uid, drop_state):
        self._ensure_c_outputs_are_set()
        app_p = ctypes.pointer(self._c_app)
        self.lib.drop_completed(app_p, six.b(uid), drop_state)

    def addInput(self, inputDrop, back=True):
        super(DynlibStreamApp, self).addInput(inputDrop, back)
        self._c_app.n_inputs += 1

    def addStreamingInput(self, streamingInputDrop, back=True):
        super(DynlibStreamApp, self).addStreamingInput(streamingInputDrop, back)
        self._c_app.n_streaming_inputs += 1


class DynlibApp(DynlibAppBase, BarrierAppDROP):
    """Loads a dynamic library into the current process and runs it"""

    def run(self):
        opened_info = prepare_c_inputs(self._c_app, self.inputs)
        self._ensure_c_outputs_are_set()
        run(self.lib, self._c_app, opened_info)

def _run_in_proc(queue, libname, oid, uid, params, inputs, outputs):

    # Step 1: initialise the library and return if there is an error
    try:
        lib, c_app = load_and_init(libname, oid, uid, params)
        queue.put(None)
    except Exception as e:
        # The other end will read this
        queue.put(e)
        return

    client = rpc.RPCClient()
    try:
        client.start()

        # Step 2: setup DropProxy objects for both inputs and outputs
        try:
            to_drop_proxy = lambda x: rpc.DropProxy(client, x[0], x[1], x[2], x[3])
            inputs = [to_drop_proxy(i) for i in inputs]
            outputs = [to_drop_proxy(o) for o in outputs]
            queue.put(None)
        except Exception as e:
            queue.put(e)
            return

        # Step 3: Finish initializing the C structure and run the application
        try:
            opened_info = prepare_c_inputs(c_app, inputs)
            prepare_c_outputs(c_app, outputs)
            run(lib, c_app, opened_info)
            queue.put(None)
        except Exception as e:
            queue.put(e)
    finally:
        client.shutdown()

class DynlibProcApp(BarrierAppDROP):
    """Loads a dynamic library in a different process and runs it"""

    def initialize(self, **kwargs):
        super(DynlibProcApp, self).initialize(**kwargs)

        if 'lib' not in kwargs:
            raise InvalidDropException(self, "library not specified")
        self.libname = kwargs.pop('lib')
        self.timeout = self._getArg(kwargs, 'timeout', 600) # 10 minutes
        self.app_params = kwargs

    def run(self):

        if not hasattr(self, '_rpc_server'):
            raise Exception('DynlibProcApp can only run within an RPC server')

        # On the sub-process we create DropProxy objects, so we need to extract
        # from our inputs/outputs their contact point (RPC-wise) information.
        # If one of our inputs/outputs is a DropProxy we already have this
        # information; otherwise we must figure it out.
        inputs = [self._get_proxy_info(i) for i in self.inputs]
        outputs = [self._get_proxy_info(o) for o in self.outputs]

        queue = multiprocessing.Queue()
        args = (queue, self.libname, self.oid, self.uid, self.app_params, inputs, outputs)
        proc = multiprocessing.Process(target=_run_in_proc, args=args)
        proc.start()

        try:
            steps = ('loading and initialising library',
                     'creating DropProxy instances',
                     'running the application')
            for step in steps:
                error = queue.get()
                if error is not None:
                    logger.error("Error in sub-process when " + step)
                    raise error
        finally:
            proc.join(self.timeout)

    def _get_proxy_info(self, x):
        if isinstance(x, rpc.DropProxy):
            return (x.hostname, x.port, x.session_id, x.uid)

        # TODO: we can't use the NodeManager's host directly here, as that
        #       indicates the address the different servers *bind* to
        #       (and, for example, can be 0.0.0.0)
        rpc_server = x._rpc_server
        host, port = rpc_server._rpc_host, rpc_server._rpc_port
        host = utils.to_externally_contactable_host(host, prefer_local=True)
        return (host, port, x._dlg_session.sessionId, x.uid)