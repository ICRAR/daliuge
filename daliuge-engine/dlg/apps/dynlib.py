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
import queue
import threading

from dlg import rpc
from dlg.ddap_protocol import AppDROPStates
from dlg.apps.app_base import AppDROP, BarrierAppDROP
from dlg.drop import track_current_drop
from dlg.exceptions import InvalidDropException

logger = logging.getLogger(f"dlg.{__name__}")

_read_cb_type = ctypes.CFUNCTYPE(
    ctypes.c_size_t, ctypes.POINTER(ctypes.c_char), ctypes.c_size_t
)

_write_cb_type = ctypes.CFUNCTYPE(
    ctypes.c_size_t, ctypes.POINTER(ctypes.c_char), ctypes.c_size_t
)

_app_running_cb_type = ctypes.CFUNCTYPE(None)

_app_done_cb_type = ctypes.CFUNCTYPE(None, ctypes.c_int)


class CDlgInput(ctypes.Structure):
    _fields_ = [
        ("uid", ctypes.c_char_p),
        ("oid", ctypes.c_char_p),
        ("name", ctypes.c_char_p),
        ("status", ctypes.c_int),
        ("read", _read_cb_type),
    ]


class CDlgStreamingInput(ctypes.Structure):
    _fields_ = [
        ("uid", ctypes.c_char_p),
        ("oid", ctypes.c_char_p),
        ("name", ctypes.c_char_p),
    ]


class CDlgOutput(ctypes.Structure):
    _fields_ = [
        ("uid", ctypes.c_char_p),
        ("oid", ctypes.c_char_p),
        ("name", ctypes.c_char_p),
        ("write", _write_cb_type),
    ]


class CDlgApp(ctypes.Structure):
    _fields_ = [
        ("appname", ctypes.c_char_p),
        ("uid", ctypes.c_char_p),
        ("oid", ctypes.c_char_p),
        ("ranks", ctypes.POINTER(ctypes.c_int32)),
        ("n_ranks", ctypes.c_uint),
        ("inputs", ctypes.POINTER(CDlgInput)),
        ("n_inputs", ctypes.c_uint),
        ("streaming_inputs", ctypes.POINTER(CDlgStreamingInput)),
        ("n_streaming_inputs", ctypes.c_uint),
        ("outputs", ctypes.POINTER(CDlgOutput)),
        ("n_outputs", ctypes.c_uint),
        ("running", _app_running_cb_type),
        ("done", _app_done_cb_type),
        ("data", ctypes.c_void_p),
    ]

    def pack_python(self):
        out = {}
        for key, _ in self._fields_:
            out[key] = repr(getattr(self, key))
        return out


def _to_c_input(i):
    """
    Convert an input drop into its corresponding C structure
    """

    input_read = i.read

    def _read(desc_, buf, n):
        x = input_read(desc_, n)
        ctypes.memmove(buf, x, len(x))
        return len(x)

    desc = i.open()
    r = _read_cb_type(functools.partial(_read, desc))
    c_input = CDlgInput(
        i.uid.encode("utf8"),
        i.oid.encode("utf8"),
        i.name.encode("utf8"),
        i.status,
        r,
    )
    return desc, c_input


def _to_c_output(o):
    """
    Convert an output drop into its corresponding C structure
    """

    def _write(_o, buf, n, **kwargs):
        return _o.write(buf[:n], **kwargs)

    w = _write_cb_type(functools.partial(_write, o))
    return CDlgOutput(
        o.uid.encode("utf8"), o.oid.encode("utf8"), o.name.encode("utf8"), w
    )


def prepare_c_inputs(c_app, inputs):
    """
    Converts all inputs to its C equivalents and sets them into `c_app`
    """

    c_inputs = []
    input_closers = []
    for i in inputs:
        desc, c_input = _to_c_input(i)
        input_closers.append(functools.partial(i.close, desc))
        c_inputs.append(c_input)
    c_app.inputs = (CDlgInput * len(c_inputs))(*c_inputs)
    c_app.n_inputs = len(c_inputs)
    return input_closers


def prepare_c_outputs(c_app, outputs):
    """
    Converts all outputs to its C equivalents and sets them into `c_app`
    """

    c_outputs = [_to_c_output(o) for o in outputs]
    c_app.outputs = (CDlgOutput * len(c_outputs))(*c_outputs)
    c_app.n_outputs = len(c_outputs)


def prepare_c_ranks(c_app, ranks):
    """
    Convert the ranks list into its C equivalent and sets them to `c_app`
    """
    if ranks is None:
        # The number of ranks is
        c_app.n_ranks = 0
    else:
        c_app.ranks = (ctypes.c_int32 * len(ranks))(*ranks)
        c_app.n_ranks = len(ranks)


def run(lib, c_app, input_closers):
    """
    Invokes the `run` method on `lib` with the given `c_app`. After completion,
    all opened file descriptors are closed.
    """
    try:
        if hasattr(lib, "run2"):
            # With run2 we pass the results as a PyObject*
            run2 = lib.run2
            run2.restype = ctypes.py_object
            result = run2(ctypes.pointer(c_app))
            if isinstance(result, Exception):
                raise result
            if result:
                raise Exception(
                    "Invocation of {}:run2 returned with status {}".format(lib, result)
                )

        elif lib.run(ctypes.pointer(c_app)):
            raise Exception("Invocation of %r:run returned with status != 0" % lib)
    finally:
        for closer in input_closers:
            closer()


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

    one_of_functions = [["run", "run2"], ["init", "init2"]]
    for functions in one_of_functions:
        found_one = False
        for fname in functions:
            if hasattr(lib, fname):
                found_one = True
                break

        if not found_one:
            raise InvalidLibrary(
                "{} doesn't have one of the functions {}".format(libname, functions)
            )

    # Create the initial contents of the C dlg_app_info structure
    # We pass no inputs because we don't know them (and don't need them)
    # at this point yet.
    # The running and done callbacks are also NULLs
    c_app = CDlgApp(
        None,
        uid.encode("utf8"),
        oid.encode("utf8"),
        None,
        0,
        None,
        0,
        None,
        0,
        None,
        0,
        ctypes.cast(None, _app_running_cb_type),
        ctypes.cast(None, _app_done_cb_type),
        None,
    )

    if hasattr(lib, "init2"):
        # With init2 we pass the params as a PyObject*
        logger.info("Extra parameters passed to application: %r", params)
        init2 = lib.init2
        init2.restype = ctypes.py_object
        result = init2(ctypes.pointer(c_app), ctypes.py_object(params))
        if isinstance(result, Exception):
            raise result
        if result:
            raise InvalidLibrary(
                "{} failed during initialization (init2)".format(libname)
            )

    elif hasattr(lib, "init"):
        # Collect the rest of the parameters to pass them down to the library
        # We need to keep them in a local variable so when we expose them to
        # the app later on via pointers we still have their contents
        local_params = [
            (str(k).encode("utf8"), str(v).encode("utf8")) for k, v in params.items()
        ]
        logger.debug("Extra parameters passed to application: %r", local_params)

        # Wrap in ctypes
        str_ptr_type = ctypes.POINTER(ctypes.c_char_p)
        two_str_type = ctypes.c_char_p * 2
        app_params = [two_str_type(k, v) for k, v in local_params]
        app_params.append(None)
        params = (str_ptr_type * len(app_params))(*app_params)

        # Let the shared library initialize this app
        # If we have a list of key/value pairs that are all strings
        if lib.init(ctypes.pointer(c_app), params):
            raise InvalidLibrary(
                "{} failed during initialization (init)".format(libname)
            )

    else:
        raise InvalidLibrary(
            "{} failed during initialization. No init or init2".format(libname)
        )

    return lib, c_app


class DynlibAppBase(object):
    def initialize(self, **kwargs):
        super(DynlibAppBase, self).initialize(**kwargs)

        if "lib" not in kwargs:
            raise InvalidDropException(self, "library not specified")

        try:
            self.lib, self._c_app = load_and_init(
                kwargs.pop("lib"), self.oid, self.uid, kwargs
            )
        except InvalidLibrary as e:
            raise InvalidDropException(self, e.args[0]) from e

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
        self.lib.data_written(app_p, uid.encode("utf8"), data, len(data))

    def dropCompleted(self, uid, drop_state):
        self._ensure_c_outputs_are_set()
        app_p = ctypes.pointer(self._c_app)
        self.lib.drop_completed(app_p, uid.encode("utf8"), drop_state)

    def addInput(self, inputDrop, back=True):
        super(DynlibStreamApp, self).addInput(inputDrop, back)
        self._c_app.n_inputs += 1

    def addStreamingInput(self, streamingInputDrop, back=True):
        super(DynlibStreamApp, self).addStreamingInput(streamingInputDrop, back)
        self._c_app.n_streaming_inputs += 1

    def generate_recompute_data(self):
        out = {"status": self.status}
        data = self._c_app.pack_python()
        if data is not None:
            out.update(data)
        return out


##
# @brief DynlibApp
# @details An application component run from a dynamic library
# @par EAGLE_START
# @param category DynlibApp
# @param tag template
# @param libpath /String/ComponentParameter/NoPort/ReadWrite//False/False/"The location of the shared object/DLL that implements this application"
# @param log_level "NOTSET"/Select/ComponentParameter/NoPort/ReadWrite/NOTSET,DEBUG,INFO,WARNING,ERROR,CRITICAL/False/False/Set the log level for this drop
# @param dropclass dlg.apps.dynlib.DynlibApp/String/ComponentParameter/NoPort/ReadWrite//False/False/Drop class
# @param base_name dynlib/String/ComponentParameter/NoPort/ReadOnly//False/False/Base name of application class
# @param execution_time 5/Float/ConstraintParameter/NoPort/ReadOnly//False/False/Estimated execution time
# @param num_cpus 1/Integer/ConstraintParameter/NoPort/ReadOnly//False/False/Number of cores used
# @param group_start False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Is this node the start of a group?
# @param input_error_threshold 0/Integer/ComponentParameter/NoPort/ReadWrite//False/False/the allowed failure rate of the inputs (in percent), before this component goes to ERROR state and is not executed
# @param n_tries 1/Integer/ComponentParameter/NoPort/ReadWrite//False/False/Specifies the number of times the 'run' method will be executed before finally giving up
# @par EAGLE_END
class DynlibApp(DynlibAppBase, BarrierAppDROP):
    """Loads a dynamic library into the current process and runs it"""

    def initialize(self, **kwargs):
        super(DynlibApp, self).initialize(**kwargs)
        self.ranks = self._popArg(kwargs, "rank", None)

    @track_current_drop
    def run(self):
        input_closers = prepare_c_inputs(self._c_app, self.inputs)
        prepare_c_ranks(self._c_app, self.ranks)
        self._ensure_c_outputs_are_set()
        run(self.lib, self._c_app, input_closers)

    def generate_recompute_data(self):
        out = {"status": self.status}
        if self._c_app is None:
            return out
        else:
            out.update(self._c_app.pack_python())
            return out


class FinishSubprocess(Exception):
    pass


def _run_in_proc(*args):
    try:
        _do_run_in_proc(*args)
    except FinishSubprocess:
        pass


def _do_run_in_proc(proc_queue, libname, oid, uid, params, inputs, outputs):
    def advance_step(f, *args, **kwargs):
        try:
            r = f(*args, **kwargs)
            proc_queue.put(None)
            return r
        except Exception as e:
            proc_queue.put(e)
            raise FinishSubprocess() from e

    # Step 1: initialise the library and return if there is an error
    lib, c_app = advance_step(load_and_init, libname, oid, uid, params)

    client = rpc.RPCClient()
    try:
        client.start()

        def setup_drop_proxies(inputs, outputs):
            to_drop_proxy = lambda proxy_info: rpc.DropProxy(client, proxy_info)
            inputs = [to_drop_proxy(i) for i in inputs]
            outputs = [to_drop_proxy(o) for o in outputs]
            return inputs, outputs

        inputs, outputs = advance_step(setup_drop_proxies, inputs, outputs)

        # Step 3: Finish initializing the C structure and run the application
        def do_run():
            input_closers = prepare_c_inputs(c_app, inputs)
            prepare_c_outputs(c_app, outputs)
            run(lib, c_app, input_closers)

        advance_step(do_run)
    finally:
        client.shutdown()


def get_from_subprocess(proc, q):
    """Gets elements from the queue, checking that the process is still alive"""
    while proc.is_alive():
        try:
            return q.get(timeout=0.1)
        except queue.Empty:
            pass
    raise RuntimeError("Subprocess died unexpectedly")


##
# @brief DynlibProcApp
# @details An application component run from a dynamic library in a different process
# @par EAGLE_START
# @param category DynlibProcApp
# @param tag template
# @param libpath /String/ComponentParameter/NoPort/ReadWrite//False/False/"The location of the shared object/DLL that implements this application"
# @param dropclass dlg.apps.dynlib.DynlibProcApp/String/ComponentParameter/NoPort/ReadWrite//False/False/Drop class
# @param log_level "NOTSET"/Select/ComponentParameter/NoPort/ReadWrite/NOTSET,DEBUG,INFO,WARNING,ERROR,CRITICAL/False/False/Set the log level for this drop
# @param execution_time 5/Float/ConstraintParameter/NoPort/ReadOnly//False/False/Estimated execution time
# @param num_cpus 1/Integer/ConstraintParameter/NoPort/ReadOnly//False/False/Number of cores used
# @param group_start False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Is this node the start of a group?
# @param input_error_threshold 0/Integer/ComponentParameter/NoPort/ReadWrite//False/False/the allowed failure rate of the inputs (in percent), before this component goes to ERROR state and is not executed
# @param n_tries 1/Integer/ComponentParameter/NoPort/ReadWrite//False/False/Specifies the number of times the 'run' method will be executed before finally giving up
# @param base_name dynlib/String/ComponentParameter/NoPort/ReadOnly//False/False/Base name of application class
# @par EAGLE_END
class DynlibProcApp(BarrierAppDROP):
    """Loads a dynamic library in a different process and runs it"""

    def initialize(self, **kwargs):
        super(DynlibProcApp, self).initialize(**kwargs)

        if "lib" not in kwargs:
            raise InvalidDropException(self, "library not specified")
        self.libname = kwargs.pop("lib")
        self.timeout = self._popArg(kwargs, "timeout", 600)  # 10 minutes
        self.app_params = kwargs
        self.proc = None

    def run(self):
        if not hasattr(self, "rpc_endpoint"):
            raise Exception("DynlibProcApp can only run within an RPC server")

        # On the sub-process we create DropProxy objects, so we need to extract
        # from our inputs/outputs their contact point (RPC-wise) information.
        # If one of our inputs/outputs is a DropProxy we already have this
        # information; otherwise we must figure it out.
        inputs = [rpc.ProxyInfo.from_data_drop(i) for i in self.inputs]
        outputs = [rpc.ProxyInfo.from_data_drop(o) for o in self.outputs]

        logger.info("Starting new process to run the dynlib on")
        multi_proc_queue = multiprocessing.Queue()
        args = (
            multi_proc_queue,
            self.libname,
            self.oid,
            self.uid,
            self.app_params,
            inputs,
            outputs,
        )
        self.proc = multiprocessing.Process(target=_run_in_proc, args=args)
        self.proc.start()

        try:
            steps = (
                "loading and initialising library",
                "creating DropProxy instances",
                "running the application",
            )
            for step in steps:
                logger.info("Subprocess %s", step)
                error = get_from_subprocess(self.proc, multi_proc_queue)
                if error is not None:
                    logger.error("Error in sub-process when %s", step)
                    raise error
        finally:
            self.proc.join(self.timeout)

    def cancel(self):
        BarrierAppDROP.cancel(self)
        try:
            self.proc.terminate()
        except multiprocessing.ProcessError:
            logger.exception("Error while terminating process %r", self.proc)
