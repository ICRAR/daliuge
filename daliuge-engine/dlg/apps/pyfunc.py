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
"""Module implementing the PyFuncApp class"""

import ast
import base64
import collections
import importlib
import inspect
import logging
import pickle

from typing import Callable
import dill

from dlg import droputils, utils
from dlg.drop import BarrierAppDROP
from dlg.exceptions import InvalidDropException
from dlg.meta import (
    dlg_bool_param,
    dlg_string_param,
    dlg_float_param,
    dlg_dict_param,
    dlg_component,
    dlg_batch_input,
    dlg_batch_output,
    dlg_streaming_input,
)

logger = logging.getLogger(__name__)


def serialize_data(d):
    return utils.b2s(base64.b64encode(pickle.dumps(d)))


def deserialize_data(d):
    return pickle.loads(base64.b64decode(d.encode("latin1")))


def serialize_func(f):

    if isinstance(f, str):
        parts = f.split(".")
        f = getattr(importlib.import_module(".".join(parts[:-1])), parts[-1])

    fser = dill.dumps(f)
    fdefaults = {}
    a = inspect.getfullargspec(f)
    if a.defaults:
        fdefaults = dict(
            zip(a.args[-len(a.defaults):], [serialize_data(d) for d in a.defaults])
        )
    logger.debug("Defaults for function %r: %r", f, fdefaults)
    return fser, fdefaults


def import_using_name(app, fname):
    # The name has the form pack1.pack2.mod.func
    parts = fname.split(".")
    if len(parts) < 2:
        msg = "%s does not contain a module name" % fname
        raise InvalidDropException(app, msg)

    modname, fname = ".".join(parts[:-1]), parts[-1]
    try:
        mod = importlib.import_module(modname, __name__)
        return getattr(mod, fname)
    except ImportError as e:
        raise InvalidDropException(
            app, "Error when loading module %s: %s" % (modname, str(e))
        )
    except AttributeError:
        raise InvalidDropException(app, "Module %s has no member %s" % (modname, fname))


def import_using_code(code):
    return dill.loads(code)


##
# @brief PyFuncApp
# @details An application that wraps a simple python function.
# The inputs of the application are treated as the arguments of the function.
# Conversely, the output of the function is treated as the output of the
# application. If the application has more than one output, the result of
# calling the function is treated as an iterable, with each individual object
# being written to its corresponding output.
# @par EAGLE_START
# @param category PythonApp
# @param tag daliuge
# @param[in] cparam/appclass Application Class/dlg.apps.pyfunc.PyFuncApp/String/readonly/False/
#     \~English Application class
# @param[in] cparam/execution_time Execution Time/5/Float/readonly/False/
#     \~English Estimated execution time
# @param[in] cparam/num_cpus No. of CPUs/1/Integer/readonly/False/
#     \~English Number of cores used
# @param[in] cparam/group_start Group start/False/Boolean/readwrite/False/
#     \~English Is this node the start of a group?
# @param[in] cparam/input_error_threshold "Input error rate (%)"/0/Integer/readwrite/False/
#     \~English the allowed failure rate of the inputs (in percent), before this component goes to ERROR state and is not executed
# @param[in] cparam/n_tries Number of tries/1/Integer/readwrite/False/
#     \~English Specifies the number of times the 'run' method will be executed before finally giving up
# @param[in] aparam/func_name Function Name//String/readwrite/False/
#     \~English Python fuction name
# @param[in] aparam/func_code Function Code//String/readwrite/False/
#     \~English Python fuction code, e.g. 'def fuction_name(args): return args'
# @param[in] aparam/pickle Pickle//bool/readwrite/False/
#     \~English Whether the python arguments are pickled.
# @param[in] aparam/func_defaults Function Defaults//String/readwrite/False/
#     \~English Mapping from argname to default value. Should match only the last part
#               of the argnames list
# @param[in] aparam/func_arg_mapping Function Arguments Mapping//String/readwrite/False/
#     \~English Mapping between argument name and input drop uids
# @par EAGLE_END
class PyFuncApp(BarrierAppDROP):
    """
    An application that wraps a simple python function.

    The inputs of the application are treated as the arguments of the function.
    Conversely, the output of the function is treated as the output of the
    application. If the application has more than one output, the result of
    calling the function is treated as an iterable, with each individual object
    being written to its corresponding output.

    Users indicate the function to be wrapped via the ``func_name`` parameter.
    Otherwise, users can also *send* over the python code using the ``func_code``
    parameter. The code needs to be base64-encoded and produced with the marshal
    module of the same Python version used to run DALiuGE.

    Both inputs and outputs are serialized using the pickle protocol.
    """

    component_meta = dlg_component(
        "PyFuncApp",
        "Py Func App.",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

    func_name = dlg_string_param("func_name", None)

    # fcode = dlg_bytes_param("func_code", None) # bytes or base64 string

    pickle = dlg_bool_param("pickle", True)

    func_arg_mapping = dlg_dict_param("func_arg_mapping", {})

    func_defaults = dlg_dict_param("func_defaults", {})

    f: Callable
    fdefaults: dict

    def initialize(self, **kwargs):
        BarrierAppDROP.initialize(self, **kwargs)

        self.fcode = self._getArg(kwargs, "func_code", None)
        if not self.func_name and not self.fcode:
            raise InvalidDropException(
                self, "No function specified (either via name or code)"
            )

        # Lookup function or import bytecode as a function
        if not self.fcode:
            self.f = import_using_name(self, self.func_name)
        else:
            if not isinstance(self.fcode, bytes):
                self.fcode = base64.b64decode(self.fcode.encode("utf8"))
            self.f = import_using_code(self.fcode)

        # Mapping from argname to default value. Should match only the last part
        # of the argnames list
        if isinstance(self.func_defaults, str):
            self.func_defaults = ast.literal_eval(self.func_defaults)

        if self.pickle:
            self.fdefaults = {name: deserialize_data(d) for name, d in self.func_defaults.items()}
        else:
            self.fdefaults = self.func_defaults

        logger.debug(f"Default values for function {self.func_name}: {self.fdefaults}")

        # Mapping between argument name and input drop uids
        logger.debug(f"Input mapping: {self.func_arg_mapping}")

    def run(self):

        # Inputs are un-pickled and treated as the arguments of the function
        # Their order must be preserved, so we use an OrderedDict
        if self.pickle:
            all_contents = lambda x: pickle.loads(droputils.allDropContents(x))
        else:
            all_contents = lambda x: ast.literal_eval(droputils.allDropContents(x).decode('utf-8'))

        inputs = collections.OrderedDict()
        for uid, drop in self._inputs.items():
            inputs[uid] = all_contents(drop)

        # Keyword arguments are made up by the default values plus the inputs
        # that match one of the keyword argument names
        argnames = inspect.getfullargspec(self.f).args

        kwargs = {
            name: inputs.pop(uid)
            for name, uid in self.func_arg_mapping.items()
            if name in self.fdefaults or name not in argnames
        }

        # The rest of the inputs are the positional arguments
        args = list(inputs.values())

        logger.debug(f"Running {self.func_name} with args={args}, kwargs={kwargs}")
        result = self.f(*args, **kwargs)

        # Depending on how many outputs we have we treat our result
        # as an iterable or as a single object. Each result is pickled
        # and written to its corresponding output
        outputs = self.outputs
        if len(outputs) == 1:
            result = [result]
        for r, o in zip(result, outputs):
            if self.pickle:
                o.write(pickle.dumps(r))  # @UndefinedVariable
            else:
                o.write(repr(r).encode('utf-8'))
