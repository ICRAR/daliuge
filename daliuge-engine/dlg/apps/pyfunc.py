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
from io import StringIO
from contextlib import redirect_stdout

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
    fdefaults = {"args": [], "kwargs": {}}
    adefaults = {"args": [], "kwargs": {}}
    a = inspect.getfullargspec(f)
    if a.defaults:
        fdefaults["kwargs"] = dict(
            zip(a.args[-len(a.defaults) :], [serialize_data(d) for d in a.defaults])
        )
        adefaults["kwargs"] = dict(
            zip(a.args[-len(a.defaults) :], [d for d in a.defaults])
        )
    logger.debug(f"Introspection of function {f}: {a}")
    logger.debug("Defaults for function %r: %r", f, adefaults)
    return fser, fdefaults


def import_using_name(app, fname):
    # If only one part check if builtin
    parts = fname.split(".")
    if len(parts) < 2:
        b = globals()['__builtins__']
        logger.debug(f"Builtins: {type(b)}")
        logger.debug(f"Function {fname}: {hasattr(b, fname)}")
        if fname in b:
            return b[fname]
        else:
            msg = "%s is not builtin and does not contain a module name" % fname
            raise InvalidDropException(app, msg)
    else:
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
# @param tag template
# @param[in] cparam/appclass Application Class/dlg.apps.pyfunc.PyFuncApp/String/readonly/False//False/
#     \~English Application class
# @param[in] cparam/execution_time Execution Time/5/Float/readonly/False//False/
#     \~English Estimated execution time
# @param[in] cparam/num_cpus No. of CPUs/1/Integer/readonly/False//False/
#     \~English Number of cores used
# @param[in] cparam/group_start Group start/False/Boolean/readwrite/False//False/
#     \~English Is this node the start of a group?
# @param[in] cparam/input_error_threshold "Input error rate (%)"/0/Integer/readwrite/False//False/
#     \~English The allowed failure rate of the inputs (in percent), before this component goes to ERROR state and is not executed
# @param[in] cparam/n_tries Number of tries/1/Integer/readwrite/False//False/
#     \~English Specifies the number of times the 'run' method will be executed before finally giving up
# @param[in] aparam/func_name Function Name//String/readwrite/False//False/
#     \~English Python function name
# @param[in] aparam/func_code Function Code//String/readwrite/False//False/
#     \~English Python function code, e.g. 'def function_name(args): return args'
# @param[in] aparam/pickle Pickle/false/Boolean/readwrite/False//False/
#     \~English Whether the python arguments are pickled.
# @param[in] aparam/func_defaults Function Defaults//String/readwrite/False//False/
#     \~English Mapping from argname to default value. Should match only the last part of the argnames list.
#               Values are interpreted as Python code literals and that means string values need to be quoted.
# @param[in] aparam/func_arg_mapping Function Arguments Mapping//String/readwrite/False//False/
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
    In this case func_name needs to specify a funtion in the standard form

    ``module.function``

    and the module needs to be accessible on the PYTHONPATH of the DALiuGE
    engine. Note that the engine is expanding the standard PYTHONPATH with
    DLG_ROOT/code. That directory is always available, even if the engine is
    running in a docker container.

    Otherwise, users can also *send* over the python code using the ``func_code``
    parameter. The code needs to be base64-encoded and produced with the marshal
    module of the same Python version used to run DALiuGE.

    Both inputs and outputs are (de-)serialized using the pickle protocol if the value
    of the respective boolean component parameter is set to True. This is also
    applied to func_defaults and func_arg_mappings.

    In addition to the input mapping the implementation also allows to set defaults
    both in the function itself and in a logical graph. If set in the logical graph
    using the func_defaults parameter, the defaults need to be specified as a
    dictionary of the form

    ``{"kwargs":{"kw1_name":kw1_value, "kw2_name":kw2_value}, "args":[arg1, arg2]}``

    The positional onlyargs will be used in order of appearance.
    """

    component_meta = dlg_component(
        "PyFuncApp",
        "Py Func App.",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

    func_name = dlg_string_param("func_name", None)

    # func_code = dlg_bytes_param("func_code", None) # bytes or base64 string

    pickle = dlg_bool_param("pickle", True)

    func_arg_mapping = dlg_dict_param("func_arg_mapping", {})

    func_defaults = dlg_dict_param("func_defaults", {})

    f: Callable
    fdefaults: dict

    def _init_func_defaults(self):
        """
        Inititalize self.func_defaults dictionary from values provided.
        Multiple options exist and some are here for compatibility.
        """
        logger.debug(f"Starting evaluation of func_defaults: {self.func_defaults}")
        if (
            isinstance(self.func_defaults, dict)
            and len(self.func_defaults) > 0
            and list(self.func_defaults.keys()) == ["kwargs", "args"]
        ):
            # we bring everything back to just kwargs, because positional args are messy
            # NOTE: This means that positional ONLY arguments won't work, but those are not used
            # too often.
            for arg in self.func_defaults["args"]:
                self.func_defaults["kwargs"][arg] = arg
                self.func_defaults = self.func_defaults["kwargs"]
        elif (
            isinstance(self.func_defaults, dict)
            and "kwargs" in self.func_defaults
            and isinstance(self.func_defaults["kwargs"], dict)
        ):
            self.func_defaults = self.func_defaults["kwargs"]
        # we came all this way, now assume that any resulting dict is correct
        if not isinstance(self.func_defaults, dict):
            logger.error(
                f"Wrong format or type for function defaults for "
                + "{self.f.__name__}: {self.func_defaults}, {type(self.func_defaults)}"
            )
            raise ValueError
        if self.pickle:
            # only values are pickled, get them unpickled
            for name, value in self.func_defaults.items():
                self.func_defaults[name] = deserialize_data(value)

        # set the function defaults from introspection
        if self.arguments:
            self.fn_npos = len(self.arguments.args) - self.fn_ndef
            self.fn_defaults = {
                name: None for name in self.arguments.args[: self.fn_npos]
            }
            logger.debug(f"initialized fn_defaults with {self.fn_defaults}")
            # deal with args and kwargs
            kwargs = (
                dict(zip(self.arguments.args[self.fn_npos :], self.arguments.defaults))
                if self.arguments.defaults
                else {}
            )
            self.fn_defaults.update(kwargs)
            logger.debug(f"fn_defaults updated with {kwargs}")
            # deal with kwonlyargs
            if self.arguments.kwonlydefaults:
                kwonlyargs = dict(
                    zip(self.arguments.kwonlyargs, self.arguments.kwonlydefaults)
                )
                self.fn_defaults.update(kwonlyargs)
                logger.debug(f"fn_defaults updated with {kwonlyargs}")

            self.fn_posargs = self.arguments.args[
                : self.fn_npos
            ]  # positional arg names

    def initialize(self, **kwargs):
        """
        The initialization of a function component is mainly dealing with mapping
        inputs and provided applicationArgs to the function arguments. All of this
        should be driven by matching names, but currently that is not being done.
        """
        BarrierAppDROP.initialize(self, **kwargs)

        self._applicationArgs = self._getArg(kwargs, "applicationArgs", {})

        self.func_code = self._getArg(kwargs, "func_code", None)

        # check for function definition arguments in applicationArgs
        self.func_def_keywords = [
            "func_code",
            "func_name",
            "func_arg_mapping",
            "pickle",
            "func_defaults"
            ]
        for kw in self.func_def_keywords:
            dum_arg = new_arg = "gIbbERiSH:askldhgol"
            if kw in self._applicationArgs:  # these are the preferred ones now
                if isinstance(
                    self._applicationArgs[kw]["value"], bool
                ):  # always transfer booleans
                    new_arg = self._applicationArgs.pop(kw)
                elif (
                    self._applicationArgs[kw]["value"]
                    or self._applicationArgs[kw]["precious"]
                ):
                    # only transfer if there is a value or precious is True
                    new_arg = self._applicationArgs.pop(kw)

            if new_arg != dum_arg:
                logger.debug(f"Setting {kw} to {new_arg['value']}")
                # we allow python expressions as values, means that strings need to be quoted
                self.__setattr__(kw, new_arg["value"])

        self.num_args = len(
            self._applicationArgs
        )  # number of additional arguments provided

        if not self.func_name and not self.func_code:
            raise InvalidDropException(
                self, "No function specified (either via name or code)"
            )

        # Lookup function or import bytecode as a function
        if not self.func_code:
            self.f = import_using_name(self, self.func_name)
        else:
            if not isinstance(self.func_code, bytes):
                self.func_code = base64.b64decode(self.func_code.encode("utf8"))
            self.f = import_using_code(self.func_code)
        # make sure defaults are dicts
        if isinstance(self.func_defaults, str):
            self.func_defaults = ast.literal_eval(self.func_defaults)
        if isinstance(self.func_arg_mapping, str):
            self.func_arg_mapping = ast.literal_eval(self.func_arg_mapping)

        self.arguments = inspect.getfullargspec(self.f)
        logger.debug(f"Function inspection revealed {self.arguments}")
        # we don't want to mess with the 'self' argument
        if self.arguments.args.count('self'): 
            self.arguments.args.remove('self')
        self.fn_nargs = len(self.arguments.args)
        self.fn_ndef = len(self.arguments.defaults) if self.arguments.defaults else 0
        self._init_func_defaults()
        logger.info(f"Args summary for '{self.func_name}':")
        logger.info(f"Args: {self.arguments.args}")
        logger.info(f"Args defaults:  {self.arguments.defaults}")
        logger.info(f"Args positional: {self.arguments.args[:self.fn_npos]}")
        logger.info(f"Args keyword: {self.arguments.args[self.fn_npos:]}")
        logger.info(f"Args supplied:  {self.func_defaults}")
        logger.info(f"VarArgs allowed:  {self.arguments.varargs}")
        logger.info(f"VarKwds allowed:  {self.arguments.varkw}")

        # Mapping between argument name and input drop uids
        logger.debug(f"Input mapping: {self.func_arg_mapping}")
        self._recompute_data = {}

    def run(self):
        """
        Function positional and keyword argument treatment:

        Function arguments can be provided in four different ways:
        1) Through an input port
        2) By specifying ApplicationArgs (one for each argument)
        3) By specifying a func_defaults dictionary in the ComponentParameters
        4) Through defaults at the time of function definition

        The priority follows the list above with input ports overruling the others.
        Function arguments in Python can be passed as positional, kw-value, positional
        only, kw-value only, and catch-all args and kwargs, which don't provide any
        hint about the names of accepted parameters. All of them are now supported. If
        positional arguments or kw-value arguments are provided by the user, but are 
        not explicitely defined in the function signiture AND args and/or kwargs are
        allowed then these arguments are passed to the function. For args this is
        somewhat risky, since the order is relevant and in this code derived from the
        order defined in the graph (same order as defined in the component description).

        Input ports will NOT be used by order (anymore), but by the IdText (name field
        in EAGLE) of the port. Since each input port requires an associated data drop,
        this provides a unique mapping. This also allows to pass values to any function
        argument through a port.

        Function argument values as well as the function code can be provided in
        serialised (pickle) form by setting the 'pickle' flag. Note that this flag
        is valid for all arguments and the code (if specified) in a global way.
        """

        # Inputs are un-pickled and treated as the arguments of the function
        # Their order must be preserved, so we use an OrderedDict
        if self.pickle:
            all_contents = lambda x: pickle.loads(x)
        else:
            all_contents = lambda x: ast.literal_eval(
                droputils.allDropContents(x).decode("utf-8")
            )

        inputs = collections.OrderedDict()
        for uid, drop in self._inputs.items():
            contents = droputils.allDropContents(drop)
            # allow for Null DROPs to be passed around
            inputs[uid] = all_contents(contents) if contents else None

        self.funcargs = {}

        # Keyword arguments are made up of the default values plus the inputs
        # that match one of the keyword argument names
        # if defaults dict has not been specified at all we'll go ahead anyway
        n_args = len(self.func_defaults)
        argnames = self.arguments.args

        # use explicit mapping of inputs to arguments first
        # TODO: Required by dlg_delayed?? Else, we should really not do this.
        kwargs = {
            name: inputs.pop(uid)
            for name, uid in self.func_arg_mapping.items()
            if name in self.func_defaults or name not in argnames
        }
        logger.debug(f"updating funcargs with {kwargs}")
        self.funcargs = kwargs

        # Fill arguments with rest of inputs
        logger.debug(f"available inputs: {inputs}")

        # if we have named ports use the inputs with
        # the correct UIDs
        logger.debug(f"Parameters found: {self.parameters}")
        posargs = self.arguments.args[:self.fn_npos]
        kwargs = {}
        self.pargs = []
        pargsDict = collections.OrderedDict(zip(posargs,[None]*len(posargs)))  # Initialize pargs dictionary
        if ('inputs' in self.parameters and isinstance(self.parameters['inputs'][0], dict)):
            logger.debug(f"Using named ports to identify inputs: "+\
                    f"{self.parameters['inputs']}")
            for i in range(min(len(inputs),self.fn_nargs +\
                len(self.arguments.kwonlyargs))):
                # key for final dict is value in named ports dict
                key = list(self.parameters['inputs'][i].values())[0]
                # value for final dict is value in inputs dict
                value = inputs[list(self.parameters['inputs'][i].keys())[0]]
                if key in posargs:
                    pargsDict.update({key:value})
                else:
                    kwargs.update({key:value})
        else:
            for i in range(min(len(inputs),self.fn_nargs)):
                kwargs.update({self.arguments.args[i]: list(inputs.values())[i]})

        logger.debug(f"updating funcargs with input ports {kwargs}")
        self.funcargs.update(kwargs)

        # Try to get values for still missing positional arguments from Application Args
        if "applicationArgs" in self.parameters:
            appArgs = self.parameters["applicationArgs"]  # we'll pop them
            _dum = [appArgs.pop(k) for k in self.func_def_keywords if k in appArgs]
            for pa in posargs:
                if pa not in self.funcargs:
                    if pa in appArgs and pa != 'self':
                        arg = appArgs.pop(pa)
                        value = arg['value']
                        ptype = arg['type']
                        if ptype in ["Complex", "Json"]:
                            try:
                                value = ast.literal_eval(value)
                            except:
                                pass
                        pargsDict.update({
                            pa:
                            value
                        })
                    elif pa != 'self' and pa not in pargsDict:
                        logger.warning(f"Required positional argument '{pa}' not found!")
            logger.debug(f"updating posargs with {list(pargsDict.values())}")
            self.pargs.extend(list(pargsDict.values()))

            # Try to get values for still missing kwargs arguments from Application kws
            kwargs = {}
            kws = self.arguments.args[self.fn_npos:]
            for ka in kws:
                if ka not in self.funcargs:
                    if ka in appArgs:
                        arg = appArgs.pop(ka)
                        value = arg['value']
                        ptype = arg['type']
                        if ptype in ["Complex", "Json"]:
                            try:
                                value = ast.literal_eval(value)
                            except:
                                pass
                        kwargs.update({
                            ka:
                            value
                        })
                    else:
                        logger.warning(f"Keyword argument '{ka}' not found!")
            logger.debug(f"updating funcargs with {kwargs}")
            self.funcargs.update(kwargs)
            vparg = []
            vkarg = {}
            logger.debug(f"Remaining AppArguments {appArgs}")
            for arg in appArgs:
                if appArgs[arg]['type'] in ['Json', 'Complex']:
                    value = ast.literal_eval(appArgs[arg]['value'])
                else:
                    value = appArgs[arg]['value']
                if appArgs[arg]['positional']:
                    vparg.append(value)
                else:
                    vkarg.update({arg:value})

            # any remaining application arguments will be used for vargs and vkwargs
            if self.arguments.varargs:
                self.pargs.extend(vparg)
            if self.arguments.varkw:
                self.funcargs.update(vkarg)

        # Fill rest with default arguments if there are any more
        kwargs = {}
        for kw in self.func_defaults.keys():
            value = self.func_defaults[kw]
            if kw not in self.funcargs:
                kwargs.update({kw: value})
        logger.debug(f"updating funcargs with {kwargs}")
        self.funcargs.update(kwargs)
        self._recompute_data["args"] = self.funcargs.copy()
        logger.debug(f"Running {self.func_name} with *{self.pargs} **{self.funcargs}")

        # we capture and log whatever is produced on STDOUT
        capture = StringIO()
        with redirect_stdout(capture):
            result = self.f(*self.pargs, **self.funcargs)
        logger.info(f"Captured output from function app '{self.func_name}': {capture.getvalue()}")
        logger.debug(f"Finished execution of {self.func_name}.")

        # Depending on how many outputs we have we treat our result
        # as an iterable or as a single object. Each result is pickled
        # and written to its corresponding output
        self.write_results(result)

    def write_results(self, result):
        outputs = self.outputs
        if len(outputs) > 0:
            if len(outputs) == 1:
                result = [result]
            for r, o in zip(result, outputs):
                p = pickle.dumps(r)
                if self.pickle:
                    logger.debug(f"Writing pickeled result {type(r)} to {o}")
                    o.write(pickle.dumps(r))  # @UndefinedVariable
                else:
                    o.write(repr(r).encode("utf-8"))

    def generate_recompute_data(self):
        return self._recompute_data
