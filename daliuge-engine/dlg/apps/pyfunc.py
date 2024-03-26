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
from enum import Enum
import importlib
import inspect
import json
import logging
import os
import pickle

from typing import Callable
import dill
from io import StringIO
from contextlib import redirect_stdout

from dlg import droputils, drop_loaders
from dlg.utils import serialize_data, deserialize_data
from dlg.named_port_utils import check_ports_dict, identify_named_ports
from dlg.apps.app_base import BarrierAppDROP
from dlg.exceptions import InvalidDropException
from dlg.meta import (
    dlg_string_param,
    dlg_enum_param,
    dlg_dict_param,
    dlg_component,
    dlg_batch_input,
    dlg_batch_output,
    dlg_streaming_input,
)

logger = logging.getLogger(__name__)


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
            zip(
                a.args[-len(a.defaults) :],
                [serialize_data(d) for d in a.defaults],
            )
        )
        adefaults["kwargs"] = dict(
            zip(a.args[-len(a.defaults) :], [d for d in a.defaults])
        )
    logger.debug(f"Introspection of function {f}: {a}")
    logger.debug("Defaults for function %r: %r", f, adefaults)
    return fser, fdefaults


def import_using_name(app, fname):
    logger.debug("Importing %s", fname)
    parts = fname.split(".")
    # If only one part check if builtin
    b = globals()["__builtins__"]
    logger.debug(f"Function: {parts[0]}: {hasattr(b, parts[0])}")
    if len(parts) < 2:
        logger.debug(f"Builtins: {type(b)}")
        logger.debug(f"Function {fname}: {hasattr(b, fname)}")
        if fname in b:
            return b[fname]
        else:
            msg = "%s is not builtin and does not contain a module name" % fname
            raise InvalidDropException(app, msg)
    elif parts[0] in b.keys():
        return b[parts[0]]
    else:
        if len(parts) > 1:
            if parts[-1] in ["__init__", "__class__"]:
                parts = parts[:-1]
            logger.debug("Recursive import: %s", parts)
            try:
                mod = importlib.import_module(parts[0], __name__)
            except ImportError as e:
                raise InvalidDropException(
                    app,
                    "Error when loading module %s: %s" % (parts[0], str(e)),
                )
            for m in parts[1:]:
                try:
                    logger.debug("Getting attribute %s", m)
                    mod = getattr(mod, m)
                except AttributeError as e:
                    try:
                        logger.debug(
                            "Trying to load backwards: %s",
                            ".".join(parts[:-1]),
                        )
                        mod = importlib.import_module(".".join(parts[:-1]), __name__)
                        mod = getattr(mod, parts[-1])
                        break
                    except ModuleNotFoundError:
                        # try again, sometimes fixes the namespace
                        mod = import_using_name(app, fname)
                        break
                    except Exception as e:
                        raise InvalidDropException(
                            app, "Problem importing module %s, %s" % (mod, e)
                        )
            logger.debug("Loaded module: %s", mod)
            return mod


def import_using_code(code):
    return dill.loads(code)


class DropParser(Enum):
    RAW = "raw"
    PICKLE = "pickle"
    EVAL = "eval"
    NPY = "npy"
    # JSON = "json"
    PATH = "path"  # input only
    DATAURL = "dataurl"  # input only


##
# @brief PythonMemberFunction
# @details A placeholder APP to aid construction of new class member function applications.
# This is mainly useful (and used) when starting a new workflow from scratch.
# @par EAGLE_START
# @param category PythonMemberFunction
# @param tag daliuge
# @param func_name object.__init__/String/ComponentParameter/NoPort/ReadWrite//False/False/Python function name
# @param func_code /String/ComponentParameter/NoPort/ReadWrite//False/False/Python function code, e.g. 'def function_name(args): return args'
# @param dropclass dlg.apps.pyfunc.PyFuncApp/String/ComponentParameter/NoPort/ReadOnly//False/False/Application class
# @param object /Object/ApplicationArgument/InputOutput/ReadWrite//False/False/object port
# @param execution_time 5/Float/ConstraintParameter/NoPort/ReadOnly//False/False/Estimated execution time
# @param num_cpus 1/Integer/ConstraintParameter/NoPort/ReadOnly//False/False/Number of cores used
# @param group_start False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Is this node the start of a group?
# @param input_error_threshold 0/Integer/ComponentParameter/NoPort/ReadWrite//False/False/The allowed failure rate of the inputs (in percent), before this component goes to ERROR state and is not executed
# @param n_tries 1/Integer/ComponentParameter/NoPort/ReadWrite//False/False/Specifies the number of times the 'run' method will be executed before finally giving up
# @param input_parser pickle/Select/ComponentParameter/NoPort/ReadWrite/raw,pickle,eval,npy,path,dataurl/False/False/Input port parsing technique
# @param output_parser pickle/Select/ComponentParameter/NoPort/ReadWrite/raw,pickle,eval,npy,path,dataurl/False/False/Output port parsing technique
#     \~English Mapping from argname to default value. Should match only the last part of the argnames list.
#               Values are interpreted as Python code literals and that means string values need to be quoted.
# @par EAGLE_END
class PyMemberApp(BarrierAppDROP):
    """A placeholder member function that just aids the generation of the palette component"""

    pass


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
# @param func_name /String/ComponentParameter/NoPort/ReadWrite//False/False/Python function name
# @param func_code /String/ComponentParameter/NoPort/ReadWrite//False/False/Python function code, e.g. 'def function_name(args): return args'
# @param dropclass dlg.apps.pyfunc.PyFuncApp/String/ComponentParameter/NoPort/ReadOnly//False/False/Application class
# @param execution_time 5/Float/ConstraintParameter/NoPort/ReadOnly//False/False/Estimated execution time
# @param num_cpus 1/Integer/ConstraintParameter/NoPort/ReadOnly//False/False/Number of cores used
# @param group_start False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Is this node the start of a group?
# @param input_error_threshold 0/Integer/ComponentParameter/NoPort/ReadWrite//False/False/The allowed failure rate of the inputs (in percent), before this component goes to ERROR state and is not executed
# @param n_tries 1/Integer/ComponentParameter/NoPort/ReadWrite//False/False/Specifies the number of times the 'run' method will be executed before finally giving up
# @param input_parser pickle/Select/ComponentParameter/NoPort/ReadWrite/raw,pickle,eval,npy,path,dataurl/False/False/Input port parsing technique
# @param output_parser pickle/Select/ComponentParameter/NoPort/ReadWrite/raw,pickle,eval,npy,path,dataurl/False/False/Output port parsing technique
#     \~English Mapping from argname to default value. Should match only the last part of the argnames list.
#               Values are interpreted as Python code literals and that means string values need to be quoted.
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
    In this case func_name needs to specify a function in the standard form

    ``module.function``

    and the module needs to be accessible on the PYTHONPATH of the DALiuGE
    engine. Note that the engine is expanding the standard PYTHONPATH with
    DLG_ROOT/code. That directory is always available, even if the engine is
    running in a docker container.

    Otherwise, users can also *send* over the python code using the ``func_code``
    parameter. The code needs to be base64-encoded and produced with the marshal
    module of the same Python version used to run DALiuGE.

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
    input_parser: DropParser = dlg_enum_param(DropParser, "input_parser", DropParser.PICKLE)  # type: ignore
    output_parser: DropParser = dlg_enum_param(DropParser, "output_parser", DropParser.PICKLE)  # type: ignore
    func_arg_mapping = dlg_dict_param("func_arg_mapping", {})
    func_defaults = dlg_dict_param("func_defaults", {})
    f: Callable
    fdefaults: dict

    def _mixin_func_defaults(self):
        """
        func_defaults can be passed in through an argument, but this is only used for
        dlg_delayed and in combination with passing func_code. For dlg_delayed the function
        and its parameters might be computed on a different host than the where the delayed
        function is called and thus the function needs to be serialized.
        """
        logger.debug(f"Starting evaluation of func_defaults: {self.func_defaults}")
        if (
            isinstance(self.func_defaults, dict)
            and len(self.func_defaults) > 0
            and list(self.func_defaults.keys()) == ["kwargs", "args"]
        ):
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
                "Wrong format or type for function defaults for %s: %r, %r",
                self.f.__name__,
                self.func_defaults,
                type(self.func_defaults),
            )
            raise ValueError
        if self.input_parser is DropParser.PICKLE:
            # only values are pickled, get them unpickled
            for name, value in self.func_defaults.items():
                self.func_defaults[name] = deserialize_data(value)
        # the fn_defaults are used afterwards, we'll drop the func_defaults
        logger.debug("fn_defaults %s", self.fn_defaults)
        logger.debug("func_defaults %s", self.func_defaults)
        self.fn_defaults = self.func_defaults

    def _init_fn_defaults(self):
        """
        Inititalize self.fn_defaults dictionary from values provided.
        Multiple options exist and some are here for compatibility.
        """
        logger.debug(f"Starting evaluation of function signature")
        self.argsig = inspect.signature(self.f)
        self.argnames = list(self.argsig.parameters.keys())
        logger.debug("Function signature: %s", self.argsig)
        args = list(self.argsig.parameters.keys())
        self.fn_nargs = len(self.argsig.parameters)
        # set defaults
        self.fn_nposkw = 0
        self.poskw = {}
        self.fn_npos = 0
        self.posonly = {}
        self.kwonly = {}
        self.fn_nkw = 0
        self.varargs = False
        self.varkw = False
        self.fn_ndef = 0
        for k, p in self.argsig.parameters.items():
            if not isinstance(p, str):
                if p.kind == p.POSITIONAL_OR_KEYWORD:
                    self.fn_nposkw += 1
                    self.poskw.update({k: p})
                elif p.kind == p.POSITIONAL_ONLY:
                    self.fn_npos += 1
                    self.posonly.update({k: p})
                elif p.kind == p.KEYWORD_ONLY:
                    self.fn_nkw += 1
                    self.kwonly.update({k: p})
                elif p.kind == p.VAR_POSITIONAL:
                    self.varargs = True
                elif p.kind == p.VAR_KEYWORD:
                    self.varkw = True
                if p.default != inspect._empty:
                    self.arguments_defaults.append(p.default)
                    self.fn_ndef += 1
                else:
                    self.arguments_defaults.append(None)
                    self.fn_ndef += 1

        logger.debug("Got signature for function %s %s", self.f, self.argsig)
        logger.debug("Got default values for arguments %s", self.arguments_defaults)
        self.fn_defaults = self.arguments_defaults
        logger.debug(f"initialized fn_defaults with {self.fn_defaults}")

    def _clean_applicationArgs(self):
        """
        remove function definition arguments in applicationArgs
        """
        self.func_def_keywords = [
            "func_code",
            "func_name",
            "func_arg_mapping",
            "input_parser",
            "output_parser",
            "func_defaults",
            "pickle",
        ]

        for kw in self.func_def_keywords:
            if kw in self._applicationArgs:  # these are the preferred ones now
                if isinstance(
                    self._applicationArgs[kw]["value"],
                    bool
                    or self._applicationArgs[kw]["value"]
                    or self._applicationArgs[kw]["precious"],
                ):
                    # only transfer if there is a value or precious is True
                    self._applicationArgs.pop(kw)

    def initialize_with_func_code(self):
        """
        This function takes over if code is passed in through an argument.
        """
        if not isinstance(self.func_code, bytes):
            self.func_code = base64.b64decode(self.func_code.encode("utf8"))
        self.f = import_using_code(self.func_code)
        self._init_fn_defaults()
        # make sure defaults are dicts
        self._mixin_func_defaults()
        if isinstance(self.func_arg_mapping, str):
            self.func_arg_mapping = ast.literal_eval(self.func_arg_mapping)

    def initialize(self, **kwargs):
        """
        The initialization of a function component is mainly dealing with mapping
        inputs and provided applicationArgs to the function arguments. All of this
        should be driven by matching names.
        """
        BarrierAppDROP.initialize(self, **kwargs)

        env = os.environ.copy()
        env.update({"DLG_UID": self._uid})
        if self._dlg_session_id:
            env.update({"DLG_SESSION_ID": self._dlg_session_id})
        elif "dlg_session_id" in kwargs:
            self._dlg_session_id = kwargs["dlg_session_id"]
            env.update({"DLG_SESSION_ID": self._dlg_session_id})

        self._applicationArgs = self._popArg(kwargs, "applicationArgs", {})

        self.func_code = self._popArg(kwargs, "func_code", None)
        self.arguments_defaults = []

        # backwards compatibility
        if "pickle" in self._applicationArgs:
            if self._applicationArgs["pickle"]["value"] == True:
                self.input_parser = DropParser.PICKLE
                self.output_parser = DropParser.PICKLE
            else:
                self.input_parser = DropParser.EVAL
                self.output_parser = DropParser.EVAL
            self._applicationArgs.pop("pickle")

        self._clean_applicationArgs()

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
            self._init_fn_defaults()
        else:
            self.initialize_with_func_code()

        logger.info(f"Args summary for '{self.func_name}':")
        logger.info(f"Args: {self.argnames}")
        logger.info(f"Args defaults:  {self.fn_defaults}")
        logger.info(f"Args pos/kw: {list(self.poskw.keys())}")
        logger.info(f"Args keyword only: {list(self.kwonly.keys())}")
        logger.info(f"VarArgs allowed:  {self.varargs}")
        logger.info(f"VarKwds allowed:  {self.varkw}")

        # Mapping between argument name and input drop uids
        logger.debug(f"Input mapping provided: {self.func_arg_mapping}")
        self._recompute_data = {}

    def run(self):
        """
        Function positional and keyword argument treatment:

        Function arguments can be provided in three different ways:
        1) Through an input port
        2) By specifying ApplicationArgs (one for each argument)
        3) Through defaults at the time of function definition

        The priority follows the list above with input ports overruling the others.
        Function arguments in Python can be passed as positional, kw-value, positional
        only, kw-value only, and catch-all args and kwargs, which don't provide any
        hint about the names of accepted parameters. All of them are now supported. If
        positional arguments or kw-value arguments are provided by the user, but are
        not explicitely defined in the function signiture AND args and/or kwargs are
        allowed then these arguments are passed to the function. For args this is
        somewhat risky, since the order is relevant and in this code derived from the
        order defined in the graph (same order as defined in the component description).

        Input ports will NOT be used by index (anymore), but by using the name of the port.
        Since each input port requires an associated data drop, this provides a unique
        mapping. This also allows to pass values to any function argument through a port.

        """
        funcargs = {}
        # Inputs are un-pickled and treated as the arguments of the function
        # Their order must be preserved, so we use an OrderedDict
        if self.input_parser is DropParser.PICKLE:
            # all_contents = lambda x: pickle.loads(droputils.allDropContents(x))
            all_contents = drop_loaders.load_pickle
        elif self.input_parser is DropParser.EVAL:

            def optionalEval(x):
                # Null and Empty Drops will return an empty byte string
                # which should propogate back to None
                content: str = droputils.allDropContents(x).decode("utf-8")
                return ast.literal_eval(content) if len(content) > 0 else None

            all_contents = optionalEval
        elif self.input_parser is DropParser.NPY:
            all_contents = drop_loaders.load_npy
        elif self.input_parser is DropParser.PATH:
            all_contents = lambda x: x.path
        elif self.input_parser is DropParser.DATAURL:
            all_contents = lambda x: x.dataurl
        else:
            raise ValueError(self.input_parser.__repr__())

        inputs = collections.OrderedDict()
        for uid, drop in self._inputs.items():
            inputs[uid] = all_contents(drop)

        outputs = collections.OrderedDict()
        for uid, drop in self._outputs.items():
            if self.output_parser is DropParser.PATH:
                outputs[uid] = drop.path
            else:
                outputs[uid] = None

        # Keyword arguments are made up of the default values plus the inputs
        # that match one of the keyword argument names
        # if defaults dict has not been specified at all we'll go ahead anyway

        # 1. Fill arguments with rest of inputs
        logger.debug(f"available inputs: {inputs}")

        posargs = list(self.posonly.keys()) + list(self.poskw.keys())
        kwargs = {}
        pargs = []
        # fill the pargsDict with positional and poskw arguments and defaults
        pargsDict = {k: v.default for k, v in self.posonly.items()}
        pargsDict.update({k: v.default for k, v in self.poskw.items()})
        logger.debug("Initial pos_kwargs dictionary: %s", pargsDict)
        # fill the keyargsDict with kwonly arguments and defaults
        keyargsDict = {k: v.default for k, v in self.kwonly.items()}
        logger.debug("Initial kwonly dictionary: %s", self.kwonly)

        # replace default argument values with provided values
        if "applicationArgs" in self.parameters:
            appArgs = self.parameters[
                "applicationArgs"
            ]  # we'll pop the identified ones
            _dum = [appArgs.pop(k) for k in self.func_def_keywords if k in appArgs]
            logger.debug(
                "Default keyword arguments removed: %s",
                [i for i in _dum],
            )
            # update the positional args
            pargsDict.update(
                {k: self.parameters[k] for k in pargsDict if k in self.parameters}
            )
            # if defined in both we use AppArgs values
            for k in appArgs:
                # check value type and interpret
                if appArgs[k]["type"] in ["Json", "Complex"]:
                    try:
                        value = ast.literal_eval(appArgs[k]["value"])
                        logger.debug(
                            f"Evaluated %s to %s",
                            appArgs[k]["value"],
                            type(value),
                        )
                        appArgs[k]["value"] = value
                    except ValueError:
                        logger.error("Unable to evaluate %s", appArgs[k]["value"])
                else:
                    value = appArgs[k]["value"]
                if k in pargsDict:
                    pargsDict.update({k: value})

            _ = [appArgs.pop(k) for k in pargsDict if k in appArgs]
            logger.debug("Updated posargs dictionary: %s", pargsDict)

            keyargsDict.update(
                {k: appArgs[k]["value"] for k in keyargsDict if k in appArgs}
            )
            logger.debug("Updated keyargs dictionary: %s", keyargsDict)

            # 2. put all remaining arguments into *args and **kwargs
            # TODO: This should only be done if the function signature allows it
            vparg = []
            vkarg = {}
            logger.debug(f"Remaining AppArguments {appArgs}")
            for arg in appArgs:
                if appArgs[arg]["type"] in ["Json", "Complex"]:
                    value = ast.literal_eval(appArgs[arg]["value"])
                else:
                    value = appArgs[arg]["value"]
                if appArgs[arg]["positional"]:
                    vparg.append(value)
                else:
                    vkarg.update({arg: value})

            # TODO: check where this is defined in signature
            self.arguments = inspect.getfullargspec(self.f)
            if self.arguments.varargs:
                logger.debug("Adding remaining *args to pargs %s", vparg)
                pargs.extend(vparg)
            if self.arguments.varkw:
                logger.debug("Adding remaining **kwargs to funcargs: %s", vkarg)
                funcargs.update(vkarg)

        # 3. replace default argument values with named input ports
        # TODO: investigate performing inputs and outputs in a single call
        if "inputs" in self.parameters and check_ports_dict(self.parameters["inputs"]):
            check_len = min(
                len(inputs),
                self.fn_nargs + self.fn_nkw,
            )
            inputs_dict = collections.OrderedDict()
            for inport in self.parameters["inputs"]:
                key = list(inport.keys())[0]
                inputs_dict[key] = {"name": inport[key], "path": inputs[key]}
            kwargs.update(
                identify_named_ports(
                    inputs_dict,
                    posargs,
                    pargsDict,
                    keyargsDict,
                    check_len=check_len,
                    mode="inputs",
                )
            )
        else:
            # Just as a fallback using the index, but this is risky!
            for i in range(min(len(inputs), self.fn_nargs)):
                kwargs.update({self.argnames[i]: list(inputs.values())[i]})

        # 4. replace default argument values with named output ports
        if "outputs" in self.parameters and check_ports_dict(
            self.parameters["outputs"]
        ):
            check_len = min(len(outputs), self.fn_nargs + self.fn_nkw)
            outputs_dict = collections.OrderedDict()
            for outport in self.parameters["outputs"]:
                key = list(outport.keys())[0]
                outputs_dict[key] = {
                    "name": outport[key],
                    "path": outputs[key],
                }

            kwargs.update(
                identify_named_ports(
                    outputs_dict,
                    posargs,
                    pargsDict,
                    keyargsDict,
                    check_len=check_len,
                    mode="outputs",
                )
            )
        logger.debug(f"Updating funcargs with values from pargsDict {pargsDict}")
        funcargs.update(pargsDict)

        logger.debug(f"Updating funcargs with values from named ports {kwargs}")
        funcargs.update(kwargs)

        self._recompute_data["args"] = funcargs.copy()

        # 5. remove self argument if this is the initializer.
        if (
            self.func_name is not None
            and self.func_name.split(".")[-1] in ["__init__", "__class__"]
            and "self" in funcargs
        ):
            funcargs.pop("self")
        logger.info(f"Running {self.func_name} with *{pargs} **{funcargs}")

        # 6. prepare for execution
        # we capture and log whatever is produced on STDOUT
        capture = StringIO()
        try:
            bind = self.argsig.bind(*pargs, **funcargs)
            logger.debug("Bound arguments: %s", bind)
        except TypeError as e:
            logger.error("Binding of arguments failed: %s", e)
            raise

        # Here is where the function is actually executed
        with redirect_stdout(capture):
            result = self.f(*bind.args, **bind.kwargs)

        logger.debug("Returned result from %s: %s", self.func_name, result)
        logger.info(
            f"Captured output from function app '{self.func_name}': {capture.getvalue()}"
        )
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
                if self.output_parser is DropParser.PICKLE:
                    logger.debug(f"Writing pickeled result {type(r)} to {o}")
                    o.write(pickle.dumps(r))
                elif self.output_parser is DropParser.EVAL:
                    o.write(repr(r).encode("utf-8"))
                elif self.output_parser is DropParser.NPY:
                    drop_loaders.save_npy(o, r)
                else:
                    ValueError(self.output_parser.__repr__())

    def generate_recompute_data(self):
        for name, val in self._recompute_data.items():
            try:
                json.dumps(val)
            except TypeError as e:
                logger.debug(e)
                self._recompute_data[name] = repr(val)
        return self._recompute_data
