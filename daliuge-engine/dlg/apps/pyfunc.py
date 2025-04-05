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
import re

from typing import Callable
import dill
from io import StringIO
from contextlib import redirect_stdout

from dlg import drop_loaders
from dlg.utils import serialize_data, deserialize_data
from dlg.named_port_utils import (
    DropParser,
    check_ports_dict,
    get_port_reader_function,
    identify_named_ports,
    replace_named_ports,
)
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
from dlg.pyext import pyext

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


def import_using_code(func_code: str, func_name: str, serialized: bool = True):
    """
    Import the function provided as a code string. Plain code as well as serialized code
    is supported. If the func_name does not match the provided func_name the load will fail.
    """
    if not serialized:
        logger.debug(f"Trying to import code from string: {func_code}")
        mod = pyext.RuntimeModule.from_string("mod", func_name, func_code)
        logger.debug("Imported function: %s", func_name)
        if func_name:
            if hasattr(mod, func_name):
                func = getattr(mod, func_name)
            else:
                logger.warning("Function with name '%s' not found!", func_name)
                raise ValueError(f"Function with name '{func_name}' not found!")
    else:
        try:
            func = dill.loads(func_code)
        except Exception as err:
            logger.warning("Unable to deserialize func_code: %s", err)
            raise
        if func_name and func_name.split(".")[-1] != func.__name__:
            raise ValueError(
                f"Function with name '{func.__name__}' instead of '{func_name}' found!"
            )
    logger.debug("Imported function: %s", func_name)
    return func


##
# @brief PythonMemberFunction
# @details A placeholder APP to aid construction of new class member function applications.
# This is mainly useful (and used) when starting a new workflow from scratch.
# @par EAGLE_START
# @param category PythonMemberFunction
# @param tag daliuge
# @param func_name object.__init__/String/ComponentParameter/NoPort/ReadWrite//False/False/Python function name
# @param func_code /String/ComponentParameter/NoPort/ReadWrite//False/False/Python function code, e.g. 'def f(args): return args'. Function name has to be 'f'!
# @param dropclass dlg.apps.pyfunc.PyFuncApp/String/ComponentParameter/NoPort/ReadOnly//False/False/Application class
# @param base_name Object/String/ComponentParameter/NoPort/ReadOnly//False/False/Base name of application class
# @param self /Object/ApplicationArgument/InputOutput/ReadWrite//False/False/Port exposing the object
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
# @details An application that wraps a python function.
# The inputs of the application are treated as the arguments of the function.
# Conversely, the output of the function is treated as the output of the
# application. If the application has more than one output, the result of
# calling the function is treated as an iterable, with each individual object
# being written to its corresponding output.
# @par EAGLE_START
# @param category PythonApp
# @param tag template
# @param func_name /String/ComponentParameter/NoPort/ReadWrite//False/False/Python function name
# @param func_code /String/ComponentParameter/NoPort/ReadWrite//False/False/Python function code, e.g. 'def func_name(args): return args'. Note that func_name above needs to match the defined name here.
# @param dropclass dlg.apps.pyfunc.PyFuncApp/String/ComponentParameter/NoPort/ReadOnly//False/False/Application class
# @param base_name pyfunc/String/ComponentParameter/NoPort/ReadOnly//False/False/Base name of application class
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
    func_arg_mapping = dlg_dict_param("func_arg_mapping", {})
    func_defaults = dlg_dict_param("func_defaults", {})
    func: Callable
    fdefaults: dict

    # backwards compatibility
    input_parser = dlg_enum_param(DropParser, "input_parser", DropParser.DILL)
    output_parser = dlg_enum_param(DropParser, "output_parser", DropParser.DILL)

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
                self.func.__name__,
                self.func_defaults,
                type(self.func_defaults),
            )
            raise ValueError
        if hasattr(self, "input_parser") and self.input_parser in [
            DropParser.PICKLE,
            DropParser.DILL,
        ]:
            # only values are pickled, get them unpickled
            for name, value in self.func_defaults.items():
                self.func_defaults[name] = deserialize_data(value)
        # the fn_defaults are used afterwards, we'll drop the func_defaults
        logger.debug("fn_defaults %s", self.fn_defaults)
        logger.debug("func_defaults %s", self.func_defaults)
        self.fn_defaults.update(self.func_defaults)

    def _init_fn_defaults(self):
        """
        Inititalize self.fn_defaults dictionary from values provided.
        Multiple options exist and some are here for compatibility.
        """
        logger.debug(f"Starting evaluation of function signature")
        self.argsig = inspect.signature(self.func)
        self.argnames = list(self.argsig.parameters.keys())
        logger.debug("Function signature: %s", self.argsig)
        args = list(self.argsig.parameters.keys())
        self.fn_nargs = len(self.argsig.parameters)
        # set defaults
        self.fn_defaults = {}
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
                    self.fn_defaults[k] = p.default
                    # self.arguments_defaults.append(p.default)
                    self.fn_ndef += 1
                else:
                    self.fn_defaults[k] = None
                    self.arguments_defaults.append(None)
                    self.fn_ndef += 1

        logger.debug("Got signature for function %s %s", self.func, self.argsig)
        logger.debug("Got default values for arguments %s", self.arguments_defaults)
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

    def _initialise_positional_args(self, argsDict: dict):
        """
        Ensure default arguments are properly setup.
        # If there are default values in the function sig
        # If there are values in the graph
        """
        for arg, value in argsDict.items():
            if arg in self._applicationArgs:
                encoding = self._applicationArgs[arg].get("encoding", self.input_parser)
            else:
                encoding = self.input_parser
            argsDict[arg] = {"value": value, "encoding": encoding}
            param_arg = self.parameters.get(arg, None)
            if param_arg:
                argsDict[arg]["value"] = param_arg

        return argsDict

    def _get_arg_info(self, arg):

        value = self._applicationArgs[arg]["value"]
        encoding = self._applicationArgs[arg].get("encoding", self.input_parser)

        return value, encoding

    def _init_appArgs(self, pargsDict: dict, keyargsDict: dict, posargs: list) -> list:
        """
        Identify and fill application arguments.

        Deals with positional and keyword arguments.

        pargsDict: dict,

        keyargsDict:

        posargs: list


        Returns:
        --------
        list, [funcargs, pargs]
        """
        pargs = []  # positional arguments
        funcargs = {}  # Function arguments
        vparg = []
        vkarg = {}

        # update the positional args
        pargsDict = self._initialise_positional_args(pargsDict)
        if self._applicationArgs:
            # if defined in both we use AppArgs values
            for arg in self._applicationArgs:
                # check value type and interpret
                value, encoding = self._get_arg_info(arg)
                if self._applicationArgs[arg]["type"] in ["Json", "Complex"]:
                    try:
                        value = ast.literal_eval(value)
                        logger.debug(f"Evaluated %s to %s", value, type(value))
                        self._applicationArgs[arg]["value"] = value
                    except ValueError:
                        logger.error(
                            "Unable to evaluate %s", self._applicationArgs[arg]["value"]
                        )
                if arg in pargsDict:
                    pargsDict[arg] = {"value": value, "encoding": encoding}
                if arg in keyargsDict:
                    keyargsDict[arg] = {"value": value, "encoding": encoding}

            _ = [
                self._applicationArgs.pop(k)
                for k in pargsDict
                if k in self._applicationArgs
            ]
            logger.debug("Updated posargs dictionary: %s", pargsDict)
            logger.debug("Updated keyargs dictionary: %s", keyargsDict)

            # Put all remaining arguments into *args and **kwargs
            # TODO: This should only be done if the function signature allows it
            logger.debug(f"Remaining AppArguments {self._applicationArgs}")
            for arg in self._applicationArgs:
                if self._applicationArgs[arg]["type"] in ["Json", "Complex"]:
                    value = ast.literal_eval(self._applicationArgs[arg]["value"])
                else:
                    value = self._applicationArgs[arg]["value"]
                if self._applicationArgs[arg]["positional"]:
                    vparg.append(value)
                else:
                    vkarg[arg] = value
        else:
            logger.debug("AppArgs/pargsDict: %s", pargsDict)
            encoding = self.input_parser if hasattr(self, "input_parser") else None

        # TODO: check where this is defined in signature
        self.arguments = inspect.getfullargspec(self.func)
        if self.arguments.varargs:
            logger.debug("Adding remaining *args to pargs %s", vparg)
            pargs.extend(vparg)
        if self.arguments.varkw:
            logger.debug("Adding remaining **kwargs to funcargs: %s", vkarg)
            funcargs.update(vkarg)

        # Extract arg and values from pargs; we no longer need the encoding
        logger.debug(f"Updating funcargs with values from pargsDict: {pargsDict}")
        tmpPargs = {arg: subdict["value"] for arg, subdict in pargsDict.items()}
        funcargs.update(tmpPargs)

        # Mixin the values from named ports
        portargs = self._ports2args(posargs, pargsDict, keyargsDict)

        logger.debug(f"Updating funcargs with values from named ports {portargs}")
        tmpPortArgs = {port: subdict["value"] for port, subdict in portargs.items()}
        funcargs.update(tmpPortArgs)

        return [funcargs, pargs]

    def _ports2args(self, posargs, pargsDict, keyargsDict) -> dict:
        """
        Replace arguments with values from ports.

        Returns:
        --------
        portargs dictionary
        """
        portargs = {}
        # 3. replace default argument values with named input ports
        iitems = self._inputs
        logger.debug("Mapping from _inputs: %s", iitems)
        logger.debug("Parameters: %s", self.parameters)
        if "inputs" in self.parameters and check_ports_dict(self.parameters["inputs"]):
            logger.debug("Mapping ports to inputs...")
            check_len = min(
                len(iitems),
                self.fn_nargs + self.fn_nkw,
            )
            inputs_dict = collections.OrderedDict()
            for inport in self.parameters["inputs"]:
                key = list(inport.keys())[0]
                inputs_dict[key] = {
                    "name": inport[key],
                    "path": None,
                    "drop": iitems[key],
                }
            parser = (
                get_port_reader_function(self.input_parser)
                if hasattr(self, "input_parser")
                else None
            )
            portargs.update(
                identify_named_ports(
                    inputs_dict,
                    posargs,
                    pargsDict,
                    keyargsDict,
                    check_len=check_len,
                    mode="inputs",
                    addPositionalToKeyword=True,
                    parser=parser,
                )
            )
        else:
            for i, input_drop in enumerate(iitems.values()):
                parser = (
                    get_port_reader_function(self.input_parser)
                    if hasattr(self, "input_parser")
                    else None
                )
                value = parser(input_drop)
                logger.debug("Port value pair: %s, %s", self.argnames[i], value)

        logger.debug(
            "Finally port mapping: %s, %s, %s", portargs, pargsDict, keyargsDict
        )
        return portargs

    def initialize_with_func_code(self):
        """
        This function takes over if code is passed in through an argument.
        """
        serialized = False
        logger.debug(f"Initializing with func_code of type {type(self.func_code)}")
        if not isinstance(self.func_code, bytes):
            try:
                self.func = import_using_code(
                    self.func_code, self.func_name, serialized=False
                )
            except (SyntaxError, NameError) as err:
                logger.warning(
                    "Problem importing code: %s. Checking whether it was serialized.",
                    err,
                )
                serialized = True
        if isinstance(self.func_code, bytes) or serialized:
            if isinstance(self.func_code, str):
                self.func_code = base64.b64decode(self.func_code.encode("utf8"))
            self.func = import_using_code(
                self.func_code, self.func_name, serialized=True
            )

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
            self.func = import_using_name(self, self.func_name)
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

        # Keyword arguments are made up of the default values plus the inputs
        # that match one of the keyword argument names
        # if defaults dict has not been specified at all we'll go ahead anyway

        # 1. Fill arguments with rest of inputs
        # TODO improve naming conventions
        posargs = list(self.posonly.keys()) + list(self.poskw.keys())
        # fill the pargsDict with positional and poskw arguments and defaults
        pargsDict = {k: v.default for k, v in self.posonly.items()}
        pargsDict.update({k: v.default for k, v in self.poskw.items()})
        logger.debug("Initial pos_kwargs dictionary: %s", pargsDict)
        # fill the keyargsDict with kwonly arguments and defaults
        keyargsDict = {k: v.default for k, v in self.kwonly.items()}
        logger.debug("Initial kwonly dictionary: %s", self.kwonly)

        for arg in pargsDict:
            pargsDict[arg] = self.fn_defaults[arg]
        for arg in keyargsDict:
            keyargsDict[arg] = self.fn_defaults[arg]

        # deal with arguments of any sort
        funcargs, pargs = self._init_appArgs(pargsDict, keyargsDict, posargs)

        self._recompute_data["args"] = funcargs.copy()

        # 5. remove self argument if this is the initializer.
        if (
            self.func_name is not None
            and self.func_name.split(".")[-1] in ["__init__", "__class__"]
            and "self" in funcargs
        ):
            funcargs.pop("self")
        logger.info(f"Running {self.func_name}")
        logger.debug(f"Arguments: *{pargs} **{funcargs}")

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
            result = self.func(*bind.args, **bind.kwargs)

        logger.debug("Returned result from %s: %s", self.func_name, result)
        logger.info(
            f"Captured output from function app '{self.func_name}': {capture.getvalue()}"
        )
        logger.debug(f"Finished execution of {self.func_name}.")

        # Depending on how many outputs we have we treat our result
        # as an iterable or as a single object. Each result is pickled
        # and written to its corresponding output
        self.write_results(result)

    def _match_parser(self, output_drop):
        """
        Match the output parser to the appropriate drop
        """

        encoding = "dill"
        component_params = self.parameters.get("componentParams")
        if not component_params:
            return self.output_parser
        if "outputs" in self.parameters and check_ports_dict(
            self.parameters["outputs"]
        ):
            for outport in self.parameters["outputs"]:
                drop_uid, drop_port = list(outport.items())[0]
                if drop_uid == output_drop.uid and drop_port in component_params:
                    param_enc = component_params[drop_port]["encoding"]
                    encoding = param_enc or encoding
        return DropParser(encoding) if encoding else self.output_parser

    def write_results(self, result):
        from dlg.droputils import listify

        if not self.outputs:
            return
        result_iter = listify(result)
        logger.debug(
            "Writing follow result to %d output: %s", len(self.outputs), result_iter
        )
        for i, o in enumerate(self.outputs):
            # result = result_iter[0]
            if len(result_iter) == 1:  # and len(self.outputs) > 1:
                # We only have one element, no need to save as a list
                result = result_iter[0]
            elif len(result_iter) > 1 and len(self.outputs) == 1:
                # We want all elements in the list to go to the output
                result = result
            else:
                # Iterate over each element of the list for each output
                # Wrap around for len(result_iter) < len(self.outputs)
                i = i % len(self.outputs)
                result = result_iter[i]

            parser = self._match_parser(o)
            if parser is DropParser.PICKLE:
                logger.debug(f"Writing pickeled result {type(result)} to {o}")
                o.write(pickle.dumps(result))
            elif parser is DropParser.EVAL or parser is DropParser.UTF8:
                encoded_result = repr(result).encode("utf-8")
                o.write(encoded_result)
            elif parser is DropParser.NPY:
                import numpy as np

                if not isinstance(result, np.ndarray):
                    try:
                        result = np.array(result)
                    except Exception as e:
                        raise (e)
                drop_loaders.save_npy(o, result)
            elif parser is DropParser.RAW:
                o.write(result)
            elif parser is DropParser.DILL:
                o.write(dill.dumps(result))
            elif parser is DropParser.BINARY:
                drop_loaders.save_binary(o, result)
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
