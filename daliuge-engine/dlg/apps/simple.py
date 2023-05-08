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
"""Applications used as examples, for testing, or in simple situations"""
import _pickle
from numbers import Number
import pickle
import random
from typing import List, Optional
import urllib.error
import urllib.request
import logging
import time
import ast
import numpy as np

from dlg import droputils, utils
from dlg.apps.app_base import BarrierAppDROP
from dlg.data.drops.container import ContainerDROP
from dlg.apps.branch import BranchAppDrop
from dlg.meta import (
    dlg_float_param,
    dlg_string_param,
    dlg_bool_param,
    dlg_int_param,
    dlg_list_param,
    dlg_component,
    dlg_batch_input,
    dlg_batch_output,
    dlg_streaming_input,
)
from dlg.exceptions import DaliugeException, DropChecksumException
from dlg.apps.pyfunc import serialize_data, deserialize_data


logger = logging.getLogger(__name__)


class NullBarrierApp(BarrierAppDROP):
    component_meta = dlg_component(
        "NullBarrierApp",
        "Null Barrier.",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

    """A BarrierAppDrop that doesn't perform any work"""

    def run(self):
        pass


##
# @brief PythonApp
# @details A placeholder APP to aid construction of new applications.
# This is mainly useful (and used) when starting a new workflow from scratch.
# @par EAGLE_START
# @param category PythonApp
# @param tag template
# @param dropclass Application Class/PythonApp/String/ComponentParameter/readonly//False/False/Application class
# @param num_cpus No. of CPUs/1/Integer/ComponentParameter/readonly//False/False/Number of cores used
# @param execution_time Execution Time/5/Float/ComponentParameter/readonly//False/False/Estimated execution time
# @param group_start Group start/False/Boolean/ComponentParameter/readwrite//False/False/Is this node the start of a group?
# @par EAGLE_END
class PythonApp(BarrierAppDROP):
    """A placeholder BarrierAppDrop that just aids the generation of the palette component"""

    pass


##
# @brief SleepApp
# @details A simple APP that sleeps the specified amount of time (0 by default).
# This is mainly useful (and used) to test graph translation and structure
# without executing real algorithms. Very useful for debugging.
# @par EAGLE_START
# @param category PythonApp
# @param tag daliuge
# @param sleep_time sleep_time/5/Integer/ApplicationArgument/readwrite//False/False/The number of seconds to sleep
# @param dropclass dropclass/dlg.apps.simple.SleepApp/String/ComponentParameter/readonly//False/False/Application class
# @param execution_time execution_time/5/Float/ComponentParameter/readonly//False/False/Estimated execution time
# @param num_cpus num_cpus/1/Integer/ComponentParameter/readonly//False/False/Number of cores used
# @param group_start group_start/False/Boolean/ComponentParameter/readwrite//False/False/Is this node the start of a group?
# @par EAGLE_END
class SleepApp(BarrierAppDROP):
    """A BarrierAppDrop that sleeps the specified amount of time (0 by default)"""

    component_meta = dlg_component(
        "SleepApp",
        "Sleep App.",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )
    pname = "sleep_time"
    sleep_time = dlg_float_param(pname, 0)

    def initialize(self, **kwargs):
        super(SleepApp, self).initialize(**kwargs)

    def run(self):
        if self.sleep_time is None:
            if len(self.inputs) > 0:
                for inp in self.inputs:
                    if inp.name == self.pname:
                        self.sleep_time = pickle.loads(
                            droputils.allDropContents(inp)
                        )
        try:
            time.sleep(self.sleep_time)
        except (TypeError, ValueError):
            self.sleep_time = 0
            time.sleep(self.sleep_time)
        logger.debug("%s slept for %s s", self.name, self.sleep_time)


##
# @brief CopyApp
# @details A simple APP that copies its inputs into its outputs.
# All inputs are copied into all outputs in the order they were declared in
# the graph. If an input is a container (e.g. a directory) it copies the
# content recursively.
# @par EAGLE_START
# @param category PythonApp
# @param tag daliuge
# @param bufsize buffer size/65536/Integer/ApplicationArgument/readwrite//False/False/Buffer size
# @param dropclass dropclass/dlg.apps.simple.CopyApp/String/ComponentParameter/readonly//False/False/Application class
# @param execution_time execution_time/5/Float/ComponentParameter/readonly//False/False/Estimated execution time
# @param num_cpus num_cpus/1/Integer/ComponentParameter/readonly//False/False/Number of cores used
# @param group_start group_start/False/Boolean/ComponentParameter/readwrite//False/False/Is this node the start of a group?
# @param n_tries Number of tries/1/Integer/ComponentParameter/readwrite//False/False/Specifies the number of times the 'run' method will be executed before finally giving up
# @param dummy_in dummy//Object/InputPort/readwrite//False/False/Dummy input port
# @param dummy_out dummy//Object/OutputPort/readwrite//False/False/Dummy output port
# @param input_parser Input Parser/pickle/Select/ApplicationArgument/readwrite/raw,pickle,eval,npy,path,dataurl/False/False/Input port parsing technique
# @param output_parser Output Parser/pickle/Select/ApplicationArgument/readwrite/raw,pickle,eval,npy,path,dataurl/False/False/Output port parsing technique
# @par EAGLE_END
class CopyApp(BarrierAppDROP):
    """
    A BarrierAppDrop that copies its inputs into its outputs.
    All inputs are copied into all outputs in the order they were declared in
    the graph.
    """

    bufsize = dlg_int_param("bufsize", 65536)

    component_meta = dlg_component(
        "CopyApp",
        "Copy App.",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

    def run(self):
        logger.debug("Using buffer size %d", self.bufsize)
        logger.info(
            "Copying data from inputs %s to outputs %s",
            [x.name for x in self.inputs],
            [x.name for x in self.outputs],
        )
        self.copyAll()
        logger.info(
            "Copy finished",
        )

    def copyAll(self):
        for inputDrop in self.inputs:
            self.copyRecursive(inputDrop)

        # logger.debug("Target checksum: %d", outputDrop.checksum)

    def copyRecursive(self, inputDrop):
        if isinstance(inputDrop, ContainerDROP):
            for child in inputDrop.children:
                self.copyRecursive(child)
        else:
            for outputDrop in self.outputs:
                droputils.copyDropContents(
                    inputDrop, outputDrop, bufsize=self.bufsize
                )


##
# @brief SleepAndCopyApp
# @par EAGLE_START
# @param category PythonApp
# @param tag daliuge
# @param sleep_time sleep_time/5/Integer/ApplicationArgument/readwrite//False/False/The number of seconds to sleep
# @param input_parser Input Parser/pickle/Select/ApplicationArgument/readwrite/raw,pickle,eval,npy,path,dataurl/False/False/Input port parsing technique
# @param output_parser Output Parser/pickle/Select/ApplicationArgument/readwrite/raw,pickle,eval,npy,path,dataurl/False/False/Output port parsing technique
# @param dropclass dropclass/dlg.apps.simple.SleepAndCopyApp/String/ComponentParameter/readonly//False/False/Application class
# @param execution_time execution_time/5/Float/ComponentParameter/readonly//False/False/Estimated execution time
# @param num_cpus num_cpus/1/Integer/ComponentParameter/readonly//False/False/Number of cores used
# @param group_start group_start/False/Boolean/ComponentParameter/readwrite//False/False/Is this node the start of a group?
# @par EAGLE_END
class SleepAndCopyApp(SleepApp, CopyApp):
    """A combination of the SleepApp and the CopyApp. It sleeps, then copies"""

    def run(self):
        SleepApp.run(self)
        CopyApp.run(self)


##
# @brief RandomArrayApp
# @details A testing APP that does not take any input and produces a random array of
# type int64, if integer is set to True, else of type float64.
# size indicates the number of elements ranging between the values low and high.
# The resulting array will be send to all connected output apps.
# @par EAGLE_START
# @param category PythonApp
# @param tag daliuge
# @param size size/100/Integer/ApplicationArgument/readwrite//False/False/The size of the array
# @param low low/0/Float/ApplicationArgument/readwrite//False/False/Low value of range in array [inclusive]
# @param high high/1/Float/ApplicationArgument/readwrite//False/False/High value of range of array [exclusive]
# @param integer integer/True/Boolean/ApplicationArgument/readwrite//False/False/Generate integer array?
# @param dropclass dropclass/dlg.apps.simple.RandomArrayApp/String/ComponentParameter/readonly//False/False/Application class
# @param input_parser Input Parser/pickle/Select/ApplicationArgument/readwrite/raw,pickle,eval,npy,path,dataurl/False/False/Input port parsing technique
# @param output_parser Output Parser/pickle/Select/ApplicationArgument/readwrite/raw,pickle,eval,npy,path,dataurl/False/False/Output port parsing technique
# @param execution_time execution_time/5/Float/ComponentParameter/readonly//False/False/Estimated execution time
# @param num_cpus num_cpus/1/Integer/ComponentParameter/readonly//False/False/Number of cores used
# @param group_start group_start/False/Boolean/ComponentParameter/readwrite//False/False/Is this node the start of a group?
# @param array array//Object.Array/OutputPort/readwrite//False/False/Port carrying the averaged array
# @par EAGLE_END
class RandomArrayApp(BarrierAppDROP):
    """
    A BarrierAppDrop that generates an array of random numbers. It does
    not require any inputs and writes the generated array to all of its
    outputs.

    Keywords:

    integer:  bool [True], generate integer array
    low:      float, lower boundary (will be converted to int for integer arrays)
    high:     float, upper boundary (will be converted to int for integer arrays)
    size:     int, number of array elements
    """

    component_meta = dlg_component(
        "RandomArrayApp",
        "Random Array App.",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

    # default values
    integer = dlg_bool_param("integer", True)
    low = dlg_float_param("low", 0)
    high = dlg_float_param("high", 100)
    size = dlg_int_param("size", 100)
    marray = []

    def initialize(self, keep_array=False, **kwargs):
        super(RandomArrayApp, self).initialize(**kwargs)
        self._keep_array = keep_array

    def run(self):
        # At least one output should have been added
        outs = self.outputs
        if len(outs) < 1:
            raise Exception(
                "At least one output should have been added to %r" % self
            )
        marray = self.generateRandomArray()
        if self._keep_array:
            self.marray = marray
        for o in outs:
            d = pickle.dumps(marray)
            o.len = len(d)
            o.write(d)

    def generateRandomArray(self):
        if self.integer:
            # generate an array of self.size integers with numbers between
            # slef.low and self.high
            marray = np.random.randint(
                int(self.low), int(self.high), size=(self.size)
            )
        else:
            # generate an array of self.size floats with numbers between
            # self.low and self.high
            marray = (np.random.random(size=self.size) + self.low) * self.high
        return marray

    def _getArray(self):
        return self.marray


##
# @brief AverageArrays
# @details A testing APP that takes multiple numpy arrays on input and calculates
# the mean or the median, depending on the value provided in the method parameter.
# Users can add as many producers to the input array port as required and the resulting array
# will also be send to all connected output apps.
# @par EAGLE_START
# @param category PythonApp
# @param tag daliuge
# @param method method/mean/Select/ApplicationArgument/readwrite/mean,median/False/False/The method used for averaging
# @param dropclass dropclass/dlg.apps.simple.AverageArraysApp/String/ComponentParameter/readonly//False/False/Application class
# @param input_parser Input Parser/pickle/Select/ApplicationArgument/readwrite/raw,pickle,eval,npy,path,dataurl/False/False/Input port parsing technique
# @param output_parser Output Parser/pickle/Select/ApplicationArgument/readwrite/raw,pickle,eval,npy,path,dataurl/False/False/Output port parsing technique
# @param execution_time execution_time/5/Float/ComponentParameter/readonly//False/False/Estimated execution time
# @param num_cpus num_cpus/1/Integer/ComponentParameter/readonly//False/False/Number of cores used
# @param group_start group_start/False/Boolean/ComponentParameter/readwrite//False/False/Is this node the start of a group?
# @param array_in Array//Object.Array/InputPort/readwrite//False/False/Port for the input array(s)
# @param array_out Array//Object.Array/OutputPort/readwrite//False/False/Port carrying the averaged array
# @par EAGLE_END
class AverageArraysApp(BarrierAppDROP):
    """
    A BarrierAppDrop that averages arrays received on input. It requires
    multiple inputs and writes the generated average vector to all of its
    outputs.
    The input arrays are assumed to have the same number of elements and
    the output array will also have that same number of elements.

    Keywords:

    method:  string <['mean']|'median'>, use mean or median as method.
    """

    from numpy import mean, median

    component_meta = dlg_component(
        "AverageArraysApp",
        "Average Array App.",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

    # default values
    methods = ["mean", "median"]
    method = dlg_string_param("method", methods[0])

    def __init__(self, oid, uid, **kwargs):
        super().__init__(oid, kwargs)
        self.marray = []

    def initialize(self, **kwargs):
        super().initialize(**kwargs)

    def run(self):
        # At least one output should have been added

        outs = self.outputs
        if len(outs) < 1:
            raise Exception(
                "At least one output should have been added to %r" % self
            )
        self.getInputArrays()
        self._avg = self.averageArray()
        for o in outs:
            d = pickle.dumps(self._avg)
            o.len = len(d)
            o.write(d)  # average across inputs

    def getInputArrays(self):
        """
        Create the input array from all inputs received. Shape is
        (<#inputs>, <#elements>), where #elements is the length of the
        vector received from one input.
        """
        ins = self.inputs
        if len(ins) < 1:
            raise Exception(
                "At least one input should have been added to %r" % self
            )
        marray = []
        for inp in ins:
            sarray = droputils.allDropContents(inp)
            if len(sarray) == 0:
                print(f"Input does not contain data!")
            else:
                sarray = pickle.loads(sarray)
                if isinstance(sarray, (list, tuple, np.ndarray)):
                    marray.extend(list(sarray))
                else:
                    marray.append(sarray)
        self.marray = marray

    def averageArray(self):
        method_to_call = getattr(np, self.method)
        return method_to_call(self.marray, axis=0)


##
# @brief GenericGatherApp
# @details App that reads all its inputs and simply writes them in
# concatenated to all its outputs. This can be used stand-alone or
# as part of a Gather. It does not do anything to the data, just
# passing it on.
#
# @par EAGLE_START
# @param category PythonApp
# @param construct Gather
# @param tag daliuge
# @param num_of_inputs num_of_inputs/4/Integer/ConstructParameter/readwrite//False/False/The Gather “width”, stating how many inputs each Gather instance will handle
# @param dropclass dropclass/dlg.apps.simple.GenericGatherApp/String/ComponentParameter/readonly//False/False/Application class
# @param input_parser Input Parser/pickle/Select/ApplicationArgument/readwrite/raw,pickle,eval,npy,path,dataurl/False/False/Input port parsing technique
# @param output_parser Output Parser/pickle/Select/ApplicationArgument/readwrite/raw,pickle,eval,npy,path,dataurl/False/False/Output port parsing technique
# @param execution_time execution_time/5/Float/ComponentParameter/readonly//False/False/Estimated execution time
# @param num_cpus num_cpus/1/Integer/ComponentParameter/readonly//False/False/Number of cores used
# @param group_start group_start/False/Boolean/ComponentParameter/readwrite//False/False/Is this node the start of a group?
# @param input input//Object/InputPort/readwrite//False/False/0-base placeholder port for inputs
# @param output output//Object/OutputPort/readwrite//False/False/Placeholder port for outputs
# @par EAGLE_END
class GenericGatherApp(BarrierAppDROP):
    def readWriteData(self):
        inputs = self.inputs
        outputs = self.outputs
        total_len = 0
        for input in inputs:
            total_len += input.len
        for output in outputs:
            output.len = total_len
            for input in inputs:
                d = droputils.allDropContents(input)
                output.write(d)

    def run(self):
        self.readWriteData()


##
# @brief GenericNpyGatherApp
# @details A BarrierAppDrop that combines one or more inputs using cummulative operations.
# @par EAGLE_START
# @param category PythonApp
# @param construct Gather
# @param tag daliuge
# @param num_of_inputs num_of_inputs/4/Integer/ConstructParameter/readwrite//False/False/The Gather “width”, stating how many inputs each Gather instance will handle
# @param function function/sum/Select/ApplicationArgument/readwrite/sum,prod,min,max,add,multiply,maximum,minimum/False/False/The function used for gathering
# @param reduce_axes reduce_axes/None/String/ApplicationArgument/readonly//False/False/The ndarray axes to reduce, None reduces all axes for sum, prod, max, min functions
# @param dropclass dropclass/dlg.apps.simple.GenericNpyGatherApp/String/ComponentParameter/readonly//False/False/Application class
# @param input_parser Input Parser/pickle/Select/ApplicationArgument/readwrite/raw,pickle,eval,npy,path,dataurl/False/False/Input port parsing technique
# @param output_parser Output Parser/pickle/Select/ApplicationArgument/readwrite/raw,pickle,eval,npy,path,dataurl/False/False/Output port parsing technique
# @param execution_time execution_time/5/Float/ComponentParameter/readonly//False/False/Estimated execution time
# @param num_cpus num_cpus/1/Integer/ComponentParameter/readonly//False/False/Number of cores used
# @param group_start group_start/False/Boolean/ComponentParameter/readwrite//False/False/Is this node the start of a group?
# @param array_in array_in//Object.Array/InputPort/readwrite//False/False/Port for the input array(s)
# @param array_out array_out//Object.Array/OutputPort/readwrite//False/False/Port carrying the reduced array
# @par EAGLE_END
class GenericNpyGatherApp(BarrierAppDROP):
    """
    A BarrierAppDrop that reduces then gathers one or more inputs using cummulative operations.
    function:  string <'sum'|'prod'|'min'|'max'|'add'|'multiply'|'maximum'|'minimum'>.

    """

    component_meta = dlg_component(
        "GenericNpyGatherApp",
        "Generic Npy Gather App.",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

    # reduce and combine operation pair names
    # reduce operation reduces the dimensionality of a ndarray
    # gather operation combines ndarrays and retains dimensionality
    functions = {
        # reduce and gather (output dimension is reduced)
        "sum": "add",  # sum reduction of inputs along an axis first then gathers across drops
        "prod": "multiply",  # prod reduction of inputs along an axis first then gathers across drops
        "max": "maximum",  # max reduction of input along an axis first then gathers across drops
        "min": "minimum",  # min reduction of input along an axis first then gathers across drops
        # gather only
        "add": None,  # elementwise addition of inputs, ndarrays must be of same shape
        "multiply": None,  # elementwise multiplication of inputs, ndarrays must be of same shape
        "maximum": None,  # elementwise maximums of inputs, ndarrays must be of same shape
        "minimum": None,  # elementwise minimums of inputs, ndarrays must be of same shape
    }
    function: str = dlg_string_param("function", "sum")  # type: ignore
    reduce_axes: list = dlg_list_param("reduce_axes", "None")  # type: ignore

    def run(self):
        if len(self.inputs) < 1:
            raise Exception(
                f"At least one input should have been added to {self}"
            )
        if len(self.outputs) < 1:
            raise Exception(
                f"At least one output should have been added to {self}"
            )
        if self.function not in self.functions:
            raise Exception(
                f"Function {self.function} not supported by {self}"
            )

        result = (
            self.reduce_gather_inputs()
            if self.functions[self.function] is not None
            else self.gather_inputs()
        )

        for o in self.outputs:
            droputils.save_numpy(o, result)

    def reduce_gather_inputs(self):
        """reduces then gathers each input drop interpreted as an npy drop"""
        result: Optional[Number] = None
        reduce = getattr(np, f"{self.function}")
        gather = getattr(np, f"{self.functions[self.function]}")
        for input in self.inputs:
            data = droputils.load_numpy(input)
            # skip gather for the first input
            result = (
                reduce(data, axis=self.reduce_axes, allow_pickle=True)
                if result is None
                else gather(
                    result,
                    reduce(data, axis=self.reduce_axes, allow_pickle=True),
                    allow_pickle=True,
                )
            )
        return result

    def gather_inputs(self):
        """gathers each input drop interpreted as an npy drop"""
        result: Optional[Number] = None
        gather = getattr(np, f"{self.function}")
        for input in self.inputs:
            data = droputils.load_numpy(input)
            # assign instead of gather for the first input
            result = (
                data
                if result is None
                else gather(result, data, allow_pickle=True)
            )
        return result


##
# @brief HelloWorldApp
# @details A simple APP that implements the standard Hello World in DALiuGE.
# It allows to change 'World' with some other string and it also permits
# to connect the single output port to multiple sinks, which will all receive
# the same message. App does not require any input.
# @par EAGLE_START
# @param category PythonApp
# @param tag daliuge
# @param greet greet/World/String/ApplicationArgument/readwrite//False/False/What appears after 'Hello '
# @param dropclass dropclass/dlg.apps.simple.HelloWorldApp/String/ComponentParameter/readonly//False/False/Application class
# @param execution_time execution_time/5/Float/ComponentParameter/readonly//False/False/Estimated execution time
# @param input_parser Input Parser/pickle/Select/ApplicationArgument/readwrite/raw,pickle,eval,npy,path,dataurl/False/False/Input port parsing technique
# @param output_parser Output Parser/pickle/Select/ApplicationArgument/readwrite/raw,pickle,eval,npy,path,dataurl/False/False/Output port parsing technique
# @param num_cpus num_cpus/1/Integer/ComponentParameter/readonly//False/False/Number of cores used
# @param group_start group_start/False/Boolean/ComponentParameter/readwrite//False/False/Is this node the start of a group?
# @param hello hello/"world"/Object/OutputPort/readwrite//False/False/The port carrying the message produced by the app.
# @par EAGLE_END
class HelloWorldApp(BarrierAppDROP):
    """
    An App that writes 'Hello World!' or 'Hello <greet>!' to all of
    its outputs.

    Keywords:
    greet:   string, [World], whom to greet.
    """

    component_meta = dlg_component(
        "HelloWorldApp",
        "Hello World App.",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

    greet = dlg_string_param("greet", "World")

    def run(self):
        ins = self.inputs
        # if no inputs use the parameter else use the input
        if len(ins) == 0:
            self.greeting = "Hello %s" % self.greet
        elif len(ins) != 1:
            raise Exception("Only one input expected for %r" % self)
        else:  # the input is expected to be a vector. We'll use the first element
            try:
                phrase = str(
                    pickle.loads(droputils.allDropContents(ins[0]))[0]
                )
            except _pickle.UnpicklingError:
                phrase = str(
                    droputils.allDropContents(ins[0]), encoding="utf-8"
                )
            self.greeting = f"Hello {phrase}"

        outs = self.outputs
        if len(outs) < 1:
            raise Exception(
                "At least one output should have been added to %r" % self
            )
        for o in outs:
            o.len = len(self.greeting.encode())
            o.write(self.greeting.encode())  # greet across all outputs


##
# @brief UrlRetrieveApp
# @details A simple APP that retrieves the content of a URL and writes
# it to all outputs.
# @par EAGLE_START
# @param category PythonApp
# @param tag daliuge
# @param url url/"https://eagle.icrar.org"/String/ApplicationArgument/readwrite//False/False/The URL to retrieve
# @param dropclass dropclass/dlg.apps.simple.UrlRetrieveApp/String/ComponentParameter/readonly//False/False/Application class
# @param execution_time execution_time/5/Float/ComponentParameter/readonly//False/False/Estimated execution time
# @param input_parser Input Parser/pickle/Select/ApplicationArgument/readwrite/raw,pickle,eval,npy,path,dataurl/False/False/Input port parsing technique
# @param output_parser Output Parser/pickle/Select/ApplicationArgument/readwrite/raw,pickle,eval,npy,path,dataurl/False/False/Output port parsing technique
# @param num_cpus num_cpus/1/Integer/ComponentParameter/readonly//False/False/Number of cores used
# @param group_start group_start/False/Boolean/ComponentParameter/readwrite//False/False/Is this node the start of a group?
# @param content content//String/OutputPort/readwrite//False/False/The port carrying the content read from the URL
# @par EAGLE_END
class UrlRetrieveApp(BarrierAppDROP):
    """
    An App that retrieves the content of a URL

    Keywords:
    URL:   string, URL to retrieve.
    """

    component_meta = dlg_component(
        "UrlRetrieveApp",
        "URL Retrieve App",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

    url = dlg_string_param("url", "")

    def run(self):
        try:
            u = urllib.request.urlopen(self.url)
        except urllib.error.URLError as e:
            raise e.reason

        content = u.read()

        outs = self.outputs
        if len(outs) < 1:
            raise Exception(
                "At least one output should have been added to %r" % self
            )
        for o in outs:
            o.len = len(content)
            o.write(content)  # send content to all outputs


##
# @brief GenericScatterApp
# @details An APP that splits about any object that can be converted to a numpy array
# into as many parts as the app has outputs, provided that the initially converted numpy
# array has enough elements. The return will be a numpy array of arrays, where the first
# axis is of length len(outputs). The modulo remainder of the length of the original array and
# the number of outputs will be distributed across the first len(outputs)-1 elements of the
# resulting array.
# @par EAGLE_START
# @param category PythonApp
# @param construct Scatter
# @param tag daliuge
# @param num_of_copies num_of_copies/4/Integer/ConstructParameter/readwrite//False/False/Specifies the number of replications of the content of the scatter construct
# @param group_start group_start/False/Boolean/ComponentParameter/readwrite//False/False/Is this node the start of a group?
# @param n_tries n_tries/1/Integer/ComponentParameter/readwrite//False/False/Specifies the number of times the 'run' method will be executed before finally giving up
# @param dropclass dropclass/dlg.apps.simple.GenericScatterApp/String/ComponentParameter/readonly//False/False/Application class
# @param input_parser Input Parser/pickle/Select/ApplicationArgument/readwrite/raw,pickle,eval,npy,path,dataurl/False/False/Input port parsing technique
# @param output_parser Output Parser/pickle/Select/ApplicationArgument/readwrite/raw,pickle,eval,npy,path,dataurl/False/False/Output port parsing technique
# @param execution_time execution_time/5/Float/ComponentParameter/readonly//False/False/Estimated execution time
# @param num_cpus num_cpus/1/Integer/ComponentParameter/readonly//False/False/Number of cores used
# @param array_in array_in//Object.Array/InputPort/readwrite//False/False/A numpy array of arrays, where the first axis is of length <numSplit>
# @param array_out array_out//Object.Array/OutputPort/readwrite//False/False/Port carrying the reduced array
# @par EAGLE_END
class GenericScatterApp(BarrierAppDROP):
    """
    An APP that splits an object that has a len attribute into <numSplit> parts and
    returns a numpy array of arrays, where the first axis is of length <numSplit>.
    """

    component_meta = dlg_component(
        "GenericScatterApp",
        "Scatter an array like object into numSplit parts",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

    # automatically populated by scatter node
    num_of_copies: int = dlg_int_param("num_of_copies", 1)

    def initialize(self, **kwargs):
        super(GenericScatterApp, self).initialize(**kwargs)

    def run(self):
        numSplit = self.num_of_copies
        cont = droputils.allDropContents(self.inputs[0])
        # if the data is of type string it is not pickled, but stored as a binary string.
        try:
            inpArray = pickle.loads(cont)
        except:
            inpArray = cont.decode()
        try:  # just checking whether the object is some object that can be used as an array
            nObj = np.array(inpArray)
        except:
            raise
        try:
            result = np.array_split(nObj, numSplit)
        except IndexError as err:
            raise err
        for i in range(numSplit):
            o = self.outputs[i]
            d = pickle.dumps(result[i])
            o.len = len(d)
            o.write(d)  # average across inputs


##
# @brief GenericNpyScatterApp
# @details An APP that splits about any axis on any npy format data drop
# into as many part./run    s as the app has outputs, provided that the initially converted numpy
# array has enough elements. The return will be a numpy array of arrays, where the first
# axis is of length len(outputs). The modulo remainder of the length of the original array and
# the number of outputs will be distributed across the first len(outputs)-1 elements of the
# resulting array.
# @par EAGLE_START
# @param construct Scatter
# @param category PythonApp
# @param tag daliuge
# @param num_of_copies num_of_copies/4/Integer/ConstructParameter/readwrite//False/False/Specifies the number of replications of the content of the scatter construct
# @param scatter_axes scatter_axes//String/ApplicationArgument/readwrite//False/False/The axes to split input ndarrays on, e.g. [0,0,0], length must match the number of input ports
# @param group_start group_start/False/Boolean/ComponentParameter/readwrite//False/False/Is this node the start of a group?
# @param dropclass dropclass/dlg.apps.simple.GenericNpyScatterApp/String/ComponentParameter/readonly//False/False/Application class
# @param input_parser Input Parser/pickle/Select/ApplicationArgument/readwrite/raw,pickle,eval,npy,path,dataurl/False/False/Input port parsing technique
# @param output_parser Output Parser/pickle/Select/ApplicationArgument/readwrite/raw,pickle,eval,npy,path,dataurl/False/False/Output port parsing technique
# @param execution_time execution_time/5/Float/ComponentParameter/readonly//False/False/Estimated execution time
# @param num_cpus num_cpus/1/Integer/ComponentParameter/readonly//False/False/Number of cores used
# @param array_in array_in//Object.Array/InputPort/readwrite//False/False/A numpy array of arrays
# @param array_out array_out//Object.Array/OutputPort/readwrite//False/False/Port carrying the reduced array
# @par EAGLE_END
class GenericNpyScatterApp(BarrierAppDROP):
    """
    An APP that splits an object that has a len attribute into <num_of_copies> parts and
    returns a numpy array of arrays.
    """

    component_meta = dlg_component(
        "GenericNpyScatterApp",
        "Scatter an array like object into <num_of_copies> parts",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

    # automatically populated by scatter node
    num_of_copies: int = dlg_int_param("num_of_copies", 1)
    scatter_axes: List[int] = dlg_list_param("scatter_axes", "[0]")

    def run(self):
        if len(self.inputs) * self.num_of_copies != len(self.outputs):
            raise DaliugeException(
                f"expected {len(self.inputs) * self.num_of_copies} outputs,\
                 got {len(self.outputs)}"
            )
        if len(self.inputs) != len(self.scatter_axes):
            raise DaliugeException(
                f"expected {len(self.inputs)} axes,\
                 got {len(self.scatter_axes)}, {self.scatter_axes}"
            )

        # split it as many times as we have outputs
        self.num_of_copies = self.num_of_copies

        for in_index in range(len(self.inputs)):
            nObj = droputils.load_numpy(self.inputs[in_index])
            try:
                result = np.array_split(
                    nObj, self.num_of_copies, axis=self.scatter_axes[in_index]
                )
            except IndexError as err:
                raise err
            for split_index in range(self.num_of_copies):
                out_index = in_index * self.num_of_copies + split_index
                droputils.save_numpy(
                    self.outputs[out_index], result[split_index]
                )


class SimpleBranch(BranchAppDrop, NullBarrierApp):
    """Simple branch app that is told the result of its condition"""

    def initialize(self, **kwargs):
        self.result = self._popArg(kwargs, "result", True)
        BranchAppDrop.initialize(self, **kwargs)

    def run(self):
        pass

    def condition(self):
        return self.result


##
# @brief PickOne
# @details App that picks the first element of an input list, passes that
# to all outputs, except the first one. The first output is used to pass
# the remaining array on. This app is useful for a loop.
#
# @par EAGLE_START
# @param category PythonApp
# @param tag daliuge
# @param dropclass dropclass/dlg.apps.simple.PickOne/String/ComponentParameter/readonly//False/False/Application class
# @param input_parser Input Parser/pickle/Select/ApplicationArgument/readwrite/raw,pickle,eval,npy,path,dataurl/False/False/Input port parsing technique
# @param output_parser Output Parser/pickle/Select/ApplicationArgument/readwrite/raw,pickle,eval,npy,path,dataurl/False/False/Output port parsing technique
# @param execution_time execution_time/5/Float/ComponentParameter/readonly//False/False/Estimated execution time
# @param num_cpus num_cpus/1/Integer/ComponentParameter/readonly//False/False/Number of cores used
# @param group_start group_start/False/Boolean/ComponentParameter/readwrite//False/False/Is this node the start of a group?
# @param rest_array_in rest_array//Object.array/InputPort/readwrite//False/FalseList of elements
# @param rest_array_out rest_array//Object.array/OutputPort/readwrite//False/False/Port carrying the rest array
# @param element element//Object.element/OutputPort/readwrite//False/False/Port carrying the first element of input array
# @par EAGLE_END
class PickOne(BarrierAppDROP):
    """
    Simple app picking one element at a time. Good for Loops.
    """

    def initialize(self, **kwargs):
        BarrierAppDROP.initialize(self, **kwargs)

    def readData(self):
        input = self.inputs[0]
        data = pickle.loads(droputils.allDropContents(input))

        # make sure we always have a ndarray with at least 1dim.
        if type(data) not in (list, tuple) and not isinstance(
            data, (np.ndarray)
        ):
            raise TypeError
        if isinstance(data, np.ndarray) and data.ndim == 0:
            data = np.array([data])
        else:
            data = np.array(data)
        value = data[0] if len(data) else None
        rest = data[1:] if len(data) > 1 else np.array([])
        return value, rest

    def writeData(self, value, rest):
        """
        Prepare the data and write to all outputs
        """
        # write rest to array output
        # and value to every other output
        for output in self.outputs:
            if output.name == "rest_array":
                d = pickle.dumps(rest)
                output.len = len(d)
            else:
                d = pickle.dumps(value)
                output.len = len(d)
            output.write(d)

    def run(self):
        value, rest = self.readData()
        self.writeData(value, rest)


##
# @brief ListAppendThrashingApp
# @details A testing APP that appends a random integer to a list num times.
# This is a CPU intensive operation and can thus be used to provide a test for application threading
# since this operation will not yield.
# The resulting array will be sent to all connected output apps.
# @par EAGLE_START
# @param category PythonApp
# @param tag daliuge
# @param size size/100/Integer/ApplicationArgument/readwrite//False/False/the size of the array
# @param dropclass dropclass/dlg.apps.simple.ListAppendThrashingApp/String/ComponentParameter/readonly//False/False/Application class
# @param input_parser Input Parser/pickle/Select/ApplicationArgument/readwrite/raw,pickle,eval,npy,path,dataurl/False/False/Input port parsing technique
# @param output_parser Output Parser/pickle/Select/ApplicationArgument/readwrite/raw,pickle,eval,npy,path,dataurl/False/False/Output port parsing technique
# @param execution_time execution_time/5/Float/ComponentParameter/readonly//False/False/Estimated execution time
# @param num_cpus num_cpus/1/Integer/ComponentParameter/readonly//False/False/Number of cores used
# @param group_start group_start/False/Boolean/ComponentParameter/readwrite//False/False/Is this node the start of a group?
# @param array array//Object.Array/OutputPort/readwrite//False/False/Port carrying the random array.
# @par EAGLE_END
class ListAppendThrashingApp(BarrierAppDROP):
    """
    A BarrierAppDrop that appends random integers to a list N times. It does
    not require any inputs and writes the generated array to all of its
    outputs.

    Keywords:

    size:     int, number of array elements
    """

    compontent_meta = dlg_component(
        "ListAppendThrashingApp",
        "List Append Thrashing",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

    def initialize(self, **kwargs):
        self.size = self._popArg(kwargs, "size", 100)
        self.marray = []
        super(ListAppendThrashingApp, self).initialize(**kwargs)

    def run(self):
        # At least one output should have been added
        outs = self.outputs
        if len(outs) < 1:
            raise Exception(
                "At least one output should have been added to %r" % self
            )
        self.marray = self.generateArray()
        for o in outs:
            d = pickle.dumps(self.marray)
            o.len = len(d)
            o.write(pickle.dumps(self.marray))

    def generateArray(self):
        # This operation is wasteful to simulate an N^2 operation.
        marray = []
        for _ in range(int(self.size)):
            marray = []
            for i in range(int(self.size)):
                marray.append(random.random())
        return marray

    def _getArray(self):
        return self.marray
