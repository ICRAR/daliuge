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
import asyncio
from numbers import Number
import pickle
import random
from typing import AsyncIterable, List, Optional
import urllib.error
import urllib.request
from overrides import overrides

import time
import ast
import numpy as np

from dlg import droputils, utils
from dlg.drop import DataDROP, InputFiredAppDROP, BranchAppDrop, ContainerDROP, NullDROP
from dlg.meta import (
    dlg_float_param, 
    dlg_string_param,
    dlg_bool_param, 
    dlg_int_param,
    dlg_list_param,
    dlg_component, 
    dlg_batch_input,
    dlg_batch_output, 
    dlg_streaming_input
)
from dlg.exceptions import DaliugeException
from dlg.apps.pyfunc import serialize_data, deserialize_data


##
# @brief CopyApp
# @details A simple APP that copies its inputs into its outputs.
# All inputs are copied into all outputs in the order they were declared in
# the graph. If an input is a container (e.g. a directory) it copies the
# content recursively.
# @par EAGLE_START
# @param category PythonApp
# @param tag daliuge
# @param[in] cparam/appclass Application Class/dlg.apps.simple.CopyApp/String/readonly/False//False/
#     \~English Application class
# @param[in] cparam/bufsize buffer size/65536/Integer/readwrite/False//False/
#     \~English Application class
# @param[in] cparam/execution_time Execution Time/5/Float/readonly/False//False/
#     \~English Estimated execution time
# @param[in] cparam/num_cpus No. of CPUs/1/Integer/readonly/False//False/
#     \~English Number of cores used
# @param[in] cparam/group_start Group start/False/Boolean/readwrite/False//False/
#     \~English Is this node the start of a group?
# @param[in] cparam/input_error_threshold "Input error rate (%)"/0/Integer/readwrite/False//False/
#     \~English the allowed failure rate of the inputs (in percent), before this component goes to ERROR state and is not executed
# @param[in] cparam/n_tries Number of tries/1/Integer/readwrite/False//False/
#     \~English Specifies the number of times the 'run' method will be executed before finally giving up
# @par EAGLE_END
class AsyncCopyApp(InputFiredAppDROP):
    """
    A streaming app drop that copies its inputs into its outputs.
    All inputs are copied into all outputs in the order they were declared in
    the graph.
    """

    component_meta = dlg_component(
        "AsyncCopyApp",
        "Async Copy App.",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

    _bufsize = dlg_int_param("bufsize", 65536)

    def run(self):
        assert len(self.inputs) == len(self.outputs)
        
        # synchronous
        #for inputDrop, outputDrop in zip(self.inputs, self.outputs):
        #    droputils.copyDropContents(inputDrop, outputDrop, bufsize=self._bufsize)

        # asynchronous
        asyncio.run(self.copyAll())

    async def copyAll(self):
        tasks = []
        for inputDrop, outputDrop in zip(self.inputs, self.outputs):
            tasks.append(asyncio.create_task(AsyncCopyApp.asyncCopyDropContents(inputDrop, outputDrop)))
        await asyncio.gather(*tasks)

    @staticmethod
    async def sync_to_async(a) -> AsyncIterable:
        for i in a:
            yield i

    @staticmethod
    async def asyncCopyDropContents(inputDrop: DataDROP, outputDrop: DataDROP):
        desc = inputDrop.open()
        s: AsyncIterable = await inputDrop.readStream(desc)
        await outputDrop.writeStream(s)
    
    @overrides
    async def readStream(self, descriptor, **kwargs) -> AsyncIterable:
        raise NotImplementedError()

    @overrides
    async def writeStream(self, stream: AsyncIterable, **kwargs):
        raise NotImplementedError()
