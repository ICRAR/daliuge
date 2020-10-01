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

import time
import numpy as np
import six
import six.moves.cPickle as pickle

from .. import droputils, utils
from ..drop import BarrierAppDROP, ContainerDROP
from ..meta import dlg_float_param, dlg_string_param
from ..meta import dlg_bool_param, dlg_int_param
from ..meta import dlg_component, dlg_batch_input
from ..meta import dlg_batch_output, dlg_streaming_input

from dlg.apps.pyfunc import serialize_data, deserialize_data

class NullBarrierApp(BarrierAppDROP):
    compontent_meta = dlg_component('NullBarrierApp', 'Null Barrier.',
                                    [dlg_batch_input('binary/*', [])],
                                    [dlg_batch_output('binary/*', [])],
                                    [dlg_streaming_input('binary/*')])

    """A BarrierAppDrop that doesn't perform any work"""
    def run(self):
        pass


class SleepApp(BarrierAppDROP):
    """A BarrierAppDrop that sleeps the specified amount of time (0 by default)"""
    compontent_meta = dlg_component('SleepApp', 'Sleep App.',
                                    [dlg_batch_input('binary/*', [])],
                                    [dlg_batch_output('binary/*', [])],
                                    [dlg_streaming_input('binary/*')])

    sleepTime = dlg_float_param('sleep time', 0)

    def initialize(self, **kwargs):
        super(SleepApp, self).initialize(**kwargs)

    def run(self):
        time.sleep(self.sleepTime)


class CopyApp(BarrierAppDROP):
    """
    A BarrierAppDrop that copies its inputs into its outputs.
    All inputs are copied into all outputs in the order they were declared in
    the graph.
    """
    compontent_meta = dlg_component('CopyApp', 'Copy App.',
                                    [dlg_batch_input('binary/*', [])],
                                    [dlg_batch_output('binary/*', [])],
                                    [dlg_streaming_input('binary/*')])

    def run(self):
        self.copyAll()

    def copyAll(self):
        for inputDrop in self.inputs:
            self.copyRecursive(inputDrop)

    def copyRecursive(self, inputDrop):
        if isinstance(inputDrop, ContainerDROP):
            for child in inputDrop.children:
                self.copyRecursive(child)
        else:
            for outputDrop in self.outputs:
                droputils.copyDropContents(inputDrop, outputDrop)


class SleepAndCopyApp(SleepApp, CopyApp):
    """A combination of the SleepApp and the CopyApp. It sleeps, then copies"""

    def run(self):
        SleepApp.run(self)
        CopyApp.run(self)


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
    compontent_meta = dlg_component('RandomArrayApp', 'Random Array App.',
                                    [dlg_batch_input('binary/*', [])],
                                    [dlg_batch_output('binary/*', [])],
                                    [dlg_streaming_input('binary/*')])
    
    # default values
    integer = dlg_bool_param('integer', True)
    low = dlg_float_param('low', 0)
    high = dlg_float_param('high', 100)
    size = dlg_int_param('size', 100)
    marray = []

    def initialize(self, **kwargs):
        super(RandomArrayApp, self).initialize(**kwargs)

    def run(self):
        # At least one output should have been added
        outs = self.outputs
        if len(outs) < 1:
            raise Exception(
                'At least one output should have been added to %r' % self)
        self.generateRandomArray()
        for o in outs:
            o.write(pickle.dumps(self.marray))

    def generateRandomArray(self):
        if self.integer:
            # generate an array of self.size integers with numbers between
            # slef.low and self.high
            marray = np.random.randint(int(self.low), int(self.high), size=(self.size))
        else:
            # generate an array of self.size floats with numbers between
            # self.low and self.high
            marray = (np.random.random(size=self.size) + self.low) * self.high
        self.marray = marray
    
    def _getArray(self):
        return self.marray


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
    compontent_meta = dlg_component('RandomArrayApp', 'Random Array App.',
                                    [dlg_batch_input('binary/*', [])],
                                    [dlg_batch_output('binary/*', [])],
                                    [dlg_streaming_input('binary/*')])

    # default values
    methods = ['mean', 'median']
    method = dlg_string_param('method', methods[0])
    marray = []

    def initialize(self, **kwargs):
        super(AverageArraysApp, self).initialize(**kwargs)

    def run(self):
        # At least one output should have been added

        outs = self.outputs
        if len(outs) < 1:
            raise Exception(
                'At least one output should have been added to %r' % self)
        self.getInputArrays()
        avg = self.averageArray()
        for o in outs:
            o.write(pickle.dumps(avg))  # average across inputs
    
    def getInputArrays(self):
        """
        Create the input array from all inputs received. Shape is
        (<#inputs>, <#elements>), where #elements is the length of the
        vector received from one input.
        """
        ins = self.inputs
        if len(ins) < 1:
            raise Exception(
                'At least one input should have been added to %r' % self)

        marray = [pickle.loads(droputils.allDropContents(inp)) for inp in ins]
        self.marray = marray


    def averageArray(self):
        
        method_to_call = getattr(np, self.method)
        return method_to_call(self.marray, axis=0)
