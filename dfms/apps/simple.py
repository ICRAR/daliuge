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

from .. import droputils
from ..drop import BarrierAppDROP, ContainerDROP


class NullBarrierApp(BarrierAppDROP):
    """A BarrierAppDrop that doesn't perform any work"""
    def run(self):
        pass

class SleepApp(BarrierAppDROP):
    """A BarrierAppDrop that sleeps the specified amount of time (0 by default)"""

    def initialize(self, **kwargs):
        super(SleepApp, self).initialize(**kwargs)
        self._sleepTime = self._getArg(kwargs, 'sleepTime', 0)

    def run(self):
        time.sleep(self._sleepTime)

class CopyApp(BarrierAppDROP):
    """
    A BarrierAppDrop that copies its inputs into its outputs.
    All inputs are copied into all outputs in the order they were declared in
    the graph.
    """
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