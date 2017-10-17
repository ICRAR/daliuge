#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2015
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
"""
Module containing the code that integrates our DROPs with Luigi.
"""

import importlib
import logging
import threading
import time

import luigi
import six

from . import droputils
from .ddap_protocol import ExecutionMode, DROPStates
from .drop import AbstractDROP, BarrierAppDROP


logger = logging.getLogger(__name__)

class RunDROPTask(luigi.Task):
    """
    A Luigi Task that, for a given DROP, either simply monitors it or
    actually executes it.

    Which of the two actions is performed depends on the nature of the
    DROP and on the execution mode set in the DROP's upstream
    objects: only BarrierAppDROP can be triggered automatically by
    their upstream objects. Since BarrierAppDROPs can reference more than one
    upstream object (their producers) we need to check all producer's execution
    mode. If all of them are set to ExecutionMode.EXTERNAL then this task
    needs to manually execute the AppDROP. In any other case this
    task simply waits until the DROP's status has moved to COMPLETED.

    The complete() test for both cases is still the same, regardless of who is
    driving the execution: the DROP must be COMPLETED and must exist.
    """

    data_obj  = luigi.Parameter()
    sessionId = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super(RunDROPTask, self).__init__(*args, **kwargs)

        drop = self.data_obj
        self.execDrop  = False
        if isinstance(drop, BarrierAppDROP):
            for inputDrop in drop.inputs:
                if inputDrop.executionMode == ExecutionMode.EXTERNAL:
                    self.execDrop = True

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("%s will execute or monitor %r?: %s" % (self.__class__, drop, ("execute" if self.execDrop else "monitor")))

        if not self.execDrop:
            self._evt = threading.Event()
            if isinstance(drop, BarrierAppDROP):
                drop.subscribe(self, 'producerFinished')
            elif isinstance(drop, AbstractDROP):
                drop.subscribe(self, 'dropCompleted')

    def handleEvent(self, e):
        self._evt.set()

    def complete(self):
        return (self.data_obj.isCompleted() and self.data_obj.exists()) or \
                self.data_obj.status == DROPStates.ERROR

    def run(self):
        if self.execDrop:
            self.data_obj.execute()
        else:
            timeout = None
            expirationDate = self.data_obj.expirationDate
            if expirationDate != -1:
                now = time.time()
                timeout = expirationDate - now
            self._evt.wait(timeout)

    def requires(self):
        """
        The list of RunDROPTask that are required by this one.
        We use self.__class__ to create the new dependencies so this method
        doesn't need to be rewritten by all subclasses
        """
        re = []
        logger.debug("Checking requirements for RunDROPTask %r", self.data_obj)

        # The requires() method will be called not only when creating the
        # initial tree of tasks, but also at runtime. For a given graph in a
        # DM that has been connected with to other graph running in a different
        # DM, it will mean that at runtime more upstream objects will be found
        # for those nodes connected to an external graph. We shouldn't schedule
        # those objects though, since they are scheduled by their own DM.
        # We simply filter then the upObjs here to return only those that are
        # actually an instance of AbstractDROP, thus removing any Pyro
        # Proxy instances from the list
        upObjs = droputils.getUpstreamObjects(self.data_obj)
        upObjs = filter(lambda drop: isinstance(drop, AbstractDROP), upObjs)

        for req in upObjs:
            logger.debug("Added requirement %r", req)
            re.append(RunDROPTask(req, self.sessionId))
        return re

class FinishGraphExecution(luigi.Task):
    """
    A Luigi Task that creates a DROP graph and waits until it has finished
    its execution fully. The DROP graph is created by feeding this Task
    with a property pgCreator parameter, which is the name of a function with
    no arguments that returns the top-level nodes of the graph.

    For a number of testing graphs please see the graphsRepository module.
    """
    sessionId = luigi.Parameter(default=time.time())
    pgCreator = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super(FinishGraphExecution, self).__init__(*args, **kwargs)
        self._req    = None

        if isinstance(self.pgCreator, six.string_types):
            parts = self.pgCreator.split('.')
            module = importlib.import_module('.'.join(parts[:-1]))
            pgCreatorFn = getattr(module, parts[-1])
            roots = pgCreatorFn()
        else:
            roots = self.pgCreator

        self._roots = droputils.listify(roots)
        self._leaves = droputils.getLeafNodes(self._roots)
        self._completed = False

    def requires(self):
        if self._req is None:
            self._req = []
            for drop in self._leaves:
                logger.debug("Adding leaf DROP as requirement to FinishGraphExecution: %r", drop.oid)
                self._req.append(RunDROPTask(drop, self.sessionId))
        return self._req

    def run(self):
        self._completed = True

    def complete(self):
        return self._completed

    @property
    def leaves(self):
        return self._leaves[:]

    @property
    def roots(self):
        return self._roots[:]