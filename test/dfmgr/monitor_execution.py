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

from dfms.data_object import InMemorySocketListenerDataObject, AppConsumer,\
    InMemoryDataObject, ContainerDataObject
from ngas_dm import PGDeployTask, PGEngine
import luigi
import threading
import logging
import time
import sys
from dfms import doutils
import random
from dfms.events.event_broadcaster import ThreadedEventBroadcaster
from test.dfmgr.ngas_dm import DataObjectTask
from dfms.ddap_protocol import ExecutionMode, DOStates

_logger = logging.getLogger(__name__)

class RunDataObjectTask(DataObjectTask):
    """
    A Luigi task that, for a given DataObject, either simply monitors it or
    actually executes it.

    Which of the two actions is performed depends on the nature of the
    DataObject and on the execution mode set in the DataObject's upstream
    objects: only AppConsumer DataObjects can be triggered automatically by
    their upstream objects. Since AppConsumer DataObjects only reference one
    upstream object (their producer) we need only to check the producer's
    execution mode, and if it's set to ExecutionMode.EXTERNAL then this task
    needs to manually execute the AppConsumer DataObject. In any other case this
    task simply waits until the DataObject's status has moved to COMPLETED.

    The complete() test for both cases is still the same, regardless of who is
    driving the execution: the DO must be COMPLETED and must exist.
    """
    def __init__(self, *args, **kwargs):
        super(RunDataObjectTask, self).__init__(*args, **kwargs)

        do = self.data_obj
        self.execDO  = isinstance(do, AppConsumer) and do.producer.executionMode == ExecutionMode.EXTERNAL
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug("%s will execute or monitor DataObject %s/%s?: %s" % (self.__class__, do.oid, do.uid, ("execute" if self.execDO else "monitor")))
        if not self.execDO:
            self._evt = threading.Event()
            def setEvtOnCompleted(e):
                if hasattr(e, 'status') and e.status == DOStates.COMPLETED:
                    self._evt.set()
            do.subscribe(setEvtOnCompleted, 'status')

    def complete(self):
        return self.data_obj.isCompleted() and self.data_obj.exists()

    def run(self):
        if self.execDO:
            self.data_obj.consume(self.data_obj.producer)
        else:
            now = time.time()
            expirationDate = self.data_obj.expirationDate
            self._evt.wait(expirationDate - now)

class SleepAndCopyApp(AppConsumer):
    """
    A simple application consumer that sleeps between 0 and 10 seconds and
    then fully copies the contents of the DataObject it consumes into itself
    """
    def run(self, dataObject):
        time.sleep(random.SystemRandom().randint(0, 1000)/100.)
        doutils.copyDataObjectContents(dataObject, self)
        self.setCompleted()

class InMemorySleepAndCopyApp(SleepAndCopyApp, InMemoryDataObject): pass

def testGraph(execMode):
    """
    A test graph that looks like this:

        |--> B1 --> C1 --|
        |--> B2 --> C2 --|
    A --|--> B3 --> C3 --| --> D
        |--> .. --> .. --|
        |--> BN --> CN --|

    B and C DOs are InMemorySleepAndCopyApp DOs (see above). D is simply a
    container. A is a socket listener, so we can actually write to it externally
    and watch the progress of the luigi tasks. We give DOs a long lifespan;
    otherwise they will expire and luigi will see it as a failed task (which is
    actually right!)

    If execMode is given we use that in all DOs. If it's None we use a mixture
    of DO/EXTERNAL execution modes.
    """

    aMode = execMode if execMode is not None else ExecutionMode.EXTERNAL
    bMode = execMode if execMode is not None else ExecutionMode.DO
    cMode = execMode if execMode is not None else ExecutionMode.DO
    dMode = execMode if execMode is not None else ExecutionMode.EXTERNAL

    lifespan = 1800
    bc = ThreadedEventBroadcaster()
    a = InMemorySocketListenerDataObject('oid:A', 'uid:A', bc, executionMode=aMode, lifespan=lifespan)
    d = ContainerDataObject('oid:D', 'uid:D', bc, executionMode=dMode, lifespan=lifespan)
    for i in xrange(random.SystemRandom().randint(10, 20)):
        b = InMemorySleepAndCopyApp('oid:B%d' % (i), 'uid:B%d' % (i), bc, executionMode=bMode, lifespan=lifespan)
        c = InMemorySleepAndCopyApp('oid:C%d' % (i), 'uid:C%d' % (i), bc, executionMode=cMode, lifespan=lifespan)
        a.addConsumer(b)
        b.addConsumer(c)
        d.addChild(c)
    return a

class FinishGraphExecution(PGDeployTask):

    driver = luigi.Parameter(default=None)

    def _deploy(self, pg_creator_function=None):
        if self.driver == 'do':
            execMode = ExecutionMode.DO
        elif self.driver == 'luigi':
            execMode = ExecutionMode.EXTERNAL
        else:
            execMode = None
        eng = PGEngine()
        return eng.process(testGraph(execMode))

    def requires(self):
        if (len(self._req) == 0):
            for dob in self._leafs:
                if _logger.isEnabledFor(logging.DEBUG):
                    _logger.debug("Adding leaf DO as requirement to PGDeployTask: %s/%s" % (dob.oid, dob.uid))
                self._req.append(RunDataObjectTask(dob, self.session_id))
        return self._req

if __name__ == '__main__':
    '''
    e.g., "python monitor_execution.py FinishGraphExecution --driver luigi" (or do)
    '''
    logging.basicConfig(format="%(asctime)-15s [%(levelname)-5s] [%(threadName)-15s] %(name)s#%(funcName)s:%(lineno)s %(msg)s", level=logging.DEBUG, stream=sys.stdout)
    luigi.run()