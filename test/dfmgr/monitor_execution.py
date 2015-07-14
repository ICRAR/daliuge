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
from dfms.doutils import EvtConsumer
from ngas_dm import DeployDataObjectTask, PGDeployTask, PGEngine
import luigi
import threading
import time
from dfms import doutils
import random
from dfms.events.event_broadcaster import ThreadedEventBroadcaster

class MonitorDataObjectTask(DeployDataObjectTask):
    """
    A Luigi task that, for a given DataObject, monitors that its status has
    changed to COMPLETED. This way we can use Luigi to simply monitor the
    execution of the DO graph, but not to drive it
    """

    def __init__(self, *args, **kwargs):
        DeployDataObjectTask.__init__(self, *args, **kwargs)
        self._evt = threading.Event()
        self.data_obj.addConsumer(EvtConsumer(self._evt))

    def complete(self):
        return self.data_obj.isCompleted()

    def run(self):
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

def testGraph():
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
    """

    lifespan = 1800
    bc = ThreadedEventBroadcaster()
    a = InMemorySocketListenerDataObject('oid:A', 'uid:A', bc, lifespan=lifespan)
    d = ContainerDataObject('oid:D', 'uid:D', bc, lifespan=lifespan)
    for i in xrange(random.SystemRandom().randint(10, 20)):
        b = InMemorySleepAndCopyApp('oid:B%d' % (i), 'uid:B%d' % (i), bc, lifespan=lifespan)
        c = InMemorySleepAndCopyApp('oid:C%d' % (i), 'uid:C%d' % (i), bc, lifespan=lifespan)
        a.addConsumer(b)
        b.addConsumer(c)
        d.addChild(c)
    return a

class FinishGraphExecution(PGDeployTask):
    def __init__(self, **kwargs):
        PGDeployTask.__init__(self, **kwargs)
        self._doTaskType = MonitorDataObjectTask
    def _deploy(self, pg_creator_function=None):
        eng = PGEngine()
        return eng.process(testGraph())

if __name__ == '__main__':
    luigi.run()