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

from dfms.data_object import InMemorySocketListenerDataObject
from dfms.events.event_broadcaster import LocalEventBroadcaster
from dfms.doutils import EvtConsumer
from ngas_dm import DeployDataObjectTask, PGDeployTask, PGEngine
import luigi
import threading
import time

class MonitorDataObjectTask(DeployDataObjectTask):
    """
    A Luigi task that, for a given DataObject, simply monitors that its
    status has changed to COMPLETED. This way we can use Luigi to simply monitor
    the execution of the DO graph, but not to drive it
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

def realGraph():
    bc = LocalEventBroadcaster()
    return InMemorySocketListenerDataObject('uid:A', 'uid:A', bc)
    pass

class MyFlowStart(PGDeployTask):
    def __init__(self, **kwargs):
        PGDeployTask.__init__(self, **kwargs)
        self._doTaskType = MonitorDataObjectTask
    def _deploy(self, pg_creator_function=None):
        eng = PGEngine()
        return eng.process(realGraph())

if __name__ == '__main__':
    luigi.run()