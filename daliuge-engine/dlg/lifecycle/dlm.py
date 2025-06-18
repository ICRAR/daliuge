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
A simple implementation of the data lifecycle manager (DLM).

The following is an extract of PDR.02.02 "DATA Sub-element design report",
section 8 "Data Life Cycle Management", where the DLM is defined:

  "The data layer needs a subsystem that automates and manages the migration of
  DataObjects through the various life cycle states. This is a consequence of
  the sheer number of parallel tasks when dealing with the very high data rate
  and volume. The goal of data life-cycle management is the optimal placement of
  data in terms of cost and performance and employs a multi-tiered storage
  system to do so. It is important to note that the life-cycle of a DataObject
  can span from minutes to many years and therefore seamlessly covers the
  processing as well as the long term storage (science archive) domain.

  Data life-cycle management has to support a number of requirements:
   1. Migration of data from one medium to another: SKA1-SYS_REQ-2728
   2. Aggregation of DataObjects into DataProducts and DataPackages: this is
      non-trivial because of concurrent workflows, SKA1-SYS_REQ-2128,
      SDP_REQ-252, and the need for data tracing, provenance and access control,
      SKA1-SYS_REQ-2821, SDP_REQ-255
   3. Migration between storage layers, includes SDP_REQ-263
   4. Replication/distribution including resilience (persistence) support,
      incl. SKA1-SYS_REQ-2350, SDP_REQ-260 - 262
   5. Retirement of expired (temporary) data, incl. SDP_REQ-256."

The requirements referenced by the above documentation read (taken from PDR03
"Requirement analysis"):

 * SKA1-SYS_REQ-2728 "Data migration design"
   The archive design shall support and facilitate migration from one medium to
   another
 * SKA1-SYS_REQ-2128 "Continuum and spectral line imaging mode"
   All three SKA1 telescopes shall be capable of operating in a Continuum and
   Spectral-line imaging mode concurrently.
 * SDP_REQ-252 "Concurrent Workflows"
   The data layer manager shall concurrently generate data products for multiple
   observing programs. It shall also support a single observing program
   concurrently generating multiple data products as well as a mix of both
   scenarios.
 * SKA1-SYS_REQ-2821 "Archive"
   There shall be an archive for each telescope, located in the Science
   Processing Centre, for storing selected science data products for subsequent
   access by users according to science data access policy.
 * SDP_REQ-255 "Tracing Data"
   It shall be possible to trace each data product in the archive back to a
   scheduling block and an observing program. Reversely, it shall be possible to
   either search by observing program or scheduling block and to subsequently
   retrieve all or part of the associated archived data. This includes relevant
   logging and monitoring information as well as quality assessment data
   collected during the observations and the standard processing.
 * SDP_REQ-263: "The data layer manager shall contain a data life cycle
   management subsystem which shall incorporate mechanisms for copying, moving
   and retiring whole physical volumes or storage units."
 * SKA1-SYS_REQ-2350 "Mirror sites"
   All data within Science Archives shall have a secondary copy located offsite
   in a secure location
 * SDP_REQ-260 "Archive Backup"
   In case of data loss from the Science Archive it shall be possible to restore
   the lost data items. Individual data products shall be retrievable from a
   backup copy within 24 hours. The backup mechanism shall support scheduled, as
   well as incremental and full backup options.
 * SDP_REQ-261 "Restoring Archive Operations of a failed site"
   There shall be a mechanism to operate the archive from a backup copy to meet
   a recovery time limit of 1 week, independent of the total size of the archive.
 * SDP_REQ-262 "Data Layer Product Distribution"
   The SDP shall provide an internal interface to allow access to bulk data and
   data contained in databases (from the science archive) in order to deliver
   data products.
 * SDP_REQ-256 "Discard Scheduling Block"
   The system shall provide a mechanism for discarding the results and
   associated data artefacts from a given active scheduling while it is still
   processed and before the next scheduling block starts.

Regarding point #5, although it's not totally clear WHEN expired DROPs must be
deleted, PDR02-02-01 "Data Layer Data Challenges" states in section 7.2,
"DataObject State Model":

  "[...] the actual physical deletion could be driven by an explicit cleanup
  mechanism [...]"

We are implementing this mechanism in the DLM as well, as part of the background
periodical checks it performs. After discussing this with Chen, it also became
apparent that external users such as the dataflow manager could probably also
request an explicit cleanup, maybe even bypassing the expiration date of DROPs.

A simpler definition of the DLM is also given in PDR03 "Requirement analysis",
section 2.C.3.2, "Data Life Cycle Manager":

  "Software component that implements a rule driven system for data movement,
  persistence and release based on hardware parameters and policies"

Other parts of the documentation also mention that the DLM is configured by a
Data Lifecycle Configurator Manager, but for the prototyping we can use a self-
configured (and/or hardcoded) DLM

@author: rtobar
"""

import logging
import random
import string
import threading
import time

from typing import Dict
from . import registry
from .hsm import manager
from .hsm.store import AbstractStore
from .. import droputils
from ..ddap_protocol import DROPStates, DROPPhases, AppDROPStates
from ..drop import AbstractDROP
from ..data.drops.container import ContainerDROP

logger = logging.getLogger(f"dlg.{__name__}")


class DataLifecycleManagerBackgroundTask(threading.Thread):
    """
    A thread that periodically runs some of the methods on the given DLM until
    signaled to stop
    """

    def __init__(self, name, dlm, period):
        threading.Thread.__init__(self, name="DLMBackgroundTask")
        self._dlm = dlm
        self._period = period
        logger.info("Starting %s running every %.3f [s]", name, self._period)

    def run(self):
        ev = self._dlm.finishedEvent
        dlm = self._dlm
        p = self._period
        while True:
            if ev.wait(p):
                break
            self.doTask(dlm)


class DROPChecker(DataLifecycleManagerBackgroundTask):
    """
    A thread that performs several checks on existing DROPs
    """

    def doTask(self, dlm):
        # Expire DROPs that are already too old
        dlm.expireCompletedDrops()

        # Check that DROPs still exist, and mark them as lost
        # if they are not found
        dlm.deleteLostDrops()


class DROPGarbageCollector(DataLifecycleManagerBackgroundTask):
    """
    A thread that performs "garbage collection" of DROPs; that is, it physically
    deleted DROPs that are marked as EXPIRED
    """

    def doTask(self, dlm):
        # The names says it all
        dlm.deleteExpiredDrops()


class DROPMover(DataLifecycleManagerBackgroundTask):
    """
    A thread that automatically moves DROPs between layers of the HSM.
    This is supposed to be based on rules and configuration parameters which we
    still don't consider here. The driving rule we currently use is how
    frequently is the DROP accessed.
    """

    def doTask(self, dlm):
        dlm.moveDropsAround()


class DropEventListener(object):
    def __init__(self, dlm):
        self._dlm = dlm

    def handleEvent(self, event):
        if event.type == "open":
            self._dlm.handleOpenedDrop(event.oid, event.uid)
        elif event.type == "status":
            if event.status == DROPStates.COMPLETED:
                self._dlm.handleCompletedDrop(event.uid)


class DataLifecycleManager:
    """
    An object that deals with automatic data drop replication and deletion.
    """

    def __init__(self, check_period=0, cleanup_period=0, enable_drop_replication=False):
        self.finishedEvent = threading.Event()
        self._reg = registry.InMemoryRegistry()
        self._listener = DropEventListener(self)
        self._enable_drop_replication = enable_drop_replication
        if enable_drop_replication:
            self._hsm = manager.HierarchicalStorageManager()
        else:
            self._hsm = None

        # TODO: When iteration over the values of _drops we always do _drops.values()
        # instead of _drops.itervalues() to get a full, thread-safe copy of the
        # dictionary values. Maybe there's a better approach for thread-safety
        # here
        self._drops: Dict[str, AbstractDROP] = {}

        self._check_period = check_period
        self._cleanup_period = cleanup_period
        self._drop_checker = None
        self._drop_garbage_collector = None

    def startup(self):
        # Spawn the background threads
        if self._check_period:
            self._drop_checker = DROPChecker("DropChecker", self, self._check_period)
            self._drop_checker.start()
        if self._cleanup_period:
            self._drop_garbage_collector = DROPGarbageCollector(
                "DropGarbageCollector", self, self._cleanup_period
            )
            self._drop_garbage_collector.start()

    def cleanup(self):
        logger.info("Cleaning up the DLM")

        # Join the background threads
        self.finishedEvent.set()
        if self._drop_checker:
            self._drop_checker.join()
        if self._drop_garbage_collector:
            self._drop_garbage_collector.join()

        # Unsubscribe to all events coming from the DROPs
        for drop in self._drops.values():
            drop.unsubscribe(self)

    #
    # Support for 'with' keyword
    #
    def __enter__(self):
        self.startup()
        return self

    def __exit__(self, typ, value, traceback):
        self.cleanup()

    def _deleteDrop(self, drop):
        logger.debug("Deleting DROP %r", drop)
        drop.delete()
        drop.status = DROPStates.DELETED

    def deleteExpiredDrops(self):
        for drop in list(self._drops.values()):
            if drop.status == DROPStates.EXPIRED:
                self._deleteDrop(drop)

    def expireCompletedDrops(self):
        now = time.time()
        for drop in self._drops.values():

            if drop.status != DROPStates.COMPLETED:
                continue

            # Expire-after-use: mark as expired if all consumers
            # are finished using this DROP
            if not drop.persist and drop.expireAfterUse:
                allDone = all(
                    c.execStatus in [AppDROPStates.FINISHED, AppDROPStates.ERROR]
                    for c in drop.consumers
                )
                if not allDone:
                    continue

            # Otherwise, we check the expiration date
            # (if no lifespan was specified for the DROP, its expiration
            # date will be -1 and it will be skipped)
            elif drop.expirationDate == -1 or now <= drop.expirationDate:
                continue

            if drop.isBeingRead():
                logger.info(
                    "%r has expired but is currently being read, "
                    "will skip expiration for the time being",
                    drop,
                )
                continue

            # Finally!
            logger.debug("Marking %r as EXPIRED", drop)
            drop.status = DROPStates.EXPIRED

    def _disappeared(self, drop):
        return drop.status != DROPStates.DELETED and not drop.exists()

    def deleteLostDrops(self):

        toRemove = []
        for drop in self._drops.values():

            # We only care about disappeared drops
            if not self._disappeared(drop):
                continue

            toRemove.append(drop.uid)
            logger.warning("%r has disappeared", drop)

            # Check if it's replicated
            uids = self._reg.getDropUids(drop)
            definitelyLost = False
            if not uids:
                definitelyLost = True
            else:

                # Replicas haven't disappeared as well, right?
                replicas = []
                for uid in uids:
                    if uid == drop.uid:
                        continue
                    siblingDrop = self._drops[uid]
                    if not self._disappeared(siblingDrop):
                        replicas.append(siblingDrop)
                    else:
                        logger.warning(
                            "%r (replicated from %r) has disappeared",
                            siblingDrop,
                            drop,
                        )
                        toRemove.append(siblingDrop.uid)

                if len(replicas) > 1:
                    logger.info(
                        "%r has still more than one replica, no action needed",
                        drop,
                    )
                elif len(replicas) == 1:
                    logger.info(
                        "Only one replica left for DROP %r, will create a new one",
                        drop,
                    )
                    self.replicateDrop(replicas[0])
                else:
                    definitelyLost = True

            if definitelyLost:
                logger.error(
                    "No available replica found for DROP %s/%s, the data is DEFINITELY LOST",
                    drop.oid,
                    drop.uid,
                )
                drop.phase = DROPPhases.LOST
                self._reg.setDropPhase(drop, drop.phase)

        # All those objects identified as lost have to go now
        for uid in toRemove:
            del self._drops[uid]

    def moveDropsAround(self):
        """
        Moves DROPs to different layers of the HSM if necessary, currently based
        only on the access times
        """
        # Big questions here that need some answers/experimentation
        #  1 "Migrating" implies that the data is no longer in its original
        #    location, but has been moved rather than copied to a new place.
        #  2 That means that the DROP doesn't represent actually the same data, or
        #    at least not in the same physical location. The question is thus
        #    whether a new DROP would be required for this migrated DROP, or if
        #    the currently existing one should be modified?
        #  3 If a new one is required, then the original, and users of the
        #    original have to be notified that a change has occurred.
        #  4 Or not? Who is actually accessing DROPs and how drop they get their
        #    references? Maybe it's the DLM who delivers the references
        #    (there is an "Open DataObject" Activity Diagram drawn, but doesn't
        #    clarify who is performing the actions and where are they taking
        #    place), and references can be considered as invalid, in which case
        #    a new reference must be retrieved via the DLM. This would probably
        #    make sense since the DLM is managing the lifecycle of the different
        #    DROP instances, and knows where they actually are.
        #  5 If somebody is currently reading from a DROP that is meant to be
        #    moved we have to not move it. This would probably not happen anyway
        #    since we're using the access times to decide which DROPs must be
        #    moved to what storage layer
        #  6 The other alternative (to the question posed in 2) is to modify the
        #    current DROP. This doesn't sound like a good idea, since we are
        #    effectively creating a new instance of the data but in a different
        #    media (plus the relative complexity of actually implementing such
        #    flexibility).
        #
        # Answering #4 is probably the key to clarify the entire situation. For
        # the time being, we simply "pass"...
        #
        # PS: The Open DataObject Activity is actually used as part of the
        # "Data Object Lifecycle (nominal)" State Diagram, used as the activity
        # that should be executed when entering the "Read" state of DROPs. This is
        # confusing because the "Open DataObject" activity includes actions like
        # "Locate DataObject" and "Resolve Location", although it's the DROP
        # that is opening itself, and thus opening it should mean simply to open
        # the underlying media. Moreover, in the same activity diagram one can
        # see the "DataLifecycleDb", which would correspond to the DLM. If this
        # activity is really run from DROPs it would mean that DROPs depend on the
        # DLM, while the DLM manages DROPs.

        for drop in self._drops.values:

            # Don't touch these
            if drop.isBeingRead():
                continue

            # EXPIRED DROPs will soon be deleted
            # DELETED DROPs drop not exist anymore
            if drop.status in (DROPStates.DELETED, DROPStates.EXPIRED):
                continue

            # 1. Data that is not being used anymore should be moved down in the
            #    hierarchy
            lastAccess = self._reg.getLastAccess(drop.oid)
            timeUnread = time.time() - lastAccess
            if lastAccess != -1 and timeUnread > 10:
                self.moveDropDown(drop)

    def addDrop(self, drop):

        # Keep track of the DROP and subscribe to the events it generates
        self._drops[drop.uid] = drop
        drop.phase = DROPPhases.GAS
        drop.subscribe(self._listener)
        self._reg.addDrop(drop)

        # TODO: We currently use a background thread that scans
        #       all DROPs for their status and compares its expiration date
        #       with the current time in order to determine which DROPs must be
        #       moved to EXPIRED. We might want to explore other alternatives to
        #       perform this task, like using threading.Timers (probably not) or
        #       any other that doesn't mean looping over all DROPs

    def remove_drops(self, drop_oids):
        """
        Remove drops from DLM's monitoring
        """
        self._drops = {
            oid: drop for oid, drop in self._drops.items() if oid not in drop_oids
        }

    def handleOpenedDrop(self, oid, uid):
        drop = self._drops[uid]
        if drop.status == DROPStates.COMPLETED:
            self._reg.recordNewAccess(oid)

    def handleCompletedDrop(self, uid):
        """
        :param string uid:
        """
        # Check the kind of storage used by this DROP. If it's already persisted
        # in a persistent storage media we don't need to save it again

        if not self._enable_drop_replication:
            return

        drop = self._drops[uid]
        if drop.persist and self.isReplicable(drop):
            logger.debug("Replicating %r because it's marked to be persisted", drop)
            try:
                self.replicateDrop(drop)
            except RuntimeError:
                logger.exception("Problem while replicating %r", drop)

    def isReplicable(self, drop):
        return not isinstance(drop, ContainerDROP)

    def replicateDrop(self, drop):
        """
        :param dlg.drop.AbstractDROP drop:
        """

        # Check that the DROP is complete already
        if drop.status != DROPStates.COMPLETED:
            raise RuntimeError("%r not in COMPLETED state" % (drop,))

        # Get the size of the DROP. This cannot currently be done in some of them,
        # like in the AbstractDROP
        size = drop.size
        if size is None:
            return

        # Check which layer of the hsm should host the replicated copy
        store = self._hsm.getSlowestStore()
        availableSpace = store.getAvailableSpace()

        if size > availableSpace:
            raise RuntimeError("Cannot replicate DROP to store %s: not enough space left")

        # Create new DROP and write the contents of the original into it
        # TODO: In a real world application this will probably happen in a separate
        #       worker thread with a working queue, or in separate threads, one
        #       for each replication task (or a mix of the two)
        newDrop, newUid = self._replicate(drop, store)

        # The DROPs (both) should now be tagged as SOLID
        newDrop.phase = DROPPhases.SOLID
        drop.phase = DROPPhases.SOLID

        # Update our own registry
        self._drops[newUid] = newDrop
        self._reg.addDropInstance(newDrop)
        self._reg.setDropPhase(drop, DROPPhases.SOLID)

    def getDropUids(self, drop):
        return self._reg.getDropUids(drop)

    def _replicate(self, drop: AbstractDROP, store: AbstractStore):

        # Dummy, but safe, new UID
        newUid = "uid:" + "".join(
            [
                random.SystemRandom().choice(string.ascii_letters + string.digits)
                for _ in range(10)
            ]
        )

        logger.debug("Creating new DROP with uid %s from %r", newUid, drop)

        # For the time being we manually copy the contents of the current DROP into it
        newDrop = store.createDrop(
            drop.oid, newUid, expectedSize=drop.size, persist=drop.persist
        )
        droputils.copyDropContents(drop, newDrop)

        logger.debug("%r successfully replicated to %r", drop, newDrop)

        return newDrop, newUid
