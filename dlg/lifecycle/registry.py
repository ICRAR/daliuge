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
'''
Module containing the base class and a simple implementation of the registry
used by the DLM to keep track of which DROPs are where, and therefore in which
phase they currently are

The registry simply (for the time being) keeps a record of:
 * Which DROPs (i.e., which oids) are out there
 * For each DROP, which instances(i.e., which uids) are out there

@author: rtobar
'''

from abc import abstractmethod, ABCMeta
import importlib
import logging
import time

from ..ddap_protocol import DROPPhases
from ..utils import prepare_sql


logger = logging.getLogger(__name__)

class DROP(object):
    oid         = None
    phase       = DROPPhases.GAS
    instances   = []
    accessTimes = []

class DROPInstance(object):
    oid     = None
    uid     = None
    storage = None


class Registry():
    '''
    Base class that imposes a given structure, subclasses should implement all
    NotImplementedError methods. Is this the
    '''

    __metaclass__ = ABCMeta

    @abstractmethod
    def addDrop(self, drop):
        """
        Adds a new DROP to the registry
        """

    @abstractmethod
    def addDropInstance(self, drop):
        """
        Adds a new DROP instance to the registry. The registry should have
        a record already for the DROP that the new instance belongs to
        """

    @abstractmethod
    def getDropUids(self, drop):
        """
        Returns a list with the UIDs of all known instances of the given
        DROP
        """

    @abstractmethod
    def setDropPhase(self, drop, phase):
        """
        Records the phase of the given DROP
        """

    @abstractmethod
    def recordNewAccess(self, oid):
        """
        Appends a new record to the list of access times of the given DROP
        (i.e., when it has been accessed for reading)
        """

    @abstractmethod
    def getLastAccess(self, oid):
        """
        Returns the last access time for the given DROP, or -1 if it has
        never been accessed
        """

    def _checkDropIsInRegistry(self, oid):
        if not oid in self._drops:
            raise Exception('DROP %s is not present in the registry' % (oid))

class InMemoryRegistry(Registry):

    def __init__(self):
        super(InMemoryRegistry, self).__init__()
        self._drops= {}

    def addDrop(self, drop):
        '''
        :param dlg.drop.AbstractDROP drop:
        '''
        # Check that the DROP is not in the registry
        dropRow = DROP()
        dropRow.oid       = drop.oid
        dropRow.phase     = drop.phase
        dropRow.instances = {drop.uid: drop}
        self._drops[dropRow.oid] = dropRow

    def addDropInstance(self, drop):
        '''
        :param dlg.drop.AbstractDROP drop:
        '''
        self._checkDropIsInRegistry(drop.oid)
        if drop.uid in self._drops[drop.oid].instances:
            raise Exception('DROP %s/%s already present in registry' % (drop.oid, drop.uid))
        self._drops[drop.oid].instances[drop.uid] = drop

    def getDropUids(self, drop):
        self._checkDropIsInRegistry(drop.oid)
        return self._drops[drop.oid].instances.keys()

    def setDropPhase(self, drop, phase):
        self._checkDropIsInRegistry(drop.oid)
        self._drops[drop.oid].phase = phase

    def recordNewAccess(self, oid):
        self._checkDropIsInRegistry(oid)
        self._drops[oid].accessTimes.append(time.time())

    def getLastAccess(self, oid):
        if oid in self._drops and self._drops[oid].accessTimes:
            return self._drops[oid].accesTimes[-1]
        else:
            return -1

class RDBMSRegistry(Registry):

    def __init__(self, dbModuleName, *connArgs):
        try:
            self._dbmod = importlib.import_module(dbModuleName)
            self._paramstyle = self._dbmod.paramstyle
            self._connArgs = connArgs
        except:
            logger.error("Cannot import module %s, RDBMSRegistry cannot start" % (dbModuleName))
            raise

    def _connect(self):
        return self._dbmod.connect(*self._connArgs)

    # The following tables should be defined in the database we're pointing at
    #
    # dlg_drop:
    #   oid   (PK)
    #   phase (int)
    #
    # dlg_dropinstance:
    #   uid     (PK)
    #   oid     (FK)
    #   dataRef ()
    #
    # dlg_dropaccesstime:
    #   oid (FK, PK)
    #   accessTime (PK)

    # A small helper class to make all methods transactional, and to create
    # connections when needed
    class transactional(object):

        def __init__(self, registry, conn):
            self._connect = registry._connect
            self._conn = conn
            self._connCreated = False

        def __enter__(self):
            if self._conn is None:
                self._conn = self._connect()
                self._connCreated = True
            return self._conn

        def __exit__(self, typ, value, traceback):
            if not self._connCreated:
                return
            if typ is None:
                self._conn.commit()
            else:
                self._conn.rollback()
                return False

            self._conn.close()

    def execute(self, cursor, sql, values=()):
        sql, values = prepare_sql(sql, self._paramstyle, values)
        cursor.execute(sql, values)

    def addDrop(self, drop, conn=None):
        with self.transactional(self, conn) as conn:
            cur = conn.cursor()
            self.execute(cur, "INSERT INTO dlg_drop (oid, phase) VALUES ({0},{1})", (drop.oid, drop.phase))
            self.addDropInstance(drop, conn)
            cur.close()

    def addDropInstance(self, drop, conn=None):
        with self.transactional(self, conn) as conn:
            cur = conn.cursor()
            self.execute(cur, 'INSERT INTO dlg_dropinstance (oid, uid, dataRef) VALUES ({0},{1},{2})', (drop.oid, drop.uid, drop.dataURL))
            cur.close()

    def getDropUids(self, drop, conn=None):
        with self.transactional(self, conn) as conn:
            cur = conn.cursor()
            self.execute(cur, 'SELECT uid FROM dlg_dropinstance WHERE oid = {0}', (drop.oid,))
            rows = cur.fetchall()
            cur.close()
            return [r[0] for r in rows]

    def setDropPhase(self, drop, phase, conn=None):
        with self.transactional(self, conn) as conn:
            cur = conn.cursor()
            self.execute(cur, 'UPDATE dlg_drop SET phase = {0} WHERE oid = {1}', (drop.oid, drop.phase))
            cur.close()

    def recordNewAccess(self, oid, conn=None):
        with self.transactional(self, conn) as conn:
            cur = conn.cursor()
            self.execute(cur, 'INSERT INTO dlg_dropaccesstime (oid, accessTime) VALUES ({0},{1})', (oid, self._dbmod.TimestampFromTicks(time.time())))
            cur.close()

    def getLastAccess(self, oid, conn=None):
        with self.transactional(self, conn) as conn:
            cur = conn.cursor()
            self.execute(cur, 'SELECT accessTime FROM dlg_dropaccesstime WHERE oid = {0} ORDER BY accessTime DESC LIMIT 1', (oid,))
            row = cur.fetchone()
            cur.close()
            if row is None:
                return -1
            return row[0]