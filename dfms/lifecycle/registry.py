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

from dfms.ddap_protocol import DOPhases


logger = logging.getLogger(__name__)

class DROP(object):
    oid         = None
    phase       = DOPhases.GAS
    instances   = []
    accessTimes = []

class DataObjectInstance(object):
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
    def addDataObject(self, dataObject):
        """
        Adds a new DROP to the registry
        """

    @abstractmethod
    def addDataObjectInstance(self, dataObject):
        """
        Adds a new DROP instance to the registry. The registry should have
        a record already for the DROP that the new instance belongs to
        """

    @abstractmethod
    def getDataObjectUids(self, dataObject):
        """
        Returns a list with the UIDs of all known instances of the given
        DROP
        """

    @abstractmethod
    def setDataObjectPhase(self, dataObject, phase):
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

    def _checkDOIsInRegistry(self, oid):
        if not self._dos.has_key(oid):
            raise Exception('DROP %s is not present in the registry' % (oid))

class InMemoryRegistry(Registry):

    def __init__(self):
        super(InMemoryRegistry, self).__init__()
        self._dos= {}

    def addDataObject(self, dataObject):
        '''
        :param dfms.data_object.AbstractDataObject dataObject:
        '''
        # Check that the DROP is not in the registry
        doRow = DROP()
        doRow.oid       = dataObject.oid
        doRow.phase     = dataObject.phase
        doRow.instances = {dataObject.uid: dataObject}
        self._dos[doRow.oid] = doRow

    def addDataObjectInstance(self, dataObject):
        '''
        :param dfms.data_object.AbstractDataObject dataObject:
        '''
        self._checkDOIsInRegistry(dataObject.oid)
        if self._dos[dataObject.oid].instances.has_key(dataObject.uid):
            raise Exception('DROP %s/%s already present in registry' % (dataObject.oid, dataObject.uid))
        self._dos[dataObject.oid].instances[dataObject.uid] = dataObject

    def getDataObjectUids(self, dataObject):
        self._checkDOIsInRegistry(dataObject.oid)
        return self._dos[dataObject.oid].instances.keys()

    def setDataObjectPhase(self, dataObject, phase):
        self._checkDOIsInRegistry(dataObject.oid)
        self._dos[dataObject.oid].phase = phase

    def recordNewAccess(self, oid):
        self._checkDOIsInRegistry(oid)
        self._dos[oid].accessTimes.append(time.time())

    def getLastAccess(self, oid):
        if oid in self._dos and self._dos[oid].accessTimes:
            return self._dos[oid].accesTimes[-1]
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
    # dfms_drop:
    #   oid   (PK)
    #   phase (int)
    #
    # dfms_dropinstance:
    #   uid     (PK)
    #   oid     (FK)
    #   dataRef ()
    #
    # dfms_dropaccesstime:
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

    def _paramNames(self, howMany):
        # Depending on the different vendor, we need to write the parameters in
        # the SQL calls using different notations. This method will produce an
        # array containing all the parameter references in the SQL statement
        # (and not its values!)
        #
        # qmark     Question mark style, e.g. ...WHERE name=?
        # numeric   Numeric, positional style, e.g. ...WHERE name=:1
        # named     Named style, e.g. ...WHERE name=:name
        # format    ANSI C printf format codes, e.g. ...WHERE name=%s
        # pyformat  Python extended format codes, e.g. ...WHERE name=%(name)s
        #
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug('Generating %d parameters with paramstyle = %s' % (howMany, self._paramstyle))
        if self._paramstyle == 'qmark':    return ['?'             for i in xrange(howMany)]
        if self._paramstyle == 'numeric':  return [':%d'%(i)       for i in xrange(howMany)]
        if self._paramstyle == 'named':    return [':n%d'%(i)      for i in xrange(howMany)]
        if self._paramstyle == 'format':   return [':%s'           for i in xrange(howMany)]
        if self._paramstyle == 'pyformat': return [':%%(n%d)s'%(i) for i in xrange(howMany)]
        raise Exception('Unknown paramstyle: %s' % (self._paramstyle))

    def _bindD(self, *data):
        if self._paramstyle in ['format', 'pyformat']:
            dataDict = {}
            [dataDict.__setitem__('n%d'%(i), d) for i,d in enumerate(data)]
            return dataDict
        else:
            return data

    def addDataObject(self, dataObject, conn=None):
        with self.transactional(self, conn) as conn:
            cur = conn.cursor()
            cur.execute("INSERT INTO dfms_drop (oid, phase) VALUES ({0},{1})".format(*self._paramNames(2)),
                        self._bindD(dataObject.oid, dataObject.phase))
            self.addDataObjectInstance(dataObject, conn)
            cur.close()

    def addDataObjectInstance(self, dataObject, conn=None):
        with self.transactional(self, conn) as conn:
            cur = conn.cursor()
            cur.execute('INSERT INTO dfms_dropinstance (oid, uid, dataRef) VALUES ({0},{1},{2})'.format(*self._paramNames(3)),
                        self._bindD(dataObject.oid, dataObject.uid, dataObject.dataURL))
            cur.close()

    def getDataObjectUids(self, dataObject, conn=None):
        with self.transactional(self, conn) as conn:
            cur = conn.cursor()
            cur.execute('SELECT uid FROM dfms_dropinstance WHERE oid = {0}'.format(*self._paramNames(1)),
                        self._bindD(dataObject.oid))
            rows = cur.fetchall()
            cur.close()
            return [r[0] for r in rows]

    def setDataObjectPhase(self, dataObject, phase, conn=None):
        with self.transactional(self, conn) as conn:
            cur = conn.cursor()
            cur.execute('UPDATE dfms_drop SET phase = {0} WHERE oid = {1}'.format(*self._paramNames(2)),
                        self._bindD(dataObject.oid, dataObject.phase))
            cur.close()

    def recordNewAccess(self, oid, conn=None):
        with self.transactional(self, conn) as conn:
            cur = conn.cursor()
            cur.execute('INSERT INTO dfms_dropaccesstime (oid, accessTime) VALUES ({0},{1})'.format(*self._paramNames(2)),
                        self._bindD(oid, self._dbmod.TimestampFromTicks(time.time())))
            cur.close()

    def getLastAccess(self, oid, conn=None):
        with self.transactional(self, conn) as conn:
            cur = conn.cursor()
            cur.execute('SELECT accessTime FROM dfms_dropaccesstime WHERE oid = {0} ORDER BY accessTime DESC LIMIT 1'.format(*self._paramNames(1)),
                        self._bindD(oid))
            row = cur.fetchone()
            cur.close()
            if row is None:
                return -1
            return row[0]