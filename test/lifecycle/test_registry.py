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
import os
import unittest

from Pyro4.naming_storage import sqlite3

from dfms.data_object import InMemoryDataObject
from dfms.lifecycle.registry import RDBMSRegistry


DBFILE = 'testing_dlm.db'

class TestRDBMSRegistry(unittest.TestCase):

    def setUp(self):
        unittest.TestCase.setUp(self)
        conn = sqlite3.connect(DBFILE)
        cur = conn.cursor()
        cur.execute('CREATE TABLE DataObject(oid varchar(64) PRIMARY KEY, phase integer)');
        cur.execute('CREATE TABLE DataObjectInstance(uid varchar(64) PRIMARY KEY, oid varchar(64), dataRef varchar(128))');
        cur.execute('CREATE TABLE DataObjectAccessTime(oid varchar(64), accessTime TIMESTAMP, PRIMARY KEY (oid, accessTime))');
        conn.close()

    def tearDown(self):
        os.unlink(DBFILE)

    def test_addDataObject(self):
        a = InMemoryDataObject('a', 'a')
        registry = RDBMSRegistry('sqlite3', DBFILE)
        registry.addDataObject(a)

        conn = sqlite3.connect(DBFILE)
        cur = conn.cursor()
        cur.execute('SELECT oid FROM DataObject');
        r = cur.fetchone()
        self.assertEquals(1, len(r))
        self.assertEquals('a', r[0])
        cur.close()
        conn.close()

    def test_addDataObjectInstances(self):

        a1 = InMemoryDataObject('a', 'a1')
        a2 = InMemoryDataObject('a', 'a2')
        registry = RDBMSRegistry('sqlite3', DBFILE)
        registry.addDataObject(a1)

        uids = registry.getDataObjectUids(a1)
        self.assertEquals(1, len(uids))
        self.assertEquals('a1', uids[0])

        registry.addDataObjectInstance(a2)
        uids = registry.getDataObjectUids(a1)
        uids.sort()
        self.assertEquals(2, len(uids))
        self.assertEquals('a1', uids[0])
        self.assertEquals('a2', uids[1])

        # Check accessing the database separately
        conn = sqlite3.connect(DBFILE)
        cur = conn.cursor()
        cur.execute("SELECT uid FROM DataObjectInstance WHERE oid = 'a'");
        rows = cur.fetchall()
        self.assertEquals(2, len(rows))
        uids = [r[0] for r in rows]
        uids.sort()
        self.assertEquals(2, len(uids))
        self.assertEquals('a1', uids[0])
        self.assertEquals('a2', uids[1])
        cur.close()
        conn.close()

    def test_dataObjectAccess(self):

        a1 = InMemoryDataObject('a', 'a1')
        registry = RDBMSRegistry('sqlite3', DBFILE)
        registry.addDataObject(a1)

        self.assertEquals(-1, registry.getLastAccess('a'))
        registry.recordNewAccess('a')

        self.assertNotEquals(-1, registry.getLastAccess('a'))