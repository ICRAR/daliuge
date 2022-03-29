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
import sqlite3
import unittest

from dlg.drop import InMemoryDROP
from dlg.lifecycle.registry import RDBMSRegistry


DBFILE = "testing_dlm.db"


class TestRDBMSRegistry(unittest.TestCase):
    def setUp(self):
        unittest.TestCase.setUp(self)
        conn = sqlite3.connect(DBFILE)  # @UndefinedVariable
        cur = conn.cursor()
        cur.execute("CREATE TABLE dlg_drop(oid varchar(64) PRIMARY KEY, phase integer)")
        cur.execute(
            "CREATE TABLE dlg_dropinstance(uid varchar(64) PRIMARY KEY, oid varchar(64), dataRef varchar(128))"
        )
        cur.execute(
            "CREATE TABLE dlg_dropaccesstime(oid varchar(64), accessTime TIMESTAMP, PRIMARY KEY (oid, accessTime))"
        )
        conn.close()

    def tearDown(self):
        os.unlink(DBFILE)

    def test_addDrop(self):
        a = InMemoryDROP("a", "a")
        registry = RDBMSRegistry("sqlite3", DBFILE)
        registry.addDrop(a)

        conn = sqlite3.connect(DBFILE)  # @UndefinedVariable
        cur = conn.cursor()
        cur.execute("SELECT oid FROM dlg_drop")
        r = cur.fetchone()
        self.assertEqual(1, len(r))
        self.assertEqual("a", r[0])
        cur.close()
        conn.close()

    def test_addDropInstances(self):

        a1 = InMemoryDROP("a", "a1")
        a2 = InMemoryDROP("a", "a2")
        registry = RDBMSRegistry("sqlite3", DBFILE)
        registry.addDrop(a1)

        uids = registry.getDropUids(a1)
        self.assertEqual(1, len(uids))
        self.assertEqual("a1", uids[0])

        registry.addDropInstance(a2)
        uids = registry.getDropUids(a1)
        uids.sort()
        self.assertEqual(2, len(uids))
        self.assertEqual("a1", uids[0])
        self.assertEqual("a2", uids[1])

        # Check accessing the database separately
        conn = sqlite3.connect(DBFILE)  # @UndefinedVariable
        cur = conn.cursor()
        cur.execute("SELECT uid FROM dlg_dropinstance WHERE oid = 'a'")
        rows = cur.fetchall()
        self.assertEqual(2, len(rows))
        uids = [r[0] for r in rows]
        uids.sort()
        self.assertEqual(2, len(uids))
        self.assertEqual("a1", uids[0])
        self.assertEqual("a2", uids[1])
        cur.close()
        conn.close()

    def test_dropAccess(self):

        a1 = InMemoryDROP("a", "a1")
        registry = RDBMSRegistry("sqlite3", DBFILE)
        registry.addDrop(a1)

        self.assertEqual(-1, registry.getLastAccess("a"))
        registry.recordNewAccess("a")

        self.assertNotEqual(-1, registry.getLastAccess("a"))
