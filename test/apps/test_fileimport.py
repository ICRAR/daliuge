#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia
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
Test the FileImport App
"""
import os
import random
import shutil
import string
import unittest

from dfms.apps.fileimport import FileImportApp


class FileImportAppTests(unittest.TestCase):

    root = '/tmp/test_file_import'

    dirs = ['%s/files/' % root,
            '%s/empty/' % root]

    files = ['%s%s' % (dirs[0],'test.fits'),
             '%s%s' % (dirs[0],'test1.fits'),
             '%s%s' % (dirs[0],'test2.txt')]

    def setUp(self):
        for d in self.dirs:
            if not os.path.exists(d):
                os.makedirs(d)
        for f in self.files:
            open(f, 'a').close()

    def tearDown(self):
        shutil.rmtree(self.root)

    def test_import(self):
        self.assertRaises(Exception, lambda: FileImportApp('a', 'b', dirname = files[0], ext = ['.fits']))
        self.assertRaises(Exception, lambda: FileImportApp('a', 'b', dirname = self.root, ext = []))
        self.assertRaises(Exception, lambda: FileImportApp('a', 'b', dirname = self.root))
        self.assertRaises(Exception, lambda: FileImportApp('a', 'b', dirname = '', ext = []))

        a = FileImportApp('a', 'b', dirname = self.root, ext = ['.fits'])
        self.assertEquals(len(a.children), 2)
        self.assertEquals(a.children[0].path, self.files[0])
        self.assertEquals(a.children[1].path, self.files[1])

        a = FileImportApp('a', 'b', dirname = self.dirs[1], ext = ['.fits'])
        self.assertEquals(len(a.children), 0)

        a = FileImportApp('a', 'b', dirname = self.root, ext = ['.txt'])
        self.assertEquals(len(a.children), 1)

        a = FileImportApp('a', 'b', dirname = self.root, ext = ['.hdf5'])
        self.assertEquals(len(a.children), 0)
