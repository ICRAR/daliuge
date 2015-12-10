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

import unittest, os, pkg_resources

from dfms.lmc.pg_generator import LGNode, LG

class TestPGGen(unittest.TestCase):

    def test_pg_generator(self):
        fp = pkg_resources.resource_filename('dfms.lg', 'web/lofar_std.json')
        #fp = os.path.realpath("{0}/../../lg/web/lofar_std.json".format(os.path.dirname(os.path.realpath(__file__))))
        lg = LG(fp)
        self.assertEquals(len(lg._done_dict.keys()), 36)
