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
import unittest

from dfms.ddap_protocol import DROPLinkType
from dfms.manager.session import Session, SessionStates


class TestSession(unittest.TestCase):

    def test_sessionStates(self):
        with Session('1') as s:
            self.assertEquals(SessionStates.PRISTINE, s.status)
            self.assertRaises(Exception, s.deploy)
            self.assertRaises(Exception, s.linkGraphParts, '', '', 0)

            s.addGraphSpec([{"oid":"A", "type":"container"}])
            self.assertEquals(SessionStates.BUILDING, s.status)

            s.deploy()
            self.assertEquals(SessionStates.RUNNING, s.status)

            # Now we can't do any of these
            self.assertRaises(Exception, s.deploy)
            self.assertRaises(Exception, s.addGraphSpec, '')
            self.assertRaises(Exception, s.linkGraphParts, '', '', 0)

    def test_addGraphSpec(self):
        with Session('1') as s:
            s.addGraphSpec([{"oid":"A", "type":"container"}])
            s.addGraphSpec([{"oid":"B", "type":"container"}])
            s.addGraphSpec([{"oid":"C", "type":"container"}])

            # Adding an existing DROP
            self.assertRaises(Exception, s.addGraphSpec, [{"oid":"A", "type":"container"}])

            # Adding invalid specs
            self.assertRaises(Exception, s.addGraphSpec, [{"oid":"D", "type":"app"}]) # missing "storage"
            self.assertRaises(Exception, s.addGraphSpec, [{"oid":"D", "type":"plain", "storage":"invalid"}]) # invalid "storage"
            self.assertRaises(Exception, s.addGraphSpec, [{"oid":"D", "type":"invalid"}]) # invalid "type"
            self.assertRaises(Exception, s.addGraphSpec, [{"oid":"D", "type":"app", "storage":"null", "outputs":["X"]}]) # missing X DROP

    def test_linking(self):
        with Session('1') as s:
            s.addGraphSpec([{"oid":"A", "type":"container"}])
            s.addGraphSpec([{"oid":"B", "type":"app", "storage":"null", "app":"dfms.apps.crc.CRCApp"}])
            s.addGraphSpec([{"oid":"C", "type":"container"}])

            # Link them now
            s.linkGraphParts('A', 'B', DROPLinkType.CONSUMER)
            s.linkGraphParts('B', 'C', DROPLinkType.OUTPUT)

            # Deploy and check that the actual DROPs are linked together
            s.deploy()
            roots = s.roots
            self.assertEquals(1, len(roots))
            a = s.roots[0]
            self.assertEquals('A', a.oid)
            self.assertEquals(1, len(a.consumers))
            b = a.consumers[0]
            self.assertEquals('B', b.oid)
            self.assertEquals(1, len(b.outputs))
            c = b.outputs[0]
            self.assertEquals('C', c.oid)