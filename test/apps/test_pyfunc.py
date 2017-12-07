#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2017
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
import random
import unittest

import six.moves.cPickle as pickle  # @UnresolvedImport

from ..manager import test_dm
from dlg import droputils
from dlg.apps.pyfunc import PyFuncApp
from dlg.ddap_protocol import DROPStates, DROPRel, DROPLinkType
from dlg.drop import InMemoryDROP
from dlg.droputils import DROPWaiterCtx
from dlg.exceptions import InvalidDropException


def func1(arg1):
    return arg1

def func2(arg1):
    return arg1 * 2;

def func3():
    return ['b', 'c', 'd']

def _PyFuncApp(oid, uid, f):
    return PyFuncApp(oid, uid, func_name=__name__ + '.' + f)

class TestPyFuncApp(unittest.TestCase):

    def test_missing_function_param(self):
        self.assertRaises(InvalidDropException, PyFuncApp, 'a', 'a')

    def test_invalid_function_param(self):
        # The function doesn't have a module
        self.assertRaises(InvalidDropException, PyFuncApp, 'a', 'a', func_name='func1')

    def test_function_invalid_module(self):
        # The function lives in an unknown module/package
        self.assertRaises(InvalidDropException, PyFuncApp, 'a', 'a', func_name='doesnt_exist.func1')

    def test_function_invalid_fname(self):
        # The function lives in an unknown module/package
        self.assertRaises(InvalidDropException, PyFuncApp, 'a', 'a', func_name='test.apps.test_pyfunc.doesnt_exist')

    def test_valid_creation(self):
        _PyFuncApp('a', 'a', 'func1')

    def _test_func1_and_2(self, f, input_data, output_data):

        a, c = [InMemoryDROP(x, x) for x in ('a', 'c')]
        b = _PyFuncApp('b', 'b', f)
        b.addInput(a)
        b.addOutput(c)

        with DROPWaiterCtx(self, c, 5):
            a.write(pickle.dumps(input_data))  # @UndefinedVariable
            a.setCompleted()

        for drop in a, b, c:
            self.assertEqual(DROPStates.COMPLETED, drop.status)
        self.assertEqual(output_data, pickle.loads(droputils.allDropContents(c)))  # @UndefinedVariable

    def test_func1(self):
        """Checks that func1 in this module works when wrapped"""
        data = os.urandom(64)
        self._test_func1_and_2('func1', data, data)

    def test_func2(self):
        """Checks that func2 in this module works when wrapped"""
        n = random.randint(0, 1e6)
        self._test_func1_and_2('func2', n, 2 * n)

    def _test_func3(self, output_drops, expected_outputs):

        a = _PyFuncApp('a', 'a', 'func3')
        for drop in output_drops:
            a.addOutput(drop)

        drops = [a] + droputils.listify(output_drops)
        a.execute()

        for drop in drops:
            self.assertEqual(DROPStates.COMPLETED, drop.status)

        for expected_output, drop in zip(expected_outputs, output_drops):
            self.assertEqual(expected_output, pickle.loads(droputils.allDropContents(drop)))  # @UndefinedVariable

    def test_func3_singleoutput(self):
        """
        Checks that func3 in this module works when wrapped as an application
        with multiple outputs.
        """
        self._test_func3([InMemoryDROP('b', 'b')], [['b', 'c', 'd']])

    def test_func3_multioutput(self):
        """
        Checks that func3 in this module works when wrapped as an application
        with multiple outputs.
        """
        output_drops = [InMemoryDROP(x, x) for x in ('b', 'c', 'd')]
        self._test_func3(output_drops, ('b', 'c', 'd'))

class PyFuncAppIntraNMTest(test_dm.NMTestsMixIn, unittest.TestCase):

    def test_input_in_remote_nm(self):
        """
        A test similar in spirit to TestDM.test_runGraphOneDOPerDom, but where
        application B is a PyFuncApp. This makes sure that PyFuncApp work fine
        across Node Managers.

        NM #1      NM #2
        =======    =============
        | A --|----|-> B --> C |
        =======    =============
        """
        g1 = [{"oid":"A", "type":"plain", "storage": "memory"}]
        g2 = [{"oid":"B", "type":"app", "app":"dfms.apps.pyfunc.PyFuncApp", "func_name": __name__ + '.func1'},
              {"oid":"C", "type":"plain", "storage": "memory", "producers":["B"]}]
        rels = [DROPRel('A', DROPLinkType.INPUT, 'B')]
        a_data = os.urandom(32)
        c_data = self._test_runGraphInTwoNMs(g1, g2, rels, pickle.dumps(a_data), c_data=None)
        self.assertEqual(a_data, pickle.loads(c_data))

    def test_output_in_remote_nm(self):
        """
        Like the above, but with this graph. In this case the output (instead of
        the input) is in a remote Node Manager.

        NM #1            NM #2
        =============    =======
        | A --> B --|----|-> C |
        =============    =======
        """
        g1 = [{"oid":"A", "type":"plain", "storage": "memory", "consumers": ['B']},
              {"oid":"B", "type":"app", "app":"dfms.apps.pyfunc.PyFuncApp", "func_name": __name__ + '.func1'}]
        g2 = [{"oid":"C", "type":"plain", "storage": "memory"}]
        rels = [DROPRel('B', DROPLinkType.PRODUCER, 'C')]
        a_data = os.urandom(32)
        c_data = self._test_runGraphInTwoNMs(g1, g2, rels, pickle.dumps(a_data), c_data=None)
        self.assertEqual(a_data, pickle.loads(c_data))