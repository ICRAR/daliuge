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
import base64
import logging
import os
import pickle
import random
import unittest

from ..manager import test_dm
from dlg import droputils
from dlg.apps import pyfunc
from dlg.ddap_protocol import DROPStates, DROPRel, DROPLinkType
from dlg.drop import InMemoryDROP
from dlg.droputils import DROPWaiterCtx
from dlg.exceptions import InvalidDropException
from dlg.common import Categories


logger = logging.getLogger(__name__)


def func1(arg1):
    return arg1


def func2(arg1):
    return arg1 * 2


def func3():
    return ["b", "c", "d"]


def func_with_defaults(a, b=10, c=20, x=30, y=40, z=50):
    """Returns a - b * c + (y - x) * z. Default is a + 300"""
    res = a - b * c + (y - x) * z
    logger.info("%r - %r * %r + (%r - %r) * %r = %r", a, b, c, y, x, z, res)
    return res


def _PyFuncApp(oid, uid, f, **kwargs):

    fname = None
    if isinstance(f, str):
        fname = f = "test.apps.test_pyfunc." + f

    fcode, fdefaults = pyfunc.serialize_func(f)
    return pyfunc.PyFuncApp(
        oid, uid, func_name=fname, func_code=fcode, func_defaults=fdefaults, **kwargs
    )


class TestPyFuncApp(unittest.TestCase):
    def test_missing_function_param(self):
        self.assertRaises(InvalidDropException, pyfunc.PyFuncApp, "a", "a")

    def test_invalid_function_param(self):
        # The function doesn't have a module
        self.assertRaises(
            InvalidDropException, pyfunc.PyFuncApp, "a", "a", func_name="func1"
        )

    def test_function_invalid_module(self):
        # The function lives in an unknown module/package
        self.assertRaises(
            InvalidDropException,
            pyfunc.PyFuncApp,
            "a",
            "a",
            func_name="doesnt_exist.func1",
        )

    def test_function_invalid_fname(self):
        # The function lives in an unknown module/package
        self.assertRaises(
            InvalidDropException,
            pyfunc.PyFuncApp,
            "a",
            "a",
            func_name="test.apps.test_pyfunc.doesnt_exist",
        )

    def test_valid_creation(self):
        _PyFuncApp("a", "a", "func1")

    def test_creation_with_code(self):
        def inner_function(x, y):
            return x + y

        _PyFuncApp("a", "a", inner_function)

    def _test_simple_functions(self, f, input_data, output_data):

        a, c = [InMemoryDROP(x, x) for x in ("a", "c")]
        b = _PyFuncApp("b", "b", f)
        b.addInput(a)
        b.addOutput(c)

        with DROPWaiterCtx(self, c, 5):
            a.write(pickle.dumps(input_data))  # @UndefinedVariable
            a.setCompleted()

        for drop in a, b, c:
            self.assertEqual(DROPStates.COMPLETED, drop.status)
        self.assertEqual(
            output_data, pickle.loads(droputils.allDropContents(c))
        )  # @UndefinedVariable

    def test_func1(self):
        """Checks that func1 in this module works when wrapped"""
        data = os.urandom(64)
        self._test_simple_functions("func1", data, data)

    def test_func2(self):
        """Checks that func2 in this module works when wrapped"""
        n = random.randint(0, 1e6)
        self._test_simple_functions("func2", n, 2 * n)

    def test_inner_func(self):
        n = random.randint(0, 1e6)

        def f(x):
            return x + 2

        self._test_simple_functions(f, n, n + 2)

    def test_lambda(self):
        n = random.randint(0, 1e6)
        self._test_simple_functions(lambda x: x / 2, n, n / 2)

    def test_inner_func_with_closure(self):
        n = random.randint(0, 1e6)

        def f(x):
            return x + n

        self._test_simple_functions(f, n, n + n)

    def test_lambda_with_closure(self):
        n = random.randint(0, 1e6)
        self._test_simple_functions(lambda x: (x + n) / 2, n, (n + n) / 2)

    def _test_func3(self, output_drops, expected_outputs):

        a = _PyFuncApp("a", "a", "func3")
        for drop in output_drops:
            a.addOutput(drop)

        drops = [a] + droputils.listify(output_drops)
        a.execute()

        for drop in drops:
            self.assertEqual(DROPStates.COMPLETED, drop.status)

        for expected_output, drop in zip(expected_outputs, output_drops):
            self.assertEqual(
                expected_output, pickle.loads(droputils.allDropContents(drop))
            )  # @UndefinedVariable

    def test_func3_singleoutput(self):
        """
        Checks that func3 in this module works when wrapped as an application
        with multiple outputs.
        """
        self._test_func3([InMemoryDROP("b", "b")], [["b", "c", "d"]])

    def test_func3_multioutput(self):
        """
        Checks that func3 in this module works when wrapped as an application
        with multiple outputs.
        """
        output_drops = [InMemoryDROP(x, x) for x in ("b", "c", "d")]
        self._test_func3(output_drops, ("b", "c", "d"))

    def _test_defaults(self, expected_out, *args, **kwargs):
        def _do_test(func, expected_out, *args, **kwargs):

            # List with (drop, value) elements
            arg_inputs = []
            # dict with name: (drop, value) items
            kwarg_inputs = {}

            translate = lambda x: base64.b64encode(pickle.dumps(x))
            i = 0
            for arg in args:
                si = "uid_%d" % i
                arg_inputs.append(InMemoryDROP(si, si, pydata=translate(arg)))
                i += 1
            for name, kwarg in kwargs.items():
                si = "uid_%d" % i
                kwarg_inputs[name] = (si, InMemoryDROP(si, si, pydata=translate(kwarg)))
                i += 1

            a = InMemoryDROP("a", "a", pydata=translate(1))
            output = InMemoryDROP("o", "o")

            app = _PyFuncApp(
                "f",
                "f",
                func,
                func_arg_mapping={name: vals[0] for name, vals in kwarg_inputs.items()},
            )
            app.addInput(a)
            app.addOutput(output)
            for drop in arg_inputs + [x[1] for x in kwarg_inputs.values()]:
                app.addInput(drop)

            with droputils.DROPWaiterCtx(self, output):
                a.setCompleted()
                for i in arg_inputs + [x[1] for x in kwarg_inputs.values()]:
                    i.setCompleted()

            self.assertEqual(
                expected_out, pickle.loads(droputils.allDropContents(output))
            )  # @UndefinedVariable

        # func_with_defaults returns a - b * c + (y - x) * z
        # defaults are: b=10, c=20, x=30, y=40, z=50
        # The value of "a" is always 1
        _do_test("func_with_defaults", expected_out, *args, **kwargs)
        _do_test(func_with_defaults, expected_out, *args, **kwargs)

    def test_defaults_empty(self):
        # 1 - b * c + (y - x) * z
        # defaults are: b=10, c=20, x=30, y=40, z=50
        self._test_defaults(301)

    def test_defaults_positional_args_only(self):
        # 1 - b * c + (y - x) * z
        # defaults are: b=10, c=20, x=30, y=40, z=50
        self._test_defaults(501, 0)
        self._test_defaults(481, 1)
        self._test_defaults(0, 1, 1, 40)
        self._test_defaults(249, 1, 2, 35)

    def test_defaults_kwargs_only(self):
        self._test_defaults(301)
        self._test_defaults(1, z=0, c=0)
        self._test_defaults(1, z=0, b=0)
        self._test_defaults(561, b=-1, y=300, z=2)

    def test_defaults_args_and_kwargs(self):
        self._test_defaults(561, -1, y=300, z=2)
        self._test_defaults(0, 1, 1, x=40)
        self._test_defaults(249, 1, 2, x=35)


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
        g1 = [{"oid": "A", "type": "plain", "storage": Categories.MEMORY}]
        g2 = [
            {
                "oid": "B",
                "type": "app",
                "app": "dlg.apps.pyfunc.PyFuncApp",
                "func_name": __name__ + ".func1",
            },
            {
                "oid": "C",
                "type": "plain",
                "storage": Categories.MEMORY,
                "producers": ["B"],
            },
        ]
        rels = [DROPRel("A", DROPLinkType.INPUT, "B")]
        a_data = os.urandom(32)
        c_data = self._test_runGraphInTwoNMs(g1, g2, rels, pickle.dumps(a_data), None)
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
        g1 = [
            {
                "oid": "A",
                "type": "plain",
                "storage": Categories.MEMORY,
                "consumers": ["B"],
            },
            {
                "oid": "B",
                "type": "app",
                "app": "dlg.apps.pyfunc.PyFuncApp",
                "func_name": __name__ + ".func1",
            },
        ]
        g2 = [{"oid": "C", "type": "plain", "storage": Categories.MEMORY}]
        rels = [DROPRel("B", DROPLinkType.PRODUCER, "C")]
        a_data = os.urandom(32)
        c_data = self._test_runGraphInTwoNMs(g1, g2, rels, pickle.dumps(a_data), None)
        self.assertEqual(a_data, pickle.loads(c_data))
