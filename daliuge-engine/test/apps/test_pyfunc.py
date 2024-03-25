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
import numpy

from dlg import droputils, drop_loaders
from dlg.apps import pyfunc
from dlg.apps.simple_functions import string2json
from dlg.ddap_protocol import DROPStates, DROPRel, DROPLinkType
from dlg.data.drops.memory import InMemoryDROP
from dlg.droputils import DROPWaiterCtx
from dlg.exceptions import InvalidDropException

from ..manager import test_dm

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


def sum_with_args_and_kwarg(a, *args, **kwargs):
    """Returns a + kwargs['b'], or only a if no 'b' is found in kwargs"""
    b = kwargs.pop("b", 0)
    return a + sum(args) + b


def _PyFuncApp(oid, uid, f, **kwargs):
    fname = None
    if isinstance(f, str):
        fname = f = "test.apps.test_pyfunc." + f
    fw_kwargs = {
        k: v for k, v in kwargs.items() if k in ["input_parser", "output_parser"]
    }
    input_kws = [
        {k: v} for k, v in kwargs.items() if k not in ["input_parser", "output_parser"]
    ]
    fcode, fdefaults = pyfunc.serialize_func(f)
    return pyfunc.PyFuncApp(
        oid,
        uid,
        func_name=fname,
        func_code=fcode,
        func_defaults=fdefaults,
        inputs=input_kws,
        **fw_kwargs,
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

    def test_function_builtin_module(self):
        # The function lives in an unknown module/package
        pyfunc.PyFuncApp("a", "a", func_name="object.__init__")

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

    def test_pickle_func(self, f=lambda x: x, input_data="hello", output_data="hello"):
        a = InMemoryDROP("a", "a")
        b = _PyFuncApp("b", "b", f)
        c = InMemoryDROP("c", "c")

        b.addInput(a)
        b.addOutput(c)

        with DROPWaiterCtx(self, c, 5):
            drop_loaders.save_pickle(a, input_data)
            a.setCompleted()
        for drop in a, b, c:
            self.assertEqual(DROPStates.COMPLETED, drop.status)
        self.assertEqual(output_data, drop_loaders.load_pickle(c))

    def test_eval_func(self, f=lambda x: x, input_data=None, output_data=None):
        input_data = [2, 2] if input_data is None else input_data
        output_data = [2, 2] if output_data is None else output_data

        a = InMemoryDROP("a", "a")
        b = _PyFuncApp(
            "b",
            "b",
            f,
            input_parser=pyfunc.DropParser.EVAL,
            output_parser=pyfunc.DropParser.EVAL,
        )
        c = InMemoryDROP("c", "c")

        b.addInput(a)
        b.addOutput(c)

        with DROPWaiterCtx(self, c, 5):
            a.write(repr(input_data).encode("utf-8"))
            a.setCompleted()
        for drop in a, b, c:
            self.assertEqual(DROPStates.COMPLETED, drop.status)
        self.assertEqual(
            output_data,
            eval(droputils.allDropContents(c).decode("utf-8"), {}, {}),
        )

    def test_string2json_func(self, f=string2json, input_data=None, output_data=None):
        input_data = '["a", "b", "c"]' if input_data is None else input_data
        output_data = ["a", "b", "c"] if output_data is None else output_data

        a = InMemoryDROP("a", "a")
        b = _PyFuncApp(
            "b",
            "b",
            f,
        )
        c = InMemoryDROP("c", "c")

        b.addInput(a)
        b.addOutput(c)

        with DROPWaiterCtx(self, c, 5):
            drop_loaders.save_pickle(a, input_data)
            a.setCompleted()
        for drop in a, b, c:
            self.assertEqual(DROPStates.COMPLETED, drop.status)
        numpy.testing.assert_equal(output_data, drop_loaders.load_pickle(c))

    def test_npy_func(self, f=lambda x: x, input_data=None, output_data=None):
        input_data = numpy.ones([2, 2]) if input_data is None else input_data
        output_data = numpy.ones([2, 2]) if output_data is None else output_data

        a = InMemoryDROP("a", "a")
        b = _PyFuncApp(
            "b",
            "b",
            f,
            input_parser=pyfunc.DropParser.NPY,
            output_parser=pyfunc.DropParser.NPY,
        )
        c = InMemoryDROP("c", "c")

        b.addInput(a)
        b.addOutput(c)

        with DROPWaiterCtx(self, c, 5):
            drop_loaders.save_npy(a, input_data)
            a.setCompleted()
        for drop in a, b, c:
            self.assertEqual(DROPStates.COMPLETED, drop.status)
        numpy.testing.assert_equal(output_data, drop_loaders.load_npy(c))

    def _test_simple_functions(self, f, input_data, output_data):
        a, c = [InMemoryDROP(x, x) for x in ("a", "c")]
        b = _PyFuncApp("b", "b", f)
        b.addInput(a)
        b.addOutput(c)

        with DROPWaiterCtx(self, c, 5):
            a.write(pickle.dumps(input_data))
            a.setCompleted()

        for drop in a, b, c:
            self.assertEqual(DROPStates.COMPLETED, drop.status)
        self.assertEqual(output_data, pickle.loads(droputils.allDropContents(c)))

    def test_func1(self):
        """Checks that func1 in this module works when wrapped"""
        data = os.urandom(64)
        self._test_simple_functions("func1", data, data)

    def test_func2(self):
        """Checks that func2 in this module works when wrapped"""
        n = random.randint(0, 1_000_000)
        self._test_simple_functions("func2", n, 2 * n)

    def test_inner_func(self):
        n = random.randint(0, 1_000_000)

        def f(x):
            return x + 2

        self._test_simple_functions(f, n, n + 2)

    def test_lambda(self):
        n = random.randint(0, 1_000_000)
        self._test_simple_functions(lambda x: x / 2, n, n / 2)

    def test_inner_func_with_closure(self):
        n = random.randint(0, 1_000_000)

        def f(x):
            return x + n

        self._test_simple_functions(f, n, n + n)

    def test_lambda_with_closure(self):
        n = random.randint(0, 1_000_000)
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
            n_args = len(args)
            n_kwargs = len(kwargs)
            # List with (drop, value) elements
            arg_inputs = []
            # dict with name: (drop, value) items
            kwarg_inputs = {}
            arg_names = [
                "b",
                "c",
                "x",
                "y",
                "z",
            ]  # neeed to use argument names
            translate = lambda x: base64.b64encode(pickle.dumps(x))
            logger.debug(f"args: {args}")
            for i in range(n_args):
                logger.debug(f"adding arg input: {args[i]}")
                si = arg_names[i]
                arg_inputs.append(InMemoryDROP(si, si, pydata=translate(args[i])))
            i = n_args
            for name, value in kwargs.items():
                si = name  # use keyword name
                kwarg_inputs[name] = (
                    si,
                    InMemoryDROP(si, si, pydata=translate(value)),
                )
                i += 1

            a = InMemoryDROP("a", "a", pydata=translate(1))
            output = InMemoryDROP("o", "o")
            kwargs = {inp.uid: inp.oid for inp in arg_inputs}
            kwargs.update({name: vals[0] for name, vals in kwarg_inputs.items()})
            kwargs["a"] = a.oid
            app = _PyFuncApp(
                "f",
                "f",
                func,
                **kwargs,
            )
            logger.debug(f"adding input: {a}")
            app.addInput(a)
            app.addOutput(output)
            logger.debug(
                f"adding inputs: {arg_inputs + [x[1] for x in kwarg_inputs.values()]}"
            )
            for drop in arg_inputs + [x[1] for x in kwarg_inputs.values()]:
                app.addInput(drop)

            with droputils.DROPWaiterCtx(self, output, timeout=300):
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
        # self._test_defaults(501, 0)
        # self._test_defaults(481, 1)
        self._test_defaults(0, 1, 1, 40)
        # self._test_defaults(249, 1, 2, x=35)

    # @unittest.skip
    def test_defaults_kwargs_only(self):
        self._test_defaults(1, z=0, c=0)
        # self._test_defaults(1, z=0, b=0)
        # self._test_defaults(561, b=-1, y=300, z=2)

    # @unittest.skip
    def test_defaults_args_and_kwargs(self):
        self._test_defaults(561, -1, y=300, z=2)
        self._test_defaults(0, 1, 1, x=40)
        self._test_defaults(249, 1, 2, x=35)

    def test_mixed_explicit_and_variable_args(self):
        args = [1, 20, 30]
        kwargs = {"b": 100}
        self.assertEqual(sum_with_args_and_kwarg(1), 1)
        self.assertEqual(sum_with_args_and_kwarg(*args, **kwargs), 151)
        self.assertEqual(sum_with_args_and_kwarg(1, 20, 30, b=100), 151)

    def test_reproducibility(self):
        from dlg.common.reproducibility.constants import ReproducibilityFlags
        from dlg.data.drops.data_base import NullDROP

        a = _PyFuncApp("a", "a", "func3")
        a.run()
        b = NullDROP("b", "b")
        a.reproducibility_level = ReproducibilityFlags.RERUN
        b.reproducibility_level = ReproducibilityFlags.RERUN
        a.setCompleted()
        b.setCompleted()
        self.assertEqual(a.merkleroot, b.merkleroot)

        a.reproducibility_level = ReproducibilityFlags.REPEAT
        a.commit()
        self.assertEqual(a.merkleroot, b.merkleroot)

        a.reproducibility_level = ReproducibilityFlags.RECOMPUTE
        a.commit()
        self.assertNotEqual(a.merkleroot, b.merkleroot)
        self.assertEqual(a.generate_merkle_data(), {"args": {}})

        a.reproducibility_level = ReproducibilityFlags.REPRODUCE
        a.commit()
        self.assertNotEqual(a.merkleroot, b.merkleroot)
        self.assertEqual(a.generate_merkle_data(), {})

        a.reproducibility_level = ReproducibilityFlags.REPLICATE_SCI
        a.commit()
        self.assertEqual(a.merkleroot, b.merkleroot)
        self.assertEqual(a.generate_merkle_data(), a.generate_rerun_data())

        a.reproducibility_level = ReproducibilityFlags.REPLICATE_COMP
        a.commit()
        self.assertNotEqual(a.merkleroot, b.merkleroot)
        self.assertEqual(a.generate_merkle_data(), a.generate_recompute_data())

        a.reproducibility_level = ReproducibilityFlags.REPLICATE_TOTAL
        a.commit()
        self.assertEqual(a.merkleroot, b.merkleroot)
        self.assertEqual(a.generate_merkle_data(), a.generate_repeat_data())


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
        g1 = [
            {
                "oid": "A",
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
            }
        ]
        g2 = [
            {
                "oid": "B",
                "categoryType": "Application",
                "dropclass": "dlg.apps.pyfunc.PyFuncApp",
                "func_name": __name__ + ".func1",
            },
            {
                "oid": "C",
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
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
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
                "consumers": ["B"],
            },
            {
                "oid": "B",
                "categoryType": "Application",
                "dropclass": "dlg.apps.pyfunc.PyFuncApp",
                "func_name": __name__ + ".func1",
            },
        ]
        g2 = [
            {
                "oid": "C",
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
            }
        ]
        rels = [DROPRel("B", DROPLinkType.PRODUCER, "C")]
        a_data = os.urandom(32)
        c_data = self._test_runGraphInTwoNMs(g1, g2, rels, pickle.dumps(a_data), None)
        self.assertEqual(a_data, pickle.loads(c_data))
