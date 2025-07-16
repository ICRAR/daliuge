#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2018
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
from asyncio.log import logger
import functools
import json
import os
import pytest
import unittest

import numpy as np

from dlg.dask_emulation import delayed as dlg_delayed
from dlg.dask_emulation import compute as dlg_compute
from dlg.common import tool
from dlg.dask_emulation import compute as dlg_compute
from dlg.dask_emulation import delayed as dlg_delayed
from dlg.utils import terminate_or_kill
from six.moves import reduce  # @UnresolvedImport

try:
    from dask import delayed as dask_delayed
    from dask import compute as dask_compute
except ImportError:
    dask_delayed = None

def none_func(a):
    return None

def add(x, y):
    return x + y

def subtract(x: float, y: float):
    return x - y


def add_list(numbers):
    import functools
    def add(x, y):
        return x + y

    return functools.reduce(add, numbers)


def subtract_list(numbers: list):
    def subtract(x: float, y: float):
        return x - y
    import functools
    return functools.reduce(subtract, numbers)


def multiply(x, y):
    return x * y


def divide(x, y):
    return x / y


def partition(x):
    return x / 2, x - x / 2


def sum_with_args(a, a1=0, a2=0, a3=0):
    """Returns a + sum((a1, a2, a3))"""
    return a + sum([a1, a2, a3])


def sum_with_kwargs(a, **kwargs):
    """Returns a + kwargs['b'], or only a if no 'b' is found in kwargs"""
    b = kwargs.pop("b", 0)
    return a + b


def sum_with_args_and_kwarg(a, *args, **kwargs):
    """Returns a + sum(args) + b, or only a +sum(args) if no 'b' is found in kwargs"""
    b = kwargs.pop("b", 0)
    return a + sum(args) + b


class MyType(object):
    """A type that is serializable but not convertible to JSON"""

    def __init__(self, x):
        self.x = x
        self.array = np.zeros(1)


try:
    json.dumps(MyType(1))
    assert False, "Should fail to serialize to JSON"
except:
    pass

def sum_with_user_defined_default(a, b=MyType(10)):
    return a + b.x


@pytest.fixture(scope="function", autouse=True)
def change_test_dir(request, monkeypatch):
    print(f">>>>> path: {request.fspath.dirname}")
    monkeypatch.chdir(request.fspath.dirname)


class _TestDelayed(object):
    """Test definitions run under non-delayed, dlg_delayed and possibly dask_delayed contexts"""

    def test_simple(self):
        """This creates the following graph

        1 --|
            |-- add --> the_sum --------|
        2 --|                           |                                       |--> part1 --|
                                        |--> divide --> division -->partition --|            |--> add -> final
        4 --|                           |                                       |--> part2 --|
            |-- substract --> the_sub --|
        3 --|
        """
        delayed = self.delayed
        compute = self.compute
        
        the_sum = delayed(add)(1.0, 2.0)
        the_sub = delayed(subtract)(4.0, 3.0)
        division = delayed(divide)(the_sum, the_sub)
        parts = delayed(partition, nout=2)(division)
        logger.debug(f"partitions: {type(parts)}")
        result = compute(delayed(add)(*parts))
        self.assertEqual(3.0, result)

    def test_args_as_lists(self):
        """Like test_simple, but some arguments are passed down as lists"""
        delayed = self.delayed
        compute = self.compute

        the_sum = delayed(add_list)([1.0, 2.0])
        the_sub = delayed(subtract_list)([4.0, 3.0])
        division = delayed(divide)(the_sum, the_sub)
        parts = delayed(partition, nout=2)(division)
        x, y = parts
        result = compute(delayed(add)(x,y))
        self.assertEqual(3.0, result)

    def test_compute_with_lists(self):
        """Make sure we can call compute() directly on the list objects"""
        delayed = self.delayed
        compute = self.compute

        def double(x):
            return x*2

        doubles = [delayed(double)(x) for x in (1.0, 2.0, 3.0, 4.0)]
        result = compute(doubles)
        self.assertEqual([2.0, 4.0, 6.0, 8.0], result)

    def test_none_arg(self):
        """Test that calling delayed(f)(None) works"""
        delayed = self.delayed
        compute = self.compute
        self.assertEqual(compute(delayed(none_func)(None)), None)

    def test_with_args(self):
        """Tests that delayed() works correctly with positional args"""
        delayed = self.delayed
        compute = self.compute
        logger.info(f"Running compute(delayed(sum_with_args)(1)), 1")
        self.assertEqual(compute(delayed(sum_with_args)(1)), 1)
        logger.info(f"Running compute(delayed(sum_with_args)(1, 20)), 21")
        self.assertEqual(compute(delayed(sum_with_args)(1, 20)), 21)
        logger.info(f"Running compute(delayed(sum_with_args)(1, 20, 30)), 51")
        self.assertEqual(compute(delayed(sum_with_args)(1, 20, 30)), 51)

    def test_with_kwargs(self):
        """Tests that delayed() works correctly with args and kwargs"""
        delayed = self.delayed
        compute = self.compute

        self.assertEqual(compute(delayed(sum_with_kwargs)(1)), 1)

    def test_with_args_and_kwargs(self):
        """Tests that delayed() works correctly with kwargs"""
        delayed = self.delayed
        compute = self.compute

        self.assertEqual(compute(delayed(sum_with_args_and_kwarg)(1)), 1)

    def test_with_user_defined_default(self):
        """Tests that delayed() works with default values that are not json-dumpable"""
        delayed = self.delayed
        compute = self.compute

        def sum_with_user_defined_default(a, b=MyType(10)):
            return a + b.x

        self.assertEqual(compute(delayed(sum_with_user_defined_default)(1)), 11)
        arg = MyType(20)
        self.assertEqual(
            compute(delayed(sum_with_user_defined_default)(1, arg)), 21
        )

    def test_with_noniterable_nout_1(self):
        """Tests that using nout=1 works as expected with non-iterable objects"""
        # Simple call
        delayed = self.delayed
        compute = self.compute

        self.assertEqual(compute(delayed(add, nout=1)(1, 2)), 3)

        # Compute a delayed that uses a delayed with nout=1
        addition = delayed(add, nout=1)(1, 2)
        self.assertEqual(compute(delayed(add)(addition, 3)), 6)

        # Like above, but the first delayed also uses nout=1
        self.assertEqual(compute(delayed(add, nout=1)(addition, 3)), 6)

    def test_with_iterable_nout_1(self):
        """Tests that using nout=1 works as expected with iterable objects"""
        # Like last test above, but result is actually iterable
        delayed = self.delayed
        compute = self.compute
        def listify(x):
            return [x]

        for x in delayed(listify, nout=1)(1):
            results = compute(delayed(add)(x, 2))
        self.assertEqual(results, 3)


class TestNoDelayed(unittest.TestCase, _TestDelayed):
    """Non-delayed tests"""

    def delayed(self, f, *_, **__):
        return f

    def compute(self, val):
        return val


class TestDlgDelayed(_TestDelayed, unittest.TestCase):
    """dlg-base tests, they start/stop the node manager and use dlg_delayed"""

    def delayed(self, f, *args, **kwargs):
        return dlg_delayed(f, *args, **kwargs)

    def setUp(self):
        env = os.environ.copy()
        env["PYTHONPATH"] = f"{env.get('PYTHONPATH', '')}:{os.path.abspath('.')}/.."
        print(f">>>> env: {env['PYTHONPATH']}")
        unittest.TestCase.setUp(self)
        self.dmProcess = tool.start_process("nm", ["-vvv"], env=env)

    def compute(self, val):
        return dlg_compute(val)

    def tearDown(self):
        terminate_or_kill(self.dmProcess, 5)
        unittest.TestCase.tearDown(self)


@unittest.skipIf(dask_delayed is None, "dask is not available")
class TestDaskDelayed(_TestDelayed, unittest.TestCase):
    """dask-base tests, they use dask_delayed"""

    def delayed(self, f, *args, **kwargs):
        return dask_delayed(f, *args, **kwargs)

    def compute(self, val):
        logger.info(f"Running compute...")
        return dask_compute(val)[0]
