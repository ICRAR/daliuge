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
import unittest

from dlg import delayed
from dlg import tool
from dlg.utils import terminate_or_kill

def add(x, y):
    return x + y

def subtract(x, y):
    return x - y

def multiply(x, y):
    return x * y

def divide(x, y):
    return x / y

def partition(x):
    return x / 2, x - x / 2

class TestDelayed(unittest.TestCase):

    def setUp(self):
        unittest.TestCase.setUp(self)
        self.dmProcess = tool.start_process('nm')

    def tearDown(self):
        terminate_or_kill(self.dmProcess, 5)
        unittest.TestCase.tearDown(self)

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

        the_sum = delayed(add)(1., 2.)
        the_sub = delayed(subtract)(4., 3.)
        division = delayed(divide)(the_sum, the_sub)
        parts = delayed(partition, nout=2)(division)
        result = delayed(add)(*parts).compute()
        self.assertEqual(3., result)