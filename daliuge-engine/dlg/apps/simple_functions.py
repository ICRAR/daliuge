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
"""Functions used as examples, for testing, or in simple situations"""


def readLines(fileName: str) -> list:
    """
    Simple function to read all lines of a file.
    """
    with open(fileName, "r") as f:
        lines = f.read().split()
    return lines


def createRange(low: int, high: int, step: int) -> range:
    """
    Create and return a range.

    Inputs:
        low: the lower boundary of the range
        high: the higher boundary of the range (excluisve)
        step: Step size between items

    Returns:
        range: iterator
    """
    return range(low, high, step)


def createMultiOut(v1=1, v2=7):
    """
    Create two outputs with different values.

    Inputs:
        v1: value 1
        v2: value 2

    Returns:
        tuple of two values
    """
    out1 = v1
    out2 = v2
    return out1, out2
