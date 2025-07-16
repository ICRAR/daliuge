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
import json
import pickle
from typing import Any


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


def createMultiOut(v1: Any = 1, v2: Any = 7) -> tuple:
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


def getMax(v1: float = 1.0, v2: float = 7.0) -> float:
    """
    Return the largest float of two different values.

    Inputs:
        v1: value 1
        v2: value 2

    Returns:
        max(v1, v2)
    """
    return max(v1, v2)


def string2json(string: str, pickle_flag: bool = False) -> list:
    """
    Simple function to convert a string to a JSON object

    Inputs:
        string: string containing a JSON representation
        pickle_flag: False, whether output should be pickled
    """
    if not string:
        string = '""'

    try:
        if not pickle_flag:
            return json.loads(string)
        else:
            return pickle.dumps(json.loads(string))
    except json.decoder.JSONDecodeError:
        return [] if not pickle_flag else pickle.dumps([])
