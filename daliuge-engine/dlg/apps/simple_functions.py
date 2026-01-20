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


##
# @brief hello_world
# @details A simple APP that implements the standard Hello World in DALiuGE.
# It allows to change 'World' with some other string and it also permits
# to connect the single output port to multiple sinks, which will all receive
# the same message. App does not require any input.
# @par EAGLE_START
# @param category PyFuncApp
# @param tag daliuge
# @param greet World/String/ApplicationArgument/InputPort/ReadWrite//False/False/What appears after 'Hello '
# @param hello "world"/Object/ApplicationArgument/OutputPort/ReadWrite//False/False/message
# @param log_level "NOTSET"/Select/ComponentParameter/NoPort/ReadWrite/NOTSET,DEBUG,INFO,WARNING,ERROR,CRITICAL/False/False/Set the log level for this drop
# @param dropclass dlg.apps.simple.HelloWorldApp/String/ComponentParameter/NoPort/ReadOnly//False/False/Application class
# @param base_name simple/String/ComponentParameter/NoPort/ReadOnly//False/False/Base name of application class
# @param execution_time 5/Float/ConstraintParameter/NoPort/ReadOnly//False/False/Estimated execution time
# @param num_cpus 1/Integer/ConstraintParameter/NoPort/ReadOnly//False/False/Number of cores used
# @param group_start False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Is this node the start of a group?
# @par EAGLE_END

def hello_world(greet: str="World"):
    """
    Return a greeting based on user input
    :param greet: The greeting, defaults to "World"
    :return: str
    """
    final_greet = greet
    if isinstance(greet, list) or isinstance(greet, np.ndarray):
        if not greet:
            final_greet = ""
        else:
            final_greet =" ".join(g for g in g) if len(greet) > 1 else greet[0]

    return f"Hello, {final_greet}"
##
# @brief random_array
# @details A testing APP that does not take any input and produces a random array of

# type int64, if integer is set to True, else of type float64.
# size indicates the number of elements ranging between the values low and high.
# The resulting array will be send to all connected output apps.
# @par EAGLE_START
# @param category PyFuncApp
# @param tag daliuge
# @param size 100/Integer/ApplicationArgument/NoPort/ReadWrite//False/False/The size of the array
# @param low 0/Float/ApplicationArgument/NoPort/ReadWrite//False/False/Low value of range in array [inclusive]
# @param high 100/Float/ApplicationArgument/NoPort/ReadWrite//False/False/High value of
# range of array [exclusive]
# @param integer True/Boolean/ApplicationArgument/NoPort/ReadWrite//False/False/Generate integer array?
# @param log_level "NOTSET"/Select/ComponentParameter/NoPort/ReadWrite/NOTSET,DEBUG,INFO,WARNING,ERROR,CRITICAL/False/False/Set the log level for this drop
# @param dropclass dlg.apps.simple.RandomArrayApp/String/ComponentParameter/NoPort/ReadOnly//False/False/Application class
# @param base_name simple/String/ComponentParameter/NoPort/ReadOnly//False/False/Base name of application class
# @param execution_time 5/Float/ConstraintParameter/NoPort/ReadOnly//False/False/Estimated execution time
# @param num_cpus 1/Integer/ConstraintParameter/NoPort/ReadOnly//False/False/Number of cores used
# @param group_start False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Is this node the start of a group?
# @param array /Object.Array/ApplicationArgument/OutputPort/ReadWrite//False/False/random array
# @par EAGLE_END

def random_array(low=0, high=100, size=100, integer: bool=True, seed: int=0):
    """
    Produce a random numpy array of integers or floats of 'size' between 'low' and 'high'

    If integer is True the array will be integers (this is default  behaviour).

    :param low: lowest value of the array
    :param high: highest value of the array
    :param size: size of the array
    :param integer: True for integer array; if False, generate a float array
    :param seed: Integer seed value for the array
    :return: np.array
    """
    np.random.seed(seed)
    if integer:
        return np.random.randint(int(low), int(high), size=size)
    return (np.random.random(size=size) + low) * high

##
# @brief retrieve_url
# @details A simple APP that retrieves the content of a URL and writes
# it to all outputs.
# @par EAGLE_START
# @param category PyFuncApp
# @param tag daliuge
# @param url None/String/ApplicationArgument/NoPort/ReadWrite//False/False/The URL to retrieve
# @param log_level "NOTSET"/Select/ComponentParameter/NoPort/ReadWrite/NOTSET,DEBUG,INFO,WARNING,ERROR,CRITICAL/False/False/Set the log level for this drop
# @param dropclass dlg.apps.simple.UrlRetrieveApp/String/ComponentParameter/NoPort/ReadOnly//False/False/Application class
# @param base_name simple/String/ComponentParameter/NoPort/ReadOnly//False/False/Base name of application class
# @param execution_time 5/Float/ConstraintParameter/NoPort/ReadOnly//False/False/Estimated execution time
# @param num_cpus 1/Integer/ConstraintParameter/NoPort/ReadOnly//False/False/Number of cores used
# @param group_start False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Is this node the start of a group?
# @param content /String/ApplicationArgument/OutputPort/ReadWrite//False/False/content read from URL
# @par EAGLE_END

def retrieve_url(url):
    """
    App that retrieves the content of a URL
    :param url: string-formatted URL
    :return:
    """
    try:
        logger.info("Accessing URL %s", url)
        return requests.get(url, timeout=30)
    except requests.exceptions.RequestException as e:
        raise e.reason

##
# @brief list_thrashing
# @details A testing APP that appends a random integer to a list num times.
# This is a CPU intensive operation and can thus be used to provide a test for application threading
# since this operation will not yield.
# The resulting array will be sent to all connected output apps.
# @par EAGLE_START
# @param category PyFuncApp
# @param tag test
# @param size 100/Integer/ApplicationArgument/NoPort/ReadWrite//False/False/the size of the array
# @param log_level "NOTSET"/Select/ComponentParameter/NoPort/ReadWrite/NOTSET,DEBUG,INFO,WARNING,ERROR,CRITICAL/False/False/Set the log level for this drop
# @param dropclass dlg.apps.simple.GenericScatterApp/String/ComponentParameter/NoPort/ReadOnly//False/False/Application class
# @param dropclass dlg.apps.simple.ListAppendThrashingApp/String/ComponentParameter/NoPort/ReadOnly//False/False/Application class
# @param base_name simple/String/ComponentParameter/NoPort/ReadOnly//False/False/Base name of application class
# @param execution_time 5/Float/ConstraintParameter/NoPort/ReadOnly//False/False/Estimated execution time
# @param num_cpus 1/Integer/ConstraintParameter/NoPort/ReadOnly//False/False/Number of cores used
# @param group_start False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Is this node the start of a group?
# @param array /Object.Array/ApplicationArgument/OutputPort/ReadWrite//False/False/random array
# @par EAGLE_END

def list_thrashing(n: int=100):
    """
    A BarrierAppDrop that appends random integers to a list N times. It does
    not require any inputs and writes the generated array to all of its
    outputs.

    :param n: size of the list, default=100
    :return: list
    """
    marray = []
    for _ in range(n):
        marray = []
        for _ in range(n):
            marray.append(random.random())
    return marray

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
