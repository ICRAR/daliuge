"""
Contains several very basic apps to test python function reproducibility.
"""
import numpy as np

from dlg.apps.pyfunc import PyFuncApp


def write_in():
    """
    :return: "world" always
    """
    return "world"


def write_out(phrase="everybody"):
    """
    Appends s to "Hello "
    :param phrase: The string to be appended
    :return: "Hello " + s
    """
    return "Hello " + phrase


def numpy_av(nums):
    """
    Finds the mean of a list of numbers using numpy.
    :param nums: The numbers to be averaged.
    :return: The mean.
    """
    return np.asscalar(np.mean(nums))


def my_av(nums):
    """
    Finds the mean of a list of numbers manually
    :param nums: The numbers to be averaged
    :return: The mean.
    """
    res = 0.0
    for num in nums:
        res += num
    return res / len(nums)


class HelloWorldPythonIn(PyFuncApp):
    """
    Wrapper app turning writeIn into a Python function app
    """

    def initialize(self, **kwargs):
        fname = "dlg.common.reproducibility.apps.write_in"
        super().initialize(func_name=fname)


class HelloWorldPythonOut(PyFuncApp):
    """
    Wrapper app turning writeOut into a Python function app
    """

    def initialize(self, **kwargs):
        fname = "dlg.common.reproducibility.apps.write_out"
        super().initialize(func_name=fname)


class NumpyAverage(PyFuncApp):
    """
    Wrapper app turning numpy_av into a Python function app
    """

    def initialize(self, **kwargs):
        fname = "dlg.common.reproducibility.apps.numpy_av"
        super().initialize(func_name=fname)


class MyAverage(PyFuncApp):
    """
    Wrapper app turning my_av into a Python function app
    """

    def initialize(self, **kwargs):
        fname = "dlg.common.reproducibility.apps.my_av"
        super().initialize(func_name=fname)
