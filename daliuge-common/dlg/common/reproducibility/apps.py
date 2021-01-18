from dlg.apps.pyfunc import PyFuncApp


def writeIn():
    return "world"


def writeOut(s="everybody"):
    return "Hello " + s


def numpy_av(nums):
    import numpy as np
    return np.asscalar(np.mean(nums))


def my_av(nums):
    res = 0.0
    for x in nums:
        res += x
    return res / len(nums)


class HelloWorldPythonIn(PyFuncApp):
    def initialize(self, **kwargs):
        fname = 'dlg.common.reproducibility.apps.writeIn'
        super(HelloWorldPythonIn, self).initialize(func_name=fname)


class HelloWorldPythonOut(PyFuncApp):
    def initialize(self, **kwargs):
        fname = 'dlg.common.reproducibility.apps.writeOut'
        super(HelloWorldPythonOut, self).initialize(func_name=fname)


class NumpyAverage(PyFuncApp):
    def initialize(self, **kwargs):
        fname = 'dlg.common.reproducibility.apps.numpy_av'
        super(NumpyAverage, self).initialize(func_name=fname)


class MyAverage(PyFuncApp):
    def initialize(self, **kwargs):
        fname = 'dlg.common.reproducibility.apps.my_av'
        super(MyAverage, self).initialize(func_name=fname)
