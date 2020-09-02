from dlg.apps.pyfunc import PyFuncApp


def writeIn():
    return "world"


def writeOut(s="everybody"):
    return "Hello " + s


class HelloWorldPythonIn(PyFuncApp):
    def initialize(self, **kwargs):
        fname = 'dlg.common.reproducibility.apps.writeIn'
        super(HelloWorldPythonIn, self).initialize(func_name=fname)


class HelloWorldPythonOut(PyFuncApp):
    def initialize(self, **kwargs):
        fname = 'dlg.common.reproducibility.apps.writeOut'
        super(HelloWorldPythonOut, self).initialize(func_name=fname)