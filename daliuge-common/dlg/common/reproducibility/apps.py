import pickle

import numpy as np
from dlg.apps.pyfunc import PyFuncApp
from dlg.apps.simple import BarrierAppDROP
from dlg.meta import dlg_batch_output, dlg_streaming_input
from dlg.meta import dlg_component, dlg_batch_input
from dlg.meta import dlg_int_param, dlg_list_param
from merklelib import MerkleTree


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


class LP_SignalGenerator(BarrierAppDROP):
    component_meta = dlg_component('LPSignalGen', 'Low-pass filter example signal generator',
                                   [dlg_batch_input('binary/*', [])],
                                   [dlg_batch_output('binary/*', [])],
                                   [dlg_streaming_input('binary/*')])

    # default values
    length = dlg_int_param('length', 512)
    srate = dlg_int_param('sample rate', 5000)
    freqs = dlg_list_param('Frequencies(int)', [440, 800, 1000, 2000])
    series = np.empty([1])

    def initialize(self, **kwargs):
        super(LP_SignalGenerator, self).initialize(**kwargs)

    def gen_sig(self):
        series = np.zeros(self.length, dtype=np.float64)
        for freq in self.freqs:
            for i in range(self.length):
                series[i] += np.sin(2 * np.pi * i * freq / self.srate)
        return series

    def run(self):
        outs = self.outputs
        if len(outs) < 1:
            raise Exception('At least one output required for %r' % self)
        self.series = self.gen_sig()
        data = pickle.dumps(self.series)
        for o in outs:
            o.len = len(data)
            o.write(data)

    def generate_recompute_data(self):
        # This will do for now
        return {'length': self.length,
                'sample_rate': self.srate,
                'frequencies': self.freqs}

    def generate_reproduce_data(self):
        # This will do for now
        return {'data_hash': MerkleTree(self.series).merkle_root}


class LP_WindowGenerator(BarrierAppDROP):
    component_meta = dlg_component('LPWindowGen', 'Low-pass filter example window generator',
                                   [dlg_batch_input('binary/*', [])],
                                   [dlg_batch_output('binary/*', [])],
                                   [dlg_streaming_input('binary/*')])

    # default values
    length = dlg_int_param('length', 512)
    cutoff = dlg_int_param('cutoff', 600)
    srate = dlg_int_param('sample_rate', 5000)
    series = np.empty([1])

    def initialize(self, **kwargs):
        super(LP_WindowGenerator, self).initialize(**kwargs)

    def sinc(self, x_val: np.float64):
        """
        Computes the sin_c value for the input float
        :param x_val:
        """
        if np.isclose(x_val, 0.0):
            return 1.0
        return np.sin(np.pi * x_val) / (np.pi * x_val)

    def gen_win(self):
        alpha = 2 * self.cutoff / self.srate
        win = np.zeros(self.length)
        for i in range(self.length):
            ham = 0.54 - 0.46 * np.cos(2 * np.pi * i / self.length)  # Hamming coefficient
            hsupp = (i - self.length / 2)
            win[i] = ham * alpha * self.sinc(alpha * hsupp)
        return win

    def run(self):
        outs = self.outputs
        if len(outs) < 1:
            raise Exception('At least one output required for %r' % self)
        self.series = self.gen_win()
        data = pickle.dumps(self.series)
        for o in outs:
            o.len = len(data)
            o.write(data)

    def generate_recompute_data(self):
        return {'length': self.length,
                'cutoff': self.cutoff,
                'sample_rate': self.srate}

    def generate_reproduce_data(self):
        return {'data_hash', MerkleTree(self.series).merkle_root}
