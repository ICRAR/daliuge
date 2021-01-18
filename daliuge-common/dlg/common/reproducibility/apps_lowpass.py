import pickle

import numpy as np
from dlg import droputils
from dlg.apps.simple import BarrierAppDROP
from dlg.meta import dlg_batch_output, dlg_streaming_input
from dlg.meta import dlg_component, dlg_batch_input
from dlg.meta import dlg_int_param, dlg_list_param, dlg_float_param
from merklelib import MerkleTree


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


class LP_AddNoise(BarrierAppDROP):
    component_meta = dlg_component('LPAddNoise', 'Adds noise to a signal generated for the low-pass filter example',
                                   [dlg_batch_input('binary/*', [])],
                                   [dlg_batch_output('binary/*', [])],
                                   [dlg_streaming_input('binary/*')])

    # default values
    mean = dlg_float_param('avg_noise', 0.0)
    std = dlg_float_param('std_deviation', 1.0)
    freq = dlg_int_param('frequency', 1200)
    srate = dlg_int_param('sample_rate', 5000)
    seed = dlg_int_param('random_seed', 42)
    alpha = dlg_float_param('noise_multiplier', 0.1)
    signal = np.empty([1])

    def initialize(self, **kwargs):
        super(LP_AddNoise).initialize(**kwargs)

    def add_noise(self):
        np.random.seed(self.seed)
        samples = self.alpha * np.random.normal(self.mean, self.std, size=len(self.signal))
        for i in range(len(self.signal)):
            samples[i] += np.sin(2 * np.pi * i * self.freq / self.srate)
        np.add(self.signal, samples, out=self.signal)
        return self.signal

    def getInputArrays(self):
        ins = self.inputs
        if len(ins) != 1:
            raise Exception('Precisely one input required for %r' % self)

        array = pickle.loads(droputils.allDropContents(ins[0]))
        self.signal = array

    def run(self):
        outs = self.outputs
        if len(outs) < 1:
            raise Exception('At least one output required for %r' % self)
        self.getInputArrays()
        sig = self.add_noise()
        data = pickle.dumps(sig)
        for o in outs:
            o.len = len(data)
            o.write(data)

    def generate_recompute_data(self):
        return {'mean': self.mean,
                'std': self.std,
                'sample_rate': self.srate,
                'seed': self.seed,
                'alpha': self.alpha}

    def generate_reproduce_data(self):
        return {'data_hash', MerkleTree(self.signal).merkle_root}
