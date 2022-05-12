"""
Implements several DALiuGE drops to build low-pass filters with various methods.
"""

import numpy as np
import pyfftw
from dlg import droputils
from dlg.apps.simple import BarrierAppDROP
from dlg.common.reproducibility.constants import system_summary
from dlg.meta import dlg_batch_output, dlg_streaming_input
from dlg.meta import dlg_component, dlg_batch_input
from dlg.meta import dlg_int_param, dlg_list_param, dlg_float_param, dlg_bool_param


def determine_size(length):
    """
    :param length:
    :return: Computes the next largest power of two needed to contain |length| elements
    """
    return int(2 ** np.ceil(np.log2(length))) - 1


class LP_SignalGenerator(BarrierAppDROP):
    """
    Generates a noisy sine signal for filtering. Effectively an input generator.
    """

    component_meta = dlg_component(
        "LPSignalGen",
        "Low-pass filter example signal generator",
        [None],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

    # default values
    length = dlg_int_param("length", 256)
    srate = dlg_int_param("sample rate", 5000)
    freqs = dlg_list_param("frequencies", [440, 800, 1000, 2000])
    noise = dlg_list_param("noise", [])
    series = None

    def add_noise(
        self, series: np.array, mean, std, freq, sample_rate, seed, alpha=0.1
    ):
        """
        A noise to the provided signal by producing random values of a given frequency
        :param series: The input (and output) numpy array signal series
        :param mean: The average value
        :param std: The standard deviation of the value
        :param freq: The frequency of the noisy signal
        :param sample_rate: The sample rate of the input series
        :param seed: The random seed
        :param alpha: The multiplier
        :return: The input series with noisy values added
        """
        np.random.seed(seed)
        samples = alpha * np.random.normal(mean, std, size=len(series))
        for i in range(len(series)):
            samples[i] += np.sin(2 * np.pi * i * freq / sample_rate)
        np.add(series, samples, out=series)
        return series

    def gen_sig(self):
        """
        Generates an initial signal
        :return: Numpy array of signal values.
        """
        series = np.zeros(self.length, dtype=np.float64)
        for freq in self.freqs:
            for i in range(self.length):
                series[i] += np.sin(2 * np.pi * i * freq / self.srate)
        return series

    def run(self):
        """
        Called by DALiuGE to start signal generation. Conditionally adds noise if parameters are set
        :return: Writes signal to output ports.
        """
        outs = self.outputs
        if len(outs) < 1:
            raise Exception("At least one output required for %r" % self)
        self.series = self.gen_sig()
        if len(self.noise) > 0:
            self.noise[0] = 1 / self.noise[0]
            self.series = self.add_noise(
                self.series,
                self.noise[2],
                self.noise[4],
                self.noise[1],
                self.srate,
                self.noise[3],
                self.noise[0],
            )

        data = self.series.tostring()
        for output in outs:
            output.len = len(data)
            output.write(data)

    def generate_recompute_data(self):
        # This will do for now
        return {
            "length": self.length,
            "sample_rate": self.srate,
            "frequencies": self.freqs,
            "status": self.status,
            "system": system_summary(),
        }


class LP_WindowGenerator(BarrierAppDROP):
    """
    Generates a Hann window for low-pass filtering.
    """

    component_meta = dlg_component(
        "LPWindowGen",
        "Low-pass filter example window generator",
        [None],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

    # default values
    length = dlg_int_param("length", 256)
    cutoff = dlg_int_param("cutoff", 600)
    srate = dlg_int_param("sample_rate", 5000)
    series = None

    def sinc(self, x_val: np.float64):
        """
        Computes the sin_c value for the input float
        :param x_val:
        """
        if np.isclose(x_val, 0.0):
            return 1.0
        return np.sin(np.pi * x_val) / (np.pi * x_val)

    def gen_win(self):
        """
        Generates the window values.
        :return: Numpy array of window series.
        """
        alpha = 2 * self.cutoff / self.srate
        win = np.zeros(self.length, dtype=np.float64)
        for i in range(int(self.length)):
            ham = 0.54 - 0.46 * np.cos(
                2 * np.pi * i / int(self.length)
            )  # Hamming coefficient
            hsupp = i - int(self.length) / 2
            win[i] = ham * alpha * self.sinc(alpha * hsupp)
        return win

    def run(self):
        """
        Called by DALiuGE to start drop execution
        :return:
        """
        outs = self.outputs
        if len(outs) < 1:
            raise Exception("At least one output required for %r" % self)
        self.series = self.gen_win()
        data = self.series.tostring()
        for output in outs:
            output.len = len(data)
            output.write(data)

    def generate_recompute_data(self):
        output = dict()
        output["length"] = self.length
        output["cutoff"] = self.cutoff
        output["sample_rate"] = self.srate
        output["status"] = self.status
        output["system"] = system_summary()
        return output


class LP_AddNoise(BarrierAppDROP):
    """
    Component to add additional noise to a signal array.
    """

    component_meta = dlg_component(
        "LPAddNoise",
        "Adds noise to a signal generated " "for the low-pass filter example",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

    # default values
    mean = dlg_float_param("avg_noise", 0.0)
    std = dlg_float_param("std_deviation", 1.0)
    freq = dlg_int_param("frequency", 1200)
    srate = dlg_int_param("sample_rate", 5000)
    seed = dlg_int_param("random_seed", 42)
    alpha = dlg_float_param("noise_multiplier", 0.1)
    signal = np.empty([1])

    def add_noise(self):
        """
        Adds noise at a specified frequency.
        :return: Modified signal
        """
        np.random.seed(self.seed)
        samples = self.alpha * np.random.normal(
            self.mean, self.std, size=len(self.signal)
        )
        for i in range(len(self.signal)):
            samples[i] += np.sin(2 * np.pi * i * self.freq / self.srate)
        np.add(self.signal, samples, out=self.signal)
        return self.signal

    def get_inputs(self):
        """
        Reads input data into a numpy array.
        :return:
        """
        ins = self.inputs
        if len(ins) != 1:
            raise Exception("Precisely one input required for %r" % self)

        array = np.fromstring(droputils.allDropContents(ins[0]))
        self.signal = np.frombuffer(array)

    def run(self):
        """
        Called by DALiuGE to start drop execution.
        :return:
        """
        outs = self.outputs
        if len(outs) < 1:
            raise Exception("At least one output required for %r" % self)
        self.get_inputs()
        sig = self.add_noise()
        data = sig.tobytes()
        for output in outs:
            output.len = len(data)
            output.write(data)

    def generate_recompute_data(self):
        return {
            "mean": self.mean,
            "std": self.std,
            "sample_rate": self.srate,
            "seed": self.seed,
            "alpha": self.alpha,
            "system": system_summary(),
            "status": self.status,
        }


class LP_filter_fft_np(BarrierAppDROP):
    """
    Uses numpy to filter a nosiy signal.
    """

    component_meta = dlg_component(
        "LP_filter_np",
        "Filters a signal with " "a provided window using numpy",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

    PRECISIONS = {
        "double": {"float": np.float64, "complex": np.complex128},
        "single": {"float": np.float32, "complex": np.complex64},
    }
    precision = {}
    # default values
    double_prec = dlg_bool_param("doublePrec", True)
    series = []
    output = np.zeros([1])

    def initialize(self, **kwargs):
        super().initialize(**kwargs)
        if self.double_prec:
            self.precision = self.PRECISIONS["double"]
        else:
            self.precision = self.PRECISIONS["single"]

    def get_inputs(self):
        """
        Reads input arrays into numpy array
        :return: Sets class series variable.
        """
        ins = self.inputs
        if len(ins) != 2:
            raise Exception("Precisely two input required for %r" % self)

        array = [np.fromstring(droputils.allDropContents(inp)) for inp in ins]
        self.series = array

    def filter(self):
        """
        Actually performs the filtering
        :return: Numpy array of filtered signal.
        """
        signal = self.series[0]
        window = self.series[1]
        nfft = determine_size(len(signal) + len(window) - 1)
        print(nfft)
        sig_zero_pad = np.zeros(nfft, dtype=self.precision["float"])
        win_zero_pad = np.zeros(nfft, dtype=self.precision["float"])
        sig_zero_pad[0 : len(signal)] = signal
        win_zero_pad[0 : len(window)] = window
        sig_fft = np.fft.fft(sig_zero_pad)
        win_fft = np.fft.fft(win_zero_pad)
        out_fft = np.multiply(sig_fft, win_fft)
        out = np.fft.ifft(out_fft)
        return out.astype(self.precision["complex"])

    def run(self):
        """
        Called by DALiuGE to start execution
        :return:
        """
        outs = self.outputs
        if len(outs) < 1:
            raise Exception("At least one output required for %r" % self)
        self.get_inputs()
        self.output = self.filter()
        data = self.output.tostring()
        for output in outs:
            output.len = len(data)
            output.write(data)

    def generate_recompute_data(self):
        return {
            "precision_float": str(self.precision["float"]),
            "precision_complex": str(self.precision["complex"]),
            "system": system_summary(),
            "status": self.status,
        }


class LP_filter_fft_fftw(LP_filter_fft_np):
    """
    Uses fftw to implement a low-pass filter
    """

    component_meta = dlg_component(
        "LP_filter_fftw",
        "Filters a signal with " "a provided window using FFTW",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

    def filter(self):
        """
        Actually performs the filtering
        :return: Filtered signal as numpy array.
        """
        pyfftw.interfaces.cache.disable()
        signal = self.series[0]
        window = self.series[1]
        nfft = determine_size(len(signal) + len(window) - 1)
        sig_zero_pad = pyfftw.empty_aligned(len(signal), dtype=self.precision["float"])
        win_zero_pad = pyfftw.empty_aligned(len(window), dtype=self.precision["float"])
        sig_zero_pad[0 : len(signal)] = signal
        win_zero_pad[0 : len(window)] = window
        sig_fft = pyfftw.interfaces.numpy_fft.fft(sig_zero_pad, n=nfft)
        win_fft = pyfftw.interfaces.numpy_fft.fft(win_zero_pad, n=nfft)
        out_fft = np.multiply(sig_fft, win_fft)
        out = pyfftw.interfaces.numpy_fft.ifft(out_fft, n=nfft)
        return out.astype(self.precision["complex"])


class LP_filter_fft_cuda(LP_filter_fft_np):
    """
    Uses pycuda to implement a low-pass filter
    """

    component_meta = dlg_component(
        "LP_filter_fft_cuda",
        "Filters a signal with " "a provided window using cuda",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

    def filter(self):
        """
        Actually performs the filtering
        :return:
        """
        import pycuda.gpuarray as gpuarray
        import skcuda.fft as cu_fft
        import skcuda.linalg as linalg
        import pycuda.driver as cuda
        from pycuda.tools import make_default_context

        cuda.init()
        context = make_default_context()
        device = context.get_device()
        signal = self.series[0]
        window = self.series[1]
        linalg.init()
        nfft = determine_size(len(signal) + len(window) - 1)
        # Move data to GPU
        sig_zero_pad = np.zeros(nfft, dtype=self.precision["float"])
        win_zero_pad = np.zeros(nfft, dtype=self.precision["float"])
        sig_gpu = gpuarray.zeros(sig_zero_pad.shape, dtype=self.precision["float"])
        win_gpu = gpuarray.zeros(win_zero_pad.shape, dtype=self.precision["float"])
        sig_zero_pad[0 : len(signal)] = signal
        win_zero_pad[0 : len(window)] = window
        sig_gpu.set(sig_zero_pad)
        win_gpu.set(win_zero_pad)

        # Plan forwards
        sig_fft_gpu = gpuarray.zeros(nfft, dtype=self.precision["complex"])
        win_fft_gpu = gpuarray.zeros(nfft, dtype=self.precision["complex"])
        sig_plan_forward = cu_fft.Plan(
            sig_fft_gpu.shape, self.precision["float"], self.precision["complex"]
        )
        win_plan_forward = cu_fft.Plan(
            win_fft_gpu.shape, self.precision["float"], self.precision["complex"]
        )
        cu_fft.fft(sig_gpu, sig_fft_gpu, sig_plan_forward)
        cu_fft.fft(win_gpu, win_fft_gpu, win_plan_forward)

        # Convolve
        out_fft = linalg.multiply(sig_fft_gpu, win_fft_gpu, overwrite=True)
        linalg.scale(2.0, out_fft)

        # Plan inverse
        out_gpu = gpuarray.zeros_like(out_fft)
        plan_inverse = cu_fft.Plan(
            out_fft.shape, self.precision["complex"], self.precision["complex"]
        )
        cu_fft.ifft(out_fft, out_gpu, plan_inverse, True)
        out_np = np.zeros(len(out_gpu), self.precision["complex"])
        out_gpu.get(out_np)
        context.pop()
        return out_np


class LP_filter_pointwise_np(LP_filter_fft_np):
    """
    Uses raw numpy to implement a low-pass filter
    """

    component_meta = dlg_component(
        "LP_filter_pointwise_np",
        "Filters a signal with " "a provided window using cuda",
        [dlg_batch_input("binary/*", [])],
        [dlg_batch_output("binary/*", [])],
        [dlg_streaming_input("binary/*")],
    )

    def filter(self):
        return np.convolve(self.series[0], self.series[1], mode="full").astype(
            self.precision["complex"]
        )
