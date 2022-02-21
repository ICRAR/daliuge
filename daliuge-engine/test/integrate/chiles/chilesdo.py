#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2015
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
import logging
import threading

from six.moves import queue as Queue  # @UnresolvedImport

from dlg.drop import BarrierAppDROP


logger = logging.getLogger(__name__)


class SourceFlux(BarrierAppDROP):
    def initialize(self, **kwargs):

        super(SourceFlux, self).initialize(**kwargs)

        self.casapy_path = self._getArg(kwargs, "casapy_path", None)
        self.timeout = self._getArg(kwargs, "timeout", 180)

    def run(self):
        inp = self.inputs[0]
        out = self.outputs[0]

        if logger.isEnabledFor(logging.INFO):
            logger.info("Calculating source flux on %s.image" % (inp.path))

        import drivecasa

        casa = drivecasa.Casapy(casa_dir=self.casapy_path, timeout=self.timeout)
        casa.run_script(['ia.open("' "%s" '")' % (inp.path + ".image")])
        casa.run_script(
            ['flux = ia.pixelvalue([128,128,0,179])["' "value" '"]["' "value" '"]']
        )
        casaout, _ = casa.run_script(["print flux"])
        flux = float(casaout[0])
        if flux > 9e-4:
            if logger.isEnabledFor(logging.INFO):
                logger.info("Valid flux found: %f" % (flux))
        out.write(str(flux))


class Clean(BarrierAppDROP):
    def initialize(self, **kwargs):

        super(Clean, self).initialize(**kwargs)

        self.timeout = self._getArg(kwargs, "timeout", 3600)
        self.casapy_path = self._getArg(kwargs, "casapy_path", None)

        self.clean_args = {
            "field": str(self._getArg(kwargs, "field", None)),
            "mode": str(self._getArg(kwargs, "mode", None)),
            "restfreq": str(self._getArg(kwargs, "restfreq", None)),
            "nchan": self._getArg(kwargs, "nchan", None),
            "start": str(self._getArg(kwargs, "start", None)),
            "width": str(self._getArg(kwargs, "width", None)),
            "interpolation": str(self._getArg(kwargs, "interpolation", None)),
            "gain": self._getArg(kwargs, "gain", None),
            "imsize": self._getArg(kwargs, "imsize", None),
            "cell": [str(x) for x in self._getArg(kwargs, "cell", [])],
            "phasecenter": str(self._getArg(kwargs, "phasecenter", None)),
            "weighting": str(self._getArg(kwargs, "weighting", None)),
            "usescratch": False,
        }

    def invoke_clean(self, q, vis, outcube):

        import drivecasa

        try:
            script = []
            casa = drivecasa.Casapy(casa_dir=self.casapy_path, timeout=self.timeout)
            drivecasa.commands.clean(
                script,
                vis_paths=vis,
                out_path=outcube,
                niter=0,
                threshold_in_jy=0,
                other_clean_args=self.clean_args,
                overwrite=True,
            )
            casa.run_script(script)
            q.put(0)

        except Exception:
            q.put(-1)
            raise

    def run(self):

        inp = self.inputs
        out = self.outputs[0]

        vis = [i.path for i in inp]

        if logger.isEnabledFor(logging.INFO):
            logger.info("Cleaning %r" % (vis))

        q = Queue.Queue()
        t = threading.Thread(target=self.invoke_clean, args=(q, vis, out.path))
        t.start()
        t.join()

        result = q.get()
        if result != 0:
            raise Exception("Error cleaning")


class Split(BarrierAppDROP):
    def initialize(self, **kwargs):

        super(Split, self).initialize(**kwargs)

        self.timeout = self._getArg(kwargs, "timeout", 3600)
        self.casapy_path = self._getArg(kwargs, "casapy_path", False)

        self.transform_args = {
            "regridms": self._getArg(kwargs, "regridms", None),
            "restfreq": str(self._getArg(kwargs, "restfreq", None)),
            "mode": str(self._getArg(kwargs, "mode", None)),
            "nchan": self._getArg(kwargs, "nchan", None),
            "outframe": str(self._getArg(kwargs, "outframe", None)),
            "interpolation": str(self._getArg(kwargs, "interpolation", None)),
            "veltype": "radio",
            "start": str(self._getArg(kwargs, "start", None)),
            "width": str(self._getArg(kwargs, "width", None)),
            "spw": "",
            "combinespws": True,
            "nspw": 1,
            "createmms": False,
            "datacolumn": "data",
        }

    def invoke_split(self, q, infile, outdir):

        import drivecasa

        try:
            script = []
            casa = drivecasa.Casapy(casa_dir=self.casapy_path, timeout=self.timeout)
            drivecasa.commands.mstransform(
                script, infile, outdir, self.transform_args, overwrite=True
            )
            casa.run_script(script)
            q.put(0)

        except Exception:
            q.put(-1)
            raise

    def run(self):
        inp = self.inputs[0]
        out = self.outputs[0]

        if logger.isEnabledFor(logging.INFO):
            logger.info("Splitting %s" % (inp.path))

        q = Queue.Queue()
        t = threading.Thread(target=self.invoke_split, args=(q, inp.path, out.path))
        t.start()
        t.join()

        result = q.get()
        if result != 0:
            raise Exception("Error splitting")
