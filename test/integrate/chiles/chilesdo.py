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
import threading
import Queue
import os
import sys
import uuid
import drivecasa
from dfms.data_object import DirectoryContainer, BarrierAppDataObject, InMemoryDataObject


class SourceFlux(BarrierAppDataObject):

    def initialize(self, **kwargs):
        
        super(SourceFlux, self).initialize(**kwargs)

        self.casapy_path = self._getArg(kwargs, 'casapy_path', None)
        self.timeout = self._getArg(kwargs, 'timeout', 180)
    

    def run(self):
        inp = self.inputs[0]
        out = self.outputs[0]

        print 'Calculating source flux on ', inp.path + '.image'

        casa = drivecasa.Casapy(casa_dir = self.casapy_path, timeout = self.timeout)
        casa.run_script(['ia.open("'"%s"'")' % (inp.path + '.image')])
        casa.run_script(['flux = ia.pixelvalue([128,128,0,179])["'"value"'"]["'"value"'"]'])
        casaout, _ = casa.run_script(['print flux'])
        flux = float(casaout[0])
        if flux > 9E-4:
            print 'Valid flux: %s' % flux
            out.write(str(flux))


class Clean(BarrierAppDataObject):

    def initialize(self, **kwargs):

        super(Clean, self).initialize(**kwargs)

        self.timeout = self._getArg(kwargs, 'timeout', 3600)
        self.casapy_path = self._getArg(kwargs, 'casapy_path', None) 

        self.clean_args = {
                        'field':  self._getArg(kwargs, 'field', None),
                        'spw': '',
                        'mode': self._getArg(kwargs, 'mode', None),
                        'restfreq': self._getArg(kwargs, 'restfreq', None),
                        'nchan': self._getArg(kwargs, 'nchan', None),
                        'start': self._getArg(kwargs, 'start', None),
                        'width': self._getArg(kwargs, 'width', None),
                        'interpolation': self._getArg(kwargs, 'interpolation', None),
                        'gain': self._getArg(kwargs, 'gain', None),
                        'imsize': self._getArg(kwargs, 'imsize', None),
                        'cell': self._getArg(kwargs, 'cell', None),
                        'phasecenter': self._getArg(kwargs, 'phasecenter', None),
                        'weighting': self._getArg(kwargs, 'weighting', None),
                        'usescratch': False }


    def invoke_clean(self, q, vis, outcube):

        try:
            script = []
            casa = drivecasa.Casapy(casa_dir = self.casapy_path, timeout = self.timeout)
            dirty_maps = drivecasa.commands.clean(script,
                                            vis_path = vis,
                                            out_path = outcube,
                                            niter = 0,
                                            threshold_in_jy = 0,
                                            other_clean_args = self.clean_args,
                                            overwrite = True)
            casa.run_script(script)
            q.put(0)

        except Exception as e:
            print str(e)
            q.put(-1)


    def run(self):

        vis = []
        inp = self.inputs
        out = self.outputs[0]

        for i in inp:
            vis.append(i.path)

        print 'Cleaning ', vis

        q = Queue.Queue()    
        t = threading.Thread(target = self.invoke_clean, args = (q, vis, out.path))
        t.start()
        t.join()

        result = q.get()
        if result != 0:
            raise Exception('Error cleaning')


class Split(BarrierAppDataObject):

    def initialize(self, **kwargs):

        super(Split, self).initialize(**kwargs)

        self.copy = self._getArg(kwargs, 'copy', False)
        self.copy_path = self._getArg(kwargs, 'copy_path', False)

        self.timeout = timeout
        self.casapy_path = casapy_path

        self.transform_args = {
                    'regridms': self._getArg(kwargs, 'regridms', None),
                    'restfreq': self._getArg(kwargs, 'restfreq', None),
                    'mode': self._getArg(kwargs, 'mode', None),
                    'nchan': self._getArg(kwargs, 'nchan', None),
                    'outframe': self._getArg(kwargs, 'outframe', None),
                    'interpolation': self._getArg(kwargs, 'interpolation', None),
                    'veltype': 'radio',
                    'start': self._getArg(kwargs, 'start', None),
                    'width': self._getArg(kwargs, 'width', None),
                    'spw': '',
                    'combinespws': True,
                    'nspw': 1,
                    'createmms': False,
                    'datacolumn': 'data' }


    def invoke_split(self, q, infile, outdir):

        try:
            script = []
            casa = drivecasa.Casapy(casa_dir = self.casapy_path, timeout = self.timeout)
            drivecasa.commands.mstransform(script, infile, outdir, self.transform_args, overwrite = True)
            casa.run_script(script)
            q.put(0)

        except Exception as e:
            print str(e)
            q.put(-1)


    def run(self):
        inp = self.inputs[0]
        out = self.outputs[0]

        print 'Splitting ', inp.path

        q = Queue.Queue()
        t = threading.Thread(target = self.invoke_split, args = (q, inp.path, out.path))
        t.start()
        t.join()
        
        result = q.get()
        if result != 0:
            raise Exception('Error splitting')
