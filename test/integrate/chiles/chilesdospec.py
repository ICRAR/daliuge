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
import os
import sys
import uuid
import threading
import json
from chilesdo import Split, Clean, SourceFlux
from dfms.data_object import DirectoryContainer, BarrierAppDataObject, InMemoryDataObject, dodict

LOCAL_FILES = os.path.dirname(os.path.realpath(__file__))
CASAPY = '/home/ec2-user/casa-release-4.4.0-el6/'
VIS_ROOT = '/home/ec2-user/data/input/'
VIS_OUT = '/home/ec2-user/data/output/'
CUBE_OUT = '/home/ec2-user/data/output/'
CUBE_NAME = 'cube1408~1412'

VIS = [
        (VIS_ROOT + '20131025_951_4_FINAL_PRODUCTS/20131025_951_4_calibrated_deepfield.ms', VIS_OUT + '20131025_951_4/', '52.64.236.25'),
        (VIS_ROOT + '20131031_951_4_FINAL_PRODUCTS/20131031_951_4_calibrated_deepfield.ms', VIS_OUT + '20131031_951_4/', '52.64.48.46'),
        (VIS_ROOT + '20131121_946_6_FINAL_PRODUCTS/20131121_946_6_calibrated_deepfield.ms', VIS_OUT + '20131121_946_6/', '52.64.236.16'),
        (VIS_ROOT + '20140105_946_6_FINAL_PRODUCTS/20140105_946_6_calibrated_deepfield.ms', VIS_OUT + '20140105_946_6/', '52.64.235.141')
        ]


def memorySpec(uid, **kwargs):
    doSpec = dodict({'oid':str(uid), 'type':'plain', 'storage':'memory'})
    doSpec.update(kwargs)
    return doSpec 


def directorySpec(uid, **kwargs):
    doSpec = dodict({'oid':str(uid), 'type':'container', 'container':'dfms.data_object.DirectoryContainer'})
    doSpec.update(kwargs)
    return doSpec 


def fluxSpec(uid, **kwargs):
    doSpec = dodict({'oid':str(uid), 'type':'app', 'app':'test.integrate.chiles.chilesdo.SourceFlux'})
    doSpec.update(kwargs)
    return doSpec 


def splitSpec(uid, **kwargs):
    doSpec = dodict({'oid':str(uid), 'type':'app', 'app':'test.integrate.chiles.chilesdo.Split'})
    doSpec.update(kwargs)
    return doSpec


def cleanSpec(uid, **kwargs):
    doSpec = dodict({'oid':str(uid), 'type':'app', 'app':'test.integrate.chiles.chilesdo.Clean'})
    doSpec.update(kwargs)
    return doSpec


if __name__ == '__main__':
    try:

        dolist = []

        vis_in_a = []

        flux_out = memorySpec(uuid.uuid1())
        dolist.append(flux_out)
        flux = fluxSpec(uuid.uuid1(), casapy_path = CASAPY)
        dolist.append(flux)

        cl = cleanSpec(uuid.uuid1(),
                        field = 'deepfield',
                        mode = 'frequency',
                        restfreq = '1420.405752MHz',
                        nchan = -1,
                        start = '',
                        width = '',
                        interpolation = 'nearest',
                        gain = 0.1,
                        imsize = [256],
                        cell = ['1.0arcsec'],
                        phasecenter = '10h01m53.9,+02d24m52s',
                        weighting = 'natural',
                        casapy_path = CASAPY)

        dolist.append(cl)

        image_out = directorySpec(uuid.uuid1(), dirname = CUBE_OUT + CUBE_NAME, exists = False)
        dolist.append(image_out)
        cl.addOutput(image_out)
        flux.addInput(image_out)
        flux.addOutput(flux_out)

        for v in VIS:
            vis_in = directorySpec(uuid.uuid1(), dirname = v[0])
            dolist.append(vis_in)
            split_out = directorySpec(uuid.uuid1(), dirname = v[1], exists = False)
            dolist.append(split_out)

            vis_in_a.append(vis_in)

            sp = splitSpec(uuid.uuid1(), 
                        regridms = True, 
                        restfreq = '1420.405752MHz',
                        mode = 'frequency',
                        nchan = 256,
                        outframe = 'lsrk',
                        interpolation = 'linear',
                        start = '1408 MHz',
                        width = '1412 kHz',
                        copy = False,
                        copy_path = None,
                        casapy_path = CASAPY,
                        location = v[2])

            dolist.append(sp)

            sp.addInput(vis_in)
            sp.addOutput(split_out)
            cl.addInput(split_out)


        print json.dumps(dolist)


    except Exception as e:
        import traceback
        traceback.print_exc()
        print str(e)
        sys.exit(-1)

    sys.exit(0)
