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
import json
import os
import sys
import uuid

from dfms.data_object import dodict


LOCAL_FILES = os.path.dirname(os.path.realpath(__file__))
CASAPY = '/home/ec2-user/casa-release-4.4.0-el6/'
VIS_ROOT = '/home/ec2-user/data/input/'
VIS_OUT = '/home/ec2-user/data/output/'
CUBE_OUT = '/home/ec2-user/data/output/'
CUBE_NAME = 'cube1408~1412'
KEY = '/home/ec2-user/.ssh/aws-sdp-sydney.pem'

# Internal AWS IP addresses. ch05 does the Clean-ing
ch01 = '172.31.4.12'
ch02 = '172.31.9.163'
ch03 = '172.31.11.184'
ch04 = '172.31.11.87'
ch05 = '172.31.0.36'
VIS = [
        (VIS_ROOT + '20131025_951_4_FINAL_PRODUCTS/20131025_951_4_calibrated_deepfield.ms', VIS_OUT + '20131025_951_4/', ch01, 'ec2-user@' + ch05 + ':' + VIS_ROOT),
        (VIS_ROOT + '20131031_951_4_FINAL_PRODUCTS/20131031_951_4_calibrated_deepfield.ms', VIS_OUT + '20131031_951_4/', ch03, 'ec2-user@' + ch05 + ':' + VIS_ROOT),
        (VIS_ROOT + '20131121_946_6_FINAL_PRODUCTS/20131121_946_6_calibrated_deepfield.ms', VIS_OUT + '20131121_946_6/', ch04, 'ec2-user@' + ch05 + ':' + VIS_ROOT),
        (VIS_ROOT + '20140105_946_6_FINAL_PRODUCTS/20140105_946_6_calibrated_deepfield.ms', VIS_OUT + '20140105_946_6/', ch02, 'ec2-user@' + ch05 + ':' + VIS_ROOT)
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

        flux_out = memorySpec(uuid.uuid1(), node = ch05)
        dolist.append(flux_out)
        flux = fluxSpec(uuid.uuid1(), casapy_path = CASAPY, node = ch05)
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
                        casapy_path = CASAPY,
                        node = ch05)

        dolist.append(cl)

        image_out = directorySpec(uuid.uuid1(), dirname = CUBE_OUT + CUBE_NAME, exists = False, node = ch05)
        dolist.append(image_out)
        cl.addOutput(image_out)
        flux.addInput(image_out)
        flux.addOutput(flux_out)

        for i, v in enumerate(VIS):
            vis_in = directorySpec('vis%d' % (i), dirname = v[0], node = v[2])
            dolist.append(vis_in)
            split_out = directorySpec(uuid.uuid1(), dirname = v[1], exists = False, node = v[2])
            dolist.append(split_out)

            sp = splitSpec(uuid.uuid1(), 
                        regridms = True, 
                        restfreq = '1420.405752MHz',
                        mode = 'frequency',
                        nchan = 256,
                        outframe = 'lsrk',
                        interpolation = 'linear',
                        start = '1408MHz',
                        width = '1412kHz',
                        copy = True,
                        copy_path = v[3],
                        copy_key = KEY,
                        casapy_path = CASAPY,
                        node = v[2])

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