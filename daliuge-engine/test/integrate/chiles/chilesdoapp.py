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

from dlg.drop import DirectoryContainer, InMemoryDROP

from .chilesdo import Split, Clean, SourceFlux

LOCAL_FILES = os.path.dirname(os.path.realpath(__file__))
CASAPY = "/home/jenkins/casa-release-4.4.0-el6/"
VIS_ROOT = "/mnt/chiles-imaging/DataFiles/"
VIS_OUT = "/mnt/chiles-output/vis/"
CUBE_OUT = "/mnt/chiles-output/cube/"
CUBE_NAME = "cube1408~1412"

VIS = [
    (
        VIS_ROOT
        + "20131025_951_4_FINAL_PRODUCTS/20131025_951_4_calibrated_deepfield.ms",
        VIS_OUT + "20131025_951_4/",
    ),
    (
        VIS_ROOT
        + "20131031_951_4_FINAL_PRODUCTS/20131031_951_4_calibrated_deepfield.ms",
        VIS_OUT + "20131031_951_4/",
    ),
    (
        VIS_ROOT
        + "20131121_946_6_FINAL_PRODUCTS/20131121_946_6_calibrated_deepfield.ms",
        VIS_OUT + "20131121_946_6/",
    ),
    (
        VIS_ROOT
        + "20140105_946_6_FINAL_PRODUCTS/20140105_946_6_calibrated_deepfield.ms",
        VIS_OUT + "20140105_946_6/",
    ),
]


class Barrier(object):
    def __init__(self, drop):
        self._evt = threading.Event()
        drop.addConsumer(self)

    def dropCompleted(self, drop, state):
        self._evt.set()

    def wait(self, timeout=None):
        return self._evt.wait(timeout)


if __name__ == "__main__":
    try:

        vis_in_a = []

        flux_out = InMemoryDROP(uuid.uuid1(), uuid.uuid1())

        flux = SourceFlux(uuid.uuid1(), uuid.uuid1(), casapy_path=CASAPY)

        cl = Clean(
            uuid.uuid1(),
            uuid.uuid1(),
            field="deepfield",
            mode="frequency",
            restfreq="1420.405752MHz",
            nchan=-1,
            start="",
            width="",
            interpolation="nearest",
            gain=0.1,
            imsize=[256],
            cell=["1.0arcsec"],
            phasecenter="10h01m53.9,+02d24m52s",
            weighting="natural",
            casapy_path=CASAPY,
        )

        image_out = DirectoryContainer(
            uuid.uuid1(), uuid.uuid1(), dirname=CUBE_OUT + CUBE_NAME, check_exists=False
        )
        cl.addOutput(image_out)
        flux.addInput(image_out)
        flux.addOutput(flux_out)

        for v in VIS:
            vis_in = DirectoryContainer(uuid.uuid1(), uuid.uuid1(), dirname=v[0])
            split_out = DirectoryContainer(
                uuid.uuid1(), uuid.uuid1(), dirname=v[1], check_exists=False
            )

            vis_in_a.append(vis_in)

            sp = Split(
                uuid.uuid1(),
                uuid.uuid1(),
                regridms=True,
                restfreq="1420.405752MHz",
                mode="frequency",
                nchan=256,
                outframe="lsrk",
                interpolation="linear",
                start="1408 MHz",
                width="1412 kHz",
                copy=False,
                casapy_path=CASAPY,
            )

            sp.addInput(vis_in)
            sp.addOutput(split_out)

            cl.addInput(split_out)

        # start
        for i in vis_in_a:
            i.setCompleted()

        # wait for flux value to be calculated
        b = Barrier(flux_out)
        res = b.wait(6000)
        if res == False:
            raise Exception("imaging timeout!")

    except Exception as e:
        import traceback

        traceback.print_exc()
        sys.exit(-1)

    sys.exit(0)
