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
"""
Similar to chilesdospec, but it generates a series of DockerApp DROPs instead of
directly generating Clean, Flux and Split DROPs.
"""

import os
import sys
import time

from dlg.drop import dropdict
from dlg.manager.client import DataIslandManagerClient
from dlg.common import Categories

LOCAL_FILES = os.path.dirname(os.path.realpath(__file__))
CASAPY = "/opt/casa-release-4.4.0-el6/"
VIS_ROOT = "/opt/data/chiles/input/"
VIS_OUT = "/opt/data/chiles/output/"
CUBE_OUT = "/opt/data/chiles/output/"
CUBE_NAME = "cube1408~1412"

# Internal AWS IP addresses. ch05 does the Clean-ing
VIS = [
    (VIS_ROOT + "20131025_951_4_FINAL_PRODUCTS", VIS_OUT + "20131025_951_4/"),
    (VIS_ROOT + "20131031_951_4_FINAL_PRODUCTS", VIS_OUT + "20131031_951_4/"),
    (VIS_ROOT + "20131121_946_6_FINAL_PRODUCTS", VIS_OUT + "20131121_946_6/"),
    (VIS_ROOT + "20140105_946_6_FINAL_PRODUCTS", VIS_OUT + "20140105_946_6/"),
]


def fileDropSpec(uid, **kwargs):
    dropSpec = dropdict(
        {
            "oid": str(uid),
            "type": "plain",
            "storage": Categories.FILE,
            "node": "localhost",
            "island": "localhost",
        }
    )
    dropSpec.update(kwargs)
    return dropSpec


def directorySpec(uid, **kwargs):
    dropSpec = dropdict(
        {
            "oid": str(uid),
            "type": "container",
            "container": "dlg.drop.DirectoryContainer",
            "node": "localhost",
            "island": "localhost",
        }
    )
    dropSpec.update(kwargs)
    return dropSpec


def casapyDockerAppSpec(uid, script):
    cmd = (
        "cd; "
        + os.path.join(CASAPY, "casapy")
        + ' --colors=NoColor --nologger --nogui -c "%s"' % (script)
    )
    return dropdict(
        {
            "oid": str(uid),
            "type": "app",
            "app": "dlg.apps.dockerapp.DockerApp",
            "image": "dfms/casapy_centos7_dfms:0.1",
            "command": cmd,
            "user": "dfms",
            "node": "localhost",
            "island": "localhost",
        }
    )


def fluxSpec(uid, **kwargs):
    script = "ia.open('%i0.image');"
    script += "flux = ia.pixelvalue([128,128,0,179])['value']['value'];"
    script += "f = open('%o0','w'); f.write(str(flux)); f.close()"
    return casapyDockerAppSpec(uid, script)


def splitSpec(uid, **kwargs):
    transform_args = kwargs.copy()
    transform_args.update({"vis": "%i0", "outputvis": "%o0"})
    script = "import shutil; shutil.rmtree('%o0', True); "
    script += "mstransform(**{})".format(repr(transform_args))
    return casapyDockerAppSpec(uid, script)


def cleanSpec(uid, **kwargs):
    clean_args = kwargs.copy()
    clean_args.update(
        {
            "niter": 0,
            "mask": "",
            "modelimage": "",
            "imagename": "%o0",
            "vis": ["%i0", "%i1", "%i2", "%i3"],
            "threshold": "0Jy",
        }
    )
    script = "import shutil; shutil.rmtree('%o0', True); "
    script += "clean(**{})".format(repr(clean_args))
    return casapyDockerAppSpec(uid, script)


if __name__ == "__main__":
    try:

        sessionId = "Chiles-Docker-%s" % (time.time(),)
        droplist = []

        flux_out = fileDropSpec("Flux", dirname=VIS_OUT)
        droplist.append(flux_out)
        flux = fluxSpec("FluxExtractor")
        droplist.append(flux)

        cl = cleanSpec(
            "Cleaning",
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
            usescratch=False,
        )
        droplist.append(cl)

        image_out = directorySpec(
            "CleanedImage", dirname=CUBE_OUT + CUBE_NAME, check_exists=False
        )
        droplist.append(image_out)
        cl.addOutput(image_out)
        flux.addInput(image_out)
        flux.addOutput(flux_out)

        for i, v in enumerate(VIS):
            vis_in = directorySpec("vis%d" % (i), dirname=v[0], check_exists=False)
            split_out = directorySpec(
                "SplitOutput_%d" % (i), dirname=v[1], check_exists=False
            )
            sp = splitSpec(
                "Splitting_%d" % (i),
                regridms=True,
                restfreq="1420.405752MHz",
                mode="frequency",
                nchan=256,
                outframe="lsrk",
                interpolation="linear",
                start="1408MHz",
                width="1412kHz",
                veltype="radio",
                spw="",
                combinespws=True,
                nspw=1,
                createmms=False,
                datacolumn="data",
            )
            sp.addInput(vis_in)
            sp.addOutput(split_out)
            cl.addInput(split_out)

            droplist.append(vis_in)
            droplist.append(split_out)
            droplist.append(sp)

        c = DataIslandManagerClient()
        c.create_session(sessionId)
        c.append_graph(sessionId, droplist)
        c.deploy_session(sessionId)

    except Exception as e:
        import traceback

        traceback.print_exc()
        sys.exit(-1)

    sys.exit(0)
