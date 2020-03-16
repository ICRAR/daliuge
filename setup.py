#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2020
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

from setuptools import setup

MAJOR = 1
MINOR = 0
PATCH = 0
VERSION = "%d.%d.%d" % (MAJOR, MINOR, PATCH)

install_requires = [
    "daliuge-common==%s" % (VERSION,),
    "daliuge-translator==%s" % (VERSION,),
    "daliuge-runtime==%s" % (VERSION,),
]

setup(
    name="daliuge",
    version=VERSION,
    description=u"Data Activated \uF9CA (flow) Graph Engine - Catch-all proto-package",
    long_description="The SKA-SDK prototype for the Execution Framework component",
    author="ICRAR DIA Group",
    author_email="rtobar@icrar.org",
    url="https://github.com/ICRAR/daliuge",
    license="LGPLv2+",
    install_requires=install_requires,
)
