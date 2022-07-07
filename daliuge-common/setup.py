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
import os

from setuptools import find_packages
from setuptools import setup

# Version information
# We do like numpy: we have a major/minor/patch hand-written version written
# here. If we find the git commit (either via "git" command execution or in a
# dlg/version.py file) we append it to the VERSION later.
# The RELEASE flag allows us to create development versions properly supported
# by setuptools/pkg_resources or "final" versions.
MAJOR = 2
MINOR = 3
PATCH = 1
VERSION = (MAJOR, MINOR, PATCH)
VERSION_FILE = "dlg/common/version.py"
RELEASE = True


def do_versioning():
    # Avoid importing, the package doesn't exist as such yet
    with open(os.path.join("dlg", "version_helper.py")) as f:
        code = f.read()
    _globals = {}
    exec(code, _globals)
    return _globals["write_version_info"](VERSION, VERSION_FILE, RELEASE)


install_requires = ["gputil>=1.4.0", "merklelib>=1.0"]

setup(
    name="daliuge-common",
    version=do_versioning(),
    description=u"Data Activated \uF9CA (flow) Graph Engine - Common functionality",
    long_description="The SKA-SDK prototype for the Execution Framework component",
    author="ICRAR DIA Group",
    author_email="dfms_prototype@googlegroups.com",
    url="https://github.com/ICRAR/daliuge",
    license="LGPLv2+",
    packages=find_packages(),
    test_suite="test",
    entry_points={
        "console_scripts": ["dlg=dlg.common.tool:run"]
    },  # One tool to rule them all
    install_requires=install_requires,
)
