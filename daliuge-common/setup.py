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
from pathlib import Path
from setuptools import find_packages
from setuptools import setup

# Version information
# We do like numpy: we have a major/minor/patch hand-written version written
# here. If we find the git commit (either via "git" command execution or in a
# dlg/version.py file) we append it to the VERSION later.
# The RELEASE flag allows us to create development versions properly supported
# by setuptools or "final" versions.


def extract_version():
    """
    Retrived the current version based on the most recent version tag, stored in daliuge-common/VERSION.
    This is then split into the individual major/minor/patch numbers.

    :return: tuple(int, int, int): major, minor, patch
    """
    TAG_VERSION_FILE = "VERSION"
    with (Path(__file__).parent / TAG_VERSION_FILE).open(encoding="utf8") as open_file:
        major, minor, patch = open_file.read().strip("v").split(".")
        print("logging details: ", major, minor, patch)
    return int(major), int(minor), int(patch)


VERSION = extract_version()
VERSION_FILE = "dlg/common/version.py"
RELEASE = True


def do_versioning():
    # Avoid importing, the package doesn't exist as such yet
    with open(os.path.join("dlg", "version_helper.py")) as f:
        code = f.read()
    _globals = {}
    exec(code, _globals) # pylint: disable=exec-used
    return _globals["write_version_info"](VERSION, VERSION_FILE, RELEASE)


install_requires = [
    "GPUtil-fix>=1.4.0",
    "np-merklelib",
    "pyzmq==25.1.1",
    "pydantic>=2.5",
    "boto3",
    "phonenumbers",
    "mailchecker",
    "ftfy",
    "toml",
    "pyyaml",
    "beautifulsoup4",
    "openpyxl",
    "xlrd",
    "xmltodict",
    "python-benedict[all]",
    "pylint",
]

extra_requires = {"paletteGen": ["dlg_paletteGen"]}

setup(
    name="daliuge-common",
    version=do_versioning(),
    description="Data Activated \uf9ca (flow) Graph Engine - Common functionality",
    long_description="The SKA-SDK prototype for the Execution Framework component",
    author="ICRAR DIA Group",
    author_email="dfms_prototype@googlegroups.com",
    url="https://github.com/ICRAR/daliuge",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Topic :: System :: Distributed Computing",
        "Topic :: Scientific/Engineering",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: GNU Lesser General Public License v2 (LGPLv2)",
        "Operating System :: POSIX :: Linux",
        "Operating System :: MacOS",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    license="LGPLv2+",
    packages=find_packages(),
    package_data={"":["VERSION"]},
    test_suite="test",
    entry_points={
        "console_scripts": ["dlg=dlg.common.tool:run"]
    },  # One tool to rule them all
    install_requires=install_requires,
    extra_requires=extra_requires,
)
