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
import subprocess

from setuptools import find_packages
from setuptools import setup

# Version information
# We do like numpy: we have a major/minor/patch hand-written version written
# here. If we find the git commit (either via "git" command execution or in a
# dlg/version.py file) we append it to the VERSION later.
# The RELEASE flag allows us to create development versions properly supported
# by setuptools/pkg_resources or "final" versions.
MAJOR = 3
MINOR = 0
PATCH = 0
RELEASE = True
VERSION = "%d.%d.%d" % (MAJOR, MINOR, PATCH)
VERSION_FILE = "dlg/translator/version.py"


def get_git_version():
    out = subprocess.check_output(["git", "rev-parse", "HEAD"])
    return out.strip().decode("ascii")


def get_version_info():
    git_version = "Unknown"
    if os.path.exists("../.git"):
        git_version = get_git_version()
    full_version = VERSION
    if not RELEASE:
        full_version = "%s.dev0+%s" % (VERSION, git_version[:7])
    return full_version, git_version


def write_version_info():
    tpl = """
# THIS FILE IS GENERATED BY SETUP.PY
# DO NOT MODIFY BY HAND
version = '%(version)s'
git_version = '%(git_version)s'
full_version = '%(full_version)s'
is_release = %(is_release)s

if not is_release:
    version = full_version
"""
    full_version, git_version = get_version_info()
    with open(VERSION_FILE, "w") as f:
        info = tpl % {
            "version": VERSION,
            "full_version": full_version,
            "git_version": git_version,
            "is_release": RELEASE,
        }
        f.write(info.strip())


# Every time we overwrite the version file
write_version_info()


def package_files(directory):
    paths = []
    for path, directories, filenames in os.walk(directory):
        for filename in filenames:
            paths.append(os.path.join("..", path, filename))
    return paths


src_files = package_files("dlg")

install_requires = [
    "daliuge-common==%s" % (VERSION,),
    "fastapi",
    "jinja2",
    "jsonschema",
    "metis>=0.2a3",
    "netifaces",
    "networkx",
    "numpy",
    "parameterized",
    "psutil",
    "pyswarm",
    "python-multipart",
    # "ruamel.yaml.clib<=0.2.2",
    "uvicorn==0.18",
    "wheel",
    "pyzmq~=25.1.0",
    "pydantic~=1.10.7",
]

setup(
    name="daliuge-translator",
    version=get_version_info()[0],
    description="Data Activated \uF9CA (flow) Graph Engine - Graph Translation",
    long_description="The SKA-SDK prototype for the Execution Framework component",
    author="ICRAR DIA Group",
    author_email="rtobar@icrar.org",
    url="https://github.com/ICRAR/daliuge",
    license="LGPLv2+",
    install_requires=install_requires,
    packages=find_packages(),
    package_data={"dlg": src_files},
    entry_points={
        "dlg.tool_commands": ["translator=dlg.translator.tool_commands"]
    },
    test_suite="test",
)
