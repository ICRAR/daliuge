#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2016
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

import subprocess

from setuptools import find_packages
from setuptools import setup
import os


# Version information
# We do like numpy: we have a major/minor/patch hand-written version written
# here. If we find the git commit (either via "git" command execution or in a
# dfms/version.py file) we append it to the VERSION later.
# The RELEASE flag allows us to create development versions properly supported
# by setuptools/pkg_resources or "final" versions.
MAJOR   = 0
MINOR   = 2
PATCH   = 0
RELEASE = False
VERSION = '%d.%d.%d' % (MAJOR, MINOR, PATCH)
VERSION_FILE = 'dfms/version.py'

def get_git_version():
    out = subprocess.check_output(['git', 'rev-parse', 'HEAD'])
    return out.strip().decode('ascii')

def get_version_info():
    git_version = 'Unknown'
    if os.path.exists('.git'):
        git_version = get_git_version()
    full_version = VERSION
    if not RELEASE:
        full_version = '%s.dev0+%s' % (VERSION, git_version[:7])
    return full_version, git_version

def write_version_info():
    tpl = """
# THIS FILE IS GENERATED BY DALIUGE'S SETUP.PY
# DO NOT MODIFY BY HAND
version = '%(version)s'
git_version = '%(git_version)s'
full_version = '%(full_version)s'
is_release = %(is_release)s

if not is_release:
    version = full_version
"""
    full_version, git_version = get_version_info()
    with open(VERSION_FILE, 'w') as f:
        info = tpl % {'version': VERSION,
                      'full_version': full_version,
                      'git_version': git_version,
                      'is_release': RELEASE}
        f.write(info.strip())

write_version_info()

# Every time we overwrite the version file

# HACK - HACK - HACK - HACK
#
# We externally make sure that numpy is installed because spead2 needs it there
# at compile time (and therefore at runtime too).

# An initial solution for this problem was to add numpy to the setup_requires
# argument of spead2's setup invocation. This solves the problem of compiling
# numpy, but requires some extra code to make numpy's include directory
# (which isn't installed in a standard location, and therefore must be queried
# to numpy itself via numpy.include_dir()) available to spead2's setup in order
# to build its C extensions correctly. The main drawback from this solution is
# that numpy's egg location remains inside the spead2's source code tree (in the
# root of the tree when using setuptools < 7, inside the .eggs/ directory when
# using setuptools>=7). This was reported in setuptool issues #209 and #391, but
# still remains an issue. Although one could live with such an installation, it
# doesn't sound ideal at all since the software is not installed when one would
# expect it to be; also permissions-based problems could arise.
#
# For the time being I'm choosing instead to simply install numpy via a pip
# command-line invocation. It will avoid any numpy mingling by spead2, and will
# return quickly if it has been already installed
#
# HACK - HACK - HACK - HACK
try:
    subprocess.check_call(['pip','install','numpy'])
except subprocess.CalledProcessError:
    try:
        subprocess.check_call(['easy_install','numpy'])
    except subprocess.CalledProcessError:
        raise Exception("Couldn't install numpy manually, sorry :(")

# The rest is pretty standard thankfully
setup(
      name='dfms',
      version=get_version_info()[0],
      description=u'Data Activated \uF9CA (flow) Graph Engine - DALiuGE',
      long_description = "The SKA-SDK prototype for the Execution Framework component",
      author='ICRAR DIA Group',
      author_email='dfms_prototype@googlegroups.com',
      url='https://github.com/SKA-ScienceDataProcessor/dfms',
      license="LGPLv2+",
      packages=find_packages(),
      package_data = {
        'dfms.manager' : ['web/*.html', 'web/static/css/*.css', 'web/static/fonts/*', 'web/static/js/*.js', 'web/static/js/d3/*'],
        'dfms.dropmake': ['web/lg_editor.html', 'web/*.css', 'web/*.js', 'web/*.json', 'web/*.map',
                          'web/img/jsoneditor-icons.png', 'web/pg_viewer.html', 'web/matrix_vis.html',
                          'lib/libmetis.*']
      },

      # Keep alpha-sorted PLEASE!
      install_requires=[
            "boto3",
            "bottle",
            "configobj",
            "docker-py <= 1.7",
            "lockfile",
            "luigi<2.0",
            "metis",
            "netifaces>=0.10.5",
            "networkx",
            # paramiko 2.0.0 requires cryptography>=1.1, which in turn
            # requires development packages to be installed on the system
            "paramiko<2.0.0",
            "psutil",
            "pyswarm",
            "python-daemon",
            "pyzmq",
            "scp",
            'six>=1.10',
            "zeroconf",
            "zerorpc >= 0.6" # 0.6 brings python3 support plus other fixes
      ],
      # Keep alpha-sorted PLEASE!

      extras_require={

        # spead is required only for a specific app and its test, which we
        # skip anyway if spead is not found
        'spead': ["spead2==0.4.0"],

        # Pyro4 and RPyC are semi-supported RPC alternatives
        # (while zerorpc is the default)
        'pyro': ['Pyro4>=4.47'], # 4.47 contains a fix we contributed
        'rpyc': ['rpyc'],

        # drive-casa is used by some manual tests under test/integrate
        'drive-casa': ["drive-casa>0.7"],
      },

      dependency_links=[
        # The latest version of netifaces doesn't compile with old Linux kernels
        # We have provided a patch that solves the issue and are waiting for
        # merging into the main repository now
        'https://bitbucket.org/rodrigo_tobar/netifaces/get/tip.zip#egg=netifaces-0.10.5',
        'https://bitbucket.org/kw/metis-python/get/tip.zip#egg=metis',
      ],
      test_suite="test",
      entry_points= {
          'console_scripts':[
              'dfmsNM=dfms.manager.cmdline:dlgNM',
              'dfmsDIM=dfms.manager.cmdline:dlgDIM',
              'dfmsMM=dfms.manager.cmdline:dlgMM',
              'dfmsReplay=dfms.manager.cmdline:dlgReplay',
              'dfmsDaemon=dfms.manager.proc_daemon:run_with_cmdline'
              'dlgNM=dfms.manager.cmdline:dlgNM',
              'dlgDIM=dfms.manager.cmdline:dlgDIM',
              'dlgMM=dfms.manager.cmdline:dlgMM',
              'dlgReplay=dfms.manager.cmdline:dlgReplay',
              'dlgDaemon=dfms.manager.proc_daemon:run_with_cmdline'
          ],
      }
)
