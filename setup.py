#!/usr/bin/env python

from setuptools import setup
from setuptools import find_packages

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
import subprocess
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
      version='0.1',
      description='Data Flow Management System',
      author='',
      author_email='',
      url='',
      packages=find_packages(),
      package_data = {
        'dfms.manager' : ['web/*.html', 'web/static/css/*.css', 'web/static/fonts/*', 'web/static/js/*.js', 'web/static/js/d3/*'],
        'dfms.dropmake': ['web/lg_editor.html', 'web/*.css', 'web/*.js', 'web/*.json', 'web/*.map',
        'web/img/jsoneditor-icons.png', 'web/pg_viewer.html', 'web/matrix_vis.html', 'lib/libmetis.*']
      },

      # Keep alpha-sorted PLEASE!
      install_requires=[
            "boto3",
            "bottle",
            "configobj",
            "docker-py <= 1.7",
            "drive-casa==0.7",
            "lockfile",
            "luigi<2.0",
            "metis",
            "netifaces",
            "networkx",
            "paramiko",
            "psutil",
            "Pyro4>=4.38",
            "pyswarm",
            "python-daemon",
            "scp",
            "tornado",
            "zeroconf",
      ],
      # Keep alpha-sorted PLEASE!

      extra_require={
        'spead': ["spead2==0.4.0"]
      },
      dependency_links=[
        'https://bitbucket.org/kw/metis-python/get/tip.zip#egg=metis',
        #'https://github.com/mrocklin/heft/archive/master.zip#egg=heft'
        ],
      test_suite="test",
      entry_points= {
          'console_scripts':[
              'dfmsNM=dfms.manager.cmdline:dfmsNM',
              'dfmsDIM=dfms.manager.cmdline:dfmsDIM',
              'dfmsMM=dfms.manager.cmdline:dfmsMM',
              'dfmsDaemon=dfms.manager.proc_daemon:run_with_cmdline'
          ],
      }
)
