#!/usr/bin/env python

from setuptools import setup
from setuptools import find_packages

setup(
      name='dfms',
      version='0.1',
      description='Data Flow Management System',
      author='',
      author_email='',
      url='',
      packages=find_packages(),
      package_data = {
        'dfms.dom' : ['web/*.html', 'web/static/css/*.css', 'web/static/js/*.js', 'web/static/js/d3/*']
      },
      install_requires=["Pyro4", "luigi", "psutil", "paramiko", "bottle", "tornado", "drive-casa"],
      test_suite="test",
      entry_points= {
          'console_scripts':[
              'dfmsDOM=dfms.dom.cmdline:main'
          ],
      }
)
