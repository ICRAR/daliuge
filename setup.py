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
      install_requires=["Pyro", "Pyro4", "luigi", "psutil"],
      test_suite="test"
)
