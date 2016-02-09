Installation
============

Requirements
-------------

The DFMS framework requires no packages apart from those listed in its setup.py
file, which are automatically retrieved when running it. The spead2 library
(one of the dfms' requirements) however requires a number of libraries
installed on the system:

* boost-python
* boost-system
* boost-devel
* gcc >= 4.8

Installing
----------

dfms is based on setuptools, and thus follows the standard python installation
procedures. For the time being dfms is not yet in PyPI, so you will have to get
the source code first::

 git clone https://github.com/SKA-ScienceDataProcessor/dfms
 cd dfms

If a system-wide installation is required, then the following
commands can be issued::

 python setup.py build
 sudo python setup.py install

If a virtualenv is loaded, then dfms can be installed on it by simply running::

 python setup.py install

Docker images
-------------

Docker images can be built using the Dockerfiles under the docker directory. Please refer to the README file in the docker directory for more information.
