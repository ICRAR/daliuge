Installation
============

Requirements
-------------

The |daliuge| framework requires no packages apart from those listed in its
``setup.py``
file, which are automatically retrieved when running it. The spead2 library
(one of the |daliuge|' optional requirements) however requires a number of libraries
installed on the system:

* boost-python
* boost-system
* boost-devel
* gcc >= 4.8

Installing
----------

|daliuge| is based on setuptools, and thus it follows the standard python installation
procedures. For the time being |daliuge| is not yet in PyPI, so you will have to get
the source code first::

 git clone https://github.com/SKA-ScienceDataProcessor/dfms
 cd dfms

If a system-wide installation is required, then the following
commands can be issued::

 python setup.py build
 sudo python setup.py install

If a virtualenv is loaded, then |daliuge| can be installed on it by simply running::

 python setup.py install

14/03/17 If issues occur during the installation try installing a few things by hand first::

  pip install python-daemon
  pip install -U pip
  pip install numpy
  python setup.py install


Docker images
-------------

Docker images can be built using the Dockerfiles under the ``docker`` directory.
Please refer to the ``README`` file in the docker directory for more information.
