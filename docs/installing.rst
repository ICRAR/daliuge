Installation
============

Docker images
-------------

Using Docker is the easiest way to get the DALiuGE system up and running. Docker images for the DALiuGE execution engine and the DALiuGE translator can be built using
the Docker-files under the ``docker`` directories. Please refer to the ``README`` files in the 
daliuge_runtime and the daliuge_translator directories for more information.


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
procedures.
The preferred way of installing the latest stable version of |daliuge|
is by using ``pip``::

 pip install --process-dependency-links daliuge

If you want to build from the latest sources you can get them from here::

 git clone https://github.com/ICRAR/daliuge
 cd daliuge

If a system-wide installation is required, then the following
commands can be issued::

 sudo pip --process-dependency-links install .

If ``pip`` is not available, you can also use a different approach with::

 python setup.py build
 sudo python setup.py install

If a virtualenv is loaded, then |daliuge| can be installed on it by simply running::

 pip install --process-dependency-links .

Again, if ``pip`` is not available, you can use the simpler form::

 python setup.py install

There is a known issue in some systems
when installing the ``python-daemon`` dependency,
which **needs** to be installed via ``pip``.

