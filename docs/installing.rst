.. _installation:

Installation Guide
==================
NOTE: |daliuge| is under heavy development and we are not regularily updating the version on PyPi and DockerHub right now. The currently best way to get going is to install and build from the latest sources which you can get from here::

 git clone https://github.com/ICRAR/daliuge
 cd daliuge


Docker images
-------------

The recommended and easiest way to get started is to use the docker container installation procedures provided to build and run the daliuge-engine and the daliuge-translator. We currently build the system in three images:

#. *icrar/daliuge-common* contains all the basic |daliuge| libraries and dependencies.
#. *icrar/daliuge-engine* is built on top of the :base image and includes the installation of the DALiuGE execution engine.
#. *icrar/daliuge-translator* is also built on top of the :base image and includes the installation of the DALiuGE translator.


This way we are trying to separate the requirements of the daliuge engine and translator from the rest of the framework, which has a less dynamic development cycle.

The *daliuge-engine* image by default runs a generic daemon, which allows to then start the Master Manager, Node Manager or DataIsland Manager. This approach allows to change the actual manager deployment configuration in a more dynamic way and adjusted to the actual requirements of the environment.

**NOTE: This guide is meant for people who are experimenting with the system. It does not cover specific needs of more complex, distributed operational deployments.**

Creating the images
^^^^^^^^^^^^^^^^^^^
Building the three images is easy, just start with the daliuge-common image by running::

  cd daliuge-common && ./build_common.sh dev && cd ..

then build the runtime::

  cd daliuge-engine&& ./build_engine.sh dev && cd ..

and last build the translator::

  cd daliuge-translator && ./build_translator.sh dev && cd ..


Running the images
^^^^^^^^^^^^^^^^^^
Running the engine and the translator is equally simple::

  cd daliuge-engine && ./run_engine.sh dev && cd ..

and::

  cd daliuge-translator && ./run_translator.sh dev && cd ..

You can use EAGLE on the URL: https://eagle.icrar.org and point your EAGLE configuration for the translator to http://localhost:8084. Congratulations! You now have access to a complete |daliuge| system on your local computer! 

More detailed information about running and controlling the |daliuge| system can be found in the :ref:`running`.

Direct Installation
-------------------
**NOTE: For most use cases the docker installation described above is recommended.** 

Requirements
^^^^^^^^^^^^
The |daliuge| framework requires no packages apart from those listed in its

``setup.py``

file, which are automatically retrieved when running it. The spead2 library (one of the |daliuge| optional requirements) however requires a number of libraries installed on the system:

#. boost-python
#. boost-system
#. boost-devel
#. gcc >= 4.8

Installing into host Python
^^^^^^^^^^^^^^^^^^^^^^^^^^^
NOTE: |daliuge| requires python 3.7 or later. It is always recommended to install |daliuge| inside a python virtual environment. Make sure that you have on created and enabled. More often than not pip requries an update, else it will always issue a warning. Thus first run::

  pip install --upgrade pip

Like for the docker installation the local installation also follows the same pattern. First install the daliuge-common part::

 cd daliuge-common && pip install -e .

then install the daliuge-engine::

 cd ../daliuge-engine && pip install -e .

and finally, if required also install the daliuge-translator::

 cd ../daliuge-translator && pip install -e .

.. include:: README ray.rst
