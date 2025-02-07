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

There are also pre-build images available on dockerHub.

This way we are trying to separate the requirements of the daliuge engine and translator from the rest of the framework, which has a less dynamic development cycle.

The *daliuge-engine* image by default runs a generic daemon, which allows to then start the Master Manager, Node Manager or DataIsland Manager. This approach allows to change the actual manager deployment configuration in a more dynamic way and adjusted to the actual requirements of the environment.

.. note::
  This guide is meant for people who are experimenting with the system. It does not cover specific needs of more complex, distributed operational deployments.

Creating the images
^^^^^^^^^^^^^^^^^^^
Building the three images is easy, just start with the daliuge-common image by running::

  cd daliuge-common && ./build_common.sh dev && cd ..

then build the runtime::

  cd daliuge-engine&& ./build_engine.sh dev && cd ..

and last build the translator::

  cd daliuge-translator && ./build_translator.sh dev && cd ..


Using development Docker images: 
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Using ``make`` we can simplify building the docker images for a development environment::

  make docker-install

This will install daliuge-common, daliuge-engine, and daliuge-translator based on the
local development state of the |daliuge| codebase.

Running the images
^^^^^^^^^^^^^^^^^^
Running the engine and the translator is equally simple::

  cd daliuge-engine && ./run_engine.sh dev && cd ..

and::

  cd daliuge-translator && ./run_translator.sh dev && cd ..


You can use EAGLE on the URL: https://eagle.icrar.org and point your EAGLE configuration for the translator to http://localhost:8084. Congratulations! You now have access to a complete |daliuge| system on your local computer!

More detailed information about running and controlling the |daliuge| system can be found in the :ref:`running`.

Running the development version of the engine and the translator follows the same logic as above::

    make docker-run


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

Installing to Local Machine
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note::
  NOTE: |daliuge| requires python 3.9 or later. If your terminal python does not meet this requirement, consider using a python manager such as `pyenv <https://github.com/pyenv/pyenv>`_ to manage multiple python installations.

Install from GitHub
"""""""""""""""""""

.. note::
  It is always recommended to use |daliuge| from inside a virtual environment to avoid breaking global dependencies of a python installation. If in doubt about creating and activating virtual environments, use the following `venv <https://docs.python.org/3/library/venv.html>`_ commands from your workspace directory::

    python -m venv .venv
    source ./.venv/bin/activate
    # ...

    # run `deactivate` to deactivate when done

The following commands are installing the |daliuge| parts directly from github. In this case you won't have access to the sources, but the system will run. First install the daliuge-common part::

  pip install 'git+https://github.com/ICRAR/daliuge.git#egg&subdirectory=daliuge-common'

then install the daliuge-engine::

  pip install 'git+https://github.com/ICRAR/daliuge.git#egg&subdirectory=daliuge-engine'

and finally, if required also install the daliuge-translator::

  pip install 'git+https://github.com/ICRAR/daliuge.git#egg&subdirectory=daliuge-translator'

Install from sources
""""""""""""""""""""

If you want to have access to the sources you can run the installation in a slightly different way. Again this should be be done from within a virtual environment. First start with cloning the repository::

  git clone https://github.com/ICRAR/daliuge

Perform the following steps to setup and install |daliuge| into a virtual environment::

  cd daliuge
  make local
  source .venv/bin/activate
  make show # Optional, use to confirm virtualenv is active
  make install
  
.. include:: README ray.rst
