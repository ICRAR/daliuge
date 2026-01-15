.. |br| raw:: html

     <br>

.. _installation:

Installation 
============
.. note::
  This guide is meant for people who are developing and testing workflows using EAGLE, or who are experimenting with the system. It does not cover specific needs of more complex, distributed operational deployments.

DALiuGE applications
---------------------
For development and testing of workflows for DALiuGE, the following runtime environments are necessary: 

* ``daliuge-translator``: This takes the workflow from EAGLE and 'unrolls' it into the workflow DAG that will be executed (see :ref:`graphs.translation`). 
* ``daliuge-engine``: This manages workflow execution across many nodes. There is a hierarchy of applications within the engine that must be run to manage the execution at different level of granularity (i.e. at the node level, the data-island level, and at the master level; see :ref:`drop.managers`).


The intricacies of the different environments and their applications will be covered in more detail in later sections of the documentation. For the purpose of starting out, it is enough to know that to start developing and testing workflows on DALiuGE, the following is necessary: 

* EAGLE workflow has a graph that is ready to translate;
* ``daliuge-translator`` is running, waiting to receive a workflow from EAGLE; and
* ``daliuge-engine`` applications (A Data Island Manager and a Node Manager) are running.

The following instructions explain how to set up your environment to achieve this. 

Installation Options
--------------------

.. list-table:: 
   :header-rows: 1

   * - I am:
     - This involves: 
     - Best option:
   * - Developing and testing graphs locally |br| (`Best option for first time users.`)
     -  * Using EAGLE in the browser 
        * Translate workflows through the browser
        * Deploy workflows locally
     - :ref:`docker install`
   * - Developing and deploying graphs that use |br| an external library (e.g. `numpy`)
     - * Developing EAGLE graphs in the browser
       * Translating through the browser or CLI  
       * Using the ``dlg`` CLI
       * Deploying through the browser or CLI
     - :ref:`pip install`
   *  - Adminstrator of a HPC cluster interested in DALiuGE
      - * Using the ``dlg`` CLI
        * Developing EAGLE graphs in the browser
        * Translating through the browser or CLI  
        * Deploying through the browser or CLI
      - :ref:`pip install`
   *  - Contributing to DALiuGE software
      - * Writing and testing the software
        * Wanting the latest features
        * Iterating on local source code
      - :ref:`direct install`

.. _docker install:

Docker Installation
--------------------
Recommended 
^^^^^^^^^^^^
.. note::

    This assumes docker is installed locally on your system before running these commands. Please review https://docs.docker.com/get-started/get-docker/ if Docker is not installed on your system.

The recommended and easiest way to get started is to use the docker containers for the daliuge-engine and daliuge-translator. To download the images for the purpose of developing workflows with the EAGLE editor, the following is all that is necessary:: 

  docker pull icrar/daliuge-engine.slim:latest
  docker pull icrar/daliuge-translator.slim:latest

These are minified docker images that contain only the necessary material for running the applications. To start running the applications, head straight to :ref:`docker run`.

Alternative: Building the images locally
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
This section assumes you have experience using Git and Docker, and want to build from the (potentially unstable) DALiuGE source code. 

First, clone the |daliuge| github repository::

  git clone https://github.com/ICRAR/daliuge
  cd daliuge

Then using the ``make`` utility we build the docker images for a development environment::

  make docker-install

This will install daliuge-common, daliuge-engine, and daliuge-translator based on the
local development state of the |daliuge| codebase.

.. raw:: html

   <details>
    <summary><a>Building images manually</a></summary>

Building the three manuall is easy, just start with the daliuge-common image by running::

  cd daliuge-common && ./build_common.sh dev && cd ..

then build the runtime::

  cd daliuge-engine&& ./build_engine.sh dev && cd ..

and last build the translator::

  cd daliuge-translator && ./build_translator.sh dev && cd ..

.. raw:: html

   </details>
   <br/>

Running the images
^^^^^^^^^^^^^^^^^^

Running the development version of the engine and the translator follows the same logic as above::

    make docker-run

You can use EAGLE on the URL: https://eagle.icrar.org and point your EAGLE configuration for the translator to http://dlg-tm.localhost. Congratulations! You now have access to a complete |daliuge| system on your local computer!

More detailed information about running and controlling the |daliuge| system can be found in the :ref:`running`.

.. raw:: html

   <details>
    <summary><a>Running images manually</a></summary>

Running the engine and the translator is equally simple::

  cd daliuge-engine && ./run_engine.sh dev && cd ..

and::

  cd daliuge-translator && ./run_translator.sh dev && cd ..

.. raw:: html

   </details>
   <br/>

.. _pip install:

PyPI Installation
-----------------

.. note::

  |daliuge| requires python 3.9 or later. It is always recommended to install |daliuge| inside it's own Python virtual environment. Make sure that you have on created and enabled. More often than not pip requries an update, else it will always issue a warning::

    pip install --upgrade pip

Inside your virtual environment, the latest version of the |daliuge| can be installed as follows::

 pip install daliuge-common && pip install daliuge-engine && pip install daliuge-translator


|daliuge| may now be run using the :ref:`CLI interface<running_with_cli>`.


Updating PyPI installation
^^^^^^^^^^^^^^^^^^^^^^^^^^

An existing installation can be updated using::

 pip install -U daliuge-common && pip install -U daliuge-engine && pip install -U daliuge-translato

.. _direct install:

Direct Installation
-------------------

.. note:: 

  |daliuge| requires python 3.9 or later.

  It is always recommended to install |daliuge| inside it's own Python virtual environment. Make sure that you have on created and enabled. More often than not pip requries an update, else it will always issue a warning::

    pip install --upgrade pip


First, clone the |daliuge| github repository::

  git clone https://github.com/ICRAR/daliuge
  cd daliuge

Perform the following steps to setup and install |daliuge| into the specific virtual environment::

  cd daliuge
  # source your virtual env
  make show # Optional, use to confirm virtualenv is active
  make install
  
.. raw:: html

   <details>
    <summary><a>Installing from source manually</a></summary>

After cloning and entering the `daliuge` folder::

  cd daliuge-common
  pip install -e .
  cd daliuge-engine
  pip install -e .
  cd daliuge-translator
  pip install -e .

.. raw:: html

   </details>
   <br/>

.. raw:: html

   <details>
    <summary><a>Alternative installation options</a></summary>

**Installing from GitHub**
 
The following commands are installing the |daliuge| parts directly from github. In this case you won't have access to the sources, but the system will run. First install the daliuge-common part::

  pip install 'git+https://github.com/ICRAR/daliuge.git#egg&subdirectory=daliuge-common'

then install the daliuge-engine::

  pip install 'git+https://github.com/ICRAR/daliuge.git#egg&subdirectory=daliuge-engine'

and finally, if required also install the daliuge-translator::

  pip install 'git+https://github.com/ICRAR/daliuge.git#egg&subdirectory=daliuge-translator'

.. raw:: html

   </details>
   <br/>

