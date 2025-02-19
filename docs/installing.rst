.. _installation:

.. |br| raw:: html

     <br>

Installation Guide
==================
.. warning:: 
  |daliuge| is under heavy development and we are not regularily updating the version on PyPi and DockerHub right now. This guide assumes you have a copy of the latest release which you can get from here::

   git clone https://github.com/ICRAR/daliuge
   cd daliuge

Installation Options
--------------------

.. list-table:: 
   :widths: 15 15 15
   :header-rows: 1

   * - I am:
     - This involves: 
     - Best option:
   * - Developing and testing graphs locally |br| (`Best option for first time users.`)
     -  * Using EAGLE in the browser 
        * Translate workflows through the browser
        * Deploy workflows locally
     - :ref:`docker install`
   * - Developing and deploying graphs remotely
     - * Using the ``dlg`` CLI
       * Developing EAGLE graphs in the browser
       * Translating through the browser or CLI  
       * Deploying through the browser or CLI
     - :ref:`direct install`
   *  - Adminstrator of a HPC cluster interested in DALiuGE
      - * Using the ``dlg`` CLI
        * Developing EAGLE graphs in the browser
        * Translating through the browser or CLI  
        * Deploying through the browser or CLI
      - :ref:`direct install`
   *  - Contributing to DALiuGE software
      - * Writing and testing the software
        * Wanting the latest features
        * Iterating on local source code
      - :ref:`direct install`

.. _docker install:

Docker images
-------------

.. note::
  This guide is meant for people who are experimenting with the system. It does not cover specific needs of more complex, distributed operational deployments.


The recommended and easiest way to get started is to use the docker container installation procedures provided to build and run the daliuge-engine and the daliuge-translator. We currently build the system in three images:

#. *icrar/daliuge-common* contains all the basic |daliuge| libraries and dependencies.
#. *icrar/daliuge-engine* is built on top of the :base image and includes the installation of the DALiuGE execution engine.
#. *icrar/daliuge-translator* is also built on top of the :base image and includes the installation of the DALiuGE translator.

This way we are trying to separate the requirements of the daliuge engine and translator from the rest of the framework, which has a less dynamic development cycle.

The *daliuge-engine* image by default runs a generic daemon, which allows to then start the Master Manager, Node Manager or DataIsland Manager. This approach allows to change the actual manager deployment configuration in a more dynamic way and adjusted to the actual requirements of the environment.

Building the images
^^^^^^^^^^^^^^^^^^^^^^^
Using ``make`` we can simplify building the docker images for a development environment::

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

You can use EAGLE on the URL: https://eagle.icrar.org and point your EAGLE configuration for the translator to http://localhost:8084. Congratulations! You now have access to a complete |daliuge| system on your local computer!

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



.. _direct install:

Direct Installation
-------------------

Installing from sources
^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. note:: 

  |daliuge| requires python 3.9 or later. It is always recommended to install |daliuge| inside it's own Python virtual environment. Make sure that you have on created and enabled. More often than not pip requries an update, else it will always issue a warning:

    pip install --upgrade pip


Perform the following steps to setup and install |daliuge| into the specific virtual environment. 

  cd daliuge
  # source virtual env
  make show # Optional, use to confirm virtualenv is active
  make install
  
.. raw:: html

   <details>
    <summary><a>Installing from source manually</a></summary>

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

