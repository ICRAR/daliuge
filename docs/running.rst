.. _running:

Running DALiuGE
==========================
As discussed in :ref:`the previous section<installation>`, the translator and the engine are separate services and can be installed and run independently. 

Depending on how you are intending to run the system startup and shutdown is slightly different. The following options for start up and shutdown are given in alignment with the installation options: 

* :ref:`Docker <docker run>`
* :ref:`PyPI<running_with_cli>`
* :ref:`Source<running_with_cli>`


.. _docker run:

|daliuge| in Docker
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The DALiUGE repository has scripts that are used to help run the Docker images. These can
be accessed with or without `git`. Using the shell scripts is not strictly necessary, but the docker command line is quite complex.

.. raw:: html

   <details>
    <summary><a>Using Git</a></summary>

This can be acheived by cloning the repository::

  git clone https://github.com/ICRAR/daliuge
  cd daliuge

.. raw:: html

   </details>
   <br/>

.. raw:: html

   <details>
    <summary><a>Not using Git</a></summary>

If you would prefer not to use git, you can fetch the latest release artifacts::

  wget "$(curl -s "https://api.github.com/repos/icrar/daliuge/releases/latest" \
        | grep "tarball_url" \
        | awk -F '"' '{print $4}')" -O daliuge-latest.tar.gz
      
  mkdir daliuge
  tar -xvf daliuge-latest.tar.gz -C daliuge --strip-components=1 

  cd daliuge

.. raw:: html

   </details>
   <br/>

Once you have access to the script code, you can run the start scripts for the translator::

   cd daliuge-translator
   ./run_translator.sh dev|dep

and the engine::

   cd daliuge-engine
   ./run_engine.sh dev|dep

The main difference between the development and the deployment version is that the development version is automatically strating a data island manager, while the deployment version is not doing that. Both are starting a Node Manager by default (see below).

.. _running_with_cli:

Running DALiuGE using the CLI
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
If |daliuge| had been installed in a virtual environment of the host system it is possible to start the managers from the command line::

    dlg dim -H 0.0.0.0 -N localhost -d

and a node manager::

    dlg nm -H 0.0.0.0 -d 

To stop the managers use::

    dlg dim -s 

and::

    dlg nm -s 

respectively.

To run the translator::

    dlg lgweb -d /tmp/ -t /tmp/


The help for the complete CLI is available by just entering dlg at the prompt::

    ❯ dlg
    Usage: /home/awicenec/.pyenv/versions/dlg/bin/dlg [command] [options]

    Commands are:
        daemon                   Starts a DALiuGE Daemon process
        dim                      Starts a Drop Island Manager
        fill                     Fill a Logical Graph with parameters
        include_dir              Print the directory where C header files can be found
        lgweb                    A Web server for the Logical Graph Editor
        map                      Maps a Physical Graph Template to resources and produces a Physical Graph
        mm                       Starts a Master Manager
        monitor                  A proxy to be used in conjunction with the dlg proxy in restricted environments
        nm                       Starts a Node Manager
        partition                Divides a Physical Graph Template into N logical partitions
        proxy                    A reverse proxy to be used in restricted environments to contact the Drop Managers
        replay                   Starts a Replay Manager
        submit                   Submits a Physical Graph to a Drop Manager
        unroll                   Unrolls a Logical Graph into a Physical Graph Template
        unroll-and-partition     unroll + partition
        version                  Reports the DALiuGE version and exits

    Try $PATH/bin/dlg [command] --help for more details

More details about the usage of the CLI can be found in the :ref:`cli` chapter.


More advanced: Starting and stopping the managers
-------------------------------------------------
|daliuge| is using three different kinds of managers:

#. Node Manager (NM), one per compute node participating in the |daliuge| cluster. The NMs are running all the component wrappers for a single node.
#. Data Island Manager (DIM), which is manageing a (sub-)set of nodes in the cluster. There could be minimum one or maximum as many as NMs Data Island Managers in a deployment. The DIM is also the entity receiving the workflow description from the translator and is then distributing the sections to the NMs.
#. Master Manager (MM), which has the information about all nodes and islands in the deployment. In many deployments the master manager is optional and not really required. If it is necessary, then there is only a single master manager running on the cluster.

Starting a master manager can be done using the dlg command::

    dlg daemon

by default this will also start a NM, but not a DIM. 

The managers are spawned off (as processes) from the daemon process, which  also exposes a REST interface allowing the user to start and stop managers. The start and stop commands follow the URL pattern [1]_::

   curl -X POST http://localhost:9000/managers/<type>/start

and::

    curl -X POST http://localhost:9000/managers/<type>/stop

where <type> is on of [node|dataisland|master]. In case of the DIM (island) it is possible to specify the nodes participating in that specific island. For example::

    curl -d '{"nodes": ["192.168.1.72","192.168.1.11"]}' -H "Content-Type: application/json" -X POST http://localhost:9000/managers/island/start

If a manager is already running or already stopped error messages are returned. In order to see which managers are running on a particular node you can use the GET method::

    curl http://localhost:9000/managers

which returns something like::

    {"master": null, "island": null, "node": 18}

In this example there is just a Node Manager running with process ID 18.

For the independent: Build and run EAGLE
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
It is also possible to start the EAGLE locally in addition as well. This requires you to clone and build the EAGLE repo into a directory separate from the |daliuge| repo::

    git clone https://github.com/ICRAR/EAGLE
    cd EAGLE
    ./build_eagle dep

To start EAGLE::

    ./run_eagle dep

This will start the EAGLE docker image built in the previous step and try to open a browser tab.

(NOTE: The usage of the EAGLE visual graph editor is covered in its own `documentation <https://eagle-dlg.readthedocs.io>`_).


Zeroconf
^^^^^^^^
The Master Manager also opens a zeroconf service, which allows the Node Managers to register and deregister and thus the MM is always up to date with the node available in the cluster. NOTE: This mechanism is currently not implemented for the DIMs, i.e. a DIM does not register with the MM automatically. Since it is not possible to guess which NM should belong to which DIM, the NMs also do not register with a DIM. For convenience and as an exception to this rule, when starting the development version of the daliuge-engine image, the single NM is automatically assigned to the DIM on localhost.

.. [1] The daemon process is listening on port 9000 by default.

