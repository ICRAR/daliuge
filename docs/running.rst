.. _running:

Startup and Shutdown Guide
==========================
The translator and the engine are separate services and can be installed and run independently. (NOTE: The EAGLE visual graph editor is covered in its own `documentation <https://eagle-dlg.readthedocs.io>`_).

Depending on how you are intending to run the system startup and shutdown is slightly different. 

Starting the docker containers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
We are providing convenience scripts to start the docker containers built according to the :ref:`installation`. Depending whether you want to run the development (dev) or the deployment (dep) version of the image there exist different startup options. Starting the translator::

   cd daliuge-translator
   ./run_translator.sh dev|dep

Similarly starting the engine::

   cd daliuge-engine
   ./run_engine.sh dev|dep

The main difference between the development and the deployment version is that the development version is automatically strating a data island manager, while the deployment version is not doing that. Both are starting a Node Manager by default (see below).

Starting and stopping the managers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
|daliuge| is using three different kinds of managers:

#. Node Manager (NM), one per compute node participating in the |daliuge| cluster. The NMs are running all the component wrappers for a single node.
#. Data Island Manager (DIM), which is manageing a (sub-)set of nodes in the cluster. There could be minimum one or maximum as many as NMs Data Island Managers in a deployment. The DIM is also the entity receiving the workflow description from the translator and is then distributing the sections to the NMs.
#. Master Manager (MM), which has the information about all nodes and islands in the deployment. In many deployments the master manager is optional and not really required. If it is necessary, then there is only a single master manager running on the cluster.

The managers are spawned off (as processes) from a daemon process, which  exposes a REST interface allowing the user to start and stop managers. The start and stop commands follow the same URL pattern [1]_::

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

Single node |daliuge|
^^^^^^^^^^^^^^^^^^^^^
As a developer the following two commands will start both the translator and the engine, including a DIM and a NM and will then allow to start deploying workflows::

    cd daliuge-translator ; ./run_translator dev ; cd ..
    cd daliuge-engine ; ./run_engine dev ; cd ..

Starting and stopping using CLI
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
If |daliuge| had been installed in a virtual environment of the host system it is possible to start the daemon from the command line and then use the same curl commands as above to start the required managers::

    dlg daemon &

This will also start a NM by default. The other options are available on the command line::
 
    ❯ dlg daemon -h
    Usage: daemon [options]

    Starts a DALiuGE Daemon process

    Options:
    -h, --help     show this help message and exit
    -m, --master   Start this DALiuGE daemon as the master daemon
    --no-nm        Don't start a NodeDropManager by default
    --no-zeroconf  Don't enable zeroconf on this DALiuGE daemon
    -v, --verbose  Become more verbose. The more flags, the more verbose
    -q, --quiet    Be less verbose. The more flags, the quieter

The CLI allows to control the whole system::

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

Zeroconf
^^^^^^^^
The Master Manager also opens a zeroconf service, which allows the Node Managers to register and deregister and thus the MM is always up to date with the node available in the cluster. NOTE: This mechanism is currently not implemented for the DIMs, i.e. a DIM does not register with the MM automatically. Since it is not possible to guess which NM should belong to which DIM, the NMs also do not register with a DIM. When starting the development version of the image the single NM is automatically assigned to the DIM on localhost.

.. [1] The daemon process is listening on port 9000 by default.

