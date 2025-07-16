.. _cli_engine:

Engine Commands
###############

The basic commands for running the DALiuGE engine were briefly covered in :ref:`running` . This section expands on the commands to facilitate more sophisticated deployements and provides a comprehensive review of the CLI to the engine.  


Running Managers
^^^^^^^^^^^^^^^^
|daliuge| is using three different kinds of managers:

#. Node Manager (NM), one per compute node participating in the |daliuge| cluster. The NMs are running all the component wrappers for a single node.
#. Data Island Manager (DIM), which is manageing a (sub-)set of nodes in the cluster. There could be minimum one or maximum as many as NMs Data Island Managers in a deployment. The DIM is also the entity receiving the workflow description from the translator and is then distributing the sections to the NMs.
#. Master Manager (MM), which has the information about all nodes and islands in the deployment. In many deployments the master manager is optional and not really required. If it is necessary, then there is only a single master manager running on the cluster.


Standard Interface 
------------------

Node Manager (NM)
******************
The most basic command a Node Manager, with some verbose logging for demonstration, is::

  > dlg nm -v

  2025-05-15 04:47:35,591 [WARNI] [...] root#setupLogging:318 Starting with level: INFO...
  2025-05-15 04:47:35,591 [ INFO] [...] dlg.dlg.manager.cmdline#launchServer:76 DALiuGE version 5.3.0 running at /home/00087932/dlg/workspace
  2025-05-15 04:47:35,591 [ INFO] [...] dlg.dlg.manager.cmdline#launchServer:77 Creating NodeManager
  2025-05-15 04:47:35,591 [ INFO] [...] dlg.dlg.manager.node_manager#__init__:278 Adding /home/00087932/dlg/code to the system path
  2025-05-15 04:47:35,591 [ INFO] [...] dlg.dlg.manager.node_manager#__init__:284 Adding /home/00087932/dlg/code/lib/python3.10/site-packages to the system path
  2025-05-15 04:47:35,593 [ INFO] [...] dlg.dlg.rpc#run_zrpcserver:266 Listening for RPC requests via ZeroRPC on tcp://127.0.0.1:6666
  2025-05-15 04:47:35,594 [ INFO] [...] dlg.dlg.manager.node_manager#_publish_events:570 Publishing events via ZeroMQ on tcp://127.0.0.1:5555
  2025-05-15 04:47:35,596 [ INFO] [...] dlg.dlg.manager.node_manager#start:134 Initializing thread pool with 8 workers
  2025-05-15 04:47:35,596 [ INFO] [...] dlg.dlg.lifecycle.dlm#__init__:149 Starting DropChecker running every 10.000 [s]
  2025-05-15 04:47:35,597 [ INFO] [...] dlg.dlg.lifecycle.dlm#__init__:149 Starting DropGarbageCollector running every 30.000 [s]
  2025-05-15 04:47:35,603 [ INFO] [...] dlg.dlg.restserver#start:45 Starting REST server on localhost:8000
  

From this output, we see a few of the defaults that the NM will start with on a given machine

  - The default host that the manager is available at is ``localhost``; the default port is 8000
  - The Manager also communicates via two other ports: the MQ port (default is 5555), and the RPC port (default is 6666)

It is possible to change the host to an alternative:: 

  > dlg nm -H 172.19.0.1 

Use a non-default server port:: 

  > dlg nm -H 172.19.0.1 -P 8999

And use custom MQ and RPC ports:: 

  > dlg nm -H 172.19.0.1 -P 8999 --event_port 5432 --rpc_port 6789
  
Typically the defaults are fine and there is no need to provide custom ports; however, if running multiple NMs on a single machine, it is necessary to make sure the ``event_port`` and ``rpc_port`` are different for each NM.

A full reference of commands that customise the behaviour of the NMs is available :ref:`below <complete_nm>`.

Data Island Manager 
*******************

The base command for the DIMs is very similar to that of the NMs:: 
   
  > dlg dim -v 
  
This will start the Data Island Manager with no Node Managers registered to it. 

It is possible to add more NMs after starting the NM (see :ref:`below <rest_interface>`), but for convenience sake, if the NMs are known ahead of time it is easier to add them to the DIM at startup::

  dlg dim -N localhost

Looking under the hood, this is doing a little more than the NM; if we re-create the full command with defaults:: 

  > dlg dim -N localhost:8000:5555:6666

It is important to note the 'protocol' being used here, which takes the form:: 

  <host><server_port><event_port><rpc_port>

Again, this is unnecessary if running multiple servers on different compute nodes::

  dlg dim -N localhost,172.19.0.1 

With nodes passed to the -N argument as a comma separated list. This will connect to hosts on both the local machine, and the machine accessible on 172.19.0.1. Both servers will be accessed on port 8000 (default) and the other defaults. The only time the default ports would need to be modified is if there was a port conflict on one of the compute nodes. 

It is also possible to modify the ports that the DIM server is running on using the ``-P/--port`` command. Further information on complete options is available under :ref:`dim commands<complete_dim>`.

Master Manager 
*************** 

To run a Master Manager and register nodes to it, simply run::

  > dlg mm -N localhost

 Note that for the MM, the nodes that need to be registered are DIMs, not NMs. 

.. _rest_interface:

The REST interface
------------------

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


Zeroconf
********
The Master Manager also opens a zeroconf service, which allows the Node Managers to register and deregister and thus the MM is always up to date with the node available in the cluster. NOTE: This mechanism is currently not implemented for the DIMs, i.e. a DIM does not register with the MM automatically. Since it is not possible to guess which NM should belong to which DIM, the NMs also do not register with a DIM. For convenience and as an exception to this rule, when starting the development version of the daliuge-engine image, the single NM is automatically assigned to the DIM on localhost.

.. [1] The daemon process is listening on port 9000 by default.


..  TODO This section is commented out as it is lower priority. We will track updates in JIRA
..  Proxy and monitor tools
..  ^^^^^^^^^^^^^^^^^^^^^^^
..  dlg Proxy 
..  ---------
..  dlg Monitor
..  ------------

Reference
^^^^^^^^^

Command: dlg daemon
-------------------

Help output::

   Usage: daemon [options]
   
   Starts a DALiuGE Daemon process
   
   Options:
     -h, --help     show this help message and exit
     -m, --master   Start this DALiuGE daemon as the master daemon
     --no-nm        Don't start a NodeDropManager by default
     --no-zeroconf  Don't enable zeroconf on this DALiuGE daemon
     -v, --verbose  Become more verbose. The more flags, the more verbose
     -q, --quiet    Be less verbose. The more flags, the quieter
   


     
Command: dlg dim
-------------------

Help output::

   Usage: dim [options]
   
   Starts a Drop Island Manager
   
   Options:
     -h, --help            show this help message and exit
     -H HOST, --host=HOST  The host to bind this instance on
     -P PORT, --port=PORT  The port to bind this instance on
     -m MAXREQSIZE, --max-request-size=MAXREQSIZE
                           The maximum allowed HTTP request size, in MB
     -d, --daemon          Run as daemon
     -s, --stop            Stop an instance running as daemon
     --status              Checks if there is daemon process actively running
     -T TIMEOUT, --timeout=TIMEOUT
                           Timeout used when checking for the daemon process
     -v, --verbose         Become more verbose. The more flags, the more verbose
     -q, --quiet           Be less verbose. The more flags, the quieter
     -l LOGDIR, --log-dir=LOGDIR
                           The directory where the logging files will be stored
     -N NODES, --nodes=NODES
                           Comma-separated list of node names managed by this DIM
     -k PKEYPATH, --ssh-pkey-path=PKEYPATH
                           Path to the private SSH key to use when connecting to
                           the nodes
     --dmCheckTimeout=DMCHECKTIMEOUT
                           Maximum timeout used when automatically checking for
                           DM presence
   
.. _complete_dim:

Command: dlg mm
-------------------
Help output::

   Usage: mm [options]
   
   Starts a Master Manager
   
   Options:
     -h, --help            show this help message and exit
     -H HOST, --host=HOST  The host to bind this instance on
     -P PORT, --port=PORT  The port to bind this instance on
     -m MAXREQSIZE, --max-request-size=MAXREQSIZE
                           The maximum allowed HTTP request size, in MB
     -d, --daemon          Run as daemon
     -s, --stop            Stop an instance running as daemon
     --status              Checks if there is daemon process actively running
     -T TIMEOUT, --timeout=TIMEOUT
                           Timeout used when checking for the daemon process
     -v, --verbose         Become more verbose. The more flags, the more verbose
     -q, --quiet           Be less verbose. The more flags, the quieter
     -l LOGDIR, --log-dir=LOGDIR
                           The directory where the logging files will be stored
     -N NODES, --nodes=NODES
                           Comma-separated list of node names managed by this MM
     -k PKEYPATH, --ssh-pkey-path=PKEYPATH
                           Path to the private SSH key to use when connecting to
                           the nodes
     --dmCheckTimeout=DMCHECKTIMEOUT
                           Maximum timeout used when automatically checking for
                           DM presence
   

Command: dlg monitor
--------------------
Help output::

   Usage: monitor [options]
   
   A proxy to be used in conjunction with the dlg proxy in restricted
   environments
   
   Options:
     -h, --help            show this help message and exit
     -H HOST, --host=HOST  The network interface the monitor is bind
     -o MONITOR_PORT, --monitor_port=MONITOR_PORT
                           The monitor port exposed to the DALiuGE proxy
     -c CLIENT_PORT, --client_port=CLIENT_PORT
                           The proxy port exposed to the client
     -p PUBLICATION_PORT, --publication_port=PUBLICATION_PORT
                           Port used to publish the list of proxies for clients
                           to look at
     -d, --debug           Whether to log debug info
   

.. _complete_nm:

Command: dlg nm
---------------
Help output::

   Usage: nm [options]
   
   Starts a Node Manager
   
   Options:
     -h, --help            show this help message and exit
     -H HOST, --host=HOST  The host to bind this instance on
     -P PORT, --port=PORT  The port to bind this instance on
     -m MAXREQSIZE, --max-request-size=MAXREQSIZE
                           The maximum allowed HTTP request size, in MB
     -d, --daemon          Run as daemon
     -s, --stop            Stop an instance running as daemon
     --status              Checks if there is daemon process actively running
     -T TIMEOUT, --timeout=TIMEOUT
                           Timeout used when checking for the daemon process
     -v, --verbose         Become more verbose. The more flags, the more verbose
     -q, --quiet           Be less verbose. The more flags, the quieter
     -l LOGDIR, --log-dir=LOGDIR
                           The directory where the logging files will be stored
     -I, --no-log-ids      Do not add associated session IDs and Drop UIDs to log
                           statements
     --no-dlm              Don't start the Data Lifecycle Manager on this
                           NodeManager
     --dlg-path=DLGPATH    Path where more DALiuGE-related libraries can be found
     --error-listener=ERRORLISTENER
                           The error listener class to be used
     --event-listeners=EVENT_LISTENERS
                           A colon-separated list of event listener classes to be
                           used
     -t MAX_THREADS, --max-threads=MAX_THREADS
                           Max thread pool size used for executing drops. 0
                           (default) means no pool.
   

Command: dlg proxy
-------------------
Help output::

   Usage: proxy [options]
   
   A reverse proxy to be used in restricted environments to contact the Drop
   Managers
   
   Options:
     -h, --help            show this help message and exit
     -d DLG_HOST, --dlg_host=DLG_HOST
                           DALiuGE Node Manager host IP (required)
     -m MONITOR_HOST, --monitor_host=MONITOR_HOST
                           Monitor host IP (required)
     -l LOG_DIR, --log_dir=LOG_DIR
                           Log directory (optional)
     -f DLG_PORT, --dlg_port=DLG_PORT
                           The port the DALiuGE Node Manager is running on
     -o MONITOR_PORT, --monitor_port=MONITOR_PORT
                           The port the DALiuGE monitor is running on
     -b, --debug           Whether to log debug info
     -i ID, --id=ID        The ID of this proxy for on the monitor side
                           (required)
   

Command: dlg replay
-------------------
Help output::

   Usage: replay [options]
   
   Starts a Replay Manager
   
   Options:
     -h, --help            show this help message and exit
     -H HOST, --host=HOST  The host to bind this instance on
     -P PORT, --port=PORT  The port to bind this instance on
     -m MAXREQSIZE, --max-request-size=MAXREQSIZE
                           The maximum allowed HTTP request size, in MB
     -d, --daemon          Run as daemon
     -s, --stop            Stop an instance running as daemon
     --status              Checks if there is daemon process actively running
     -T TIMEOUT, --timeout=TIMEOUT
                           Timeout used when checking for the daemon process
     -v, --verbose         Become more verbose. The more flags, the more verbose
     -q, --quiet           Be less verbose. The more flags, the quieter
     -l LOGDIR, --log-dir=LOGDIR
                           The directory where the logging files will be stored
     -S STATUS_FILE, --status-file=STATUS_FILE
                           File containing a continuous graph status dump
     -g GRAPH_FILE, --graph-file=GRAPH_FILE
                           File containing a physical graph dump

Command: dlg include_dir
-------------------------

Help output::

  <python virtualenv>/lib/pythonX.X/site-packages/dlg/apps
   
  
