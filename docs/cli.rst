.. _cli:

CLI User Guide
==============
As briefly highlighted in the :ref:`running` there is a complete Command Line Interface (CLI) available to control the managers and translate, partition and deploy graphs. This makes the whole system independent of EAGLE or a web browser and also allows the system to be scripted (although we recommend to do this in Python following the :ref:`api`). The available functionality of the CLI depends on which parts of the |daliuge| execution framework are actually installed on the python virtualenv.

Basic Usage
^^^^^^^^^^^
In order to be able to use the CLI at least daliuge-common needs to be installed. In that case the functionality is obviously very limited, but it shows already the basic usage::

    ❯ dlg
    Usage: /home/awicenec/.pyenv/versions/dlg/bin/dlg [command] [options]

    Commands are:
        version                  Reports the DALiuGE version and exits

    Try $PATH/bin/dlg [command] --help for more details

If daliuge-engine is also installed it is a bit more interesting::

    ❯ dlg
    Usage: dlg [command] [options]

    Commands are:
        daemon                   Starts a DALiuGE Daemon process
        dim                      Starts a Drop Island Manager
        include_dir              Print the directory where C header files can be found
        mm                       Starts a Master Manager
        monitor                  A proxy to be used in conjunction with the dlg proxy in restricted environments
        nm                       Starts a Node Manager
        proxy                    A reverse proxy to be used in restricted environments to contact the Drop Managers
        replay                   Starts a Replay Manager
        version                  Reports the DALiuGE version and exits

    Try dlg [command] --help for more details


If *only* the daliuge-translator is installed this changes to::

    ❯ dlg
    Usage: dlg [command] [options]

    Commands are:
        fill                     Fill a Logical Graph with parameters
        lgweb                    A Web server for the Logical Graph Editor
        map                      Maps a Physical Graph Template to resources and produces a Physical Graph
        partition                Divides a Physical Graph Template into N logical partitions
        submit                   Submits a Physical Graph to a Drop Manager
        unroll                   Unrolls a Logical Graph into a Physical Graph Template
        unroll-and-partition     unroll + partition
        version                  Reports the DALiuGE version and exits

    Try dlg [command] --help for more details

If everything is installed the output is a merge of all three::

    ❯ dlg
    Usage: dlg [command] [options]

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

    Try dlg [command] --help for more details

Subcommand usage
^^^^^^^^^^^^^^^^
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
----------------
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
   

Command: dlg fill
-----------------
Help output::

   Usage: fill [options]
   
   Fill a Logical Graph with parameters
   
   Options:
     -h, --help            show this help message and exit
     -v, --verbose         Become more verbose. The more flags, the more verbose
     -q, --quiet           Be less verbose. The more flags, the quieter
     -o OUTPUT, --output=OUTPUT
                           Where the output should be written to (default:
                           stdout)
     -f, --format          Format JSON output (newline, 2-space indent)
     -L LOGICAL_GRAPH, --logical-graph=LOGICAL_GRAPH
                           Path to the Logical Graph (default: stdin)
     -p PARAMETER, --parameter=PARAMETER
                           Parameter specification (either 'name=value' or a JSON
                           string)
     -R, --reproducibility
                           Level of reproducibility. Default 0 (NOTHING). Accepts '-1'-'8'"
                           Refer to dlg.common.reproducibility.constants for more explanation.
   

Command: dlg include_dir
------------------------
Help output::

   /home/awicenec/.pyenv/versions/3.8.10/envs/dlg/lib/python3.8/site-packages/dlg/apps
   

Command: dlg lgweb
------------------
Help output::

   Usage: lgweb [options]
   
   A Web server for the Logical Graph Editor
   
   Options:
     -h, --help            show this help message and exit
     -d LG_PATH, --lgdir=LG_PATH
                           A path that contains at least one sub-directory, which
                           contains logical graph files
     -t PGT_PATH, --pgtdir=PGT_PATH
                           physical graph template path (output)
     -H HOST, --host=HOST  logical graph editor host (all by default)
     -p PORT, --port=PORT  logical graph editor port (8084 by default)
     -v, --verbose         Enable more logging
   
   If you have no Logical Graphs yet and want to see some you can grab a copy
   of those maintained at:
   
   https://github.com/ICRAR/daliuge-logical-graphs
   
   

Command: dlg map
----------------
Help output::

   Usage: map [options]
   
   Maps a Physical Graph Template to resources and produces a Physical Graph
   
   Options:
     -h, --help            show this help message and exit
     -v, --verbose         Become more verbose. The more flags, the more verbose
     -q, --quiet           Be less verbose. The more flags, the quieter
     -o OUTPUT, --output=OUTPUT
                           Where the output should be written to (default:
                           stdout)
     -f, --format          Format JSON output (newline, 2-space indent)
     -H HOST, --host=HOST  The host we connect to to deploy the graph
     -p PORT, --port=PORT  The port we connect to to deploy the graph
     -P PGT_PATH, --physical-graph-template=PGT_PATH
                           Path to the Physical Graph to submit (default: stdin)
     -N NODES, --nodes=NODES
                           The nodes where the Physical Graph will be
                           distributed, comma-separated
     -i ISLANDS, --islands=ISLANDS
                           Number of islands to use during the partitioning
   

Command: dlg mm
---------------
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
   

Command: dlg partition
----------------------
Help output::

   Usage: partition [options]
   
   Divides a Physical Graph Template into N logical partitions
   
   Options:
     -h, --help            show this help message and exit
     -v, --verbose         Become more verbose. The more flags, the more verbose
     -q, --quiet           Be less verbose. The more flags, the quieter
     -o OUTPUT, --output=OUTPUT
                           Where the output should be written to (default:
                           stdout)
     -f, --format          Format JSON output (newline, 2-space indent)
     -N PARTITIONS, --partitions=PARTITIONS
                           Number of partitions to generate
     -i ISLANDS, --islands=ISLANDS
                           Number of islands to use during the partitioning
     -a ALGO, --algorithm=ALGO
                           algorithm used to do the partitioning
     -A ALGO_PARAMS, --algorithm-param=ALGO_PARAMS
                           Extra name=value parameters used by the algorithms
                           (algorithm-specific)
     -P PGT_PATH, --physical-graph-template=PGT_PATH
                           Path to the Physical Graph Template (default: stdin)
   

Command: dlg proxy
------------------
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
   

Command: dlg submit
-------------------
Help output::

   Usage: submit [options]
   
   Submits a Physical Graph to a Drop Manager
   
   Options:
     -h, --help            show this help message and exit
     -v, --verbose         Become more verbose. The more flags, the more verbose
     -q, --quiet           Be less verbose. The more flags, the quieter
     -H HOST, --host=HOST  The host we connect to to deploy the graph
     -p PORT, --port=PORT  The port we connect to to deploy the graph
     -P PG_PATH, --physical-graph=PG_PATH
                           Path to the Physical Graph to submit (default: stdin)
     -s SESSION_ID, --session-id=SESSION_ID
                           Session ID (default: <pg_name>-<current-time>)
     -S, --skip-deploy     Skip the deployment step (default: False)
     -w, --wait            Wait for the graph execution to finish (default:
                           False)
     -i POLL_INTERVAL, --poll-interval=POLL_INTERVAL
                           Polling interval used for monitoring the execution
                           (default: 10)
     -R REPRODUCIBILITY, --reproducibility=REPRODUCIBILITY
                           Fetch (and output) reproducibility data for final execution graph.
                           (default: False)
   

Command: dlg unroll
-------------------
Help output::

   Usage: unroll [options]
   
   Unrolls a Logical Graph into a Physical Graph Template
   
   Options:
     -h, --help            show this help message and exit
     -v, --verbose         Become more verbose. The more flags, the more verbose
     -q, --quiet           Be less verbose. The more flags, the quieter
     -o OUTPUT, --output=OUTPUT
                           Where the output should be written to (default:
                           stdout)
     -f, --format          Format JSON output (newline, 2-space indent)
     -L LG_PATH, --logical-graph=LG_PATH
                           Path to the Logical Graph (default: stdin)
     -p OID_PREFIX, --oid-prefix=OID_PREFIX
                           Prefix to use for generated OIDs
     -z, --zerorun         Generate a Physical Graph Template that takes no time
                           to run
     --app=APP             Force an app to be used in the Physical Graph. 0=Don't
                           force, 1=SleepApp, 2=SleepAndCopy
   

Command: dlg unroll-and-partition
---------------------------------
Help output::

   Usage: unroll-and-partition [options]
   
   unroll + partition
   
   Options:
     -h, --help            show this help message and exit
     -v, --verbose         Become more verbose. The more flags, the more verbose
     -q, --quiet           Be less verbose. The more flags, the quieter
     -o OUTPUT, --output=OUTPUT
                           Where the output should be written to (default:
                           stdout)
     -f, --format          Format JSON output (newline, 2-space indent)
     -L LG_PATH, --logical-graph=LG_PATH
                           Path to the Logical Graph (default: stdin)
     -p OID_PREFIX, --oid-prefix=OID_PREFIX
                           Prefix to use for generated OIDs
     -z, --zerorun         Generate a Physical Graph Template that takes no time
                           to run
     --app=APP             Force an app to be used in the Physical Graph. 0=Don't
                           force, 1=SleepApp, 2=SleepAndCopy
     -N PARTITIONS, --partitions=PARTITIONS
                           Number of partitions to generate
     -i ISLANDS, --islands=ISLANDS
                           Number of islands to use during the partitioning
     -a ALGO, --algorithm=ALGO
                           algorithm used to do the partitioning
     -A ALGO_PARAMS, --algorithm-param=ALGO_PARAMS
                           Extra name=value parameters used by the algorithms
                           (algorithm-specific)
   

Command: dlg version
--------------------
Help output::

   Version: 1.0.0
   Git version: Unknown
 

