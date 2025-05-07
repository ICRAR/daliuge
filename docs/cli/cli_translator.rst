.. _cli_translator:

Translator Commands
###################
   
Translating graphs via CLI
**************************
The CLI allows you to take an EAGLE graph and perform all translation:

1. Unrolling from Logical to Physical Graph Template; then
2. Partitioning the graph across N number of compute nodes; and finally
3. Mapping the the partitions to specific nodes (i.e. actual runtime IPAddress and Port configurations)
   
The following section provides a series of examples on how to produce the intermediary graph products in order to achieve a fully-translated graph that could be submitted to the DAliUGE Engine. 

.. note:: 
   
   If you have no Logical Graphs yet and want to play around with some, please look at our examples in the `EAGLE Graph repository <https://github.com/ICRAR/EAGLE-graph-repo>`_, which is maintained by our development team. 

Unrolling a graph
=================

Partitioning the graph
======================

Mapping the graph
==================

Translate graphs through EAGLE UI
*********************************

If you are building graphs and want to test and deploy them through the web interface, 
you can use the `lgweb` server. This provides a user interface for the translation of a Logical Graph to a Physical Graph Template, as well as submission to the Data Island and Node Managers for graph execution. 

To start with, simply start the server as follows::

   dlg lgweb -d /tmp/ -t /tmp/ -v

Full information on the options that may be provided to the `lgweb` interface are available :ref:`below <lgweb>`.

Reference
*********

Command: dlg fill
=================
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
   

   

ComCommand: dlg partition
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
                           

.. _lgweb:
Command: dlg lgweb
===================
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
   
 

Command: dlg version
--------------------
Help output::

   Version: 1.0.0
   Git version: Unknown
 

