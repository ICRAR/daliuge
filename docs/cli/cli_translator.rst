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

.. _unrolling_cli:

Unrolling a graph
=================

As discussed, the workflow graph produced in EAGLE is a Logical Graph Template (LGT). The LGT is a logical representation of the operations that must occur in the execution of the graph; therefore, it is not a formal Directed Acyclic Graph (DAG) and does not represent the actual sequence of tasks that will take place during the running of the workflow. In order to get that sequence of tasks, we need to `unroll` the LGT into its Physical Graph Template (PGT). The PGT contains all the planned tasks the engine will allocate and run on compute resources. 

For an example LGT from EAGLE, 'parallel_loop.graph', unrolling is as simple as:: 

   dlg unroll -L parallel_loop.graph -o parallel_loop_unrolled.graph 

There are additional options that allow you to modify what apps are produced in the PGT (i.e. for testing purposes); for further information, please review the full list of :ref:`unroll commands<unroll_complete>`.

.. _partitioning_cli:

Partitioning the graph
======================

Once we have the initial PGT, we need to `partition` the work across the compute nodes onto which we will be allocating the tasks. This forms the basis of the scheduling that the DALiuGE engine does at runtime (mapping tasks to compute resources). 

A basic partition requires knowing the number of partitions we want to create ahead of time; for example, if we want to take advantage of 2 compute nodes:: 

  dlg partition -P parallel_loop_unrolled.graph -N 2 -o parallel_loop_PGT.graph
  
This will produce an updated ``.graph`` file with the DROPs distributed across 2 nodes. What this means in practices is the ``#node`` and ``island`` values have been added to each DROP in the JSON file. The ``#islands`` will default to 1, and the METIS partitioning algorithm is used by default. 

The following provides an example that specifies both the algorithm (-a/--algorithm) and number of islands (-i/--islands) for a larger 'cluster' of 4 nodes::

   dlg partition -P parallel_loop_unrolled.graph -i 2 -N 4 -a mysarkar -o parallel_loop_PGT.graph

.. note:: 
   The partitioning method is contingent upon the inherent parallelism of the PGT. This means that some graphs cannot be split across both multiple data islands *and* multiple nodes. In this situation, consider reducing the number of data islands. 
   
Typically, it is not necessary to separate the `unroll` and `partition` actions, so we provide the `unroll-and-partition` option:: 

    dlg unroll-and-partition-L parallel_loop.graph -i 2 -N 4 -a mysarkar -o parallel_loop_PGT.graph

A full list of options is available at :ref:`partition_complete`.

.. _mapping_cli:

Mapping the graph
==================

Finally, we want to map the graph to a set of resources at runtime. For the small 2-node partition, we can do the following::

   dlg map -P parallel_loop_PGT.graph -i 1 -N <island_a_hostname>,<node_a_hostname>,<node_b_hostname> -o parallel_loop_PG.graph

It is important to note that the nodes we pass to -N **include** the Data Island Managers (DIMs), so we must provide a list of ** three (3)** hosts. The translator then uses the `-i` number to determine the number of nodes in the node-list are DIMs, starting from the front of the list. 

Submitting the graph
====================

.. note:: 
   
   It is necessary to have Node Managers and Data Island Managers running in order to submit.
   Please review :ref:`cli_engine` for further details.

We submit the graph to the DIM::

   dlg submit -P parallel_loop_PG.graph -H <island_hostname> -p <island_port>
   

And voila! A graph has been translated, partitioned, mapped to resources, and then submitted to those resources. 

Complete translation and submission chain 
==========================================

The `dlg` translator CLI allows for the piping of stdin to each subsequent step of the pipeline, allowing us to do everything in one command::

   dlg unroll-and-partition -L parallel_loop.graph -a mysarkar | dlg map -i 1 -N <island_a_hostname>,<node_a_hostname>,<node_b_hostname>  | dlg submit -H localhost -p 8001


.. _lgweb_cli:
   
Translate and submit graphs through EAGLE UI
********************************************

If you are building graphs and want to test and deploy them through the web interface, 
you can use the `lgweb` server. This provides a user interface for the translation of a Logical Graph to a Physical Graph Template, as well as submission to the Data Island and Node Managers for graph execution. 

To start with, simply start the server as follows::

   dlg lgweb -d /tmp/ -t /tmp/ -v

Full information on the options that may be provided to the `lgweb` interface are available :ref:`below <lgweb>`.

This allows you to translate and deploy from EAGLE, and visualise the progression of the graph.   

Reference
*********

.. _unroll_complete:

Command: dlg unroll
===================

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

.. _partition_complete:

Command: dlg partition
-------------------------
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
 

