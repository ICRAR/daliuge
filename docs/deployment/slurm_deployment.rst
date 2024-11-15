.. _slurm_deployment:

Slurm Deployment
=====================================

Usage and options
-----------------

- Non-OOD support requires the use of the create_dlg_job.py script. 

Script has two configuration approaches: 

- Command line interface (CLI)
- Configuration files:
   - Facility INI [Experimental]
   - Slurm template [Experimental]

Command-line Interface (CLI)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The CLI allows the user to submit a remote SLURM job from their local machine, which will
spin up the requested number of DALiuGE Island and Node Managers and run the graph. 

The minimal requirements for submitting a job via the command-line are: 

- The facility (e.g. Setonix, Hyades, Galaxy)
- The graph (either logical or physical, but not both). 
- Specifying if remote or local submission
- The remote user account 

All other options have defaults provided. Thus the most basic job submission will look like::

   python create_dlg_job.py -a 1 -f setonix -L /path/to/graph/ArrayLoop.graph -U user_name

However, the defaults for jobs submissions will lead to limited use of the available resources (i.e. number of nodes provisioned) and won't account for specific job durations. DALiuGE Translator options are also available, so it is possible to specify what partitioning algorithm is preferred. A more complete job submission, that takes advantage of the SLURM and environment options, will look something like::

   python create_dlg_job.py -a 1 -n 32 -s 1 -t 60 -A pso -u -f setonix -L/path/to/graph/ArrayLoop.graph -v 4 --remote --submit -U user_name

This performs the following: 

- Submits and runs a remote job to Pawsey's Setonix (`-f setonix`) machine
- Uses 1 data island manager (-s 1) and requests 32 nodes (-n 32) for a job duration of 60 minutes (-t)
- Translates the Logical Graph (-L) using the PSO algorithm (-A PSO). 

Facility INI
~~~~~~~~~~~~~~~~~~~~~
Currently, deploying onto a HPC facility requires using the facilities DALiuGE already supports, or adding a brand new class entry to the deploy/config/__init__.py file. 
To make deployment more flexible and easier to expand to feasibly any facility, we have added (experimental) support for using an INI configuration file for facility deployment parameters. 

The following configuration is an example deployment that contains all variables necessary to deploy onto a remove system:: 

   [ENVIRONMENT]
   ACCOUNT = pawsey0411
   USER = test
   LOGIN_NODE = setonix.pawsey.org.au
   HOME_DIR = /scratch/${ACCOUNT}
   DLG_ROOT = ${HOME_DIR}/${USER}/dlg
   LOG_DIR = ${DLG_ROOT}/log
   MODULES = 
   VENV = source /software/projects/${ACCOUNT}/venv/bin/activate
   EXEC_PREFIX = srun -l

A user can create and reference their own .ini file using these parameters, and run with the --config_file option::

   python create_dlg_job.py -a 1 -n 1 -s 1 -u -f setonix -L ~/github/EAGLE_test_repo/eagle_test_graphs/daliuge_tests/dropmake/logical_graphs/ArrayLoop.graph -v 5 --remote --submit -U rbunney --config_file example_config.ini

SLURM Template
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A basic example that replicates the current SLURM script that is created by :code:`create_dlg_job.py`. ::

   #!/bin/bash --login

   #SBATCH --nodes=2
   #SBATCH --ntasks-per-node=1
   #SBATCH --cpus-per-task=2
   #SBATCH --job-name=DALiuGE-$SESSION_ID
   #SBATCH --time=00:45:00
   #SBATCH --error=err-%j.log

   export DLG_ROOT=$DLG_ROOT
   source /software/projects/pawsey0411/venv/bin/activate

Complete command-line options
-----------------------------

Help output::

   create_dlg_job.py -a [1|2] -f <facility> [options]

   create_dlg_job.py -h for further help

   Options:
   -h, --help            show this help message and exit
   -a ACTION, --action=ACTION
                           1 - create/submit job, 2 - analyse log
   -l LOG_ROOT, --log-root=LOG_ROOT
                           The root directory of the log file
   -d LOG_DIR, --log-dir=LOG_DIR
                           The directory of the log file for parsing
   -L LOGICAL_GRAPH, --logical-graph=LOGICAL_GRAPH
                           The filename of the logical graph to deploy
   -A ALGORITHM, --algorithm=ALGORITHM
                           The algorithm to be used for the translation
   -O ALGORITHM_PARAMS, --algorithm-parameters=ALGORITHM_PARAMS
                           Parameters for the translation algorithm
   -P PHYSICAL_GRAPH, --physical-graph=PHYSICAL_GRAPH
                           The filename of the physical graph (template) to
                           deploy
   -t JOB_DUR, --job-dur=JOB_DUR
                           job duration in minutes
   -n NUM_NODES, --num_nodes=NUM_NODES
                           number of compute nodes requested
   -i, --visualise_graph
                           Whether to visualise graph (poll status)
   -p, --run_proxy       Whether to attach proxy server for real-time
                           monitoring
   -m MON_HOST, --monitor_host=MON_HOST
                           Monitor host IP (optional)
   -o MON_PORT, --monitor_port=MON_PORT
                           The port to bind DALiuGE monitor
   -v VERBOSE_LEVEL, --verbose-level=VERBOSE_LEVEL
                           Verbosity level (1-3) of the DIM/NM logging
   -c CSV_OUTPUT, --csvoutput=CSV_OUTPUT
                           CSV output file to keep the log analysis result
   -z, --zerorun         Generate a physical graph that takes no time to run
   -y, --sleepncopy      Whether include COPY in the default Component drop
   -T MAX_THREADS, --max-threads=MAX_THREADS
                           Max thread pool size used for executing drops. 0
                           (default) means no pool.
   -s NUM_ISLANDS, --num_islands=NUM_ISLANDS
                           The number of Data Islands
   -u, --all_nics        Listen on all NICs for a node manager
   -S, --check_with_session
                           Check for node managers' availability by
                           creating/destroy a session
   -f FACILITY, --facility=FACILITY
                           The facility for which to create a submission job
                           Valid options: ['galaxy_mwa', 'galaxy_askap',
                           'magnus', 'galaxy', 'setonix', 'shao', 'hyades',
                           'ood', 'ood_cloud']
   --submit              If set to False, the job is not submitted, but the
                           script is generated
   --remote              If set to True, the job is submitted/created for a
                           remote submission
   -D DLG_ROOT, --dlg_root=DLG_ROOT
                           Overwrite the DLG_ROOT directory provided by the
                           config
   -C, --configs         Display the available configurations and exit
   -U USERNAME, --username=USERNAME
                           Remote username, if different from local

   Experimental Options:
      Caution: These are not properly tested and likely tobe rough around
      the edges.

      --config_file=CONFIG_FILE
                           Use INI configuration file.
      --slurm_template=SLURM_TEMPLATE
                           Use SLURM template file for job submission. WARNING:
                           Using this command will over-write other job-
                           parameters passed here.

