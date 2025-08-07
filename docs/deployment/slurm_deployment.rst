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
- The graph: This is either: 

   - Locally stored, and either a logical or physical graph; or 
   - Remotely stored in a GitHub or GitLab repository. 
- Specify if it is a remote or local submission
- If remote, user account 
- The number of Node and Data Island Managers required. 

All other options have defaults provided. Thus the most basic job submission will look like::

   dlg remote-submit -a submit -n 1 -s 1 -f setonix -L /path/to/graph/ArrayLoop.graph -U user_name

However, the defaults for jobs submissions will lead to limited use of the available resources (i.e. number of nodes provisioned) and won't account for specific job durations. DALiuGE Translator options are also available, so it is possible to specify what partitioning algorithm is preferred. A more complete job submission, that takes advantage of the SLURM and environment options, will look something like::

   dlg remote-submit -a submit -n 32 -s 1 -t 60 -A pso -u -f setonix -L/path/to/graph/ArrayLoop.graph -v 4 --remote --submit -U user_name

This performs the following: 

- Submits and runs a remote job to Pawsey's Setonix (`-f setonix`) machine
- Uses 1 data island manager (-s 1) and requests 32 nodes (-n 32) for a job duration of 60 minutes (-t)
- Translates the Logical Graph (-L) using the PSO algorithm (-A PSO). 

Configuration .ini
~~~~~~~~~~~~~~~~~~~~~
To make deployment more flexible and easier to expand to any facility, we have support for using an INI configuration file that directly maps to all CLI arguments. Documentation on how to setup the configuration file using defaults is available at :ref:`cli_remote`.

.. note::
   It is recommended to use the .ini file to use the remote graph support (i.e. GitHub or GitLab access). Otherwise, the command line argument becomes too complex to manage.  

The following configuration is an example deployment that contains all variables necessary to deploy onto a remove system:: 


   [DEPLOYMENT]
   remote=True
   submit=False

   [ENGINE]
   NUM_NODES = 1
   NUM_ISLANDS = 1
   ALL_NICS =

   [GRAPH]
   ; Local
   LOGICAL_GRAPH =
   PHYSICAL_GRAPH =

   ; Remote
   GITHUB = True
   ; GITLAB = True
   USER_ORG = ICRAR
   REPO = EAGLE-graph-repo
   BRANCH = master
   PATH = examples/HelloWorld-Universe.graph

   [FACILITY]
   USER = <MY_USERNAME>
   ACCOUNT = pawsey0411
   LOGIN_NODE = setonix.pawsey.org.au
   HOME_DIR = /scratch/${ACCOUNT}
   DLG_ROOT = ${HOME_DIR}/${USER}/dlg
   LOG_DIR = ${DLG_ROOT}/log
   MODULES = <MODULES NECESSARY FOR SOFTWARE>
   VENV = source /software/projects/${ACCOUNT}/dlg-venv/bin/activate
   EXEC_PREFIX = srun -l

A user can create and reference their own .ini file using these parameters, and run with the --config_file option::

   dlg remote-submit -a submit --config_file example_config.ini
   
SLURM Template
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
There are significantly more SLURM options than are practical as CLI options. The SLURM template is an experimental feature that allows you to specify additional SBATCH options that are not currently supported in the CLI. The template will be prefixed to the final SLURM script that runs the DALiuGE job on the remote system. 

A basic example that replicates the current SLURM script that is created by :code:`create_dlg_job.py` is available in dlg/deploy/config/default.slurm ::

   #!/bin/bash --login

   #SBATCH --nodes=2
   #SBATCH --ntasks-per-node=1
   #SBATCH --cpus-per-task=2
   #SBATCH --job-name=DALiuGE-$SESSION_ID # NECESSARY, DO NOT REMOVE 
   #SBATCH --time=00:45:00
   #SBATCH --error=err-%j.log

   export DLG_ROOT=$DLG_ROOT # DO NOT CHANGE - use .INI file or CLI 
   source /software/projects/pawsey0411/venv/bin/activate
   # Keep an empty line in the file

.. note:: 
   Settings defined in the SLURM template will over-write anything passed via the CLI _and_ the .INI. For example, the `source` for a virtualenv declared in the .slurm file will overwrite the VENV environment variable in the .INI file. This may change in the future depending on the extent of the features we add. 

Running with a SLURM template is similar to the .ini method:: 
   
   python create_dlg_job.py -a submit --config_file example_config.ini --slurm_template example.slurm
