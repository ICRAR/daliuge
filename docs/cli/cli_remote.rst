.. _cli_remote:
   
Remote Deployment Commands
##########################
The DALiuGE CLI provides support for submitting graphs to a remote environment, such as a local network cluster, a supercomputing facility, or even the cloud. 

The following assumptions are made prior to discussing Remote Deployment: 

- DALiuGE is already installed within a Python virtual environment in the remote cluster
- All required libraries and data is available in the remote cluster, either before or during the execution of the graph. 
- The cluster is running SLURM.

Remote deployment support in DALiuGE creates a SLURM job that starts up a DALiuGE engine on the nodes allocated to the SLURM job. This requires setting up the remote submission and any additional parameters you want to have for the SLURM-DALiUGE job. 

The following sections detail how to configure a remote submission and how to submit the graph using the CLI. 

Configuration
^^^^^^^^^^^^^

.. warning::
    Custom environment configuration is in 'Beta' at the moment and there may still be rough edges. Please consider `submitting an issue <https://github.com/ICRAR/daliuge/issues/new/choose>`_ if you identify one. 

Remote deploment setup occurs from your local DALiuGE installtion. 

The folling command initiates the remote setup::   

    dlg config --setup

    > Do you want to create a $HOME/.config/dlg directory to store your custom configuration files and scripts  (y/n)?

Selecting ``y`` will set up the config directory, along with the default environ ini files and the slurm submission scripts::

    User Configs (~/.config/dlg)
    ----------------------------
    Environments (--config_file):
        setonix.ini
        default.ini
    Slurm scripts (--slurm_template):
        default.slurm
        setonix.slurm

    DALiuGE Defaults (-f/--facility):
    -----------------------------------
        galaxy_mwa
        galaxy_askap
        magnus
        galaxy
        setonix
        shao
        hyades
        ood
        ood_cloud

The above can be listed at any time after initially created using the ``list`` option:

    dlg config --list

The idea behind the User configurations and Slurm templates is that individual runtime requirements will differ between users and between graphs. 
    
Custom Environments 
===================

The custom environment files allow you to set parameters for the remote environment which will be used during submission of the batch job. This includes things like username, login node, virtual environment etc. 

It is recommended to start with the default.ini file provided in the User config directory, and edit it according to what works for your situation.

Custom SLURM scripts
====================

It is possible you need to do more with your DALiUGE SLURM submission than the basics; for example, if your graph being submitted requires additional environments to be loaded. Like the custom environments, we recommend starting with the default.slurm file and then customising it according to your needs. The setonix.slurm example may provide useful guidance on how we have customised a submission for our own local HPC system. 
    
Deployment
^^^^^^^^^^

The precedence order for default submission is:
    - Facility (-f) 
    - Environment config (--config-file) will overwrite Facility
    - SLURM template (--slurm_template) will overwrite Environment

Thus some environment variables that may be set in the ``.ini`` file will be overwritten if a SLURM template file is used that also defines those variables. 

The following is a complete example::

    dlg create -a submit -n 1 -s 1 -u -f setonix 
    -L ~/github/EAGLE_test_repo/eagle_test_graphs/daliuge_tests/dropmake/logical_graphs/ArrayLoop.graph 
    -v 5 --remote --submit 
    --config_file setonix.ini --slurm_template setonix.slurm
    
Reference
^^^^^^^^^

Command: dlg config
===================
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

Command: dlg create
===================

Help output::
        
    Usage: 
    remote-submit --action [submit|analyse] -f <facility> [options]

    remote-submit -h for further help

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
      --ssh_key=SSH_KEY     Path to ssh private key

      Experimental Options:
        Caution: These are not properly tested and likely tobe rough around
        the edges.

        --config_file=CONFIG_FILE
                            Use INI configuration file.
        --slurm_template=SLURM_TEMPLATE
                            Use SLURM template file for job submission. WARNING:
                            Using this command will over-write other job-
                            parameters passed here.
