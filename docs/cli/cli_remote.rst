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
    Custom environment configuration is in 'beta' at the moment and there may still be rough edges. Please consider `submitting an issue <https://github.com/ICRAR/daliuge/issues/new/choose>`_ if you identify one. 

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

    dlg remote-submit -a submit 
    -L <my_logical_graph>
    -v 5 --remote --submit 
    --config_file setonix.ini --slurm_template setonix.slurm
    
Reference
^^^^^^^^^

Command: dlg config
===================
Help output::

    Usage: config [options]

    Options:
      -h, --help  show this help message and exit
      --setup     Setup local '$HOME/.config/dlg' directory to store custom
                  environment config and slurm scripts
      -l, --list  List the available configuration for DALiuGE deployment.

Command: dlg remote-submit
===========================

Help output::
        
    Usage: 
    remote-submit --action [submit|analyse] -f <facility> [options]

    remote-submit -h for further help

    Options:
      -h, --help            show this help message and exit
      -a ACTION, --action=ACTION
                            **submit** job or **analyse** log
      -A ALGORITHM, --algorithm=ALGORITHM
                            The algorithm to be used for the translation
      -O ALGORITHM_PARAMS, --algorithm-parameters=ALGORITHM_PARAMS
                            Parameters for the translation algorithm
      --submit              If set to False, the job is not submitted, but the
                            script is generated
      --remote              If set to True, the job is submitted/created for a
                            remote submission
      -c CSV_OUTPUT, --csvoutput=CSV_OUTPUT
                            CSV output file to keep the log analysis result
      -U USERNAME, --username=USERNAME
                            Remote username, if different from local
      --ssh_key=SSH_KEY     Path to ssh private key

      Engine options:
        DALiuGE engine configuration and runtime options

        -l LOG_ROOT, --log-root=LOG_ROOT
                            The root directory of the log file
        -d LOG_DIR, --log-dir=LOG_DIR
                            The directory of the log file for parsing
        -n NUM_NODES, --num_nodes=NUM_NODES
                            Number of compute nodes requested
        -s NUM_ISLANDS, --num_islands=NUM_ISLANDS
                            The number of Data Islands
        -u, --all_nics      Listen on all NICs for a node manager
        -S, --check_with_session
                            Check for node managers' availability by
                            creating/destroy a session
        -D DLG_ROOT, --dlg_root=DLG_ROOT
                            Overwrite the DLG_ROOT directory provided by the
                            config
        -v VERBOSE_LEVEL, --verbose-level=VERBOSE_LEVEL
                            Verbosity level (1-3) of the DIM/NM logging

      Slurm options:
        Slurm job script options. Note: These will be overwritten if using
        --slurm_template

        -T MAX_THREADS, --max-threads=MAX_THREADS
                            Max thread pool size used for executing drops. 0
                            (default) means no pool.
        -f FACILITY, --facility=FACILITY
                            The facility for which to create a submission job
                            Valid options: ['galaxy_mwa', 'galaxy_askap',
                            'magnus', 'galaxy', 'setonix', 'shao', 'hyades',
                            'ood', 'ood_cloud']
        -t JOB_DUR, --job-dur=JOB_DUR
                            job duration in minutes

      Monitor proxy options:
        Start and configure the monitoring proxy.

        -p, --run_proxy     Whether to attach proxy server for real-time
                            monitoring
        -m MON_HOST, --monitor_host=MON_HOST
                            Monitor host IP (optional)
        -o MON_PORT, --monitor_port=MON_PORT
                            The port to bind DALiuGE monitor
        -i, --visualise_graph
                            Whether to visualise graph (poll status)

      Graph Component options:
        Update component DROPs for testing.

        -z, --zerorun       Generate a physical graph that takes no time to run
        -y, --sleepncopy    Whether include COPY in the default Component drop

      Remote graph options:
        Options for graphs stored in remote repositories. Currently supported:
        GitHub, GitLab

        --github            Access graph from remote repository
        --gitlab            Access graph from remote repository
        --user_org=USER_ORG
        --repo=REPO         
        --branch=BRANCH     
        --path=PATH         

      Local graph options:
        Options for locally stored graphs

        -L LOGICAL_GRAPH, --logical-graph=LOGICAL_GRAPH
                            The filename of the logical graph to deploy
        -P PHYSICAL_GRAPH, --physical-graph=PHYSICAL_GRAPH
                            The filename of the physical graph (template) to
                            deploy

      Graph config options:
        Options for selecting the active graph configuration.  Only one option
        is used: priority in descending is fill, id, name

        --config_name=CONFIG_NAME
                            The name of the config as it appears in the graph
        --config_id=CONFIG_ID
                            The id of the config
        --fill_config=FILL_CONFIG
                            Use stdin to fill graph with config provided at
                            runtime using 'dlg fill_config'


      Remote configuration Options:
        Remote deployment configuration options based on configuration and
        template files.

        --config_file=CONFIG_FILE
                            Use INI configuration file.
        --slurm_template=SLURM_TEMPLATE
                            Use SLURM template file for job submission. WARNING:
                            Using this command will over-write other job-
                            parameters passed here.
