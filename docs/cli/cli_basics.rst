.. _cli_base:

Basics
==============
As briefly highlighted in the :ref:`running` there is a complete Command Line Interface (CLI) available to control the managers and translate, partition and deploy graphs. This makes the whole system independent of EAGLE or a web browser and also allows the system to be scripted (although we recommend to do this in Python following the :ref:`api`). The available functionality of the CLI depends on which parts of the |daliuge| execution framework are actually installed on the python virtualenv.

In order to be able to use the CLI at least daliuge-common needs to be installed. In that case the functionality is obviously very limited, but it shows already the basic usage::

   > dlg 
   Usage: /home/00087932/github/daliuge-new/.venv/bin/dlg [command] [options]


   Base commands for dlg CLI
       version                  Reports the DALiuGE version and exits


If daliuge-engine is also installed it is a bit more interesting::

   > dlg 
   Usage: /home/00087932/github/daliuge-new/.venv/bin/dlg [command] [options]


   Base commands for dlg CLI
       version                  Reports the DALiuGE version and exits

   DROP Manager Commands
       daemon                   Starts a DALiuGE Daemon process
       dim                      Starts a Drop Island Manager
       mm                       Starts a Master Manager  
       monitor                  A proxy to be used in conjunction with the dlg proxy in restricted environments
       nm                       Starts a Node Manager    
       proxy                    A reverse proxy to be used in restricted environments to contact the Drop Managers
       replay                   Starts a Replay Manager  

   Remote environment configuration and deployment
       config                   Manage dlg config environment
       create                   Create a DALiuGE graph to a remote computing environment


If *only* the daliuge-translator is installed this changes to::

    ❯ dlg
    Commands for unrolling and partitioning graphs using the dlg translator.
         fill                     Fill a Logical Graph with parameters
         tm                       Starts the Translator Manager
         map                      Maps a Physical Graph Template to resources and produces a Physical Graph
         partition                Divides a Physical Graph Template into N logical partitions
         submit                   Submits a Physical Graph to a Drop Manager
         unroll                   Unrolls a Logical Graph into a Physical Graph Template
         unroll-and-partition     unroll + partition       

If everything is installed the output is a merge of all three::

    ❯ dlg
    Usage: /home/00087932/github/daliuge-new/.venv/bin/dlg [command] [options]


    Base commands for dlg CLI
         version                  Reports the DALiuGE version and exits

    DROP Manager Commands
         daemon                   Starts a DALiuGE Daemon process
         dim                      Starts a Drop Island Manager
         mm                       Starts a Master Manager  
         monitor                  A proxy to be used in conjunction with the dlg proxy in restricted environments
         nm                       Starts a Node Manager    
         proxy                    A reverse proxy to be used in restricted environments to contact the Drop Managers
         replay                   Starts a Replay Manager  

    Remote environment configuration and deployment
         config                   Manage dlg config environment
         create                   Create a DALiuGE graph to a remote computing environment

    Commands for unrolling and partitioning graphs using the dlg translator.
         fill                     Fill a Logical Graph with parameters
         tm                       Starts the Translator Manager
         map                      Maps a Physical Graph Template to resources and produces a Physical Graph
         partition                Divides a Physical Graph Template into N logical partitions
         submit                   Submits a Physical Graph to a Drop Manager
         unroll                   Unrolls a Logical Graph into a Physical Graph Template
         unroll-and-partition     unroll + partition       

    Utility commands
         include_dir              Print the directory where C header files can be found

    Wrapper for dlg_paletteGen
         palette                  Generate palettes for EAGLE

    Try /home/00087932/github/daliuge-new/.venv/bin/dlg [command] --help for more details

.. note:: 

    You may notice above the ``palette`` command. If you have the dlg_paletteGen installed, 
    the ``dlg`` CLI will provide a wrapper around it. 
