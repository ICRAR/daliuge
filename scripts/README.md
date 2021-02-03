# Start and stop scripts

This directory contains a few convenience scripts to start and stop the DALiuGE system on a local machine.

## Starting the whole system

The easiest way to get going is to use the run_system.sh script.

    run_system.sh

This will start the translator (LG2PGT), the DALiuGE daemon, the island manager and the node manager. The node manager will be started interactively with DEBUG level output.

## Starting and stopping the engine

This will start the daemon and the managers, but not the translator. It will start the node manager in interactive mode and after Ctrl+C is pressed everything will be shut-down again.

    ./run_engine

Despite the fact that with Ctrl+C everything is terminated, there is still a separate script to shutdown the engine.

    ./stop_engine

## Starting single parts

All the parts can be also started individually.

### Starting and stopping the translator

    ./start_translator.sh
    ./stop_translator.sh

### Starting and stopping the daemon

    ./start_daemon.sh

Stopping the daemon is equivalent to stopping the engine

    ./stop_engine.sh

### Starting the Data Island and Node Manager

    ./start_dim.sh
    ./start_nm.sh

The start_nm.sh script is starting the manager in interactive mode with log output to the screen and thus will need to be stopped using Ctrl+C. To start the node manager in daemon mode please use:

    ./start_nm_daemon


Stop the managers using:

    ./stop_dim.sh
    ./stop_nm_daemon.sh
