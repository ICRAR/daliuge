# This is the master script starting everything required to have the DALiuGE engine
# operational using docker containers. Since the node manager is started interactively
# the log-output will be displayed on the screen. When aborting the command with
# Ctrl+C the container will be stopped and removed as well.
./start_daemon.sh
sleep 1
./start_managers.sh
./stop_engine.sh
