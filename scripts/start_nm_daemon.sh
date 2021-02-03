# This script starts the node-manager on an already running daliuge-engine docker container
# in DAEMON mode.
echo "Starting the DALiuGE Node manager in interactive mode:"
docker exec -ti daliuge-engine bash -c "dlg nm -vd -H 0.0.0.0 --dlg-path=/var/dlg_home/code"
