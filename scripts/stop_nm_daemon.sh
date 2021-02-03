# This script stops the node-manager on an already running daliuge-engine docker container.
# NOTE: This only applies to a node manager started in daemon mode.
echo "Starting the DALiuGE Node manager in interactive mode:"
docker exec -ti daliuge-engine bash -c "dlg nm -s"
