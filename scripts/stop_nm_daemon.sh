# This script stops the node-manager on an already running daliuge-engine docker container.
# NOTE: This only applies to a node manager started in daemon mode.
echo "Stopping the DALiuGE Node manager in interactive mode:"
docker exec daliuge-engine bash -c "dlg nm -s"
