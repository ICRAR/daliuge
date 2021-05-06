# This script starts the node-manager on an already running daliuge-engine docker container.
echo "Starting the DALiuGE Node manager in interactive mode:"
docker exec -ti daliuge-engine bash -c "dlg nm -vv -H 0.0.0.0 --dlg-path=/var/dlg_home/code -w /var/dlg_home/workspace -l /var/dlg_home/logs"
