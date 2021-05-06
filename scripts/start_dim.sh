# This script starts the island-manager on an already running daliuge-engine docker container.
echo "Starting the DALiuGE Data Island manager"
docker exec -ti daliuge-engine bash -c "dlg dim -d -H 0.0.0.0 -N localhost -w /var/dlg_home/workspace -l /var/dlg_home/logs"
