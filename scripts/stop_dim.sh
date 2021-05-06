# This script stops the island-manager on an already running daliuge-engine docker container.
echo "Stopping the DALiuGE Data Island manager"
docker exec -ti daliuge-engine bash -c "dlg dim -s"
