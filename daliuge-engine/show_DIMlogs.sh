# This utility script just shows the Node Manager log file of a locally running docker-engine
docker exec -ti daliuge-engine /bin/bash -c "tail -f /var/dlg_home/logs/dlgDIM.log"