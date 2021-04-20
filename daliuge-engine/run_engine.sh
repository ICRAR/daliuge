docker run --rm -td --name daliuge-engine -v /var/dlg_home:/var/dlg_home -v /var/run/docker.sock:/var/run/docker.sock -p 5555:5555 -p 6666:6666 -p 8000:8000 -p 8001:8001 -p 8002:8002 -p 9000:9000  icrar/daliuge-engine:ray
sleep 2
./start_local_managers.sh