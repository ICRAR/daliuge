docker run --shm-size=1g --ipc="shareable" --rm -td --name daliuge-engine -v /var/dlg_home:/var/dlg_home -v /var/run/docker.sock:/var/run/docker.sock -p 5555:5555 -p 6666:6666 -p 8000:8000 -p 8001:8001 -p 8002:8002 -p 9000:9000  icrar/daliuge-engine:ray
sleep2
./start_local_managers.sh
# start the plasma store. NOTE: the container has been started with shareable memory
# which means that the plasma store is accessible across containers on the same host.
plasma_store_server -m 1000000000 -s /var/dlg_home/tmp/plasma 1> /dev/null 2> /dev/null &
