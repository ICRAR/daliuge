# run the daliuge engine on docker with all the required ports mapped and volumes mounted. 
# The /var/dlg_home mount allows to share data back to the host and is also used as a way
# to get additional component code exposed to the engine (see the start_nm.sh script).
# The container is started in daemon mode, but there is not too much to see anyway.
#
# After this typically one would run the start_managers.sh script.
echo "Starting the DALiuGE engine container"
docker run --rm -td --name daliuge-engine -v /var/run/docker.sock:/var/run/docker.sock -v /var/dlg_home:/var/dlg_home -p 5555:5555 -p 6666:6666 -p 8000:8000 -p 8001:8001 -p 8002:8002 -p 9000:9000  icrar/dlg_ray:1.1 > /dev/null
