DOCKER_OPTS="\
--shm-size=1g --ipc=shareable \
--rm \
--name daliuge-engine \
-v /var/dlg_home:/var/dlg_home \
-v /var/run/docker.sock:/var/run/docker.sock \
-p 5555:5555 -p 6666:6666 \
-p 8000:8000 -p 8001:8001 \
-p 8002:8002 -p 9000:9000 \
" 

case "$1" in
    "dep")
        VCS_TAG=`git describe --tags --abbrev=0|sed s/v//`
        echo "Running Engine deployment version in background..."
        echo "docker run -td "${DOCKER_OPTS}"  icrar/daliuge-engine:${VCS_TAG}"
        docker run -td ${DOCKER_OPTS}  icrar/daliuge-engine:${VCS_TAG}
        exit 1;;
    "dev")
        export VCS_TAG=`git rev-parse --abbrev-ref HEAD | tr '[:upper:]' '[:lower:]'`
        echo "Running Engine development version in foreground..."
        DOCKER_OPTS+="-v ${PWD}/dlg/manager:/root/dlg/lib/python3.8/site-packages/dlg/manager"
        echo "docker run -ti ${DOCKER_OPTS}  icrar/daliuge-engine:${VCS_TAG}"
        docker run -ti ${DOCKER_OPTS}  icrar/daliuge-engine:${VCS_TAG} bash
        exit 1;;
    *)
        echo "Usage run_engine.sh <dep|dev>"
        exit 1;;
esac

# TODO: The following command just leaves a defunct process, but not the plasma store running (problem of quoting??)
# docker  exec -t daliuge-engine bash -c 'plasma_store -m 600000000 -s /var/dlg_home/tmp/plasma 1> /var/dlg_home/logs/plasma 2> /var/dlg_home/logs/plasma &'
