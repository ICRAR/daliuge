DOCKER_OPTS="\
--shm-size=1g --ipc=shareable \
--rm \
--name daliuge-engine \
-v /var/run/docker.sock:/var/run/docker.sock \
-p 5555:5555 -p 6666:6666 \
-p 8000:8000 -p 8001:8001 \
-p 8002:8002 -p 9000:9000 \
" 

case "$1" in
    "dep")
        VCS_TAG=`git describe --tags --abbrev=0|sed s/v//`
        DOCKER_OPTS+="-v /var/dlg_home:/var/dlg_home"
        echo "Running Engine deployment version in background..."
        echo "docker run -td "${DOCKER_OPTS}"  icrar/daliuge-engine:${VCS_TAG}"
        docker run -td ${DOCKER_OPTS}  icrar/daliuge-engine:${VCS_TAG}
        exit 1;;
    "dev")
        export VCS_TAG=`git rev-parse --abbrev-ref HEAD | tr '[:upper:]' '[:lower:]'`
        echo "Running Engine development version in background..."
        mkdir -p /tmp/.dlg/var/dlg_home/workspace
        mkdir -p /tmp/.dlg/var/dlg_home/testdata
        mkdir -p /tmp/.dlg/var/dlg_home/code
        DOCKER_OPTS+="-v ${PWD}/dlg/manager:/root/dlg/lib/python3.8/site-packages/dlg/manager"
        DOCKER_OPTS+=" -v /tmp/.dlg/var/dlg_home:/tmp/.dlg/var/dlg_home"
        echo "docker run -td ${DOCKER_OPTS}  icrar/daliuge-engine:${VCS_TAG}"
        docker run -td ${DOCKER_OPTS}  icrar/daliuge-engine:${VCS_TAG}
#        docker run -td ${DOCKER_OPTS} icrar/dlg-engine:casa
        sleep 3
        ./start_local_managers.sh
        exit 1;;
    *)
        echo "Usage run_engine.sh <dep|dev>"
        exit 1;;
esac
