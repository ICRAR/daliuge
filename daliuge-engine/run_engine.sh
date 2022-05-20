#!/bin/bash
DOCKER_OPTS="\
--shm-size=2g --ipc=shareable \
--rm \
$([[ $(nvidia-docker version) ]] && echo '--gpus=all' || echo '') \
--name daliuge-engine \
-v /var/run/docker.sock:/var/run/docker.sock \
-p 5555:5555 -p 6666:6666 \
-p 8000:8000 -p 8001:8001 \
-p 8002:8002 -p 9000:9000 \
--user $(id -u):$(id -g) \
" 
common_prep ()
{
    mkdir -p ${DLG_ROOT}/workspace
    mkdir -p ${DLG_ROOT}/testdata
    mkdir -p ${DLG_ROOT}/code
    # get current user and group id and prepare passwd and group files
    DOCKER_GID=`python3 -c "from dlg.prepareUser import prepareUser; print(prepareUser(DLG_ROOT='${DLG_ROOT}'))"`
    DOCKER_OPTS=${DOCKER_OPTS}" --group-add ${DOCKER_GID}"
    DOCKER_OPTS=${DOCKER_OPTS}" -v ${DLG_ROOT}/workspace/settings/passwd:/etc/passwd"
    DOCKER_OPTS=${DOCKER_OPTS}" -v ${DLG_ROOT}/workspace/settings/group:/etc/group"
    DOCKER_OPTS=${DOCKER_OPTS}" -v ${DLG_ROOT}:${DLG_ROOT} --env DLG_ROOT=${DLG_ROOT}"
}

export VCS_TAG=`git describe --tags --abbrev=0|sed s/v//`
export C_TAG=`git rev-parse --abbrev-ref HEAD | tr '[:upper:]' '[:lower:]'`
case "$1" in
    "dep")
        DLG_ROOT="/var/dlg_home"
        if [ ! -d ${DLG_ROOT} ]
        then
            echo "Deployment version requires access to a directory /var/dlg_home, but that does not exist!"
            echo "Please either create and grant access to $USER or build and run the development version."
        else
            common_prep
            echo "Running Engine deployment version in background..."
            echo "docker run -td "${DOCKER_OPTS}"  icrar/daliuge-engine:${VCS_TAG}"
            docker run -td ${DOCKER_OPTS}  icrar/daliuge-engine:${VCS_TAG}
            exit 0
        fi;;
    "dev")
        export DLG_ROOT="$HOME/dlg"
        common_prep
        echo "Running Engine development version in background..."
        echo "docker run -td ${DOCKER_OPTS}  icrar/daliuge-engine:${C_TAG}"
        docker run -td ${DOCKER_OPTS}  icrar/daliuge-engine:${C_TAG}
        sleep 3
        ./start_local_managers.sh
        exit 0;;
    "casa")
        DLG_ROOT="/tmp/dlg"
        export VCS_TAG=`git rev-parse --abbrev-ref HEAD | tr '[:upper:]' '[:lower:]'`
        echo "Running Engine development version in background..."
        common_prep
        CONTAINER_NM="icrar/daliuge-engine:${VCS_TAG}-casa"
        echo "docker run -td ${DOCKER_OPTS}  ${CONTAINER_NM}"
        docker run -td ${DOCKER_OPTS}  ${CONTAINER_NM}
        sleep 3
        ./start_local_managers.sh
        exit 0;;
    "slim")
        export DLG_ROOT="$HOME/dlg"
        export VCS_TAG=`git rev-parse --abbrev-ref HEAD | tr '[:upper:]' '[:lower:]'`
        common_prep
        echo "Running Engine development version in background..."
        echo "docker run -td ${DOCKER_OPTS}  icrar/daliuge-engine.slim:${C_TAG}"
        docker run -td ${DOCKER_OPTS}  icrar/daliuge-engine.slim:${C_TAG}
        sleep 3
        ./start_local_managers.sh
        exit 0;;
    *)
        echo "Usage run_engine.sh <dep|dev|slim>"
        exit 0;;
esac
