#!/usr/bin/env bash
###############################################################################
# This script sets up the docker container with the necessary information to
#
#
#
###############################################################################

# Setup DLG ENV Variables
#!/usr/bin/env bash

DOCKER_OPTS="\
--shm-size=2g --ipc=shareable \
--rm \
$([[ $(nvidia-docker version) ]] && echo '--gpus=all' || echo '') \
--name daliuge-engine \
--hostname dlg-engine \
-v /var/run/docker.sock:/var/run/docker.sock \
-p 5555:5555 -p 6666:6666 \
-p 8000:8000 -p 8001:8001 \
-p 8002:8002 -p 9000:9000 \
--user $(id -u):$(id -g) \
"
curl -o prepareUser.py https://raw.githubusercontent.com/ICRAR/daliuge/refs/tags/v4.9.0/daliuge-engine/dlg/prepareUser.py
curl -o passwd.template https://raw.githubusercontent.com/ICRAR/daliuge/refs/tags/v4.9.0/daliuge-engine/dlg/passwd.template
curl -o group.template https://raw.githubusercontent.com/ICRAR/daliuge/refs/tags/v4.9.0/daliuge-engine/dlg/group.template

DLG_ROOT="${DLG_ROOT:-$HOME/dlg}"
mkdir -p ${DLG_ROOT}/workspace
mkdir -p ${DLG_ROOT}/testdata
mkdir -p ${DLG_ROOT}/code
# get current user and group id and prepare passwd and group files
DOCKER_GID=`python3 -c "from prepareUser import prepareUser; print(prepareUser(DLG_ROOT='${DLG_ROOT}'))"`
DOCKER_OPTS=${DOCKER_OPTS}" --group-add ${DOCKER_GID}"
DOCKER_OPTS=${DOCKER_OPTS}" -v ${DLG_ROOT}/workspace/settings/passwd:/etc/passwd"
DOCKER_OPTS=${DOCKER_OPTS}" -v ${DLG_ROOT}/workspace/settings/group:/etc/group"
DOCKER_OPTS=${DOCKER_OPTS}" -v /proc:/proc"
DOCKER_OPTS=${DOCKER_OPTS}" -v ${DLG_ROOT}:${DLG_ROOT} --env DLG_ROOT=${DLG_ROOT}"

export DLG_ROOT=$HOME/dlg
export UID=$(id -u)
export GID=$(id -g)
# Get most recent daliuge-engine image
export TAG="local"

#mkdir -p ${DLG_ROOT}/workspace
#mkdir -p ${DLG_ROOT}/testdata
#mkdir -p ${DLG_ROOT}/code
# Setup workspace

docker compose -f docker/engine-compose.yml up -d

docker exec -u root daliuge-engine bash -c "service avahi-daemon stop > /dev/null 2>&1 && service dbus restart > /dev/null 2>&1 && service avahi-daemon start > /dev/null 2>&1"
ENGINE_NAME=`docker exec daliuge-engine sh -c "hostname"`
ENGINE_IP=`docker exec daliuge-engine sh -c "hostname --ip-address"`
curl -X POST http://${ENGINE_IP}:9000/managers/node/start
curl -X POST http://${ENGINE_IP}:9000/managers/island/start

#curl -X POST http://${ENGINE_IP}:8001/api/node/dlg-engine.local:8000;;
# exit 0
echo
echo "Engine NAME/IP address: http://${ENGINE_NAME}.local:8000 / ${ENGINE_IP}"

