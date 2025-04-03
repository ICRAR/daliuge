#!/usr/bin/env bash
if [ $2 ]
then
	export VCS_TAG=$2
	export C_TAG=$VCS_TAG
else
	export VCS_TAG=`git describe --tags --abbrev=0 --always|sed s/v//`
	export C_TAG=`git rev-parse --abbrev-ref HEAD | tr '[:upper:]' '[:lower:]'`
	if [ $C_TAG=="master" ]; then VCS_TAG=$C_TAG; fi
fi
case "$1" in
    "dep")
        echo "Running Translator deployment version in background..."
        docker run -h dlg-trans --name daliuge-translator --rm -td -p 8084:8084 icrar/daliuge-translator:${VCS_TAG}
        sleep 3
        docker exec -u root daliuge-translator bash -c "service avahi-daemon stop > /dev/null 2>&1 && service dbus restart > /dev/null 2>&1 && service avahi-daemon start > /dev/null 2>&1";;
    "dev")
        echo "Running Translator development version in background..."
        docker run -d -h dlg-trans --volume $PWD/dlg/dropmake:/dlg/lib/python3.8/site-packages/dlg/dropmake --name daliuge-translator --rm -t -p 8084:8084 icrar/daliuge-translator:${C_TAG}
        sleep 3
        docker exec -u root daliuge-translator bash -c "service avahi-daemon stop > /dev/null 2>&1 && service dbus restart > /dev/null 2>&1 && service avahi-daemon start > /dev/null 2>&1";;
    "slim")
        echo "Running Translator development version in background..."
        docker run -d -h dlg-trans --volume $PWD/dlg/dropmake:/dlg/lib/python3.8/site-packages/dlg/dropmake --name daliuge-translator --rm -t -p 8084:8084 icrar/daliuge-translator.slim:${VCS_TAG}
        TRANS_IP=`docker exec daliuge-translator sh -c "hostname --ip-address"`
        echo "Translator URL: http://${TRANS_IP}:8084"
        exit 0;;
    "casa")
        export VCS_TAG=`git rev-parse --abbrev-ref HEAD| tr '[:upper:]' '[:lower:]'`-casa
        echo "Running Translator development version in foreground..."
        docker run -h dlg-trans --volume $PWD/dlg/dropmake:/dlg/lib/python3.8/site-packages/dlg/dropmake --name daliuge-translator --rm -t -p 8084:8084 icrar/daliuge-translator:${VCS_TAG}
        sleep 3
        docker exec -u root daliuge-translator bash -c "service avahi-daemon stop > /dev/null 2>&1 && service dbus restart > /dev/null 2>&1 && service avahi-daemon start > /dev/null 2>&1";;
    *)
        echo "Usage run_translator.sh <dep|dev|slim|casa>"
        exit 0;;
esac
sleep 3
TRANS_NAME=`docker exec daliuge-translator sh -c "hostname"`
TRANS_IP=`docker exec daliuge-translator sh -c "hostname --ip-address"`
echo "Translator URL: http://${TRANS_NAME}:8084"
