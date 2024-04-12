#!/usr/bin/env bash
case "$1" in
    "dep")
        VCS_TAG=`git describe --tags --abbrev=0|sed s/v//`
        echo "Running Translator deployment version in background..."
        docker run -h dlg-trans --name daliuge-translator --rm -td -p 8084:8084 icrar/daliuge-translator:${VCS_TAG};;
    "dev")
        export VCS_TAG=`git rev-parse --abbrev-ref HEAD| tr '[:upper:]' '[:lower:]'`
        echo "Running Translator development version in foreground..."
        docker run -h dlg-trans --volume $PWD/dlg/dropmake:/dlg/lib/python3.8/site-packages/dlg/dropmake --name daliuge-translator --rm -t -p 8084:8084 icrar/daliuge-translator:${VCS_TAG};;
    "casa")
        export VCS_TAG=`git rev-parse --abbrev-ref HEAD| tr '[:upper:]' '[:lower:]'`-casa
        echo "Running Translator development version in foreground..."
        docker run -h dlg-trans --volume $PWD/dlg/dropmake:/dlg/lib/python3.8/site-packages/dlg/dropmake --name daliuge-translator --rm -t -p 8084:8084 icrar/daliuge-translator:${VCS_TAG};;
    *)
        echo "Usage run_translator.sh <dep|dev|casa>"
        exit 0;;
esac
sleep 3
TRANS_NAME=`docker exec daliuge-translator sh -c "hostname"`
TRANS_IP=`docker exec daliuge-translator sh -c "hostname --ip-address"`
echo "Translator URL: http://${TRANS_NAME}.local:8084"
