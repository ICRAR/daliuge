#!/bin/env bash
case "$1" in
    "dep")
        VCS_TAG=`git describe --tags --abbrev=0|sed s/v//`
        echo "Running Translator deployment version in background..."
        docker run --name daliuge-translator --rm -td -p 8084:8084 icrar/daliuge-translator:${VCS_TAG}
        exit 0;;
    "dev")
        export VCS_TAG=`git rev-parse --abbrev-ref HEAD| tr '[:upper:]' '[:lower:]'`
        echo "Running Translator development version in foreground..."
        docker run --volume $PWD/dlg/dropmake:/dlg/lib/python3.8/site-packages/dlg/dropmake --name daliuge-translator --rm -t -p 8084:8084 icrar/daliuge-translator:${VCS_TAG}
        exit 0;;
    "casa")
        export VCS_TAG=`git rev-parse --abbrev-ref HEAD| tr '[:upper:]' '[:lower:]'`-casa
        echo "Running Translator development version in foreground..."
        docker run --volume $PWD/dlg/dropmake:/dlg/lib/python3.8/site-packages/dlg/dropmake --name daliuge-translator --rm -t -p 8084:8084 icrar/daliuge-translator:${VCS_TAG}
        exit 0;;
    "local")
        DLG_ROOT="${DLG_ROOT:-$HOME/dlg}"
        res="$(dlg lgweb -h| grep -c 'Unknown command')"
        if [[ $res == 1 ]]
        then
            echo "dlg lgweb command could not be found!"
            echo "Please install daliuge-translator."
            exit
        fi
        echo "Starting translator locally in foreground.."
        dlg lgweb -vv -d /tmp -t /tmp > $DLG_ROOT/logs/dlgLGWEB.log 2>&1 &
        mkdir -p $DLG_ROOT/pid
        echo $! > $DLG_ROOT/pid/dlgLGWEB.pid
        echo
        echo "Use any of the following URLs to access the DIM:"
        python -c "from dlg.utils import get_local_ip_addr; print([f'http://{addr}:8084' for addr,name in get_local_ip_addr() if not name.startswith('docker')])"
        echo "Log file can be found in ${DLG_ROOT}/log/dlgLGWEB.log"
        ;;
    *)
        echo "Usage run_translator.sh <dep|dev|casa|local>"
        exit 0;;
esac
