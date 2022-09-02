#!/bin/env bash
# This utlity script stops a locally running translator
MYPID=$$
echo $MYPID
case "$1" in
    "docker")
        docker stop daliuge-translator;;
    "local")
        DLG_ROOT="${DLG_ROOT:-$HOME/dlg}"
        echo "Stopping translator.."
        if [[ $DLG_ROOT/pid/dlgLGWEB.pid ]]
        then
            cat $DLG_ROOT/pid/dlgLGWEB.pid | xargs kill -HUP
        fi
        ;;
    *)
        echo "Usage: stop_translator.sh <docker|local>"
        echo "       default: docker"
        docker stop daliuge-translator;;
esac