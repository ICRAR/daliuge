#!/usr/bin/env bash
# This utlity script stops a locally running docker-engine container
case "$1" in
    "docker")
        docker stop daliuge-engine;;
    "local")
        echo "Stopping managers.."
        dlg nm -s
        dlg dim -s;;
    *)
        echo "Usage: stop_engine.sh <docker|local>"
        echo "       default: docker"
        docker stop daliuge-engine;;
esac
