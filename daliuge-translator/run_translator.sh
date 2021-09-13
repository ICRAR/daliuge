case "$1" in
    "dep")
        VCS_TAG=`git describe --tags --abbrev=0|sed s/v//`
        echo "Running Translator deployment version in background..."
        docker run --name daliuge-translator --rm -td -p 8084:8084 icrar/daliuge-translator:${VCS_TAG}
        exit 1;;
    "dev")
        export VCS_TAG=`git rev-parse --abbrev-ref HEAD| tr '[:upper:]' '[:lower:]'`
        echo "Running Translator development version in foreground..."
        docker run --volume $PWD/dlg/dropmake:/root/dlg/lib/python3.8/site-packages/dlg/dropmake --name daliuge-translator --rm -td -p 8084:8084 icrar/daliuge-translator:${VCS_TAG}
        exit 1;;
    "casa")
        export VCS_TAG=`git rev-parse --abbrev-ref HEAD| tr '[:upper:]' '[:lower:]'`-casa
        echo "Running Translator development version in foreground..."
        docker run --volume $PWD/dlg/dropmake:/root/dlg/lib/python3.8/site-packages/dlg/dropmake --name daliuge-translator --rm -t -p 8084:8084 icrar/daliuge-translator:${VCS_TAG}
        exit 1;;
    *)
        echo "Usage run_translator.sh <dep|dev|casa>"
        exit 1;;
esac