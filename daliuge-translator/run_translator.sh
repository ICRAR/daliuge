case "$1" in
    "dep")
        VCS_TAG=`git describe --tags --abbrev=0|sed s/v//`
        echo "Running Translator deployment version in background..."
        docker run --name daliuge-translator --rm -t -p 8084:8084 icrar/daliuge-translator:ray
        exit 1;;
    "dev")
        export VCS_TAG=`git rev-parse --abbrev-ref HEAD`
        echo "Running Translator development version in foreground..."
        docker run --volume $PWD/dlg/dropmake:/home/ray/dlg/lib/python3.8/site-packages/dlg/dropmake --name daliuge-translator --rm -t -p 8084:8084 icrar/daliuge-translator:ray
        exit 1;;
    *)
        echo "Usage run_translator.sh <dep|dev>"
        exit 1;;
esac