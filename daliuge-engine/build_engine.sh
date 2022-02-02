#!/bin/bash
# script builds the daliuge-engine docker container either with a tag referring to the current
# branch name or with a release tag depending whether this is a development or deployment
# version.

case "$1" in
    "dep")
        export VCS_TAG=`git describe --tags --abbrev=0|sed s/v//`
        echo "Building daliuge-engine version using tag ${VCS_TAG}"
        echo $VCS_TAG > dlg/manager/web/VERSION
        cp ../LICENSE dlg/manager/web/.
        docker build --build-arg VCS_TAG=${VCS_TAG} --no-cache -t icrar/daliuge-engine:${VCS_TAG} -f docker/Dockerfile .
        echo "Build finished!"
        exit 0 ;;
    "dev")
        C_TAG="master"
        [[ ! -z $2 ]] && C_TAG=$2
        export VERSION=`git describe --tags --abbrev=0|sed s/v//`
        export VCS_TAG=`git rev-parse --abbrev-ref HEAD | tr '[:upper:]' '[:lower:]'`
        echo "Building daliuge-engine development version using daliuge-common:${C_TAG}"
        echo "$VERSION:$VCS_TAG" > dlg/manager/web/VERSION
        git rev-parse --verify HEAD >> dlg/manager/web/VERSION
        cp ../LICENSE dlg/manager/web/.
        docker build --build-arg VCS_TAG=${C_TAG} --no-cache -t icrar/daliuge-engine:${VCS_TAG} -f docker/Dockerfile.dev .
        echo "Build finished!"
        exit 0;;
    "devall")
        C_TAG="master"
        [[ ! -z $2 ]] && C_TAG=$2
        export VERSION=`git describe --tags --abbrev=0|sed s/v//`
        export VCS_TAG=`git rev-parse --abbrev-ref HEAD | tr '[:upper:]' '[:lower:]'`
        echo "Building daliuge-engine development version using daliuge-common:${C_TAG}"
        echo "$VERSION:$VCS_TAG" > dlg/manager/web/VERSION
        git rev-parse --verify HEAD >> dlg/manager/web/VERSION
        cp ../LICENSE dlg/manager/web/.
        docker build --build-arg VCS_TAG=${C_TAG} --no-cache -t icrar/daliuge-engine:${VCS_TAG} -f docker/Dockerfile.devall .
        echo "Build finished!"
        exit 0;;
    "casa")
        export VCS_TAG=`git rev-parse --abbrev-ref HEAD | tr '[:upper:]' '[:lower:]'`
        echo "Building daliuge-engine development version"
        docker build --build-arg VCS_TAG=${VCS_TAG}-casa --no-cache -t icrar/daliuge-engine:${VCS_TAG}-casa -f docker/Dockerfile.casa .
        echo "Build finished!"
        exit 0;;
    *)
        echo "Usage: build_engine.sh <dep|dev|devall|casa>"
        exit 0;;
esac
