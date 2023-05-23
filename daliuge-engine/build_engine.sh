#!/usr/bin/env bash
# script builds the daliuge-engine docker container either with a tag referring to the current
# branch name or with a release tag depending whether this is a development or deployment
# version.

export VCS_TAG=`git describe --tags --abbrev=0|sed s/v//`
export DEV_TAG=`git rev-parse --abbrev-ref HEAD | tr '[:upper:]' '[:lower:]'`

case "$1" in
    "dep")
        echo "Building daliuge-engine version using tag ${VCS_TAG}"
        echo $VCS_TAG > dlg/manager/web/VERSION
        cp ../LICENSE dlg/manager/web/.
        docker build --build-arg USER=${USER} --build-arg VCS_TAG=${VCS_TAG} --no-cache -t icrar/daliuge-engine:${VCS_TAG} -f docker/Dockerfile .
        echo "Build finished!"
        exit 0 ;;
    "dev")
        C_TAG="master"
        [[ ! -z $2 ]] && C_TAG=$2
        export VERSION=`git describe --tags --abbrev=0|sed s/v//`
        export VCS_TAG=DEV_TAG
        echo "Building daliuge-engine development version using daliuge-common:${C_TAG}"
        echo "$VERSION:$VCS_TAG" > dlg/manager/web/VERSION
        git rev-parse --verify HEAD >> dlg/manager/web/VERSION
        cp ../LICENSE dlg/manager/web/.
        docker build --build-arg USER=${USER} --build-arg VCS_TAG=${C_TAG} --no-cache -t icrar/daliuge-engine:${DEV_TAG} -f docker/Dockerfile.dev .
        echo "Build finished!"
        exit 0;;
    "devall")
        [[ ! -z $2 ]] && C_TAG=$2
        export VERSION=`git describe --tags --abbrev=0|sed s/v//`
        export VCS_TAG=`git rev-parse --abbrev-ref HEAD | tr '[:upper:]' '[:lower:]'`
        echo "Building daliuge-engine development version using daliuge-common:${DEV_TAG}"
        echo "$VERSION:$VCS_TAG" > dlg/manager/web/VERSION
        git rev-parse --verify HEAD >> dlg/manager/web/VERSION
        cp ../LICENSE dlg/manager/web/.
        docker build --build-arg USER=${USER} --build-arg VCS_TAG=${VCS_TAG} --no-cache -t icrar/daliuge-engine:${DEV_TAG} -f docker/Dockerfile.devall .
        echo "Build finished!"
        exit 0;;
    "slim")
        C_TAG="master"
        echo "Building daliuge-engine slim version ${VCS_TAG} using daliuge-common:${VCS_TAG}"
        docker build --build-arg USER=${USER} --build-arg VCS_TAG=${VCS_TAG} --no-cache -t icrar/daliuge-engine.big:${VCS_TAG} -f docker/Dockerfile .
        echo "Build finished! Slimming the image now"
        echo ">>>>> docker-slim output <<<<<<<<<"
        docker run -it --rm -v /var/run/docker.sock:/var/run/docker.sock dslim/docker-slim build --include-shell \
            --include-path /etc --include-path /usr/local/lib --include-path /usr/local/bin --include-path /usr/lib/python3.8 \
            --include-path /usr/lib/python3 --include-path /dlg --include-path /daliuge --publish-exposed-ports=true \
            --http-probe=true --tag=icrar/daliuge-engine:${VCS_TAG}\
            icrar/daliuge-engine.big:${VCS_TAG} \
	    ;;
    *)
        echo "Usage: build_engine.sh <dep|dev|devall|slim>"
        exit 0;;
esac
