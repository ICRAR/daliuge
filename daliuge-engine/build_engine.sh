#!/bin/bash
# script builds the daliuge-engine docker container either with a tag referring to the current
# branch name or with a release tag depending whether this is a development or deployment
# version.

case "$1" in
    "dep")
        export VCS_TAG=`git describe --tags --abbrev=0|sed s/v//`
        echo "Building daliuge-engine version using tag ${VCS_TAG}"
        docker build --build-arg VCS_TAG=${VCS_TAG} --no-cache -t icrar/daliuge-engine:${VCS_TAG} -f docker/Dockerfile .
        echo "Build finished!"
        exit 1 ;;
    "dev")
        export VCS_TAG=`git rev-parse --abbrev-ref HEAD | tr '[:upper:]' '[:lower:]'`
        echo "Building daliuge-engine development version"
        docker build --build-arg VCS_TAG=${VCS_TAG} --no-cache -t icrar/daliuge-engine:${VCS_TAG} -f docker/Dockerfile .
        echo "Build finished!"
        exit 1;;
    "casa")
        export VCS_TAG=`git rev-parse --abbrev-ref HEAD | tr '[:upper:]' '[:lower:]'`
        echo "Building daliuge-engine development version"
        docker build --build-arg VCS_TAG=${VCS_TAG}-casa --no-cache -t icrar/daliuge-engine:${VCS_TAG}-casa -f docker/Dockerfile.casa .
        echo "Build finished!"
        exit 1;;
    *)
        echo "Usage: build_engine.sh <dep|dev>"
        exit 1;;
esac
