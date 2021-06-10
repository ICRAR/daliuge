#!/bin/bash
# script builds the daliuge-translator docker container either with a tag referring to the current
# branch name or with a release tag depending whether this is a development or deployment
# version.

case "$1" in
    "dep")
        export VCS_TAG=`git describe --tags --abbrev=0|sed s/v//`
        echo "Building daliuge-translator version ${VCS_TAG}"
        docker build --build-arg VCS_TAG=${VCS_TAG} --no-cache -t icrar/daliuge-translator:${VCS_TAG} -f docker/Dockerfile .
        echo "Build finished!"
        exit 1 ;;
    "dev")
        export VCS_TAG=`git rev-parse --abbrev-ref HEAD | tr '[:upper:]' '[:lower:]'`
        echo "Building daliuge-translator development version using tag ${VCS_TAG}"
        # The complete casa and arrow installation is only required for the Plasma streaming
        # and should not go much further.
        docker build --build-arg VCS_TAG=${VCS_TAG} --no-cache -t icrar/daliuge-translator:${VCS_TAG} -f docker/Dockerfile .
        echo "Build finished!"
        exit 1;;
    *)
        echo "Usage: build_translator.sh <dep|dev>"
        exit 1;;
esac
