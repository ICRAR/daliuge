#!/bin/bash

# Make sure we're standing in the correct place
dir=$(dirname $0)
cd $dir

# Go!
if [ -f ../../setup.py ]; then
    cd ../..
    docker build --no-cache -t dfms/centos7:latest -f docker/dfms/Dockerfile_incontext .
else
    docker build --no-cache -t dfms/centos7:latest .
fi

