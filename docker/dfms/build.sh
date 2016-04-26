#!/bin/bash

# Make sure we're standing in the correct place
dir=$(dirname $0)
cd $dir

# Go!
docker build --no-cache -t dfms/centos7:latest .
