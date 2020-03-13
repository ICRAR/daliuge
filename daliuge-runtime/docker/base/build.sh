#!/bin/bash

# Make sure we're standing in the correct place
dir=$(dirname $0)
cd $dir

# We need an SSH private key to serve as the identity
# of the dfms user created inside the container
# If there is none yet, we create one
if [ ! -f dfms_docker.pem ]
then
	ssh-keygen -t rsa -N '' -f dfms_docker.pem
fi

# Go!
docker build -t dfms/centos7:base .
