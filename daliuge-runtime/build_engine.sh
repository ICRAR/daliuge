#!/bin/bash

# Go!
docker build --no-cache -t icrar/daliuge-engine:latest -f docker/exec-engine/Dockerfile .
