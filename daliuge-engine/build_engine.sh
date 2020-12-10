#!/bin/bash

# Go!
docker build --no-cache -t icrar/daliuge-engine:ray -f docker/Dockerfile .
