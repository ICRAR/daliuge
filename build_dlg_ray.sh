#!/bin/bash

# Helper script for building the DALiuGE Ray docker image.
# For more details please refer to the Dockerfile
docker build --no-cache -t icrar/dlg_ray:1.5 .
