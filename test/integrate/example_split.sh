#!/bin/bash
# launch casapy to run the split script, serving as an example ONLY

export CH_CASA_DIR=/home/rdodson/Software/Casa/casa-release-4.3.0-el6
#export CH_CASA_DIR=/home/jason/casa/casa-stable-4.5.37

# PLEASE let CH_SCRIPT_DIR point to where the example_split.py is
export CH_SCRIPT_DIR=/home/sdp/script
#export CH_SCRIPT_DIR=/home/jason/dfms/test/integrate

$CH_CASA_DIR/casapy --nologger -c $CH_SCRIPT_DIR/example_split.py

