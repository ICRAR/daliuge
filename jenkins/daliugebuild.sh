#!/bin/bash -l

if [ $# -gt 0 ]; then
    TOPDIR=$1
else
    TOPDIR=daliuge
    ENVDIR=daliuge_env
fi
#
# first we need to build daliuge
# via a virtualenv
# 
# test are we on galaxy
if [[ $(hostname -s) = galaxy-? ]]; then
    module load virtualenv
fi

mkdir ${WORKSPACE}/${ENVDIR}
virtualenv ${ENVDIR}
cd ${WORKSPACE}/${ENVDIR}/bin
if [ $? -ne 0 ]; then
    echo "Error: Failed to chdir to  ${WORKSPACE}/${ENVDIR}/bin"
    exit 1
fi

source activate

cd $WORKSPACE/${TOPDIR}
if [ $? -ne 0 ]; then
    echo "Error: Failed to chdir to  ${WORKSPACE}/${TOPDIR}"
    exit 1
fi

#
#
#
#
#
python setup.py install
if [ $? -ne 0 ]; then
    echo "Error: installation failed"
    exit 1
fi

cd ${WORKSPACE}/${ENVDIR}/bin
deactivate

