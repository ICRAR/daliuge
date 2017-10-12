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
cd $WORKSPACE

if [[ $(hostname -s) = galaxy-? ]]; then
    module load virtualenv
fi

mkdir ${WORKSPACE}/${ENVDIR}
virtualenv -p python2.7 ${WORKSPACE}/${ENVDIR}
cd ${WORKSPACE}/${ENVDIR}/bin
if [ $? -ne 0 ]; then
    echo "Error: Failed to chdir to  ${WORKSPACE}/${ENVDIR}/bin"
fi

source ./activate
pip install --trusted-host pypi.python.org python-daemon


cd $WORKSPACE/${TOPDIR}
if [ $? -ne 0 ]; then
    echo "Error: Failed to chdir to  ${WORKSPACE}/${TOPDIR}"
fi

#
#
#
#
#
pip install --trusted-host pypi.python.org .

if [ $? -ne 0 ]; then
    echo "Error: installation failed"
fi

cd ${WORKSPACE}/${ENVDIR}/bin
deactivate

