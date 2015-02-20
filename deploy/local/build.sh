#!/bin/bash
# setup python path to src
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
pushd ./ >> /dev/null
cd $DIR
cd ../../
export PYTHONPATH=$(pwd)/src/:$PYTHONPATH
popd >> /dev/null

# compile
python -m py_compile ../../src/dfms/*.py
exit $?



