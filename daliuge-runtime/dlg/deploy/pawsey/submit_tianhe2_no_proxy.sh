#!/bin/bash 


# the pip-installed mpi4py module in the virtual_env somehow does not work with the
# Magnus MPI environment, so we need to load the system mpi4py module, which means
# to load system python 2.7 module first to make sure everything is in 2.7

source /HOME/ac_shao_tan_1/lbq/bashrc

DLG_MON_HOST="sdp-dfms.ddns.net"
DLG_MON_PORT="8098"
APP_ROOT="/HOME/ac_shao_tan_1/lbq/dfms/dfms/deploy/tianhe"
SID=$(date +"%Y-%m-%d_%H-%M-%S")
LOG_DIR=$APP_ROOT"/logs/"$SID
mkdir $LOG_DIR # to remove potential directory creation conflicts later
#FILENAME="/HOME/ac_shao_tan_1/lbq/dfms/dfms/deploy/tianhe/th_test.json"
GRAPH_ID="0"
CLUSTER="Tianhe2"
#mpirun -np 3 /HOME/ac_shao_tan_1/lbq/lib/python/bin/python $APP_ROOT"/start_dfms_th.py" -l $LOG_DIR -m $DLG_MON_HOST -o $DLG_MON_PORT -f $FILENAM
#mpirun -np 1 /HOME/ac_shao_tan_1/lbq/lib/python/bin/python $APP_ROOT"/start_dfms_th.py" -l $LOG_DIR -g $GRAPH_ID 

mpirun -n 10 -N 1 /HOME/ac_shao_tan_1/lbq/lib/python/bin/python $APP_ROOT"/start_dfms_cluster.py" -l $LOG_DIR -g $GRAPH_ID -d -c $CLUSTER
