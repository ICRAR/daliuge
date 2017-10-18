#!/bin/bash --login

#SBATCH --nodes=10
#SBATCH --ntasks-per-node=1
#SBATCH --job-name=archive_deployment
#SBATCH --time=02:00:00
#SBATCH --account=mwaops
#SBATCH --error=err-%j.log

module swap PrgEnv-cray PrgEnv-gnu
module load python/2.7.10
# the pip-installed mpi4py module in the virtual_env somehow does not work with the
# Magnus MPI environment, so we need to load the system mpi4py module, which means
# to load system python 2.7 module first to make sure everything is in 2.7
module load mpi4py
DLG_MON_HOST="sdp-dfms.ddns.net"
DLG_MON_PORT="8098"
APP_ROOT="/group/mwaops/cwu/dfms"
SID=$(date +"%Y-%m-%d_%H-%M-%S")
LOG_DIR=$APP_ROOT"/logs/"$SID
mkdir -m $LOG_DIR # to remove potential directory creation conflicts later
aprun -B /home/cwu/dfms_env/bin/python $APP_ROOT"/cluster/start_dfms_cluster.py" -l $LOG_DIR -m $DLG_MON_HOST -o $DLG_MON_PORT
