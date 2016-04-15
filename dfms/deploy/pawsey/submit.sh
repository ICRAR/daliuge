#!/bin/bash --login

#SBATCH --nodes=5
#SBATCH --ntasks-per-node=1
#SBATCH --job-name=dfms_deployment
#SBATCH --time=00:02:00
#SBATCH --account=pawsey0129
#SBATCH --error=err-%j.log

module swap PrgEnv-cray PrgEnv-gnu
module load python/2.7.10
# the pip-installed mpi4py module in the virtual_env somehow does not work with the
# Magnus MPI environment, so we need to load the system mpi4py module, which means
# to load system python 2.7 module first to make sure everything is in 2.7
module load mpi4py
DFMS_MON_HOST="sdp-dfms.ddns.net"
DFMS_MON_PORT="8098"
APP_ROOT="/scratch/pawsey0129/dfms"
SID=$(date +"%Y-%m-%d_%H-%M-%S")
LOG_DIR=$APP_ROOT"/logs/"$SID
mkdir $LOG_DIR # to remove potential directory creation conflicts later
aprun -B $APP_ROOT"/pyenv/bin/python" $APP_ROOT"/cluster/start_dfms_cluster.py" -l $LOG_DIR -m $DFMS_MON_HOST -o $DFMS_MON_PORT
