#!/bin/bash --login

#SBATCH --nodes=5
#SBATCH --ntasks-per-node=1
#SBATCH --job-name=dfms_deployment
#SBATCH --time=00:01:00
#SBATCH --account=pawsey0129
#SBATCH --error=myjob-%j.err

#source /scratch/pawsey0129/dfms/pyenv/bin/activate
module swap PrgEnv-cray PrgEnv-gnu
module load mpi4py

#cur_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
#aprun -B python $cur_dir"/start_dfms_cluster.py"
APP_ROOT="/scratch/pawsey0129/dfms"
SID=$(date +"%Y-%m-%d_%H-%M-%S")
LOG_DIR=$APP_ROOT"/logs/"$SID
aprun -B $APP_ROOT"/pyenv/bin/python" $APP_ROOT"/cluster/start_dfms_cluster.py" $LOG_DIR
