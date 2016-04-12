#!/bin/bash --login

#SBATCH --nodes=5
#SBATCH --ntasks-per-node=1
#SBATCH --job-name=dfms_deployment
#SBATCH --time=00:10:00
#SBATCH --account=pawsey0129
#SBATCH --error=myjob-%j.err

source /scratch/pawsey0129/dfms/pyenv/bin/activate
module swap PrgEnv-cray PrgEnv-gnu
module load mpi4py

#cur_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
#aprun -B python $cur_dir"/start_dfms_cluster.py"
aprun -B python /scratch/pawsey0129/dfms/cluster/start_dfms_cluster.py
