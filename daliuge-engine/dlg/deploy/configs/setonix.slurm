#!/bin/bash --login

#SBATCH --account=pawsey0411
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=2
#SBATCH --job-name=DALiuGE-$SESSION_ID
#SBATCH --time=00:45:00
#SBATCH --error=err-%j.log

module use /group/askap/modulefiles
module load singularity/4.1.0-mpi
module load py-mpi4py/3.1.5-py3.11.6
module load py-numpy/1.26.1

export DLG_ROOT=$DLG_ROOT
source /scratch/pawsey0411/rbunney/git/dlg_env/bin/activate