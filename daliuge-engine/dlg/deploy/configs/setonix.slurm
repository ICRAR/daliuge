#!/bin/bash --login

#SBATCH --nodes=2
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=2
#SBATCH --job-name=DALiuGE-$SESSION_ID
#SBATCH --time=00:45:00
#SBATCH --error=err-%j.log

export DLG_ROOT=$DLG_ROOT
source /software/projects/pawsey0411/venv/bin/activate