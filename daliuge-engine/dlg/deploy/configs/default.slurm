#!/bin/bash --login

#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=2
#SBATCH --job-name=DALiuGE-$SESSION_ID
#SBATCH --time=30
#SBATCH --error=err-%j.log
export DLG_ROOT=$DLG_ROOT

VENV=$DLG_ROOT/venv