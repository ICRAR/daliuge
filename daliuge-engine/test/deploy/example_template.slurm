#!/bin/bash --login

#SBATCH --nodes=16
#SBATCH --ntasks-per-node=2
#SBATCH --cpus-per-task=4
#SBATCH --mem=0
#SBATCH --job-name=DALiuGE-$SESSION_ID
#SBATCH --time=00:60:00
#SBATCH --error=err-%j.log
export DLG_ROOT=$DLG_ROOT

VENV=$DLG_ROOT/venv