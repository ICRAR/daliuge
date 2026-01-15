#!/bin/bash --login

#SBATCH --account=pawsey0411
#SBATCH --nodes=$NUM_NODES
#SBATCH --job-name=DALiuGE-$SESSION_ID
#SBATCH --time=$JOB_DURATION
#SBATCH --error=logs/err-%j.log
#SBATCH --mem=16G

newgrp pawsey0411
umask 002

export DLG_ROOT=$DLG_ROOT
$MODULES
$VENV
