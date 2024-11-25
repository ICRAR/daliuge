#!/bin/bash --login

#SBATCH --nodes=16
#SBATCH --ntasks-per-node=2
#SBATCH --cpus-per-task=4
#SBATCH --mem=0
#SBATCH --job-name=DALiuGE-SLURM_TestSession
#SBATCH --time=00:45:00
#SBATCH --error=err-%j.log

export DLG_ROOT=/scratch/pawsey0411/test/dlg
source /software/projects/pawsey0411/venv/bin/activate

srun -l python3 -m dlg.deploy.start_dlg_cluster --log_dir /scratch/pawsey0411/test/dlg/workspace/SLURM_TestSession --physical-graph "/scratch/pawsey0411/test/dlg/workspace/SLURM_TestSession/SLURM_HelloWorld_simplePG.graph"   --verbose-level 1  --max-threads 0 --app 0 --num_islands 1   --ssid SLURM_TestSession