#!/bin/bash --login

#SBATCH --nodes=6
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=2
#SBATCH --job-name=DALiuGE-EAGLE_TestSession
#SBATCH --time=00:60:00
#SBATCH --error=err-%j.log

export DLG_ROOT=/scratch/pawsey0411/$USER/dlg
source /software/projects/pawsey0411/venv/bin/activate

srun -l python3 -m dlg.deploy.start_dlg_cluster --log_dir /scratch/pawsey0411/$USER/dlg/workspace//home/00087932/github/EAGLE_TestSession --physical-graph "/home/00087932/github/EAGLE_test_repo/eagle_test_graphs/daliuge_tests/engine/graphs/SLURM_HelloWorld_simplePG.graph"   --verbose-level 1  --max-threads 0 --app 0 --num_islands 1   --ssid EAGLE_TestSession