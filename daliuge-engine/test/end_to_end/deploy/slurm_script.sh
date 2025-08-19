#!/bin/bash --login

#SBATCH --nodes=6
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=2
#SBATCH --job-name=DALiuGE-SLURM_TestSession
#SBATCH --time=00:45:00
#SBATCH --error=err-%j.log

module use /group/askap/modulefiles
module load singularity/4.1.0-mpi
module load py-mpi4py/3.1.5-py3.11.6
module load py-numpy/1.26.4

export DLG_ROOT=/scratch/pawsey0411/test/dlg
source /software/projects/pawsey0411/venv/bin/activate

srun -l python3 -m dlg.deploy.start_dlg_cluster --log_dir /scratch/pawsey0411/test/dlg/workspace/SLURM_TestSession --physical-graph "/scratch/pawsey0411/test/dlg/workspace/SLURM_TestSession/SLURM_HelloWorld_simplePG.graph"   --verbose-level 1  --max-threads 0 --app 0 --num_islands 1   --ssid SLURM_TestSession
