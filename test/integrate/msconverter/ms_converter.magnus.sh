#!/bin/sh 


for i in $(seq 8 -1 1)
do
	sbatch -p debugq --account=$project --time=1:00:00 --nodes=$i --ntasks-per-node=24 --dependency=singleton ms_converter.sh
done





