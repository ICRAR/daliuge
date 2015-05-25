#!/bin/bash

file_tsm=$scratch/data/1067892840_tsm.ms
file_adios=$scratch/stripe1/1067892840_adios.ms

rm -rf $file_adios

aprun -B $SLURM_SUBMIT_DIR/ms_converter $file_tsm $file_adios

