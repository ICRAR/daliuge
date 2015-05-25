#!/bin/bash
#PBS -l walltime=12:00:00
#PBS -l nodes=1:ppn=12
#PBS -l mem=100gb

file_tsm=/scratch/jason/1067892840_tsm.ms
file_adios=/scratch/jason/1067892840_adios.ms

rm -rf $file_adios

/home/jason/adiosStMan/tools/ms_converter $file_tsm $file_adios

