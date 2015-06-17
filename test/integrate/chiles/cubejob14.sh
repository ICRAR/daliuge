#####PBS -l nodes=4:ncpus=2,mem=35gb
#####PBS -l walltime=12:00:00
#####PBS -N CHILES
#####PBS -A rdodson
#####PBS -M richard.dodson@icrar.org
#####PBS -m abe
#####PBS -t 0-7
#####PBS -o $PWD/run-12.out
#####PBS -e $PWD/run-12.err

# For non-PBS testing
export PBS_ARRAYID=0
export PBS_JOBID=909999
#
# total number of jobs
export CH_NUM_JOB=1 ## Should match total from ARRAY (-t) line 
#
# each run has a unique id (converted from pbs_job_id, e.g.19625[0].pleiades.icrar.org)
export CH_RUN_ID=$PBS_JOBID
#
# previously successful cvel run id (e.g. 22984), or set it to "N/A" if do not reuse existing splits
#export CH_PREV_CVEL_RUN_ID=9999
#
##### Allows to run subsections #####
#export CH_PARTS_ONLY=0
#If 0 do all stages, if 1 start at stage one, if 2 start at stage two, etc
#If negitive do _only_ that stage
#
# target field
export CH_TARGET_FIELD='deepfield'

#export CH_OBS_DIR=/mnt/hidata/mmeyer/chiles/final_products/
export CH_OBS_DIR=/mnt/chiles-imaging/DataFiles/
# the index of the first / last observation (to be split) listed in "ls -l $OBS_DIR" 
# or "ls -l $CH_VIS_DIR/$CH_PREV_CVEL_RUN_ID" if CH_PREV_CVEL_RUN_ID is set
export CH_OBS_FIRST=0
export CH_OBS_LAST=3

#	  SpwID  Name           #Chans   Frame   Ch0(MHz)  ChanWid(kHz)  TotBW(kHz) BBC Num  Corrs  
#	  0      EVLA_L#A0C0#0    2048   TOPO     951.000        15.625     32000.0      12  RR  LL
#	  1      EVLA_L#A0C0#1    2048   TOPO     983.000        15.625     32000.0      12  RR  LL
#	  2      EVLA_L#A0C0#2    2048   TOPO    1015.000        15.625     32000.0      12  RR  LL
#	  3      EVLA_L#A0C0#3    2048   TOPO    1047.000        15.625     32000.0      12  RR  LL
#	  4      EVLA_L#A0C0#4    2048   TOPO    1079.000        15.625     32000.0      12  RR  LL
#	  5      EVLA_L#A0C0#5    2048   TOPO    1111.000        15.625     32000.0      12  RR  LL
#	  6      EVLA_L#A0C0#6    2048   TOPO    1143.000        15.625     32000.0      12  RR  LL
#	  7      EVLA_L#A0C0#7    2048   TOPO    1175.000        15.625     32000.0      12  RR  LL
#	  8      EVLA_L#A0C0#8    2048   TOPO    1207.000        15.625     32000.0      12  RR  LL
#	  9      EVLA_L#A0C0#9    2048   TOPO    1239.000        15.625     32000.0      12  RR  LL
#	  10     EVLA_L#A0C0#10   2048   TOPO    1271.000        15.625     32000.0      12  RR  LL
#	  11     EVLA_L#A0C0#11   2048   TOPO    1303.000        15.625     32000.0      12  RR  LL
#	  12     EVLA_L#A0C0#12   2048   TOPO    1335.000        15.625     32000.0      12  RR  LL
#	  13     EVLA_L#A0C0#13   2048   TOPO    1367.000        15.625     32000.0      12  RR  LL
#	  14     EVLA_L#A0C0#14   2048   TOPO    1399.000        15.625     32000.0      12  RR  LL

export CH_SLCT_FREQ=1 # whether to select frequencies, e.g. either spw = 1:10~11Mhz or spw = 1
export CH_SPW=ALL # ALL, 1~2, etc.ALL means '*'
export CH_FREQ_MIN=1408 # MHz -- must be int (could be float if required, but makecube would need changing)
export CH_FREQ_MAX=1412 # MHz -- must be int
export CH_FREQ_STEP=4   # MHz -- must be int
export CH_FREQ_WIDTH=15.625 # width per channel in kHz -- float value

export CH_MODE_DEBUG=0 # In the debug mode, CASA routines are not called but only printed
export CH_BKP_SPLIT=0 # whether to make a backup copy of the split vis files

# each obs will create a sub-directory under this
export CH_VIS_DIR=/mnt/chiles-output/split_vis
export CH_VIS_BK_DIR=/mnt/chiles-output/backup_split_vis

# NOTE - ON pleiades, do not set this to /scratch
export CH_CUBE_DIR=/mnt/chiles-output/split_cubes
#export CH_OUT_NAME=$PWD/cubes/comb_$CH_FREQ_MIN~$CH_FREQ_MAX.image
export CH_OUT_DIR=/mnt/chiles-output/cubes

export CH_SPLIT_TIMEOUT=4000 # 1 hour
export CH_CLEAN_TIMEOUT=4000

# create a separate casa_work directory for each casa process
# NOTE - ON pleiades, do not set this to /scratch
export CH_CASA_WORK_DIR=/mnt/chiles-output/casa_work_dir
# TODO - create it if not there
#mkdir -p $CH_CASA_WORK_DIR/$PBS_JOBID
#export TOP=$PWD
#cd $CH_CASA_WORK_DIR/$PBS_JOBID

# point to casapy installation
#CH_CASA_SOURCE=/mnt/gleam/software/casapy-42.0.28322-021-1-64b
#CH_CASA_SOURCE=/home/apopping/Software/casapy-42.1.29047-001-1-64b
#CH_CASA_SOURCE=/home/apopping/Software/casapy-41.0.24668-001-64b-2
#CH_CASA_SOURCE=/home/rdodson/Software/Casa/casapy-42.1.29047-001-1-64b
CH_CASA_SOURCE=/home/ec2-user/casapy-42.2.30986-1-64b
#export PYTHONPATH=$TOP/chiles:$PYTHONPATH

#module load libxslt
# run casapy
$CH_CASA_SOURCE/casapy --nologger --log2term -c ./makecube14.py
