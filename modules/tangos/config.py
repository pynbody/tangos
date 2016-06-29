"""Configuration module for tangos

Rather than change anything directly in this file, you can create a config_local.py with the variable you
want to override and it will automatically take precedence.
"""

import os
import sys

home = os.environ['HOME']

if sys.platform=='darwin' :
    home+='/Science/'

db = os.environ.get("HALODB_DEFAULT_DB", home+"/sim_analysis/data.db")
base = os.environ.get("HALODB_ROOT", home+"/db_galaxies/")

default_fileset_handler_class = "pynbody.ChangaOutputSetHandler"

num_multihops_max_default = 100

min_halo_particles = 1000


file_ignore_pattern = []

#############################
# Preamble to give to IDL
#############################

# Cambridge:
idl_preamble = "!path= !path+':'+expand_path('~/amiga_tools/')"
idl_proc_name = 'ahf_grp_stat'
idl_pass_z = False

newstyle_amiga = True

# Elektra:
# idl_preamble = "!path= !path+':'+expand_path('~app/amiga_tools/')"

##############################################
# The command that actually launches IDL
##############################################
idl_command = "idl"


###########################################################################
# The command that actually launches amiga (can be a job submission script)
###########################################################################

# Cambridge, using queing system...

amiga_command = """qsub <<EOF
#!/bin/bash
#PBS -l nodes=1:ppn=4,walltime=0:15:00
#PBS -l mem=7000mb
#PBS -m n

cd """+os.getcwd()+"""/%s
mpirun -np 1 ~/amiga-v0.0/bin/AHFstep %s
EOF"""

ignore_error = True
ignore_idl_error = True


# Cambridge, no Queing system
#amiga_command = "cd %s; mpirun -np 1 -disable-dev-check ~/amiga-v0.0/bin/AHFstep %s"


# Elektra:
#amiga_command = "cd %s; ~abrooks/mpich2-1.1.1p1/bin/mpiexec -n 1 ~abrooks/amiga-v0.0/bin/AHFstep %s"


# Berg, new amiga

amiga_command = "cd %s; mpirun -np 1 AHF %s"

##############################################
# Amiga parameters (after the filenames etc)
##############################################

amiga_params = """512
8
8
0
20
1
0
"""

amiga_params = """[AHF]
ic_filename = %s
ic_filetype = 90
outfile_prefix = %s
LgridDomain = 512
LgridMax = %d
NperDomCell = 8.0
NperRefCell = 8.0
VescTune = 1.0
NminPerHalo = 64
RhoVir = 1
Dvir = -1
MaxGatherRad = 1.0
LevelDomainDecomp = 7
NcpuReading = 1
[TIPSY]
TIPSY_BOXSIZE = %.5e
TIPSY_MUNIT = %.5e
TIPSY_VUNIT = %.5e
TIPSY_EUNIT = %.5e
TIPSY_OMEGA0 = %s
TIPSY_LAMBDA0 = %s
"""


# Maximum number of levels deep to look for snapshots
# Rather than change this, you probably want to put a number on the command line
max_traverse_depth = 3

# Set to true to only run Alyson's script.
# Rather than change this, you probably want to have 'idl' as an argument on the command line
only_idl = False


try:
    from .config_local import *
except:
    pass