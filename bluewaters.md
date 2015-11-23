Running the database on the BlueWaters computer
---------------------------------------------------
To run on BlueWaters, you will need to use the bluewaters version of halo_database, located currently in the mjt_BW branch (this will likely be merged with the master branch eventually). This branch uses mpi4py rather than pypar, which is currently default. The latter doesn't play nice with the installed MPI on BlueWaters machines. All necessary code (except for pynbody) should be installed on BlueWaters under the module called bwpy. Just as with any other computer, simply clone the halo_database repository and change branches to mjt_BW. 

To submit a job doing a database calculation, you need a submission script with the following general syntax. Notice that on BlueWaters aprun is used instead of mpirun and we are required to import all necessary modules and defile all necessary environment variables (as well as source the .bashrc file). This is because the environment is not automatically preserved on compute nodes.

```
#! /bin/bash                        #Getting this line correct is crucial... otherwise some important commands won't work
#PBS -N getdata.db.R25
#PBS -lnodes=12:ppn=32              #note that we are not assigning mpiprocs here... aprun handles that
#PBS -l walltime=24:00:00
#PBS -M m.tremmel6@gmail.com
#####PBS -q high
#PBS -m abe

cd $PBS_O_WORKDIR
. /opt/modules/default/init/bash    #this allows one to call the command "module"
source $HOME/.bashrc                #note that if any of the following commands are in this file, you can disregard them here
module unload craype-hugepages8M    #make sure that craype-hugepages8M is off
module load cray-mpich              #load in the mpi environment
module load bwpy/0.2.0              #load in the python environment (version 0.1.0 does not work, but 0.2.0 should be default)
export HALODB_ROOT=/u/sciteam/tremmel/scratch/Romulus/
export HALODB_DEFAULT_DB=/u/sciteam/tremmel/scratch/Romulus/DatabaseFiles/cosmo25/data_romulus25.db
####setenv HALODB_ROOT /u/sciteam/tremmel/scratch/Romulus
####setenv HALODB_DEFAULT_DB  /u/sciteam/tremmel/scratch/Romulus/DatabaseFiles/cosmo25/data_romulus25.db
SIMS="cosmo25"
echo $HALODB_DEFAULT_DB

#the following code is useful for automatically defining parameters for aprun.
#Think of "procsPerNode" as "mpiprocs" on other computers.
procsPerNode=8
procs=`expr $PBS_NUM_NODES \* $procsPerNode`
ncore=`expr 32 \/ $procsPerNode`
ppnode=`expr $ncore - 1`

#Run Code using aprun rather than mpirun
aprun -n $procs -N $procsPerNode -d $ncore db_writer.py Mvir Vvir --for $SIMS --partial-load --htype 0
