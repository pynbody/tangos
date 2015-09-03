This repository contains the complete code for the halo database, which ingests runs and calculates various properties of the halos (including profiles, images etc) then exposes them through a python interface and webserver.

Quick-start: if you already have a .db file and want to run the webserver
-------------------------------------------------------------------------

If running on a remote server, you will need to forward the appropriate port using `ssh address.of.remote.server -L5000:localhost:5000`. Then follow these instructions:

1. Clone the repository
2. Make sure you have an up-to-date version of python, then type `pip install pylons formalchemy` to install the required web frameworks
3. Put your database file in the halo_database folder, named `data.db` - or edit the `environment.sh` to specify a different location
4. Type `./webserver.sh` to run the web server
5. Browse to <http://localhost:5000>


Making your own database from scratch
-------------------------------------

1. You need to gather all the simulations that you are going to process in one root folder. This is likely to be some sort of scratch folder on a supercomputer somewhere. The path to this folder needs to be in the environment variable `HALODB_ROOT`. You can either specify this yourself somehow, or edit the `environment.sh`. 
2. Type `source environment.sh` (or get the environment variables `HALODB_ROOT`, `HALODB_DEFAULT_DB`, `PYTHONPATH` and `PATH` set up appropriately in some other way if you prefer)
3. Now add your first simulation. Type `db_manager.py add <simulation_path>` where `<simulation_path>` is the path to the folder containing simulation snapshots *relative to `HALODB_ROOT`* (regardless of what your working directory is)

If everything works OK, you should see some text scroll up the screen (possibly in red) about things being created. Normally this process is reasonably quick, but it can slow down depending on the format of halo files etc. The database is being created with empty slots for every timestep and halo.

To check what's happened type `db_manager.py recent-runs 1`. Here, `recent-runs` refers to runs on the database, not runs of the simulation. You should see something like this:

```
Run ID =  140
Command line =  /Users/app/Science/halo_database/tools//db_manager.py add h516.cosmo25cmb.3072g1MbwK1C52
Host =  Rhododendron.local
Username =  app
Time =  03/09/15 18:42
>>>    1 simulations
>>>    12 timesteps
>>>    942 halos
>>> 8 simulation properties
```

It tells you how many simulations, timesteps, halos, and properties were added to the database by your command.

Populating the database
-----------------------

So now you probably want to actually put some properties into your database? For a tiny simulation, you can do this on a single node. Let's say you want to add the `SSC` property (that means 'shrink sphere center') and the `dm_density_profile` (sorta what it says on the tin).

You should be able to do this:
```
db_writer.py SSC dm_density_profile --for <simulation_path> --part 1 1
```
Hopefully that's fairly self-explanatory except maybe the `--part` bit, which is inherited because the DB writer wants to be running in parallel. The `--part` directive just says "you are node 1 of 1". It's necessary to say this unless you are running on MPI.

The database checkpoints as it goes along (every few minutes or so). You can interrupt it when you feel like it and it'll automatically resume from where it got to. Once again, you can get a summary of progress with `db_manager.py recent-runs 1`, which will spit out something like this:

```
Run ID =  141
Command line =  /Users/app/Science/halo_database/tools//db_writer.py SSC dm_density_profile --for h516.cosmo25cmb.3072g1MbwK1C52 --part 1 1
Host =  Rhododendron.local
Username =  app
Time =  03/09/15 18:56
>>>    169 halo properties
```

Note that `db_writer.py` has a lot of options to customize what it calculates and for which halos. Type `db_writer.py -h` for information.

Populating the database (MPI - preferred)
-----------------------------------------

With MPI, you automatically distribute the tasks between nodes. This is far preferable. But it does mean you need to get python and MPI to understand each other. If you have an MPI compiler avaiable, this is pretty easy - you just type `pip install pypar` and it's all done. 

Now you can use `mpirun` on `db_writer.py` just like you would with any other parallel task. However be careful: *by default every processor will load its own copy of the data*. This is time-efficient but memory-wasteful. If you can get away with it (and you often can with zoom simulations), it's all fine. 

If you can't get away with it, either you can reduce the number of processes per core in the normal way (using qsub directives etc), or alternatively the database can *partial load* data, i.e. just ingest one halo at a time. Partial loading is pretty efficient but be aware that  calculations that need the surroundings of the halo (e.g. for outflows etc) will fail.

Here's an example qsub script from pleiades for processing a small uniform volume. Note this also shows you the use of `db_timelink.py` to generate the merger trees.

```
#PBS -S /bin/bash
#PBS -N db-volume
#PBS -l select=2:ncpus=20:mpiprocs=15:model=ivy:bigmem=False
#PBS -l walltime=3:00:00
#PBS -j oe
cd $PBS_O_WORKDIR

source ~/halo_database/environment.sh

SIMS="romulus8.256gst3.bwBH"

mpirun ~/sim_analysis/chain/db_writer.py Mvir Vvir dm_density_profile dm_alpha_500pc Sub --for $SIMS --partial-load
mpirun db_writer.py stellar_image_faceon --hmax 100 --backwards --for $SIMS --partial-load
mpirun ~/sim_analysis/chain/db_timelink.py for $SIMS
mpirun ~/sim_analysis/chain/add_bh.py for $SIMS
mpirun ~/sim_analysis/chain/db_writer.py BH_mass --for $SIMS --htype 1 --partial-load
# htype 1 in the line above means "do this for the black hole pseudo halos, not the regular halos". 
```
