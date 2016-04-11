This repository contains the complete code for the halo database, which ingests runs and calculates various properties of the halos (including profiles, images etc) then exposes them through a python interface and webserver.

Before you start
-------------------------------------
For the database to function properly, you must first source the `environment.sh` (or environment.csh, if working in a cshell) files, which specify the appropriate paths. Sourcing the environment file also reads the user-made file called `environment_local.sh` (or csh). This file doesn't exist by default, but should be created/edited whenever you wish to analyze a new database file.

The `environment_local.sh` file should only include the following lines
```
export HALODB_ROOT=~/scratch/Romulus/ 
export HALODB_DEFAULT_DB=~/scratch/Romulus/DatabaseFiles/cosmo25/data_romulus25.db 
```
or, if you are working in cshell, the `environment_local.csh` file should have the following lines
```
setenv HALODB_ROOT /nobackupp8/mtremmel/Romulus/
setenv HALODB_DEFAULT_DB /nobackupp8/mtremmel/DataBaseFiles/romulus8/data_romulus8.db
```
The top line in each example points to the parent directory for all of your simulation data directories. If you don't have any simulations (i.e. you are just using a database object already created) then you should not have to worry about this variable. The second line points to the database object you wish to analyze.

Any edits you make to this file will require you to again source the `environment.sh` file.

Remember, you *must* source `environment.sh` *every* time you start a new session on your computer prior to booting up the database, either with the webserver or the python interface (see below)

Quick-start: if you already have a .db file and want to run the webserver
-------------------------------------------------------------------------

If running on a remote server, you will need to forward the appropriate port using `ssh address.of.remote.server -L5000:localhost:5000`. Then follow these instructions:

1. Clone the repository
2. Make sure you have an up-to-date version of python, then type `pip install pylons formalchemy` to install the required web frameworks
3. Put your database file in the halo_database folder, named `data.db` - or create/edit a file called `environment_local.sh` to specify a different location and/or filename (see instructions above)
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
db_writer.py SSC dm_density_profile --for <simulation_path> --backend null
```
Hopefully that's fairly self-explanatory except maybe the `--backend null` bit, which is inherited because the DB writer wants to be running in parallel. Instead, `--backend null` says "you have no parallelism, just use one core".

The database checkpoints as it goes along (every few minutes or so). You can interrupt it when you feel like it and it'll automatically resume from where it got to. Once again, you can get a summary of progress with `db_manager.py recent-runs 1`, which will spit out something like this:

```
Run ID =  141
Command line =  /Users/app/Science/halo_database/tools//db_writer.py SSC dm_density_profile --for h516.cosmo25cmb.3072g1MbwK1C52 --backend null
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

mpirun db_writer.py Mvir Vvir dm_density_profile dm_alpha_500pc Sub --for $SIMS --partial-load
mpirun db_writer.py stellar_image_faceon --hmax 100 --backwards --for $SIMS --partial-load
mpirun db_timelink.py for $SIMS
mpirun add_bh.py for $SIMS
mpirun db_writer.py BH_mass --for $SIMS --htype 1 --partial-load
# htype 1 in the line above means "do this for the black hole pseudo halos, not the regular halos". 
```
The Python Interface for Analysis
-----------------------------------------

Now that you have your database loaded in and different values calculated and stored, its time to use it for science.

First, you have to check to make sure you have the correct path to your simulation repository in `environment_local.sh  Next is to make sure the syntax matches your environment. The default environment.sh is written for a bash environment. If you are working in cshell, simply change syntax (e.g. export -> setenv). *However* if you do this, do you will need to change the header in webserver.sh (e.g `#!/usr/bin/csh`).

Now open up your python environment and load in your database.
```
>>> import halo_db as db
>>> sim = db.get_simulation('romulus8.256gst3.bwBH')         #get a target simulation from the repository
>>> sim.timesteps                                            #list the available timesteps
[<TimeStep(<Simulation("romulus8.256gst3.bwBH")>,"romulus8.256gst3.bwBH.000045") z=19.93 t=0.18 Gyr>,
 <TimeStep(<Simulation("romulus8.256gst3.bwBH")>,"romulus8.256gst3.bwBH.000054") z=17.88 t=0.21 Gyr>,
 <TimeStep(<Simulation("romulus8.256gst3.bwBH")>,"romulus8.256gst3.bwBH.000072") z=14.96 t=0.27 Gyr>,
 <TimeStep(<Simulation("romulus8.256gst3.bwBH")>,"romulus8.256gst3.bwBH.000102") z=11.93 t=0.37 Gyr>,
 <TimeStep(<Simulation("romulus8.256gst3.bwBH")>,"romulus8.256gst3.bwBH.000128") z=10.23 t=0.46 Gyr>,
 .....
```
Now, load in a single timestep from the simulation.
```
>>> db.get_timestep('romulus8.256gst3.bwBH/%2560')
<TimeStep(<Simulation("romulus8.256gst3.bwBH")>,"romulus8.256gst3.bwBH.002560") z=0.50 t=8.64 Gyr>
>>> step1 = _
```
The entire simulation step including all halos is loaded as `step1`. Note the syntax in the agrgument for `get_timestep` is `simname/%step`. You cal also use the % syntax to avoid witing the entire simulation name, e.g.  `db.get_timestep('romulus8%/%2560')`. Using the timestep object one can access individual halos.

```
>>> step1.halos[0]                       #look at the largest halo
<Halo 1 of <TimeStep(... z=0.50 ...)> | NDM=4977308 NStar=1662499 NGas=1196008 >
>>> step1.halos[0]['Mvir']                 #Look at the virial Mass of the largest halo
1989033156003.6301
>>> step1.halos[0].keys()                #see a list of the loaded properties for halo 1
[u'SSC',
 u'Rvir',
 u'Mvir',
 u'Mgas',
 u'Mbar',
 u'Mstar',
 u'dm_density_profile',
 u'dm_mass_profile',
 u'tot_density_profile',
 u'tot_mass_profile',
 u'gas_density_profile',
 u'gas_mass_profile',
 u'star_density_profile',
 u'star_mass_profile',
 u'dm_alpha_500pc',
 u'dm_alpha_1kpc',
 u'gas_image_sideon',
 u'stellar_image_sideon',
 u'gas_image_faceon',
 u'stellar_image_faceon',
 u'gas_image_original',
 u'stellar_image_original']
 ```
 You can also load in data for all halos at once using `gather_property`. Note that it returns a list even with one arguement given. You can give it as many value key names as you want and it will return a list of arrays for each.
```
>>> mvir, = step1.gather_property('Mvir')              #get the virial mass for every halo in the simulation
>>> mvir, cen = step1.gather_property('Mvstar','SSC')   #get both the stellar mass AND the center of the halo
```
Some properties are not inherently already saved in the database, but can be calculated on the fly from already stored properties (we call these "live calculations"). These are generally called as functions and can be used with either `gather_property` or the `calculate` function for a single halo. For example, the property `Vvir` is calculated using `Mvir` and `Rvir` that are already loaded into the database. To calculate for a single halo, `h`, one would type `h.calculate('Vvir()')`. Note the extra set of "()" in the attribute name. This will return the live-calculated value for halo h. To do it for an entire step, you would do `step.gather_property('Vvir()')`. 

One can also make their own calculatiosn on the fly. For example, `h.calculate(Mgas/Mstar)` will calculate the ratio of those two properties. In general, arithmetic involving *, +, -, and / all work for any already calculated (or live calculated) halo property.

For more information on live calculations including more examples and syntax explanation, go [here] (live_calculation.md).

Linking Halos Across Timesteps
------------------------------------

One other important live calculation are the earlier and later functions. These take an integer argument and will return either the descendant halo or main progenitor halo. You can then easily retrieve information about that future/past halo. For example, `h.calculate(later(5).Mvir)` returns the virial mass of the descendant halo 5 snapshots later than the current snapshot that houses halo `h`. This strategy can also be useful in linking the properties of all halos in two different snapshots.

```
step.gather_property('Mstar', 'earlier(10).Mstar')  #returns both the stellar mass of all halos in the current step and the stellar mass of each halo's descendant 10 snapshots earlier.
```

Again, please visit the [live calculatsions page](live_calculation.md) for more information and examples.

Profile properties
-----------------------------------------
There are many properties that are profiles, meaning they represent some value at different radii within a given halo. Such properties are themselves arrays and can be called just like any other property, e.g. `h['StarMass_encl']` will give an array of the stellar mass enclosed within different radii. These types of values are particularly useful for giving the user the freedom to return a given property at any radius they wish. For this, we use the `at()` function (see the [live calculations page](live_calculation.md) for more details on functions like this).

```
h.calculate('at(10,StarMass_encl)')  #returns the stellar mass within 10 kpc from center
h.calculate('at(Rvir/2,StarMass_encl)') #returns the stellar mass at one half the virial radius of the halo
h.calculate('at(Rvir/2,StarMass_encl/GasMass_encl)') #returns the ratio of stellar mass to Gas Mass witin one half the virial radius
```

The last example above also shows how one can perform multiple live calculations at once to derive a single property.

