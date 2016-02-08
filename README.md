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
 u'Vvir',
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
One major strength of the data base is the linking of halos acorss time.  The function `gather_linked_property` is similar to the above, but will return a two demensional array, with quantities for each halo at the current timestep and at the previous timestep.
```
>>> db.get_timestep('romulus8.256gst3.bwBH/%2552')   #first read in the previous timestep
>>> step2 = _
>>> step1.gather_linked_property(step2,'Mvir')       #first column is the current step, second is the previous one
Found 1501 halos in common
[array([[  1.98903316e+12,   1.98116762e+12],
        [  7.26636744e+11,   7.24125555e+11],
        [  6.89803158e+11,   6.88452929e+11],
        ..., 
        [  1.09008265e+09,   1.09190501e+09],
        [  1.08419140e+09,   1.08393742e+09],
        [  1.07352488e+09,   1.07331242e+09]])]
```
Note that this will only work on *adjacent timesteps*

Some properties are themselves arrays, such as mass profiles. If  your property is an array, when calling gather_proeprty you can specify which index to gather

```
>>> step1.gather_property('StarMass_encl//10')
```
This will return a 1-d array filled with the 10th value in each halo's StarMass_encl array. In this case, the property is the Stellar mass enclused by a given radius. Unless otherwise noted, every profile property like this is binned in 100 pc intervals. So,  more specifically, this example retuns the stellar mass enclosed in the inner 1 kpc of the halo.

If the Rhalf_[V,U,J,K,I,R] properties have been calculated for the halo (the half-light radius in different bands) you can also use "Rhalf_[V,U,J,K,I,R]" as an identifier. For example, the following returns the stellar mass within the  V-band half light radius for each halo

```
step1.gather_property('StarMass_encl//Rhalf_V')
```
Just like how gather_property with multiple properties in the argument will only return results for halos with both properties included, this will only return data for halos where both StarMass_encl and Rhalf_V have been calculated.

Finally, if we want to study a particular halo in detail, we can load it in as its own object and then aquire time series data on that halo and its progenitors at previous timesteps. To do this we use the function `get_halo` which carries a similar argument syntax, but with one extra component: `simname/%step/halonumber`. We will trace the halo back in time using the function `earliest.property_cascade(<property>)` function.
```
>>> db.get_halo('romulus8.256gst3.bwBH/%2560/1')
<Halo 1 of <TimeStep(... z=0.50 ...)> | NDM=4977308 NStar=1662499 NGas=1196008 >
>>> h = _
>>> h.earliest.property_cascade('Mvir')   #The function takes in  any property in h.keys() except images
array([  1.20783658e+09,   2.76221415e+09,   3.81488357e+09,
         6.76882025e+09,   8.92294277e+09,   2.55281867e+10,
         4.83871878e+10,   5.47559418e+10,   7.07271842e+10,
         9.02300850e+10,   1.25683465e+11,   1.29329152e+11,
         1.65832854e+11,   1.85550295e+11,   1.90878505e+11,
         2.15603091e+11,   2.18132769e+11,   2.69072119e+11,
         2.70083939e+11,   3.14911746e+11,   3.44944910e+11,
         3.53728087e+11,   4.05120204e+11,   5.56876045e+11,
         5.72826066e+11,   6.56326002e+11,   1.42140774e+12,
         1.63484359e+12,   1.80043950e+12,   1.86123429e+12,
         1.86347417e+12,   1.76513065e+12,   1.73259171e+12,
         1.72794707e+12,   1.81462015e+12,   1.90725257e+12,
         1.98116762e+12,   1.98903316e+12])
>>> h.earliest.property_cascade('z')      #...including some new ones like redshift...
array([ 11.92667287,  10.23120912,   9.9654623 ,   8.95954232,
         7.99544143,   6.99173217,   6.22752112,   5.9997759 ,
         5.49389304,   4.98886168,   4.55232998,   4.49625335,
         3.99624439,   3.59548895,   3.49736876,   2.99695863,
         2.96365129,   2.50930483,   2.49710535,   2.16362263,
         1.99818409,   1.88981316,   1.66627764,   1.49975392,
         1.47943355,   1.32027672,   1.18258336,   1.06190289,
         0.99940303,   0.95496197,   0.85929534,   0.77300839,
         0.74959449,   0.6946194 ,   0.62295176,   0.55705851,
         0.49984008,   0.49616842])

>>> h.earliest.property_cascade('t')       #...and time!
array([ 0.37250148,  0.45991652,  0.47672711,  0.55069369,  0.64147085,
        0.76586919,  0.89026752,  0.93397504,  1.04492491,  1.1794096 ,
        1.32061852,  1.34079122,  1.54588037,  1.75096951,  1.8081255 ,
        2.15442357,  2.18132051,  2.6116715 ,  2.62511997,  3.0420225 ,
        3.29081917,  3.4723735 ,  3.90272449,  4.28264373,  4.33307549,
        4.76342648,  5.19377748,  5.62412848,  5.86956303,  6.05447947,
        6.48483047,  6.91518146,  7.0395798 ,  7.34553246,  7.77588346,
        8.20623445,  8.60968851,  8.63658545])


