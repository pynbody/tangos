

Quick-start: if you already have a .db file and want to run the webserver
-------------------------------------------------------------------------

If running on a remote server, you will need to forward the appropriate port using `ssh address.of.remote.server -L5000:localhost:5000`. Then follow these instructions:

1. Clone the repository
2. Type `python setup.py install` (or `python setup.py develop`, see above)
3. Put your database file in your home folder, named `data.db` - or point the environment variable `TANGOS_DB_CONNECTION` to an alternate path (see above)
4. Type `tangos serve`
5. Browse to <http://localhost:6543>

Making your own database from scratch
-------------------------------------

1. You need to gather all the simulations that you are going to process in one root folder. This is likely to be some sort of scratch folder on a supercomputer somewhere. If it's not your home folder, the path to this folder needs to be in the environment variable `TANGOS_SIMULATION_FOLDER`.
2. If you don't want the database to be created in your home folder, set the environment variable `TANGOS_DB_CONNECTION` to the path you want created, e.g. `~/databases/my_db.sqlite`
3. Now add your first simulation. Type `tangos add <simulation_path>` where `<simulation_path>` is the path to the folder containing simulation snapshots *relative to `TANGOS_SIMULATION_FOLDER`* (regardless of what your working directory is)

If everything works OK, you should see some text scroll up the screen (possibly in red) about things being created. Normally this process is reasonably quick, but it can slow down depending on the format of halo files etc. The database is being created with empty slots for every timestep and halo.

To check what's happened type `tangos recent-runs 1`. Here, `recent-runs` refers to runs on the database, not runs of the simulation. You should see something like this:

```
Run ID =  140
Command line =  tangos add h516.cosmo25cmb.3072g1MbwK1C52
Host =  Rhododendron.local
Username =  app
Time =  03/09/15 18:42
>>>    1 simulations
>>>    12 timesteps
>>>    942 halos
>>>    8 simulation properties
```

It tells you how many simulations, timesteps, halos, and properties were added to the database by your command.

Copying AHF properties into the database
----------------------------------------

One of the quickest ways to populate the database is to use what AHF already calculated for you. Suppose you want to import the
Mvir and Rvir columns from the `.AHF_halos` file. Then you simply type: `tangos import-properties Mvir Rvir`. Now running
`tangos recent-runs 1` should show you what you just did:

```
Run ID =  141
Command line =  tangos import-properties Mvir Rvir
Host =  Rhododendron.local
Username =  app
Time =  03/09/15 18:50
>>>    3860 halo properties
```


Populating the database with other properties
-----------------------

So now you probably want to actually put some properties into your database? For a tiny simulation, you can do this on a single node. Let's say you want to add the `shrink_center` property (that means 'shrink sphere center') and the `dm_density_profile` (sorta what it says on the tin).

You should be able to do this:
```
tangos write shrink_center dm_density_profile --for <simulation_name> --backend null
```
Hopefully that's fairly self-explanatory except maybe the `--backend null` bit, which is inherited because the DB writer wants to be running in parallel. Instead, `--backend null` says "you have no parallelism, just use one core".

The database checkpoints as it goes along (every few minutes or so). You can interrupt it when you feel like it and it'll automatically resume from where it got to. Once again, you can get a summary of progress with `tangos recent-runs 1`, which will spit out something like this:

```
Run ID =  142
Command line =  /Users/app/Science/halo_database/tools//tangos write shrink_center dm_density_profile --for h516.cosmo25cmb.3072g1MbwK1C52 --backend null
Host =  Rhododendron.local
Username =  app
Time =  03/09/15 18:56
>>>    169 halo properties
```

Note that `tangos write` has a lot of options to customize what it calculates and for which halos. Type `tangos write -h` for information.


Generating halo merger trees
----------------------------

To start making the database useful, you probably want to generate some merger tree information, allowing your later analysis
to link properties between timesteps.

To do this you type:
```
tangos link --for <simulation_name> --backend null
```
again assuming you don't want to parallelise using MPI. But these steps can be speeded up by distributing tasks, so read on...

Do it with MPI
--------------

With MPI, you automatically distribute the tasks between nodes. This is far preferable. But it does mean you need to get python and MPI to understand each other. If you have an MPI compiler avaiable, this is pretty easy - you just type `pip install mpi4py` and it's all done.

Now you can use `mpirun` on `tangos write` just like you would with any other parallel task. However be careful: *by default every processor will load its own copy of the data*. This is time-efficient but memory-wasteful. If you can get away with it (and you often can with zoom simulations), it's all fine.

If you can't get away with it, you can reduce the number of processes per core in the normal way (using qsub directives etc)... or, you could try selecting an appropriate *load mode*. This is done by passing the argument `--load-mode=XXX` to `tangos write`, where `XXX` is one of the following:

* `--load-mode=partial`: only the data for a single halo at a time is loaded. Partial loading is pretty efficient but be aware that  calculations that need the surroundings of the halo (e.g. for outflows etc) will fail.
* `--load-mode=server`: rank 0 of your MPI processes will load a (single) entire snapshot at a time and pass only the bits of the data needed along to all other ranks. This has the advantage over `--load-mode=partial` of allowing the calculations to request the surroundings of the halo (see above). However it has the disadvantage that rank 0 must load an entire snapshot (all arrays that are required). For really big simulations that might be tricky.
* `--load-mode=server-partial`: a hybrid approach where rank 0 loads only what is required to help the other ranks figure out what they need to load — for example, if a property requests a sphere surrounding the halo, the entire snapshot's position arrays will be loaded on rank 0, but no other data. The data on the individual ranks is loaded via partial loading (see `--load-mode=partial` above).

Here's an example qsub script from pleiades for processing a small uniform volume. Note this also shows you the use of `tangos link` to generate the merger trees.

```
#PBS -S /bin/bash
#PBS -N db-volume
#PBS -l select=2:ncpus=20:mpiprocs=15:model=ivy:bigmem=False
#PBS -l walltime=3:00:00
#PBS -j oe
cd $PBS_O_WORKDIR

source ~/halo_database/environment.sh

SIMS="romulus8.256gst3.bwBH"

mpirun tangos write Mvir Vvir dm_density_profile dm_alpha_500pc Sub --for $SIMS --load-mode=partial --backend mpi4py
mpirun tangos write stellar_image_faceon --hmax 100 --backwards --for $SIMS --load-mode=partial --backend mpi4py
mpirun tangos link --for $SIMS --backend mpi4py
mpirun tangos import-changa-bh for $SIMS --backend mpi4py
mpirun tangos write BH_mass --for $SIMS --type BH --load-mode=partial --backend mpi4py
# type BH in the line above means "do this for the black holes, not the regular halos".
```
The Python Interface for Analysis
-----------------------------------------

Now that you have your database loaded in and different values calculated and stored, its time to use it for science.

First, you have to check to make sure you have the correct path to your simulation repository in `environment_local.sh`. Next is to make sure the syntax matches your environment. The default environment.sh is written for a bash environment. If you are working in cshell, simply change syntax (e.g. export -> setenv). *However* if you do this, do you will need to change the header in webserver.sh (e.g `#!/usr/bin/csh`).

Now open up your python environment and load in your database.
```
>>> import tangos as db
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
The entire simulation step including all halos is loaded as `step1`. Note the syntax in the argument for `get_timestep` is `simname/%step`. You cal also use the % wildcard syntax to avoid writing the entire simulation name, e.g.  `db.get_timestep('romulus8%/%2560')`. Using the timestep object one can access individual halos (indexed starting at zero, even if the halo finder calls it halo 1):

```
>>> step1.halos[0]                       #look at the largest halo
<Halo 1 of <TimeStep(... z=0.50 ...)> | NDM=4977308 NStar=1662499 NGas=1196008 >
>>> step1.halos[0]['Mvir']                 #Look at the virial Mass of the largest halo
1989033156003.6301
>>> step1.halos[0].keys()                #see a list of the loaded properties for halo 1
[u'shrink_center',
 u'Rvir',
 ...
 u'gas_image_original',
 u'stellar_image_original']
```

You can also load in data for all halos at once using `calculate_all`. This is *much* faster than accessing each halo in turn. Note that it returns a list even with one argument given. You can give it as many value key names as you want and it will return a list of arrays for each.

```
>>> mvir, = step1.calculate_all('Mvir')              #get the virial mass for every halo in the simulation
>>> mvir, cen = step1.calculate_all('Mstar','shrink_center')   #get both the stellar mass AND the center of the halo
```

Calculations
------------

Some properties are not inherently already saved in the database, but can be calculated on the fly from already stored properties (we call these "live calculations"). These are called a bit like functions and can be used with either `calculate_all` or the `calculate` function for a single halo.

For example, the property `Vvir` is calculated using `Mvir` and `Rvir` that are already loaded into the database. To calculate for a single halo, `h`, one would type `h.calculate('Vvir()')`. Note the extra set of "()" in the attribute name, because this is a *function* that the database will call.  To do it for an entire step, you would do `step.calculate_all('Vvir()')`.

Once again, the reason for using this approach is that the database is able to vastly optimise the calculation compared to the performance you'd get by manually going through each halo doing the calculation in your own code.

You can also make custom calculations on the fly. For example, `h.calculate('Mgas/Mstar')` will calculate the ratio of those two properties. In general, arithmetic involving *, +, -, and / all work for any already calculated (or live calculated) halo property.

For more information on live calculations including more examples and syntax explanation, go [here] (live_calculation.md).

Linking Halos Across Timesteps
------------------------------------

Two important live calculation built-in functions are the `earlier` and `later` functions. These take an integer argument and will return either the descendant halo or main progenitor halo. You can then easily retrieve information about that future/past halo. For example, `h.calculate('later(5).Mvir')` returns the virial mass of the descendant halo 5 snapshots later than the current snapshot that houses halo `h`. This strategy can also be useful in linking the properties of all halos in two different snapshots.

```
step.calculate_all('Mstar', 'earlier(10).Mstar')  #returns both the stellar mass of all halos in the current step and the stellar mass of each halo's descendant 10 snapshots earlier.
```

Note that there is another function, `calculate_for_descendants` to use if you to get a property for a single halo but at all timesteps. Again, please visit the [live calculations page](live_calculation.md) for more information and examples.


Profile properties
-----------------------------------------
There are many properties that are profiles, meaning they represent some value at different radii within a given halo. Such properties are themselves arrays and can be called just like any other property, e.g. `h['StarMass_encl']` will give an array of the stellar mass enclosed within different radii. These types of values are particularly useful for giving the user the freedom to return a given property at any radius they wish. For this, we use the `at()` function (see the [live calculations page](live_calculation.md) for more details on functions like this).

```
h.calculate('at(10,StarMass_encl)')  #returns the stellar mass within 10 kpc from center
h.calculate('at(Rvir/2,StarMass_encl)') #returns the stellar mass at one half the virial radius of the halo
h.calculate('at(Rvir/2,StarMass_encl/GasMass_encl)') #returns the ratio of stellar mass to Gas Mass witin one half the virial radius
```

The last example above also shows how one can perform multiple live calculations at once to derive a single property.
