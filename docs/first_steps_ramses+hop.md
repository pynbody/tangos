Tangos Tutorial – Ramses+HOP
============================

Initial set up
--------------

Make sure you have followed the [initial set up instructions](index.md). Then download the
[raw simulation data](http://star.ucl.ac.uk/~app/tangos/tutorial_ramses.tar.gz) required for this tutorial.
Unpack the tar file either in your home folder or the folder that you pointed the `TANGOS_SIMULATION_FOLDER` environment
variable to.

Import the simulation
---------------------

At the unix command line type:

```
tangos_manager add tutorial_ramses --min-particles 100
```

The process should take about a minute on a standard modern computer, during which you'll see a bunch of log messages 
scroll up the screen.
 
 Let's pick this command apart
 
  * `tangos_manager` is the command-line tool to administrate your tangos database
  * `add` is a subcommand to add a new simulation
  * `tutorial` identifies the simulation we're adding
  * `--min-particles 100` imports only halos/groups with at least 100 particles. 

 
Note that all _tangos_ command-line tools provide help. For example `tangos_manager --help` will show you all subcommands, and `tangos_manager add --help` will tell you more about the possible options for adding a simulation.
  
At this point, the database knows about the existence of timesteps and their halos and groups in our simulation, but nothing about the properties of those halos or groups. We need to add more information before the database is useful.

Generate the merger trees
-------------------------

The merger trees are most simply generated using pynbody's bridge function. To do this, type

```
tangos_timelink --for tutorial_ramses
```

The construction of each merger tree should take a couple of minutes,  and again you'll see a log scroll up the screen while it happens.

If you want to speed up this process, it can be [MPI parallelised](mpi.md).

Add the first property
----------------------
 
Next, we will add some properties to the halos so that we can start to do some science. Because this is a _zoom_ simulation,
we only want to do science on the highest resolution regions. The first thing to calculate is therefore which halos fall
in that region. From your shell type:
```bash
tangos_writer contamination_fraction --for tutorial_ramses
```

Here,
 * `tangos_writer` is the main script for adding properties to a tangos database;
 * `contamination_fraction` is the name of a built-in property which returns the fraction of dark matter particles
   which come from outside the high resolution region.
   
If you want to speed up this process, it can be [MPI parallelised](mpi.md).

Once the command terminates, you can check that each halo now has a `contamination_fraction` associated with it, either
in the web interface or from python, for example:

```python
import tangos
tangos.get_halo("tutorial/output_00010/halo_1")['contamination_fraction'] # -> returns the appropriate fraction
```

Add some more interesting properties
------------------------------------

Let's finally do some science. We'll add dark matter density profiles; from your shell type:
 
 ```bash
tangos_writer dm_density_profile --with-prerequisites --include-only="contamination_fraction<0.01"  
```

If you want to speed up this process, it can be [MPI parallelised](mpi.md).

Here,
 * `tangos_writer` is the same script you called above to add properties to the database
 * `dm_density_profile` is an array representing the dark matter density profile; to see all available properties
   you can call `tangos_manager list-possible-haloproperties`
 * `--with-prerequisites` automatically includes  any underlying properties that are required to perform the calculation. In this case,
   the `dm_density_profile` calculation actually needs to know an accurate center for the halo (known as `shrink_center`),
   so that calculation will be automatically performed and stored
 * `--include-only` allows an arbitrary filter to be applied, specifying which halos the properties should be calculated
   for. In the present case, we use that to insist that only "high resolution" halos are included (specifically, those
   with a fraction of low-res particles smaller than 1%)
 
 
 Explore what's possible
 -----------------------
 
 Now that you have a minimal functioning _tangos_ database, proceed to the [data exploration](data_exploration.md) tutorial.