Tangos Tutorial – Changa + AHF
==============================

Initial set up
--------------

Make sure you have followed the [initial set up instructions](index.md).

Next, download the
[raw simulation data](https://zenodo.org/record/5155467/files/tutorial_changa.tar.gz?download=1) required for this tutorial.
Unpack the tar file either in your home folder or the folder that you pointed the `TANGOS_SIMULATION_FOLDER` environment
variable to.

For most Linux or macOS systems, the following typed at your bash command line will download the required data and
unpack it in the correct location:

```bash
cd $TANGOS_SIMULATION_FOLDER
curl https://zenodo.org/record/5155467/files/tutorial_changa.tar.gz?download=1 | tar -xz
```


Import the simulation
---------------------

At the unix command line type:

```bash
tangos add tutorial_changa
```

The process should take about a minute on a standard modern computer, during which you'll see a bunch of log messages
scroll up the screen.

Let's pick this command apart

  * `tangos` is the command-line tool to administrate your tangos database
  * `add` is a subcommand to add a new simulation
  * `tutorial_changa` identifies the simulation we're adding

Note that all _tangos_ command-line tools provide help. For example `tangos --help` will show you all subcommands, and `tangos add --help` will tell you more about the possible options for adding a simulation.

At this point, the database knows about the existence of timesteps and their halos in our simulation, but nothing about the properties of those halos or groups. We need to add more information before the database is useful.


Import some AHF-defined properties
----------------------------------

At the unix command line type:

```bash
tangos import-properties Mvir Rvir --for tutorial_changa
```

The process should take less than a minute on a standard modern computer, during which you'll see a bunch of log messages scroll up the screen.

The example command line lists two properties, `Mvir` and `Rvir` to import from the stat files. The added directive
`--for tutorial_changa` specifies which simulation you want to apply this operation to. It's not strictly
necessary to add this if you only have one simulation in your database.

Generate the merger trees
-------------------------

The merger trees are most simply generated using pynbody's bridge function to do this, type

```bash
tangos link --for tutorial_changa
```

which builds the merger tree for the halos. Again, the `--for tutorial_changa` may be omitted if it's the
only simulation in your tangos database. Note that in this tutorial, only a few timesteps are provided. This makes the merger
trees a little boring (see the Ramses and Gadget tutorial datasets for more interesting merger trees).

The construction of each merger tree should take a few minutes,  and again you'll see a log scroll up the screen while it happens.

If you want to speed up this process, it can be [MPI parallelised](mpi.md).

Add the first property
----------------------

Next, we will add some properties to the halos so that we can start to do some science. Because this is a _zoom_ simulation,
we only want to do science on the highest resolution regions. The first thing to calculate is therefore which halos fall
in that region. From your shell type:
```bash
tangos write contamination_fraction --for tutorial_changa
```

Here,
 * `tangos write` is the main script for adding properties to a tangos database;
 * `contamination_fraction` is the name of a built-in property which returns the fraction of dark matter particles
   which come from outside the high resolution region.

If you want to speed up this process, it can be [MPI parallelised](mpi.md).

If you want to see how your database is looking, you can skip ahead to [data exploration](#explore-whats-possible),
though so far there's not a huge amount of interest to see.

Add some more interesting properties
------------------------------------

Let's finally do some science. We'll add density profiles, thumbnail images, and star formation rates;
from your shell type:

```bash
tangos write dm_density_profile gas_density_profile uvi_image SFR_histogram --with-prerequisites --include-only="contamination_fraction<0.01" --include-only="NDM()>1000" --for tutorial_changa
```

Here,
 * `tangos write` is the same script you called above to add properties to the database
 * `dm_density_profile` is an array representing the dark matter density profile; to see all available properties
   you can call `tangos list-possible-properties`
 * `--with-prerequisites` automatically includes  any underlying properties that are required to perform the calculation. In this case,
   the `dm_density_profile` calculation actually needs to know an accurate center for the halo (known as `shrink_center`),
   so that calculation will be automatically performed and stored
 * `--include-only` allows an arbitrary filter to be applied, specifying which halos the properties should be calculated
   for. In the present case, we use that to insist that only "high resolution" halos are included (specifically, those
   with a fraction of low-res particles smaller than 1%) – and, more than that, there must be 1000 dark matter particles
   in a halo before we calculate these properties (otherwise it's too small for us to care).


This is the slowest process in all the _tangos_ tutorials; there is a
[specific example](mpi.md#tangos write_example) in the [MPI parallelisation document](mpi.md) showing how to make
best use of a multi-core system to speed things up.

Tangos error handling
---------------------

While running this case the log may contain some errors such as
`Number of smoothing particles exceeds number of particles in tree`. Don't panic, this is normal! You're seeing
the effect of attempting to smooth over a very small number of star or gas particles in some tiny halos.

If keen, one can alter the `--include-only` clause to prevent any such errors occuring but it's not really necessary:
_tangos_ isolates errors that occur in individual halo calculations; it reports them and then moves onto the next
calculation or halo.


Explore what's possible
-----------------------

Now that you have a minimal functioning _tangos_ database, you can proceed to the [data exploration](data_exploration.md)
tutorial.

However, you can also [enable even more functionality](black_holes_and_crossmatching.md) by  adding a companion simulation
which has black hole (AGN) feedback
