Tangos Tutorial – Gadget+Rockstar
=================================

Initial set up
--------------

This tutorial imports a gadget-run simulation with a [Rockstar](https://bitbucket.org/gfcstanford/rockstar/)
halo catalogue and [consistent-trees](https://bitbucket.org/pbehroozi/consistent-trees) merger information.

Make sure you have followed the [initial set up instructions](index.md).

Then download the raw simulation data
required for this tutorial. You need two files:

 - the simulations snapshots, [tutorial_gadget.tar.gz](https://zenodo.org/record/5155467/files/tutorial_gadget.tar.gz?download=1)
 - the rockstar catalogues, [tutorial_gadget_rockstar.tar.gz](https://zenodo.org/record/5155467/files/tutorial_gadget_rockstar.tar.gz?download=1)

required for this tutorial.

Unpack both tar files either in your home folder or the folder that you pointed the `TANGOS_SIMULATION_FOLDER` environment
variable to.

For most Linux or macOS systems, the following typed at your bash command line will download the required data and
unpack it in the correct location:

```bash
cd $TANGOS_SIMULATION_FOLDER
curl https://zenodo.org/record/5155467/files/tutorial_gadget.tar.gz?download=1 | tar -xz
curl https://zenodo.org/record/5155467/files/tutorial_gadget_rockstar.tar.gz?download=1 | tar -xz
```

Import the simulation
---------------------

At the unix command line type:

```bash
tangos add tutorial_gadget_rockstar --min-particles 100
```

The process should take about a minute on a standard modern computer, during which you'll see a bunch of log messages
scroll up the screen.

 Let's pick this command apart

  * `tangos` is the command-line tool to administrate your tangos database
  * `add` is a subcommand to add a new simulation
  * `tutorial_gadget_rockstar` identifies the simulation we're adding. Note that _tangos_ automatically spots the
    rockstar outputs in this folder and adapts its behaviour accordingly. If you add `tutorial_gadget`, it'll instead
    see the SubFind catalogues (see the [alternative tutorial](first_steps_gadget+subfind.md)).
  * `--min-particles 100` imports only halos/groups with at least 100 particles.
  (The default value is 1000 particles, but this tutorial dataset is fairly low resolution so we'll keep these small halos.)


Note that all _tangos_ command-line tools provide help. For example `tangos --help` will show you all subcommands, and `tangos add --help` will tell you more about the possible options for adding a simulation.

At this point, the database knows about the existence of timesteps and their halos and groups in our simulation, but nothing about the properties of those halos or groups. We need to add more information before the database is useful.


Import rockstar's properties
---------------------------

At the unix command line type:

```bash
tangos import-properties Mvir Rvir X Y Z --for tutorial_gadget_rockstar
```

The process should take about a minute on a standard modern computer, during which you'll see a bunch of log messages scroll up the screen.

The example command line lists a few properties, `Mvir`, `Rvir`, `X`, `Y` and `Z` to import from
the Rockstar `.list` files. The added directive
`--for tutorial_gadget_rockstar` specifies which simulation you want to apply this operation to. It's not strictly
necessary to add this if you only have one simulation in your database.

Import the merger trees
-------------------------

The merger trees can be imported from consistent-trees.  To do this type

```bash
tangos import-consistent-trees --for tutorial_gadget_rockstar
```

Note that you can also use the built-in tree builder, as described in other tutorials such as the
[SubFind example](first_steps_gadget+subfind.md). But compared to the default implementation, _consistent
trees_ has the significant advantage of including "phantom halos" -- i.e. halos which go missing at one
timestep then reappear again. These are represented by `PhantomHalo` objects within _tangos_ and show up
in the web merger tree tool as a dashed line.

Importing the merger tree should take a minute or so,  and again you'll see a log scroll
up the screen while it happens.


Add some more interesting properties
------------------------------------

Let's finally do some science. We'll add dark matter density profiles; from your shell type:

```bash
tangos write dm_density_profile --with-prerequisites --include-only="NDM()>5000" --type=halo --for tutorial_gadget_rockstar
```

If you want to speed up this process, it can be [MPI parallelised](mpi.md).

Here,
 * `tangos write` is the same script you called above to add properties to the database
 * `dm_density_profile` is an array representing the dark matter density profile; to see all available properties
   you can call `tangos list-possible-properties`
 * `--with-prerequisites` automatically includes  any underlying properties that are required to perform the calculation. In this case,
   the `dm_density_profile` calculation actually needs to know an accurate center for the halo (known as `shrink_center`),
   so that calculation will be automatically performed and stored
 * `--include-only` allows an arbitrary filter to be applied, specifying which halos the properties should be calculated
   for. In the present case, we use that to insist that only halos with more than 5000 particles have their density profiles
   calculated
 * `--type=halo` calculates the properties only for halos (as opposed to groups)



Explore what's possible
-----------------------

Now that you have a minimal functioning _tangos_ database, proceed to the [data exploration](data_exploration.md) tutorial.
