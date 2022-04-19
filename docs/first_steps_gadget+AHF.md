Tangos Tutorial – Gadget+AHF
============================

Initial set up
--------------

This tutorial imports a gadget-run simulation with an [AHF](http://popia.ft.uam.es/AHF/Download.html)
halo catalogue and its associated merger tree information.

Make sure you have followed the [initial set up instructions](index.md).

Then download the raw simulation data
required for this tutorial. You need two files:

 - the simulations snapshots, [tutorial_gadget.tar.gz](https://zenodo.org/record/5155467/files/tutorial_gadget.tar.gz?download=1)
 - the AHF catalogues and the merger tree files, [tutorial_gadget_ahf.tar.gz](https://zenodo.org/record/5155467/files/tutorial_gadget_ahf.tar.gz?download=1)

required for this tutorial.

Unpack both tar files either in your home folder or the folder that you pointed the `TANGOS_SIMULATION_FOLDER` environment
variable to.

For most Linux or macOS systems, the following typed at your bash command line will download the required data and
unpack it in the correct location:

```bash
cd $TANGOS_SIMULATION_FOLDER
curl https://zenodo.org/record/5155467/files/tutorial_gadget.tar.gz?download=1 | tar -xz
curl https://zenodo.org/record/5155467/files/tutorial_gadget_ahf.tar.gz?download=1 | tar -xz
```

Import the simulation
---------------------

At the unix command line type:

```bash
tangos add tutorial_gadget_ahf --min-particles 100
```

The process should take about a minute on a standard modern computer, during which you'll see a bunch of log messages
scroll up the screen.

 Let's pick this command apart

  * `tangos` is the command-line tool to administrate your tangos database
  * `add` is a subcommand to add a new simulation
  * `tutorial_gadget_ahf` identifies the simulation we're adding. Note that _tangos_ automatically spots the
    AHF outputs in this folder and adapts its behaviour accordingly. If you add `tutorial_gadget`, it'll instead
    see the SubFind catalogues (see the [alternative tutorial](first_steps_gadget+subfind.md)).
  * `--min-particles 100` imports only halos/groups with at least 100 particles.
  (The default value is 1000 particles, but this tutorial dataset is fairly low resolution so we'll keep these small halos.)


Note that all _tangos_ command-line tools provide help. For example `tangos --help` will show you all subcommands, and `tangos add --help` will tell you more about the possible options for adding a simulation.

At this point, the database knows about the existence of timesteps and their halos and groups in our simulation, but nothing about the properties of those halos or groups. We need to add more information before the database is useful.


Import AHF's properties
---------------------------

At the unix command line type:

```bash
tangos import-properties Mvir Rvir Xc Yc Zc --for tutorial_gadget_ahf
```

The process should take about a minute on a standard modern computer, during which you'll see a bunch of log messages scroll up the screen.

The example command line lists a few properties, `Mvir`, `Rvir`, `Xc`, `Yc` and `Zc` to import from
the AHF `AHF_halos` files. The added directive
`--for tutorial_gadget_ahf` specifies which simulation you want to apply this operation to. It's not strictly
necessary to add this if you only have one simulation in your database.

Import the merger trees
-------------------------

The merger trees can be imported from the AHF `AHF_mtree` files. Further information on how to create or handle the merger tree files with
the codes from the AHF toolbox see AHF's [documentation](http://popia.ft.uam.es/AHF/Download.html). To import the merger trees type

```bash
tangos import-ahf-trees --for tutorial_gadget_ahf
```

Note that you can also use the built-in tree builder, as described in other tutorials such as the
[SubFind example](first_steps_gadget+subfind.md). But compared to the default implementation, _ahf
trees_ has the significant runtime advantages.

Importing the merger tree should take a minute or so,  and again you'll see a log scroll
up the screen while it happens.


Add some more interesting properties
------------------------------------

Let's finally do some science. We'll add dark matter density profiles; from your shell type:

```bash
tangos write dm_density_profile --with-prerequisites --include-only="NDM()>5000" --type=halo --for tutorial_gadget_ahf
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
