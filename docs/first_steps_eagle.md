Tangos Tutorial – Eagle runs (Gadget HDF + Subfind)
===================================================

Initial set up
--------------

Make sure you have followed the [initial set up instructions](index.md).

Next, download the raw simulation data required for this tutorial. We will use the Eagle
particle data from a 25 Mpc box (RefL0025N0376); to access this you need to [sign up for a free account](http://icc.dur.ac.uk/Eagle/database.php) and download the
individual snapshots, unpacking them in your home folder or the folder that you pointed the `TANGOS_SIMULATION_FOLDER` environment
variable to.

For most Linux or macOS systems, the following typed at your bash command line will download the required data and
unpack it in the correct location:

```bash
echo "Enter Eagle username:"
read EAGLE_USERNAME
echo "Enter Eagle password:"
read -s EAGLE_PASSWORD
cd $TANGOS_SIMULATION_FOLDER
for i in {1..28};
do
   curl -u $EAGLE_USERNAME:$EAGLE_PASSWORD "http://data.cosma.dur.ac.uk:8080/eagle-snapshots//download?run=RefL0025N0376&snapnum=$i" | tar -x
done
```

*This will require 184GB of disk space and take a long time, depending on the speed of your internet connection.*


Import the simulation
---------------------

At the unix command line type:

```bash
tangos add eagle/RefL0025N0376 --max-objects 2000
```

Let's pick this command apart.

  * `tangos` is the command-line tool to administrate your tangos database
  * `add` is a subcommand to add a new simulation
  * `RefL0025N0376` identifies the simulation we're adding
  * `--max-objects 2000` specifies that a maximum of 2000 halos and 2000 groups per timestep should be added.
    This prevents us from picking up loads of tiny objects at low redshift that are unlikely to enter a realistic analysis.
    Note there is also a minimum number of particles per halo, which by default is 1000; you can alter this
    using `--min-particles``.

Note that all _tangos_ command-line tools provide help. For example `tangos --help` will show you all subcommands, and `tangos add --help` will tell you more about the possible options for adding a simulation.

At this point, the database knows about the existence of timesteps and their halos in our simulation, but nothing about the properties of those halos or groups. We need to add more information before the database is useful.


Import the relationship between halos and groups
------------------------------------------------

At the unix command line type:

```bash
tangos import-properties --type group --for RefL0025N0376
tangos import-properties --for RefL0025N037
```

This will import the child/parent relationships between the groups and the halos.
You'll see a bunch of log messages scroll up the screen describing the progress.


Generate the merger trees
-------------------------

Merger trees can be generated using pynbody's bridge function; to do this, type

```bash
tangos link --for RefL0025N0376
tangos link --type group --for RefL0025N0376
```

which builds the merger tree for the halos and then for the groups.

The construction of each merger tree can take a while, but you'll see a log scroll up the screen while it happens.

If you want to speed up this process, it can be [MPI parallelised](mpi.md).

Add the first property
----------------------

Next, we will add some properties to the halos so that we can start to do some science. First let's just add the
mass (as determined by SubFind) of the groups, and then further properties for the halos (i.e. subgroups):
```bash
tangos write finder_mass --type group --for RefL0025N0376
tangos write SFR_10Myr finder_dm_mass BH_mass shrink_center finder_mass --for RefL0025N0376
```

Here,
 * `tangos write` is the main script for adding properties to a tangos database;
 * `finder_mass` is the name of a built-in property which returns the total mass of all particles in the group.
 * `SFR_10Myr` is the name of a built-in property which returns the star formation rate averaged over the last
   10 Myr. Note that this is grouped together with a second built-in property, `SFR_100Myr` which gives you an
   average of 100Myr; you get both even if you only request one.
 * `finder_dm_mass` returns the total mass of dark matter, and is grouped with `finder_gas_mass` and `finder_star_mass`.
 * `BH_mass` returns the mass of the black holes, and is grouped with `BH_mdot` (the accretion rate).
 * `shrink_center` returns the center of the halo, as determined by the shrinking sphere algorithm.

If you want to speed up this process, it can be [MPI parallelised](mpi.md). To save memory, we would recommend
controlling the parallelisation using `--load-mode=server`. See the [MPI page](mpi.md) for more information.

Explore what's possible
-----------------------

Now that you have a minimal functioning _tangos_ database, you can proceed to the [data exploration](data_exploration.md)
tutorial.
