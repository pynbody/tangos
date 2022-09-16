Tangos Tutorial – Black holes
=============================

This tutorial covers adding black holes and crossmatching simulations to make halo-to-halo comparisons.
It extends the [first steps with Changa+AHF tutorial](first_steps_changa+ahf.md)
by adding a simulation with black holes. The simulation is started from identical initial
conditions and therefore serves as a comparison to quantify the effect of AGN feedback.

Download, add and process the new simulation
--------------------------------------------

First, download the
[raw simulation data](https://zenodo.org/record/5155467/files/tutorial_changa_blackholes.tar.gz?download=1) required
for this tutorial.
Unpack the tar file either in your home folder or the folder that you pointed the `TANGOS_SIMULATION_FOLDER`
environment
variable to.

Next, refer back to the [first steps with Changa+AHF tutorial](first_steps_changa+ahf.md).
Follow all the steps there but replacing `tutorial_changa` with `tutorial_changa_blackholes`.

Note that if you are using Michael Tremmel's black hole implementation in Changa, you need to run his
pre-processing script to generate the black hole logs (such as `.shortened.orbit` and `.mergers`) from
the raw output logs.


Crosslink the simulations
-------------------------

Next we'll identify which halo corresponds to which across the simulations. This allows us to make one-to-one
comparisons to isolate the effects of AGN. From your UNIX shell type:

```
tangos crosslink tutorial_changa tutorial_changa_blackholes
```

If you want to speed up this process, it can be [MPI parallelised](mpi.md).

Add properties to the black holes
---------------------------------

Black holes are added to the _tangos_ database with a specialised script that scans each simulation output
for black hole particles. Type:

```
tangos import-changa-bh --sims tutorial_changa_blackholes
```

This scans through the timesteps, adds black holes from each snapshot, and links them together using merger
information from changa's output `.mergers` file.

However no properties are associated with the black holes until you ask for them. Property calculations
can be applied to black holes (and other objects) in just the same way as halos, using the `tangos write`
shell command. Type:

```
tangos write BH_mass BH_mdot_histogram --for tutorial_changa_blackholes --type bh
```

Here
 - `BH_mass` and `BH_mdot_histogram` are properties referring to, respectively, the mass of
   the black hole at a given timestep and the recent accretion history. Note that `BH_mdot_histogram`
   is a [histogram property](histogram_properties.md) that can be reassembled across time
   in different ways, like `SFR_histogram`.
 - `--for tutorial_changa_blackholes` idenfities that we are only adding these properties to that
   particular simulation
 - `--type bh` is a new directive, indicating the writer should be applied to all black hole
   objects in the simulation (rather than regular halos).

If you want to speed up the processes above, `tangos import-changa-bh` and `tangos write` can both
be [MPI parallelised](mpi.md).

Explore
-------

Now return to the [data exploration tutorials](data_exploration.md) to explore what you've
created.
