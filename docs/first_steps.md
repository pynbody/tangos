Generating your first tangos database
-------------------------------------

There are currently various sample simulations available to illustrate the use of tangos. They are respectively:

* A uniform resolution volume run with GADGET and with SubFind halo catalogues - [click here](first_steps_gadget+subfind.md);
* An alternative set of Rockstar halo catalogues based on the same GADGET simulation as above - [click here](first_steps_gadget+rockstar.md);
* An alternative set of AHF halo catalogues based on the same GADGET simulation as above - [click here](first_steps_gadget+AHF.md);
* A zoom simulation run with RAMSES and with HOP catalogues - [click here](first_steps_ramses+hop.md);
* A zoom simulation run with Changa (similar to Gasoline) with AHF catalogues - [click here](first_steps_changa+ahf.md). This simulation also has baryons
  unlike the others which contain only dark matter. An additional sample Changa simulation is available
  [with black holes](black_holes_and_crossmatching.md).
* You can also use the publicly-available Eagle particle data to try
  analysing [a full uniform volume galaxy formation simulation](first_steps_eagle.md), although this takes slightly
  longer and requires a large amount of disk space.

Adding simulations to _tangos_ is not strongly dependent on the underlying format so to get to grips with
that process you can try any of the tutorials.

If you want to build the database used for the [analysis tutorials](data_exploration.md), you should import
all of the above. However you can also download the ready-made database file if you don't want to
go through importing three separate simulations.

As a final example, you can also [import a simulation using yt](using_with_yt.md) as the underlying library.
