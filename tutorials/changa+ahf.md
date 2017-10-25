Tangos Tutorial – Changa + AHF
==============================

Import the simulation
---------------------

At the unix command line type:

```
tangos_manager add pioneer50h128.1536gst1.bwK1
```

The process should take about a minute on a standard modern computer, during which you'll see a bunch of log messages 
scroll up the screen.
 
 Let's pick this command apart
 
  * `tangos_manager` is the command-line tool to administrate your tangos database
  * `add` is a subcommand to add a new simulation
  * `pioneer50h128.1536gst1.bwK1` identifies the simulation we're adding
 
Note that all _tangos_ command-line tools provide help. For example `tangos_manager --help` will show you all subcommands, and `tangos_manager add --help` will tell you more about the possible options for adding a simulation.
  
At this point, the database knows about the existence of timesteps and their halos in our simulation, but nothing about the properties of those halos or groups. We need to add more information before the database is useful.


Import some AHF-defined properties
----------------------------------

At the unix command line type:

```
tangos_import_from_ahf Mvir Rvir --sims pioneer50h128.1536gst1.bwK1
```

The process should take less than a minute on a standard modern computer, during which you'll see a bunch of log messages scroll up the screen.

The example command line lists two properties, `Mvir` and `Rvir` to import from the stat files. The added directive 
`--sims pioneer50h128.1536gst1.bwK1` specifies which simulation you want to apply this operation to. It's not strictly
necessary to add this if you only have one simulation in your database.

Generate the merger trees
-------------------------

The merger trees are most simply generated using pynbody's bridge function to do this, type

```
tangos_timelink --sims pioneer50h128.1536gst1.bwK1
```

which builds the merger tree for the halos. Again, the `--sims pioneer50h128.1536gst1.bwK1` may be omitted if it's the
only simulation in your tangos database. Note that in this tutorial, only a few timesteps are provided. This makes the merger
trees a little boring (see the Ramses and Gadget tutorial datasets for more interesting merger trees).

The construction of each merger tree should take a few minutes,  and again you'll see a log scroll up the screen while it happens.

If you want to speed up this process, it can be [MPI parallelised](mpi.md).


Now let's take a look at what we've created
-------------------------------------------

We're ready to explore the simulation. Depending on your preferences you might prefer to explore with the web service or direct from python. 