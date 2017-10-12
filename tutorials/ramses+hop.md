Tangos Tutorial – Ramses+HOP
============================

Import the simulation
---------------------

At the unix command line type:

```
tangos_manager add tutorial --handler pynbody.RamsesHOPOutputSetHandler --min-particles 100
```

The process should take about a minute on a standard modern computer, during which you'll see a bunch of log messages 
scroll up the screen.
 
 Let's pick this command apart
 
  * `tangos_manager` is the command-line tool to administrate your tangos database
  * `add` is a subcommand to add a new simulation
  * `tutorial` identifies the simulation we're adding
  * `--handler pynbody.RamsesHOPOutputSetHandler` identifies the _handler_ for our simulation. A handler defines how to load a simulation and its associated halo catalogue. Here we'll use `pynbody`'s ability to load gadget and subfind files. 
  * `--min-particles 100` imports only halos/groups with at least 100 particles. 

 
Note that all _tangos_ command-line tools provide help. For example `tangos_manager --help` will show you all subcommands, and `tangos_manager add --help` will tell you more about the possible options for adding a simulation.
  
At this point, the database knows about the existence of timesteps and their halos and groups in our simulation, but nothing about the properties of those halos or groups. We need to add more information before the database is useful.

Generate the merger trees
-------------------------

The merger trees are most simply generated using pynbody's bridge function. To do this, type

```
tangos_timelink
```

The construction of each merger tree should take a couple of minutes,  and again you'll see a log scroll up the screen while it happens.

If you want to speed up this process, it can be MPI parallelised, if you have MPI and `mpi4py` on your machine. This is straight-forward with, for example, anaconda python distributions – just type `conda install mpi4py`. 

Once this has successfully installed, you can instead run `tangos_timelink` within MPI with the following invocation:

```
mpirun -np 5 tangos_timelink --backend mpi4py
```
Here,
 * `mpirun -np 5` instructs MPI on your machine to run 5 processes. This would be appropriate for a 4-core machine because there is always one manager process and 4 worker processes. If you want to run across multiple nodes, only the first node should have one more process running than cores available. Consult your system administrator to figure this out. 
 * `tangos_timelink` is the script to run
 * `--backend mpi4py` crucially instructs tangos_timelink to parallelise using the mpi4py library. Alternatively you can use the `pypar` library. *If you specify no backend tangos will default to running in single-processor mode which means MPI will launch 5 processes that are not aware of each other's prescence. This is not what you want. However limitations in the MPI library mean it's not possible for tangos to auto-detect it has been MPI-launched.*
 
  For large simulations, you may need to use a machine with lots of memory  and/or use fewer processes than you have cores available because, when time-linking, each process loads two timesteps at once.
  
  