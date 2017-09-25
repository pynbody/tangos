Tangos Tutorial
===============

Import the simulation
---------------------

At the unix command line type:

```
tangos_manager add tutorial --handler pynbody.SubfindOutputSetHandler --min-particles 100
```

The process should take about a minute on a standard modern computer, during which you'll see a bunch of log messages scroll up the screen.
 
 Let's pick this command apart
 
  * `tangos_manager` is the command-line tool to administrate your tangos database
  * `add` is a subcommand to add a new simulation
  * `tutorial` identifies the simulation we're adding
  * `--handler pynbody.SubfindOutputSetHandler` identifies the _handler_ for our simulation. A handler defines how to load a simulation and its associated halo catalogue. Here we'll use `pynbody`'s ability to load gadget and subfind files. 
  * `--min-particles 100` imports only halos/groups with at least 100 particles. 

 
Note that all _tangos_ command-line tools provide help. For example `tangos_manager --help` will show you all subcommands, and `tangos_manager add --help` will tell you more about the possible options for adding a simulation.
  
At this point, the database knows about the existence of timesteps and their halos and groups in our simulation, but nothing about the properties of those halos or groups. We need to add more information before the database is useful.


Load in subfind's properties
----------------------------

At the unix command line type:

```
tangos_import_from_subfind
```

The process should take about a minute on a standard modern computer, during which you'll see a bunch of log messages scroll up the screen.

Note that if you did not correctly specify the Subfind handler in the step above, this will just generate an error. To recover from that situation, the easiest thing is to delete the database file you created and start again.

Generate the merger trees
-------------------------

The merger trees are most simply generated using pynbody's bridge function to do this, type

```
tangos_timelink
```

which builds the merger tree for the halos, and then you probably also want to run

```
tangos_timelink --type group
```
to make the merger tree for the groups.

The construction of each merger tree should take a couple of minutes,  and again you'll see a log scroll up the screen while it happens.

Now let's take a look at what we've created
-------------------------------------------

We're ready to explore the simulation. Depending on your preferences you might prefer to explore with the web service or direct from python.