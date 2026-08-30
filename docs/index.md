# tangos

tangos builds a database of *summary* data for your own cosmological and zoom
simulations -- masses, profiles, images, star formation histories, merger trees --
so that questions spanning an entire simulation are answered in milliseconds,
without touching the raw particle data again. It is the same idea as the public
[Eagle](http://icc.dur.ac.uk/Eagle/database.php) and
[MultiDark](https://www.cosmosim.org/cms/documentation/projects/multidark-bolshoi-project/)
databases, for your own runs.

You can then explore that database from python, or from a web browser:

[![Tangos and its web server](images/video_play.png)](https://www.youtube.com/watch?v=xHyzJmNsVMw)

## Start here

**New to tangos?** You do not need any simulation data to learn what tangos
does, and almost nothing here needs pynbody either -- only one call in the
quick-start, which says so where it appears. Install it, download the ready-made tutorial database,
and query it:

1. [Installation](installation.rst) -- tangos and the tutorial database, in
   three steps.
2. [The tangos data model](explanation/concepts.rst) -- simulations, timesteps,
   objects, properties and links. Fifteen minutes here saves hours later,
   because every other page assumes it.
3. [Quick-start](tutorials/quickstart.rst) -- find a halo, read its properties,
   plot an image of a galaxy.
4. [Time series and populations](tutorials/time_series.rst) -- follow a halo
   back through its progenitors and plot how it grew.
5. [Live calculations](tutorials/live_calculations.rst) -- the language that
   makes the database more than a table of numbers.
6. [The web interface](tutorials/webserver.rst) -- the same database in a
   browser.

**Building a database from your own simulation** starts at
[making your first database](first_steps.md), which covers the supported
simulation codes and halo finders. You will need the raw simulation output and
a reader for it, usually pynbody.

**Configuring tangos** -- where the database lives, what gets written, how the
merger trees are thinned -- is covered in
[configuration](configuration.rst).

## Acknowledging the code

When using tangos, please acknowledge it by citing the release paper:
Pontzen & Tremmel, 2018, ApJS 237, 2.
[DOI 10.3847/1538-4365/aac832](https://doi.org/10.3847/1538-4365/aac832);
[arXiv:1803.00010](https://arxiv.org/pdf/1803.00010.pdf). Optionally you can
also cite the Zenodo DOI for the specific version of tangos that you are using,
which may be found [here](https://doi.org/10.5281/zenodo.1243070).

```{toctree}
:maxdepth: 2
:hidden:
:caption: Setup

installation
configuration
```

```{toctree}
:maxdepth: 2
:hidden:
:caption: Tutorials

explanation/concepts
tutorials/quickstart
tutorials/time_series
tutorials/live_calculations
tutorials/webserver
```

```{toctree}
:maxdepth: 2
:hidden:
:caption: Building a database

first_steps
first_steps_gadget+AHF
first_steps_gadget+rockstar
first_steps_gadget+subfind
first_steps_ramses+hop
first_steps_changa+ahf
first_steps_eagle
using_with_yt
custom_input_handlers
```

```{toctree}
:maxdepth: 2
:hidden:
:caption: Going further

custom_properties
histogram_properties
black_holes_and_crossmatching
tracking
mpi
rdbms
advanced
```

```{toctree}
:maxdepth: 2
:hidden:
:caption: Reference

live_calculation
reference/api/index
```

```{toctree}
:maxdepth: 1
:hidden:
:caption: Older material

data_exploration
Data exploration with python
data_exploration_webserver
old
```
