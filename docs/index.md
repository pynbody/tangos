_Tangos_ lets you build a database (along the lines of [Eagle](http://icc.dur.ac.uk/Eagle/database.php)
or [MultiDark](https://www.cosmosim.org/cms/documentation/projects/multidark-bolshoi-project/))
 for your own cosmological and zoom simulations.

Once tangos is set up, you'll be able to access your simulations from a web browser:

[![Tangos and its web server](images/video_play.png)](https://www.youtube.com/watch?v=xHyzJmNsVMw)


Acknowledging the code
----------------------
When using _tangos_, please acknowledge it by citing the release paper:
Pontzen & Tremmel, 2018, ApJS 237, 2. [DOI 10.3847/1538-4365/aac832](https://doi.org/10.3847/1538-4365/aac832);  [arXiv:1803.00010](https://arxiv.org/pdf/1803.00010.pdf). Optionally you can also cite the Zenodo DOI for the specific version of _tangos_ that you are using, which may be found [here](https://doi.org/10.5281/zenodo.1243070).


Installation: the very quick version
------------

You have a few options. For the most recent published version use:
```
pip install tangos
```
or, for pre-release versions (for example if you are working with pynbody v2) use:
```
pip install --pre tangos
```
or, for the latest version from the repository use
```
pip install git+https://github.com/pynbody/tangos.git
```

Pip should check for and install the _minimum_ prerequisites, but doesn't install _pynbody_. That's because _tangos_ is
written to be agnostic about how the underlying simulation snapshots are read, and so you might prefer to use _yt_ for example.
If you want to use _pynbody_, install it at the same time, e.g.

```
pip install tangos pynbody
```


Installation with tests and ancillary dependencies
--------------------------------------------------

If you wish to run the test suite (which is advised) or are planning to develop using tangos,
it is preferable to keep the source repository handy, in which case instead of the instructions
above use:
```
git clone https://github.com/pynbody/tangos.git
cd tangos
pip install -e .[test]
```

The `[test]` specified ensures that additional packages needed for testing are installed. This includes _pynbody_ and _yt_,
as well as _pytest_.

Once installed, you should check that _tangos_ is functioning correctly by entering the `tests` folder and
typing `pytest`. You should see a bunch of text scrolling by, ultimately finishing with the simple message `OK`.
If you get a failure message instead of `OK`, report it (with as much detail of your setup as possible) in the
github issue tracker.

Setting up paths
----------------

By default tangos will look for raw simulation data in your home folder and create its database file there as well.
If you don't want it to do this, you can set the environment variables `TANGOS_SIMULATION_FOLDER` (for the simulation folder)
and `TANGOS_DB_CONNECTION` (for the database file). For example, in bash:

```
export TANGOS_SIMULATION_FOLDER=/path/to/simulations/folder/
export TANGOS_DB_CONNECTION=/path/to/sqlite.db
```
or, in cshell:
```
setenv TANGOS_SIMULATION_FOLDER /path/to/simulations/folder/
setenv TANGOS_DB_CONNECTION /path/to/sqlite.db
```
The top line in each example points to the parent directory for all of your simulation data directories.
If you don't have any simulations (i.e. you are just using a database object already created) then you
should not have to worry about this variable. The second line points to the database object you wish to analyze;
by default this will be a sqlite file but you can also specify a
[sqlalchemy URL](http://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls); see also the notes on
MySQL / MariaDB below.

Remember, you will need to set these environment variables *every* time you start a new session on your computer prior
to booting up the database, either with the webserver or the python interface (see below).



Where next?
-----------

Now that you've set up the basics, you can either [make your first _tangos_ database](first_steps.md)
using some tutorial data or [download an existing database to perform data analysis](data_exploration.md).
