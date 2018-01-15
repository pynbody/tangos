_Tangos_ lets you build a database (along the lines of [Eagle](http://icc.dur.ac.uk/Eagle/database.php) 
or [MultiDark](https://www.cosmosim.org/cms/documentation/projects/multidark-bolshoi-project/))
 for your own cosmological and zoom simulations. 
 
Getting started with tangos
---------------------------
**This is a beta version of tangos**. When _tangos_ is released there will be an accompanying paper.
_Tangos_ is GPL-licenced but good scientific practice requires you to acknowledge its use. Until the paper is
available please use the following acknowledgement or equivalent:

> This work made use of the _tangos_ analysis stack (Pontzen et al in prep); see www.github.com/pynbody/tangos.


Installation
------------

To install _tangos_ first clone the repository, then use the standard setuptools `install` command:

```
git clone https://github.com/pynbody/tangos.git
cd tangos
python setup.py install
```

Note that this is preferable to installing using `pip install git+https://github.com/pynbody/tangos.git`;
although using pip in this way will work, you will be missing ancillary files (like the tests and the
`.ini` files for launching the web server).

This should check for and install the _minimum_ prerequisites, but doesn't install _pynbody_. That's because _tangos_ is
written to be agnostic about how the underlying simulation snapshots are read so in principle you could use e.g. _yt_.
For all current tutorials, _pynbody_ is the preferred reading system and so for an easy life you should install it:

```
pip install git+https://github.com/pynbody/pynbody.git
```

To run the tests, you will also need to install _yt_ e.g. using `pip install yt`. 
Once installed, you should check that _tangos_ is functioning correctly by entering the `tests` folder and
typing `nosetests`. You should see a bunch of text scrolling by, ultimately finishing with the simple message `OK`.
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
[sqlalchemy URL](http://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls).

Remember, you will need to set these environment variables *every* time you start a new session on your computer prior
to booting up the database, either with the webserver or the python interface (see below).

Where next?
-----------

Now that you've set up the basics, you can either [make your first _tangos_ database](first_steps.md)
using some tutorial data or [download an existing database to perform data analysis](data_exploration.md).
