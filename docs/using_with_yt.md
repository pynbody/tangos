Using tangos with yt
====================

The core of _tangos_ is agnostic about the way in which raw simulation data is loaded and processed.
The developers of _tangos_ use _pynbody_ for this purpose but it can be adapted for other libraries
with relative ease.

As a demonstration, embryonic _yt_ support is included.

Initial set-up: tangos+yt
--------------------------

Make sure you have followed the [initial set up instructions](index.md). Additionally,
you will need to install yt version 3.4.0 or later.

Next, download the [raw simulation data](https://zenodo.org/record/5155467/files/tutorial_changa.tar.gz?download=1). Unpack the tar file either in your
home folder or the folder that you pointed the `TANGOS_SIMULATION_FOLDER` environment
variable to.

Finally, download the [tutorial_changa_yt](https://zenodo.org/record/5155467/files/tutorial_changa_yt.tar.gz?download=1)
tar. Unpack the tar file in the same folder as you unpacked tutorial_changa. The `tutorial_changa_yt`
folder contains a series of symlinks that present the dataset with a slightly different files structure
that is compatible with _yt_. (Specifically, _yt_ can't cope with the AHF
files being in the same folder as the tipsy files.)

Using yt to add a simulation
----------------------------

At the unix command line type:

```
tangos add tutorial_changa_yt --handler=yt.YtInputHandler
```

The process should take about a minute on a standard modern computer, during which you'll see a bunch of log messages
scroll up the screen.

 Let's pick this command apart

  * `tangos` is the command-line tool to administrate your tangos database
  * `add` is a subcommand to add a new simulation
  * `tutorial_changa_yt` identifies the simulation we're adding
  * `--handler=yt.YtInputHandler` requests that _tangos_ uses yt as the "handler" for raw simulation and halo files.


Verify the underlying data is being read by yt
----------------------------------------------

For interest, you might like to check that the data really is now being handled by yt. Open a python session and type
```python
import tangos
tangos.get_halo("tutorial_changa_yt/%960/halo_1").load()
```
You should see that the returned dataset is indeed a yt object. If you previously also ran the changa+AHF tutorial
with pynbody, you can also verify that
```python
tangos.get_halo("tutorial_changa/%960/halo_1").load()
```
continues to return a `pynbody` `SimSnap` as before. Tangos allows simulations loaded with different libraries to
coexist.

Import some AHF-defined properties
----------------------------------

At the unix command line type:

```
tangos import-properties Mvir Rvir --for tutorial_changa_yt
```

The process should take less than a minute on a standard modern computer,
during which you'll see a bunch of log messages scroll up the screen.

The example command line lists two properties, `Mvir` and `Rvir` to import from the stat files. The added directive
`--for tutorial_changa_yt` specifies which simulation you want to apply this operation to. It's not strictly
necessary to add this if you only have one simulation in your database.


Add some more interesting properties
------------------------------------

Add a density profile using:

```bash
tangos write dm_density_profile --for tutorial_changa
```

Here,
 * `tangos write` is the same script you called above to add properties to the database
 * `dm_density_profile` is an array representing the dark matter density profile; to see all available properties
   you can call `tangos list-possible-properties`.

You might note that `dm_density_profile` is also the name of a property that can be calculated using
`pynbody` (see the [changa+AHF tutorial](first_steps_changa+ahf.md)). _Tangos_ is able to select the correct piece of
code to work with _yt_ instead of _pynbody_. For more information on how this is accomplished,
see the [custom output handler documentation](custom_input_handlers.md).
