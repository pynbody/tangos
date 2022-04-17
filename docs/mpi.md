MPI Parallelisation
-------------------

If you want to speed up time-consuming `tangos` operations from your command line, such as `tangos link` and
`tangos write`, you can run them in parallel if you have MPI and `mpi4py` on your machine. This is straight-forward
with, for example, anaconda python distributions – just type `conda install mpi4py`.

Once this has successfully installed, you can run `tangos link` or `tangos write` within MPI with

```
mpirun -np N [normal tangos command here] --backend mpi4py
```
Here,
 * `mpirun -np N` instructs MPI on your machine to run N processes. For simple setups, this would normally be
    the number of processors plus one; for example you'd choose 5 for a 4-core machine.
    The extra process is optimal because there is always one manager process (which requires relatively little CPU)
    and 4 worker processes.
    If you want to run across multiple nodes, only the first node should have one more process running than cores available.
    Consult your system administrator to figure this out.
 *  Then you type your normal set of commands, e.g. `tangos link ...` or `tangos write ...`.
 * `--backend mpi4py` crucially instructs tangos to parallelise using the mpi4py library.
   Alternatively you can use the `pypar` library.
   *If you specify no backend tangos will default to running in single-processor mode which means MPI will launch N processes
   that are not aware of each other's presence. This is very much not what you want.
   Limitations in the MPI library mean it's not possible for tangos to reliably auto-detect it has been MPI-launched.*


Advanced options and memory implications: tangos write
-------------------------------------------------------

For tangos write, there are multiple parallelisation modes. The default mode parallelises at the snapshot level.
Each worker loads an entire snapshot at once, then works through the halos within that snapshot. This is highly efficient
in terms of limiting disk access and communication requirements. However, for large simulations it can cause memory
usage to be overly high.

To control the parallelisation, `tangos write` accepts a `--load-mode` argument:


* `--load-mode=partial`: This strategy is similar to the default described above. However, only the data for a single
  halo at a time is loaded. Each time the writer moves onto the next halo, the corresponding particles are loaded.
  Partial loading is pretty efficient but be aware that calculations that need particle data outside the halo
  (for example see [the virial radius example in the custom properties tutorial](custom_properties.md#using-the-particle-data-outside-the-halo))
   will fail.
* `--load-mode=server`: This strategy is completely different. The different processes now all work on a single timestep
  at a time. Rank 0 of your MPI processes will load that (single) entire snapshot and pass
   only the bits of the data needed along to all other ranks. This has the advantage over
   `--load-mode=partial` of allowing the calculations to request the surroundings of the halo (see above).
   However it has the disadvantage that rank 0 must load an entire snapshot (all arrays that are required).
   For really enormous simulations that might still be tricky.
* `--load-mode=server-partial`: a hybrid approach where rank 0 loads only what is required to help the other ranks
   figure out what they need to load — for example, if a property requests a sphere surrounding the halo,
   the entire snapshot's position arrays will be loaded on rank 0, but no other data.
   The data on the individual ranks is loaded via partial loading (see `--load-mode=partial` above).


tangos write example
---------------------

Let's consider the longest process in the tutorials which involves writing images and more to
the [changa+AHF](first_steps_changa+ahf.md) tutorial simulation.

Some of the underlying _pynbody_ manipulations are already parallelised. One can therefore experiment
with different configurations but experience suggests the best option is to
[switch off all _pynbody_ parallelisation](https://pynbody.github.io/pynbody/tutorials/threads.html)
(i.e. set the number of threads to 1) and allow _tangos_ complete control. This is because only some _pynbody_ routines
are parallelised whereas _tangos_ is close to [embarassingly parallel](https://en.wikipedia.org/wiki/Embarrassingly_parallel).
Once pynbody threading is disabled, the version of the above command that is most efficient is:

 ```bash
mpirun -np 5 tangos write dm_density_profile gas_density_profile uvi_image --with-prerequisites --include-only="NDM()>5000" --include-only="contamination_fraction<0.01" --for tutorial_changa --backend mpi4py --load-mode server
```

for a machine with 4 processors. Why did we specify `--load-mode=server`? Let's consider the possibilities:

 * With the default load mode, the smoothing will be calculated across entire snapshots which is wasteful. It's an
   N log N calculation and N is needlessly large.
 * With `--load-mode=partial` the calculation will fail because the density profiles ask for particles that may be
   outside the region identified by the halo finder
 * With `--load-mode=server-partial`, everything will be fine and relatively efficient
   but we will keep reading data off the disk rather than keeping it in RAM. This might be useful for large simulations,
   but isn't needed here.
 * So we're left with `--load-mode=server`, which is extremely efficient because the SPH smoothing
   lengths and densities are calculated using small particle numbers N.

Memory implications: tangos link
------------------------------------

For `tangos link`, parallelisation is currently implemented only at the snapshot level. Suppose you have a simulation
with M particles. Each worker loads needs to store at least 2M integers at any one time (possibly more depending on the
underlying formats) in order to work out the merger tree.

Consequently for large simulations, you may need to use a machine with lots of memory and/or use fewer processes than you have
cores available.

It would be possible to implement algorithms where the data is more distributed – or to load in halo trees generated by
other tools. If demand is sufficient for either of these abilities, support could
be prioritised for future versions of _tangos_.
