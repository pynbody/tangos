## Parallelisation


If you want to speed up time-consuming `tangos` operations from your command line, you can run many of them in parallel. For this you can either use python's built-in `multiprocessing` module, or MPI.

* If your computation is taking place on a single node, `multiprocessing` is probably your best option
* If your computation is taking place across multiple nodes,
 you must use MPI.

The eligible `tangos` commands for parallel execution are:

* `tangos link`
* `tangos write`
* `tangos crosslink`
* `tangos add` (since 1.8.0)

### How to run in parallel with `multiprocessing` (since 1.8.0)

From your command line or job script use:

```
tangos [normal tangos command here] --backend multiprocessing-<N>
```

where `<N>` should be replaced by the number of processes to
use.  See notes below on how to choose the number of processes (it's _not_ necessarily just the number of cores available).

### How to run in parallel with `mpi4py`

You first need to install MPI and `mpi4py` on your machine. This is straight-forward
with, for example, anaconda python distributions – just type `conda install mpi4py`. With regular python distributions, you need to install MPI on your machine and then `pip install mpi4py` (which will compile the python bindings).

Once this has successfully installed, you can run
in parallel, using the following from your command line:

```
mpirun -np <N> [normal tangos command here] --backend mpi4py
```
Here,
 * `<N>` should be replaced by the number of processes you wish to launch; e.g. for 10 processes, start with `mpirun -np 10`
 *  Then you type your normal set of commands, e.g. `tangos link ...` or `tangos write ...`.
 * `--backend mpi4py` crucially instructs tangos to parallelise using the mpi4py library.

 Alternatively you can use the `pypar` library to interface with MPI.
   *If you specify no backend tangos will default to running in single-processor mode which means MPI will launch N processes
   that are not aware of each other's presence. This is very much not what you want.
   Limitations in the MPI library mean it's not possible for tangos to reliably auto-detect it has been MPI-launched.*


### Important options for tangos write


For tangos write, there are multiple parallelisation modes. For best results, it's important to understand which is appropriate for your use case -- they balance file IO, memory usage and throughput differently.

The default mode parallelises at the snapshot level.
Each worker loads an entire snapshot at once, then works through the halos within that snapshot. This is highly efficient
in terms of communication requirements. However, for large simulations it can cause memory
usage to be overly high, and it also means that shared disks can be asked for a lot of IO all at the same time.

In case either the memory or IO requirements of this approach are not feasible, `tangos write` accepts a `--load-mode` argument:

* `--load-mode=server`: This strategy is completely different. The different processes now all work on a single timestep
  at a time. The single server process will load that (single)entire snapshot and pass only the bits of the data needed along to all other ranks. This vastly lowers memory requirements (only one full simulation is in memory at
a given time). However it increases communication needs between processes, which can cause significant performance degredation.

* `--load-mode=server-shared-mem`: available from version 1.9.0 onwards, this is the most powerful option, but it only works if all your processes are on the same physical machine. A server process handles loading data as above, making the memory and IO requirements the same as `--load-mode=server`. But then, in `server-shared-mem` mode, the server makes the data available to all other processes through _shared memory_, which is extremely efficient.


### Older load modes

The below are still available, but are less flexible and
are rarely likely to be the best option.

* `--load-mode=partial`: This strategy is similar to the default described above. However, only the data for a single
  halo at a time is loaded. Each time the writer moves onto the next halo, the corresponding particles are loaded.
  Partial loading is pretty efficient but be aware that calculations that need particle data outside the halo
  (for example see [the virial radius example in the custom properties tutorial](custom_properties.md#using-the-particle-data-outside-the-halo))
   will fail.
* `--load-mode=server-partial`: a hybrid approach where rank 0 loads only what is required to help the other ranks
   figure out what they need to load — for example, if a property requests a sphere surrounding the halo,
   the entire snapshot's position arrays will be loaded on rank 0, but no other data.
   The data on the individual ranks is loaded via partial loading (see `--load-mode=partial` above).

## tangos write worked example


Let's consider the longest process in the tutorials which involves writing images and more to
the [changa+AHF](first_steps_changa+ahf.md) tutorial simulation.

Some of the underlying _pynbody_ manipulations are already parallelised. One can therefore experiment
with different configurations but experience suggests the best option is to
[switch off all _pynbody_ parallelisation](https://pynbody.github.io/pynbody/tutorials/threads.html)
(i.e. set the number of threads to 1) and allow _tangos_ complete control. This is because only some _pynbody_ routines
are parallelised whereas _tangos_ is close to [embarassingly parallel](https://en.wikipedia.org/wiki/Embarrassingly_parallel).
Once pynbody threading is disabled, the version of the above command that is most efficient is:

 ```bash
mpirun -np 5 tangos write dm_density_profile gas_density_profile uvi_image --with-prerequisites --include-only="NDM()>5000" --include-only="contamination_fraction<0.01" --for tutorial_changa --backend mpi4py --load-mode server-shared-mem
```

for a machine with 4 processors.

**How many processes?** Note that the number of processes was actually one more than the number of processors. This is because the server process generally only uses the CPU while other processes are blocked. So, using only 4 processes would leave one processor idle most of the time.


**What load mode?** Here we chose `--load-mode=server-shared-mem` by considering the possibilities:

 * With the default load mode, the smoothing will be calculated across entire snapshots which is wasteful. It's an
   N log N calculation and N is needlessly large.
 * With `--load-mode=partial` the calculation will fail because the density profiles ask for particles that may be
   outside the region identified by the halo finder
 * With `--load-mode=server-partial`, everything will be fine and relatively efficient
   but we will keep reading data off the disk rather than keeping it in RAM. This might be useful for large simulations,
   but isn't needed here.
 * So we're left with `--load-mode=server` (for multi-node calculations) or `--load-mode=server-shared-mem` (for single-node calculations). Either is efficient in terms of computation and memory usage, but the latter is also efficient
  in terms of having minimal communication overheads between processes.

tangos link / crosslink
-----------------------

For `tangos link` and `tangos crosslink`, parallelisation is currently implemented only at the snapshot level. Suppose you have a simulation
with M particles. Each worker loads needs to store at least 2M integers at any one time (possibly more depending on the
underlying formats) in order to work out the merger tree.

Consequently for large simulations, you may need to use a machine with lots of memory and/or use fewer processes than you have
cores available.

tangos add
----------

The `tangos add` tool is also only parallelised at the snapshot
level, but it does not need to load much data per snapshot and so
this does not normally pose a problem.
