.. Checked by AP 30/10/26

.. _parallel:

Running tangos in parallel
==========================

Building a database is dominated by a few slow steps -- above all ``tangos
write``, which loads every snapshot and calculates properties for every object
in it. These can run on many cores at once, either on one machine or across the
nodes of a cluster. This page shows how, and how to choose the options that
decide how well it goes.

.. note:: Before you start, make sure you have a database you can add to, and
 the simulation data it was built from; :doc:`/tutorials/first_database` builds
 one. Everything here applies to any simulation format.

Which commands run in parallel
------------------------------

Six commands divide their work between processes when given a parallel backend:

* ``tangos add``, ``tangos import-properties``, ``tangos link``, ``tangos
  crosslink`` and ``tangos import-changa-bh``, which give each process whole
  timesteps (or pairs of timesteps) to work on;
* ``tangos write``, which has more options; see :ref:`parallel_load_modes`.

Every other command runs serially, whatever you pass it.

Choosing a backend
------------------

On a single machine, use python's own ``multiprocessing``:

.. code-block:: bash

   tangos write dm_density_profile --for tutorial_changa --backend multiprocessing-5

``multiprocessing-5`` starts five processes. One of them is a *server*, which
hands out work and coordinates writes to the database rather than calculating
anything, so this command has four processes doing the work. See
:ref:`parallel_how_many` for how many to ask for.

.. versionadded:: 1.8.0
   The ``multiprocessing`` backend, and parallel ``tangos add``.

Across the nodes of a cluster, use MPI. Install MPI and ``mpi4py`` first: with
conda, ``conda install mpi4py`` does both; otherwise install MPI through your
system or cluster and then ``pip install mpi4py``, which compiles against it.
Then launch through ``mpirun``, and tell tangos to use it:

.. code-block:: bash

   mpirun -np 5 tangos write dm_density_profile --for tutorial_changa --backend mpi4py

Here, too, one of the five processes is the server.

.. warning::

   **Always pass** ``--backend`` **under** ``mpirun``. Without it, tangos cannot
   tell that MPI launched it, so ``mpirun -np 5`` starts five independent
   serial copies of the command, each doing all of the work and competing to
   write the same results.

.. warning::

   An argument containing a space breaks when ``--backend`` is present: tangos
   strips ``--backend`` from the command line before parsing it, and in doing so
   loses the shell's quoting. So ``--include-only="NDM() > 1000"`` arrives as
   three separate words and the command fails, while
   ``--include-only="NDM()>1000"`` works. Leave the spaces out of filter
   expressions when running in parallel.

.. The --backend quoting bug is a stage 6 item in KNOWN_ISSUES.md; update the
   warning above when it is fixed.

.. _parallel_load_modes:

Choosing how ``tangos write`` loads data
----------------------------------------

``tangos write`` can share out the work in different ways, which trade memory,
disk access and communication between processes against each other. You choose
with ``--load-mode``; which is best depends on the size of your snapshots and
on whether your processes share one machine.

*Default* (no ``--load-mode``)
   Each process takes a whole timestep, loads the entire snapshot, and works
   through every object in it. Processes hardly need to talk to each other, so
   this is efficient -- but every process holds a whole snapshot in memory, and
   they all read from disk at once. Fine for zoom simulations; often too much
   for large volumes.

``--load-mode=server``
   All processes work on the same timestep together. The server loads the
   snapshot once and sends each process only the particles for the objects it
   is calculating. Only one snapshot is in memory at a time, at the cost of
   much more communication between processes.

``--load-mode=server-shared-mem``
   As ``server``, but the server shares the snapshot with the other processes
   through shared memory rather than sending it to them. This keeps the low
   memory use of ``server`` without its communication cost, and is usually the
   best choice -- but all processes must be on the same machine.

   .. versionadded:: 1.9.0

Two older modes remain, but are rarely the best option:

``--load-mode=partial``
   As the default, but each process loads only one object's particles at a
   time, straight from disk. Efficient, but a calculation that needs particles
   outside the object fails -- and that includes the built-in profiles and
   images, which work on a sphere around each object.

``--load-mode=server-partial``
   The server loads only what is needed to work out which particles each
   calculation needs (for example, the positions of every particle, to find
   those in a sphere around an object), and each process then loads its own
   particles from disk as in ``partial``.

The server modes divide the objects of one timestep between processes, so
they need at least two processes besides the server to gain anything.

A worked example
^^^^^^^^^^^^^^^^

The slowest step of :doc:`/tutorials/first_database` calculates profiles and
images for the ChaNGa tutorial simulation. On a machine with four cores, the
most efficient way to run it is:

.. code-block:: bash

   tangos write dm_density_profile gas_density_profile uvi_image --with-prerequisites --include-only="NDM()>1000" --include-only="contamination_fraction<0.01" --for tutorial_changa --backend multiprocessing-5 --load-mode server-shared-mem

Why ``server-shared-mem``? The profiles and images are calculated from all the
particles in a sphere around each halo, not only those the halo finder assigned
to it. Going through the alternatives:

* With the default mode, each process holds a whole snapshot and cuts each
  sphere out of it. To make the gas images, pynbody first works out a smoothing
  length for every gas particle it has been given -- here, the whole snapshot --
  which costs :math:`N \log N` in the number of particles, and most of that work
  is for particles in no sphere at all. The server modes hand each process just
  the sphere, so the smoothing is done for those particles alone.
* With ``partial``, the calculations fail: ``partial`` loads only the halo
  finder's particles, and cannot provide a sphere.
* With ``server-partial``, everything would work, and fairly efficiently, but
  each process would keep reading particles from disk rather than sharing them
  in memory. That helps for snapshots too large for one machine's memory, which
  these are not.
* That leaves ``server`` and ``server-shared-mem``, which both keep one copy of
  the snapshot in memory. ``server`` sends each sphere's particles from the
  server to the process that needs them; ``server-shared-mem`` lets every
  process read them directly from the server's memory, which is much faster but
  possible only because all the processes share one machine. On several nodes,
  ``server`` would be the choice.

pynbody parallelises some of its own operations with threads. When tangos is
running several processes, these mostly compete with each other; experience
suggests that it is best to switch pynbody's threading off and let tangos do all
of the parallelisation. Put this in ``~/.pynbodyrc``:

.. code-block:: ini

   [general]
   number_of_threads: 1

(see `pynbody's notes on parallelism
<https://pynbody.readthedocs.io/latest/tutorials/parallelism.html>`_ for more).

.. _parallel_how_many:

How many processes?
^^^^^^^^^^^^^^^^^^^

The example asks for five processes on four cores. The server spends most of its
time waiting, so a process per core *plus one* for the server keeps every core
busy. Memory can limit you before cores do: in the default and ``partial``
modes every process holds its own data, so check that the number you ask for
fits in memory.

``tangos link`` and ``tangos crosslink`` are parallel only at the level of
timesteps, and each process needs to hold at least two integers per particle
in the simulation to match objects between snapshots. For large simulations,
that can mean using fewer processes than you have cores. ``tangos add`` loads
little per snapshot, so is rarely limited this way.

.. _pickle_results:

If using sqlite on a network file system
----------------------------------------

sqlite keeps the whole database in one file, and coordinates writers with file
locks. tangos makes its processes take turns to commit, but with many processes
waiting their turn -- especially on the network file systems of clusters, which
can be slow to release locks -- a run can still fail with errors saying the
database is locked, or just become very slow due to thrashing the database file.

One answer is to move the database to a server (see
:doc:`/how-to/database_backends`). Another, which keeps sqlite, is to have
``tangos write`` save its results to files rather than to the database, and commit
them all afterwards in one go:

.. code-block:: bash

   mpirun -np 5 tangos write dm_density_profile --for tutorial_changa --backend mpi4py --pickle-results
   tangos write-pickled-results tangos_results/*.pickle

The first command calculates exactly as before, but each process saves its
results as pickle files in ``tangos_results/`` instead of committing them. To
use another folder, name it after the option: ``--pickle-results my_folder``. The second command, run
serially once the first has finished, commits every file to the database,
attributing the results to the run that calculated them, and deletes the files
once they are all committed.

Until you commit them, the results are not in the database, so nothing that reads
it -- another ``tangos write``, or your own analysis -- can see them.
``write-pickled-results`` also suspends the database's indexes while it works, so
make sure nothing else is using the database at the time.

.. versionadded:: 1.12.0

Running under a batch scheduler
-------------------------------

On a shared cluster you will not run these commands interactively; you submit them.
The details differ between sites, but the shape of the job script does not. Here is a
PBS example that builds a database for one ChaNGa simulation with AHF halos, from
end to end, on a single node:

.. code-block:: bash

   #PBS -S /bin/bash
   #PBS -N tangos-build
   #PBS -l select=1:ncpus=64:mpiprocs=32
   #PBS -l walltime=3:00:00
   #PBS -j oe
   cd $PBS_O_WORKDIR

   # activate whatever python environment tangos is installed into, and set
   # TANGOS_DB_CONNECTION / TANGOS_SIMULATION_FOLDER, as the compute nodes do not
   # necessarily inherit your login environment

   SIMS="my_simulation"

   tangos add $SIMS
   tangos import-properties Mvir Rvir --for $SIMS
   mpirun tangos link --for $SIMS --backend mpi4py
   mpirun tangos write dm_density_profile --with-prerequisites --for $SIMS --load-mode=server-shared-mem --backend mpi4py
   mpirun tangos import-changa-bh --for $SIMS --backend mpi4py
   mpirun tangos write BH_mass --load-mode=server-shared-mem --for $SIMS --type BH --backend mpi4py

``mpiprocs`` is deliberately lower than ``ncpus``. Each process needs memory as well as a
core, so you may need to do this depending on the size of your simulation. 

``tangos add`` and ``import-properties`` run serially here, because they are quick
next to the rest. The profiles use ``--load-mode=server``, not ``server-shared-mem``,
because the processes are spread over two nodes.

The last two lines are specific to ChaNGa black holes; drop them for a simulation
without them. ``--type BH`` says "do this for the black holes rather than the halos".

If the job runs out of time, submit it again. ``tangos write`` carries on where
it stopped (:ref:`large_simulations_restart` explains how), and ``tangos link``
skips pairs of timesteps that are already linked; ``tangos add``, given a
simulation that is already in the database, adds only timesteps it does not yet
have.

.. seealso::

   :doc:`/how-to/large_simulations` for reducing how much work there is to do in
   the first place, and :doc:`/reference/api/parallel` for using tangos'
   parallel tasks from your own python code.
