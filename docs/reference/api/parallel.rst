.. Last checked by AP: 2026-08-29

.. _api_parallel:

Parallel analysis from python
===============================

.. currentmodule:: tangos.parallel_tasks

``tangos.parallel_tasks`` is mostly an internal message-passing layer used by
``tangos write`` and the other command-line tools -- a property class does
**not** need to be aware of it, and :doc:`/mpi` documents the command-line side.
But it has a genuine seven-symbol surface for parallelising your *own*
analysis script the same way the CLI tools parallelise theirs, and that is all
this page documents.

.. autosummary::

   use
   launch
   distributed
   synchronized
   ExclusiveLock
   barrier
   parallelism_is_active

.. autofunction:: use

The backend name is one of ``"multiprocessing-N"`` (fork ``N`` worker
processes), ``"mpi4py"``, or ``"null"`` (no parallelism; the default) --
the same strings accepted by the CLI tools' ``--backend`` option, without the
now-dead ``pypar`` alternative that :doc:`/mpi` still mentions (see
``KNOWN_ISSUES.md``).

.. autofunction:: launch

.. warning::
   ``launch`` closes the database connection and re-opens it after forking
   into the backend's worker processes. Any ORM object (a
   :class:`~tangos.core.simulation.Simulation`, a halo, ...) fetched *before*
   calling ``launch`` is attached to a session that no longer exists once your
   function starts running under it, and must be re-queried from inside the
   function you pass in.

.. autofunction:: distributed

.. autofunction:: synchronized

``distributed`` divides a work list across ranks, so each item is processed
once in total; ``synchronized`` gives every rank the same full list, for when
each rank must see every item (for example, to build the same in-memory
lookup table everywhere). Both support ``allow_resume`` for restarting a
partially-completed job.

.. autoclass:: ExclusiveLock
   :undoc-members:

Context manager for the "only one rank may write to the database at a time"
pattern -- acquire it before any write your parallel script performs.

.. autofunction:: barrier

.. autofunction:: parallelism_is_active

.. seealso::

   :doc:`building` for the tools (``PropertyWriter``, ``SimulationAdderUpdater``,
   ...) that already call these functions for you -- most users only need this
   page when writing a *standalone* parallel script over an existing database.
