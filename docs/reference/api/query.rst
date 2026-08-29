.. Last checked by AP: 2026-08-29

.. _api_query:

Finding objects and connecting to a database
=============================================

.. currentmodule:: tangos

These are the functions almost every tangos script starts with: open a
database, then look up a simulation, timestep or halo from it.

.. autosummary::

   get_object
   get_halo
   get_timestep
   get_simulation
   get_item
   get_items
   all_simulations
   all_creators
   get_haloproperty
   core.init_db
   core.get_default_session
   core.set_default_session
   core.get_default_engine
   core.close_db
   core.close_session
   core.sim_query_from_name_list

Looking up an object
---------------------

.. autofunction:: get_object

.. autofunction:: get_halo

.. note::
   ``get_halo`` and ``get_object`` are the same function under two names.
   ``get_object`` is the name to use in new code -- it covers every object
   typetag (halo, BH, group, tracker, phantom), not only halos. ``get_halo`` is
   kept because it is the name used throughout the existing tutorials; it is
   not currently marked deprecated, and both spellings will keep working.

.. autofunction:: get_timestep

.. autofunction:: get_simulation

.. autofunction:: get_item

.. autofunction:: get_items

What is in this database?
--------------------------

.. autofunction:: all_simulations

.. autofunction:: all_creators

.. autofunction:: get_haloproperty

The halo-path syntax
---------------------

``get_object``, ``get_item`` and the string form of ``get_timestep`` all parse
the same slash-separated path syntax. There is one canonical spelling:

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Path
     - Resolves to
   * - ``my_simulation``
     - a :class:`~tangos.core.simulation.Simulation` (``get_simulation``)
   * - ``my_simulation/step_0001``
     - a :class:`~tangos.core.timestep.TimeStep` (``get_timestep``); the
       timestep component may contain a ``%`` SQL wildcard, e.g.
       ``my_simulation/%0001``
   * - ``my_simulation/step_0001/1``
     - object number 1 of the default typetag (halo) in that timestep
   * - ``my_simulation/step_0001/halo_1``
     - the same object, with its typetag spelled out explicitly
   * - ``my_simulation/step_0001/BH_3``
     - black hole number 3 in that timestep; ``group_``, ``tracker_`` and
       ``phantom_`` work the same way

Both ``.../1`` and ``.../halo_1`` are accepted and mean the same thing for the
``halo`` typetag; only the second form works for the other four typetags. The
tutorials use both spellings interchangeably, which is confusing on first
read -- prefer the explicit ``typetag_number`` form in your own code. The
parsing lives in
:meth:`SimulationObjectBase.typecode_and_number_from_human_identifier
<tangos.core.halo.SimulationObjectBase.typecode_and_number_from_human_identifier>`
and :func:`get_timestep`.

Connecting to a database
-------------------------

.. currentmodule:: tangos.core

.. autofunction:: init_db

.. autofunction:: get_default_session

.. autofunction:: set_default_session

.. autofunction:: get_default_engine

.. autofunction:: close_db

.. autofunction:: close_session

.. autofunction:: sim_query_from_name_list

.. seealso::

   :doc:`objects` for the classes these functions return, and
   :doc:`/rdbms` for choosing between sqlite, MySQL and PostgreSQL.
