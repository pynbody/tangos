.. Last checked by AP: 2026-08-29

.. _api_input_handlers:

Reading a new simulation format
=================================

.. currentmodule:: tangos.input_handlers

An input handler is how tangos reads a particular simulation code's raw
output. This page documents the subclassing contract and every handler tangos
ships with; for a walkthrough, see :doc:`/custom_input_handlers`, and for
which handler goes with which finder, see ``reference/simulation_formats``
(forthcoming).

.. autosummary::

   HandlerBase
   get_named_handler_class
   pynbody.PynbodyInputHandler

The contract
-------------

.. autoclass:: HandlerBase
   :members: enumerate_timestep_extensions, enumerate_objects, get_properties,
             get_timestep_properties, load_timestep,
             load_timestep_without_caching, load_object, load_region,
             load_tracked_region, available_object_property_names_for_timestep,
             iterate_object_properties_for_timestep, get_stat_file,
             best_matching_handler, handler_class_name, strip_slashes,
             halo_stat_file_class_name
   :undoc-members:
   :member-order: bysource

.. note::
   ``HandlerBase`` also has a ``quicker`` flag, set (to ``False``) in
   ``__init__`` rather than declared on the class, so it does not appear in
   the listing above; setting it on an instance lets a handler cut corners
   (skip a resolution check, estimate it from file size instead) for speed.
   ``match_objects`` -- used to identify the same object across two timesteps
   for time-linking -- is **not** part of this contract, despite
   :doc:`/custom_input_handlers` presenting it as one; it is defined on
   :class:`~tangos.input_handlers.pynbody.PynbodyInputHandler` below (and,
   separately, on the yt and Gadget-4/HBT+ handlers). See ``KNOWN_ISSUES.md``.

.. autofunction:: get_named_handler_class

The default handler
---------------------

.. autoclass:: tangos.input_handlers.pynbody.PynbodyInputHandler
   :members: match_objects, create_bridge
   :undoc-members:
   :show-inheritance:

Most third-party handlers for particle-based codes should subclass this one
rather than :class:`HandlerBase` directly.

.. currentmodule:: tangos.input_handlers.finding

.. autoclass:: PatternBasedFileDiscovery
   :members: patterns, auxiliary_file_patterns, enable_autoselect,
             best_matching_handler
   :undoc-members:

A new handler almost always mixes this in: ``patterns`` is the list of glob
patterns used to recognise the format's snapshot files on disk, and is what
most concrete handlers below actually override.

The bundled handlers
----------------------

Every class here is a legal value of ``tangos add --handler=...`` -- for
example ``--handler=pynbody.ChangaInputHandler``. Their ``patterns`` and
``auxiliary_file_patterns`` class attributes (inherited from
:class:`~tangos.input_handlers.finding.PatternBasedFileDiscovery`) are the
part of each worth reading; :doc:`the simulation-formats reference
<input_handlers>` (forthcoming, ``reference/simulation_formats``) lists what
each assumes about the files on disk in one table.

.. currentmodule:: tangos.input_handlers.pynbody

.. autoclass:: GadgetSubfindInputHandler
   :show-inheritance:
.. autoclass:: Gadget4HDFSubfindInputHandler
   :show-inheritance:
.. autoclass:: Gadget4HBTPlusInputHandler
   :members: match_objects
   :undoc-members:
   :show-inheritance:
.. autoclass:: GadgetRockstarInputHandler
   :show-inheritance:
.. autoclass:: AHFInputHandler
   :show-inheritance:
.. autoclass:: GadgetAHFInputHandler
   :show-inheritance:
.. autoclass:: RamsesAHFInputHandler
   :show-inheritance:
.. autoclass:: ChangaInputHandler
   :show-inheritance:
.. autoclass:: ChangaIgnoreIDLInputHandler
   :show-inheritance:
.. autoclass:: ChangaUseIDLInputHandler
   :show-inheritance:
.. autoclass:: ChangaAHFv1InputHandler
   :show-inheritance:

.. currentmodule:: tangos.input_handlers.ramsesHOP

.. autoclass:: RamsesHOPInputHandler
   :show-inheritance:
.. autoclass:: RamsesAdaptaHOPInputHandler
   :show-inheritance:

.. currentmodule:: tangos.input_handlers.yt

.. autoclass:: YtInputHandler
   :members: match_objects
   :undoc-members:
   :show-inheritance:
.. autoclass:: YtRamsesRockstarInputHandler
   :show-inheritance:
.. autoclass:: YtChangaAHFInputHandler
   :show-inheritance:
.. autoclass:: YtEnzoRockstarInputHandler
   :show-inheritance:

.. currentmodule:: tangos.input_handlers.eagle

.. autoclass:: EagleLikeInputHandler
   :show-inheritance:

See :doc:`/first_steps_eagle`.

.. currentmodule:: tangos.input_handlers.caterpillar

.. autoclass:: CaterpillarInputHandler
   :show-inheritance:

Halo-finder statistics files
------------------------------

.. currentmodule:: tangos.input_handlers.halo_stat_files

.. autoclass:: HaloStatFile
   :members: iter_rows, all_columns
   :undoc-members:
   :member-order: bysource

.. autoclass:: AHFStatFile
   :show-inheritance:
.. autoclass:: RockstarStatFile
   :show-inheritance:
.. autoclass:: AmigaIDLStatFile
   :show-inheritance:

Subclass ``HaloStatFile`` for a new finder's stat-file layout; ``iter_rows``
is what a handler's ``get_properties`` calls to import finder-computed columns
into the database.

ChaNGa black hole logs
------------------------

``tangos import-changa-bh`` is a documented workflow with no ``HandlerBase``
relationship of its own; these classes are what it reads.

.. currentmodule:: tangos.input_handlers.changa_bh

.. autoclass:: BHLogData
   :members: get_at_stepnum, get_at_stepnum_for_id, get_last_entry_for_id,
             determine_merger_ratio, get_for_named_snapshot,
             get_for_named_snapshot_for_id
   :undoc-members:
   :member-order: bysource

.. autoclass:: BlackHolesLog
   :members: get_existing_or_new
   :undoc-members:
   :show-inheritance:

.. autoclass:: ShortenedOrbitLog
   :show-inheritance:

Merger-tree importers
-----------------------

``tangos import-ahf-trees`` and ``tangos import-consistent-trees`` read these.

.. autoclass:: tangos.input_handlers.ahf_trees.AHFTree
   :undoc-members:

.. autoclass:: tangos.input_handlers.consistent_trees.ConsistentTrees
   :undoc-members:

.. seealso::

   :doc:`properties` for the proxy-object classes used to report subhalo
   parentage from within a handler, and :doc:`building` for
   :class:`~tangos.tools.add_simulation.SimulationAdderUpdater`, which drives a
   handler from python.
