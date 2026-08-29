.. Last checked by AP: 2026-08-29

.. _api_building:

Building and maintaining a database from python
==================================================

Every class on this page has a ``tangos <subcommand>`` command-line front end
(documented at ``reference/cli``, forthcoming). They are documented here too
because the CLI cannot be scripted with dynamic arguments, and a script that
builds or maintains many simulations in a loop needs the objects directly.

.. currentmodule:: tangos.tools

.. autosummary::

   GenericTangosTool
   add_simulation.SimulationAdderUpdater
   property_writer.PropertyWriter
   crosslink.TimeLinker
   crosslink.CrossLinker
   property_deleter.PropertyDeleter
   db_importer.DBImporter
   merger_tree_patcher.MergerTreePatcher
   merger_tree_patcher.MergerTreePruner
   timestep_thinner.TimestepThinner
   property_importer.PropertyImporter
   pickled_results_writer.PickledResultsWriter
   tangos.tracking.new
   tangos.core.tracking.update_tracker_halos

The tool base class
---------------------

.. autoclass:: GenericTangosTool
   :members: tool_name, tool_description, add_parser_arguments,
             process_options, run_calculation_loop, parallel
   :undoc-members:
   :member-order: bysource

Subclassing this (``add_tools`` walks ``__subclasses__``) is a real plugin
point: your class becomes a ``tangos <tool_name>`` subcommand automatically.
Only ``run_calculation_loop`` and the constructor are documented individually
below for each built-in tool; ``add_parser_arguments``/``process_options`` are
argparse plumbing covered by ``reference/cli`` (forthcoming, via
sphinx-argparse) rather than repeated here.

Adding and updating simulations
---------------------------------

.. autoclass:: tangos.tools.add_simulation.SimulationAdderUpdater
   :members: __init__, scan_simulation_and_add_all_descendants
   :undoc-members:
   :show-inheritance:

``tangos add`` in python. The constructor reads ``min_halo_particles`` and
``max_num_objects`` from the ``configuration`` settings (forthcoming page) as
defaults, and takes ``renumber``; all three end up as plain attributes on the
instance, which you can reassign before calling
``scan_simulation_and_add_all_descendants``.

Writing properties
--------------------

.. autoclass:: tangos.tools.property_writer.PropertyWriter
   :show-inheritance:

``tangos write`` in python.

Linking timesteps
-------------------

.. autoclass:: tangos.tools.crosslink.TimeLinker
   :show-inheritance:
.. autoclass:: tangos.tools.crosslink.CrossLinker
   :show-inheritance:
.. autoclass:: tangos.tools.crosslink.GenericLinker
   :show-inheritance:

``tangos link`` (``TimeLinker``, within one simulation) and ``tangos
crosslink`` (``CrossLinker``, between two simulations sharing particle IDs).

Deleting, importing and thinning
-----------------------------------

.. autoclass:: tangos.tools.property_deleter.PropertyDeleter
   :show-inheritance:

``tangos delete-properties``.

.. autoclass:: tangos.tools.db_importer.DBImporter
   :show-inheritance:

``tangos import`` -- merges another sqlite database's rows into this one; the
obvious sequel to :doc:`/rdbms`.

.. autoclass:: tangos.tools.merger_tree_patcher.MergerTreePatcher
   :show-inheritance:
.. autoclass:: tangos.tools.merger_tree_patcher.MergerTreePruner
   :show-inheritance:

``tangos patch-trees`` / ``tangos prune-trees``.

.. autoclass:: tangos.tools.timestep_thinner.TimestepThinner
   :show-inheritance:

``tangos thin-timesteps``.

.. autoclass:: tangos.tools.property_importer.PropertyImporter
   :show-inheritance:

``tangos property-import``.

.. autoclass:: tangos.tools.pickled_results_writer.PickledResultsWriter
   :show-inheritance:

The object behind ``--pickle-results`` / ``tangos write-pickled-results``:
write calculation results to pickle files instead of directly to the
database, for merging in later -- the usual workaround when concurrent
sqlite writers are causing lock contention.

Merger-tree and black-hole importers
----------------------------------------

One class per halo finder / log format.

.. autoclass:: tangos.tools.ahf_merger_tree_importer.AHFTreeImporter
   :show-inheritance:
.. autoclass:: tangos.tools.consistent_trees_importer.ConsistentTreesImporter
   :show-inheritance:
.. autoclass:: tangos.tools.subfind_merger_tree_importer.SubfindTreeImporter
   :show-inheritance:
.. autoclass:: tangos.tools.changa_bh_importer.ChangaBHImporter
   :show-inheritance:

Trackers
---------

.. currentmodule:: tangos.tracking

.. autofunction:: new

The entry point of :doc:`/tracking`: create a tracker from a set of pynbody
particles. The return value is the tracker *number* (unique within the
simulation), not a :class:`~tangos.core.tracking.TrackData` object -- fetch
the object afterwards with ``simulation.trackers`` if you need it.

.. currentmodule:: tangos.core.tracking

.. autofunction:: update_tracker_halos

Re-materialises tracker objects for timesteps added after a tracker was
created; needed whenever you add new timesteps to a simulation that already
has trackers.

.. seealso::

   :doc:`parallel` for parallelising a script built from these tools, and
   :doc:`testing` for a database to build against without downloading
   simulation data.
