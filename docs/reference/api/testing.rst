.. Last checked by AP: 2026-08-29

.. _api_testing:

Generating a database without simulation data
================================================

``tangos.testing`` is a test-support package, but three of its objects are
useful well beyond the test suite: they are the only way to get a working
tangos database -- with timesteps, halos, properties and merger-tree links --
without pynbody, yt, or any real particle data. If you want to try the
live-calculation language or ``calculate_all`` without a multi-gigabyte
download, start here.

.. currentmodule:: tangos.testing

.. autosummary::

   simulation_generator.SimulationGeneratorForTests
   init_blank_db_for_testing
   blank_db_for_testing
   db_diff.TangosDbDiff

Building a synthetic simulation
----------------------------------

.. autoclass:: tangos.testing.simulation_generator.SimulationGeneratorForTests
   :members: __init__, add_timestep, add_objects_to_timestep,
             add_properties_to_halos, add_bhs_to_timestep,
             add_properties_to_bhs, link_last_halos, link_last_bhs,
             assign_bhs_to_halos
   :undoc-members:
   :member-order: bysource

A minimal example -- two timesteps, five halos each, linked into a merger
tree, with no pynbody and no particle data::

   from tangos.testing import init_blank_db_for_testing
   from tangos.testing.simulation_generator import SimulationGeneratorForTests

   session = init_blank_db_for_testing()
   generator = SimulationGeneratorForTests("my_test_sim", session=session)
   generator.add_timestep()
   generator.add_objects_to_timestep(5)
   generator.add_timestep()
   generator.add_objects_to_timestep(5)
   generator.link_last_halos()

.. warning::
   ``add_objects_to_timestep(n)`` assigns particle counts descending from
   ``1000`` in steps of ``100``; with ``n=10`` the tenth halo gets ``NDM=0``,
   and ``link_last_halos()`` on a timestep containing that halo raises
   ``ZeroDivisionError`` (confirmed). Keep ``n`` to 9 or fewer, or pass an
   explicit ``NDM=`` list, until this is fixed -- see ``KNOWN_ISSUES.md``.

.. autofunction:: init_blank_db_for_testing

.. autofunction:: blank_db_for_testing

Comparing two databases
-------------------------

.. autoclass:: tangos.testing.db_diff.TangosDbDiff
   :members: set_tolerance, compare
   :undoc-members:

The engine behind ``tangos diff``; useful directly when comparing a re-run
against a reference database, for example to confirm a refactor of a property
class has not changed its output.

.. note::
   Everything else in ``tangos.testing`` -- ``assert_halolists_equal``,
   ``halolists_equal``, ``autorevert``, ``assert_connections_all_closed``,
   ``SqlExecutionTracker``, ``using_parallel_tasks`` -- is pytest support for
   tangos' own test suite (SQL-statement counting, connection-leak
   assertions, decorators for the test suite) and is not documented here.
