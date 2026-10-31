.. Last checked by AP: 2026-08-29

.. _api_relation_finding:

Following links between objects
=================================

.. currentmodule:: tangos.relation_finding

``tangos.relation_finding`` is the machinery behind ``halo.previous``,
``halo.earliest``, :meth:`~tangos.core.halo.SimulationObjectBase.calculate_for_progenitors`,
the merger tree, and histogram reassembly. Its keyword arguments are exactly
what a ``strategy_kwargs=`` dictionary accepts wherever tangos lets you pass
one.

.. autosummary::

   HopStrategy
   HopMajorProgenitorStrategy
   HopMajorDescendantStrategy
   MultiHopStrategy
   MultiHopAllProgenitorsStrategy
   MultiHopMajorProgenitorsStrategy
   MultiHopMajorDescendantsStrategy
   MultiHopMostRecentMergerStrategy
   MultiSourceMultiHopStrategy
   multi_source.MultiSourceAllMajorProgenitorsStrategy
   multi_source.MultiSourceAllMajorDescendantsStrategy
   tree.MergerTree

One hop
--------

.. autoclass:: HopStrategy
   :members: all, first, count, weights, all_and_weights, temp_table
   :undoc-members:
   :member-order: bysource

Every strategy on this page accepts an ``order_by`` argument: a name, or list
of names, from ``'weight'`` (the default), ``'time_asc'``, ``'time_desc'``,
``'halo_number_asc'``, ``'halo_number_desc'`` and (multi-hop strategies only)
``'nhops'``.

.. autoclass:: HopMajorProgenitorStrategy
   :show-inheritance:

.. autoclass:: HopMajorDescendantStrategy
   :show-inheritance:

These two are what ``halo.previous`` and ``halo.next`` use internally.

Multiple hops
--------------

.. autoclass:: MultiHopStrategy
   :members: __init__
   :undoc-members:
   :show-inheritance:

``nhops_max`` caps the path length; ``directed`` is one of ``'across'``
(follow both progenitor and descendant links), ``'forwards'`` or
``'backwards'``; ``target`` restricts the search to a single timestep or
simulation; ``include_startpoint`` optionally includes ``halo_from`` itself in
the results.

.. autoclass:: MultiHopAllProgenitorsStrategy
   :show-inheritance:

.. autoclass:: MultiHopMajorProgenitorsStrategy
   :show-inheritance:

.. autoclass:: MultiHopMajorDescendantsStrategy
   :show-inheritance:

``MultiHopMajorProgenitorsStrategy`` and ``MultiHopMajorDescendantsStrategy``
are the default strategies used by
:meth:`~tangos.core.halo.SimulationObjectBase.calculate_for_progenitors` and
:meth:`~tangos.core.halo.SimulationObjectBase.calculate_for_descendants`
respectively; ``MultiHopAllProgenitorsStrategy`` walks the whole tree rather
than following only the major branch, and is what the ``'sum'`` reassembly
option uses.

.. autoclass:: MultiHopMostRecentMergerStrategy
   :show-inheritance:

Used by :func:`tangos.examples.mergers.get_mergers_of_major_progenitor` (see
:doc:`examples`) to identify the most recent merger onto a halo's major
branch.

Matching many objects at once
-------------------------------

.. autoclass:: MultiSourceMultiHopStrategy
   :members: all, sources, temp_table
   :undoc-members:
   :show-inheritance:

This is the fast way to cross-match every object in one timestep against
another timestep or simulation in a single query, rather than looping over
objects and running a one-hop strategy on each.

.. autoclass:: tangos.relation_finding.multi_source.MultiSourceAllMajorProgenitorsStrategy
   :show-inheritance:

.. autoclass:: tangos.relation_finding.multi_source.MultiSourceAllMajorDescendantsStrategy
   :show-inheritance:

.. note::
   These two are defined in ``tangos/relation_finding/multi_source.py`` but,
   unlike every other strategy on this page, are **not** re-exported from
   ``tangos.relation_finding``'s top level -- import them from
   ``tangos.relation_finding.multi_source`` directly. This looks like an
   oversight rather than a deliberate choice; see ``KNOWN_ISSUES.md``.

The merger tree
-----------------

.. autoclass:: tangos.relation_finding.tree.MergerTree
   :members:
   :undoc-members:
   :member-order: bysource

A python-side alternative to the merger-tree viewer in the web interface:
build one with ``MergerTree(halo).construct()``, then call ``.summarise()``
for a text tree or ``.plot()`` for a matplotlib rendering.

Feeding a result into a further SQLAlchemy query
---------------------------------------------------

.. currentmodule:: tangos.temporary_halolist

.. autofunction:: halo_query

.. autofunction:: enumerated_halo_query

.. autofunction:: all_halos_with_duplicates

.. autofunction:: halolink_query

.. autofunction:: temporary_halolist_table

``temp_table()``, above, and these functions are two halves of the same idiom:
a strategy's ``temp_table()`` result is a SQL temporary table of matching
object ids, and the ``temporary_halolist`` functions are how you turn that
into a further filtered query. :meth:`SimulationObjectBase.calculate_for_descendants
<tangos.core.halo.SimulationObjectBase.calculate_for_descendants>` uses this
pattern internally.

.. seealso::

   :doc:`examples` for a complete worked use of
   ``MultiHopMostRecentMergerStrategy``.
