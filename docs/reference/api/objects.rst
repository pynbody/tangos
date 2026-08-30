.. Last checked by AP: 2026-08-29

.. _api_objects:

The object model
==================

Every tangos database holds the same chain of objects: a
:class:`~tangos.core.simulation.Simulation` contains
:class:`~tangos.core.timestep.TimeStep`\ s, each of which contains halos (and
BHs, groups, trackers, phantoms), each of which carries properties and links to
other objects. This page documents that chain. For the bigger picture, see
:ref:`concepts`.

.. note::
   Despite appearances, ``tangos.core.Halo`` does **not** exist.
   ``tangos/core/__init__.py`` exports only
   :class:`~tangos.core.halo.SimulationObjectBase`,
   :class:`~tangos.core.timestep.TimeStep`,
   :class:`~tangos.core.simulation.Simulation`,
   :class:`~tangos.core.halo_data.HaloProperty` and
   :class:`~tangos.core.halo_data.HaloLink` at the top level. The concrete
   typetag classes (``Halo``, ``BH``, ``Group``, ``Tracker``, ``PhantomHalo``)
   live at ``tangos.core.halo.Halo`` and so on -- use the full module path
   when importing them directly. (``tangos.core.__all__`` in fact omits
   ``TimeStep``, ``Simulation``, ``HaloProperty`` and ``HaloLink`` too, even
   though they are all importable; ``from tangos.core import *`` therefore
   does not deliver the object model, while naming any of them explicitly
   does.)

.. autosummary::

   tangos.core.simulation.Simulation
   tangos.core.timestep.TimeStep
   tangos.core.halo.SimulationObjectBase
   tangos.core.halo.Halo
   tangos.core.halo.BH
   tangos.core.halo.Group
   tangos.core.halo.Tracker
   tangos.core.halo.PhantomHalo
   tangos.core.halo_data.HaloProperty
   tangos.core.halo_data.HaloLink
   tangos.core.simulation.SimulationProperty
   tangos.core.creator.Creator
   tangos.core.dictionary.DictionaryItem
   tangos.core.dictionary.get_lexicon
   tangos.core.tracking.TrackData

Simulations and timesteps
--------------------------

.. autoclass:: tangos.core.simulation.Simulation
   :members: __getitem__, keys, get, timesteps, trackers, path,
             get_output_handler, cache_properties
   :undoc-members:
   :member-order: bysource

.. autoclass:: tangos.core.timestep.TimeStep
   :members: calculate_all, __getitem__, halos, bhs, groups, trackers, phantoms,
             next, previous, earliest, latest, get_next, load, load_region,
             redshift, time_gyr, path
   :undoc-members:
   :member-order: bysource

.. note::
   ``TimeStep.keys()`` is not implemented -- it raises
   ``RuntimeError("Not implemented")`` unconditionally, unlike
   :meth:`Simulation.keys <tangos.core.simulation.Simulation.keys>` and
   :meth:`SimulationObjectBase.keys
   <tangos.core.halo.SimulationObjectBase.keys>`, which both work. It is
   omitted from this page rather than documented as if it worked; see
   ``KNOWN_ISSUES.md``. ``TimeStep.gather_property`` is a pre-1.0 alias kept
   only for compatibility.

Halos, BHs, groups, trackers and phantoms
-------------------------------------------

Every object a timestep contains -- halo, black hole, group, tracker or
phantom -- is an instance of :class:`SimulationObjectBase
<tangos.core.halo.SimulationObjectBase>`; the five subclasses below only set a
``tag`` string and, for ``Tracker``, ``Group`` and ``PhantomHalo``, a
specialised constructor. This is the class whose methods you actually call.

.. autoclass:: tangos.core.halo.SimulationObjectBase
   :members: __getitem__, get, get_data, get_objects, get_description, keys,
             calculate, calculate_for_progenitors, calculate_for_descendants,
             load, next, previous, earliest, latest, path, basename,
             halo_number, finder_id, finder_offset, NDM, NStar, NGas,
             timestep, links, reverse_links, properties,
             class_from_tag, object_typecode_from_tag,
             object_typetag_from_code,
             typecode_and_number_from_human_identifier
   :undoc-members:
   :member-order: bysource

.. note::
   ``SimulationObjectBase.property_cascade`` / ``.reverse_property_cascade``
   are pre-1.0 aliases for :meth:`calculate_for_progenitors
   <tangos.core.halo.SimulationObjectBase.calculate_for_progenitors>` /
   :meth:`calculate_for_descendants
   <tangos.core.halo.SimulationObjectBase.calculate_for_descendants>`, kept
   only for compatibility. ``SimulationObjectBase.plot(name, ...)`` is **not**
   usable -- it calls ``.plot()`` on the fetched
   :class:`~tangos.core.halo_data.HaloProperty`, which has no such method, so
   it always raises ``AttributeError``; it is left off this page. See
   ``KNOWN_ISSUES.md``.

The typetags:

.. autoclass:: tangos.core.halo.Halo
   :members: tag
   :undoc-members:
   :show-inheritance:

.. autoclass:: tangos.core.halo.BH
   :members: tag
   :undoc-members:
   :show-inheritance:

.. autoclass:: tangos.core.halo.Group
   :members: tag
   :undoc-members:
   :show-inheritance:

.. autoclass:: tangos.core.halo.Tracker
   :members: tag
   :undoc-members:
   :show-inheritance:

.. autoclass:: tangos.core.halo.PhantomHalo
   :members: tag
   :undoc-members:
   :show-inheritance:

Properties and links
----------------------

.. autoclass:: tangos.core.halo_data.HaloProperty
   :members: data, data_raw, description, x_values,
             get_data_with_reassembly_options, data_is_array, deprecated
   :undoc-members:
   :member-order: bysource

.. autoclass:: tangos.core.halo_data.HaloLink
   :members: halo_from, halo_to, weight, relation
   :undoc-members:

.. autoclass:: tangos.core.simulation.SimulationProperty
   :undoc-members:

Provenance and property names
-------------------------------

.. autoclass:: tangos.core.creator.Creator
   :members: print_info
   :undoc-members:

.. autoclass:: tangos.core.dictionary.DictionaryItem
   :undoc-members:

.. autofunction:: tangos.core.dictionary.get_lexicon

Property names are interned into :class:`DictionaryItem
<tangos.core.dictionary.DictionaryItem>` rows rather than stored as raw
strings; :func:`get_lexicon` is the way to list every property name a
database actually has.

Trackers
---------

.. autoclass:: tangos.core.tracking.TrackData
   :members: particles, select, create_objects, create_links, halo_number
   :undoc-members:

.. seealso::

   :doc:`/tracking` for the workflow that produces a
   :class:`TrackData <tangos.core.tracking.TrackData>`, and :doc:`building`
   for :func:`tangos.tracking.new` and
   :func:`~tangos.core.tracking.update_tracker_halos`.

Getters for ``get_objects`` and ``get_description``
------------------------------------------------------

:meth:`SimulationObjectBase.get_objects
<tangos.core.halo.SimulationObjectBase.get_objects>` and
:meth:`~tangos.core.halo.SimulationObjectBase.get_description` accept a
``getters`` argument, documented in their own docstrings as one of these
classes:

.. currentmodule:: tangos.core.extraction_patterns

.. autoclass:: HaloPropertyGetter
.. autoclass:: HaloPropertyValueGetter
.. autoclass:: HaloPropertyRawValueGetter
.. autoclass:: HaloPropertyValueWithReassemblyOptionsGetter
.. autoclass:: HaloLinkGetter
.. autoclass:: HaloLinkTargetGetter
