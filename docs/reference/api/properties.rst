.. Last checked by AP: 2026-08-29

.. _api_properties:

Writing your own properties
=============================

.. currentmodule:: tangos.properties

Every property that ``tangos write`` can compute is provided by a subclass of
:class:`PropertyCalculation`. For a walkthrough, see :doc:`/custom_properties`;
this page is the contract those subclasses implement.

.. autosummary::

   PropertyCalculation
   LivePropertyCalculation
   LivePropertyCalculationInheritingMetaProperties
   TimeChunkedProperty
   pynbody.PynbodyPropertyCalculation
   pynbody.spherical_region.SphericalRegionPropertyCalculation
   yt.YtPropertyCalculation
   providing_class
   all_properties
   all_property_classes
   all_providing_classes
   instantiate_class
   instantiate_classes

The base class
--------------

.. autoclass:: PropertyCalculation
   :members: names, requires_particle_data, works_with_handler,
             calculate, requires_property, preloop, region_specification,
             live_calculate, live_calculate_named, calculate_from_db,
             accept, no_proxies, index_of_name, get_simulation_property,
             mark_timer, all_classes,
             plot_x0, plot_xdelta, plot_x_values, plot_x_extent, plot_extent,
             plot_xlabel, plot_ylabel, plot_yrange, plot_xlog, plot_ylog,
             plot_clabel, get_interpolated_value
   :undoc-members:
   :member-order: bysource

.. note::
   ``HaloProperties`` is a pre-1.0 alias for :class:`PropertyCalculation` and
   is retained only for compatibility.

Backend-specific base classes
-----------------------------

.. autoclass:: tangos.properties.pynbody.PynbodyPropertyCalculation
   :show-inheritance:

.. autoclass:: tangos.properties.pynbody.spherical_region.SphericalRegionPropertyCalculation
   :members: region_specification
   :undoc-members:
   :show-inheritance:

.. autoclass:: tangos.properties.yt.YtPropertyCalculation
   :show-inheritance:

.. note::
   ``PynbodyHaloProperties`` and ``SphericalRegionHaloProperties`` are the
   pre-1.0 names for these two classes, kept only for compatibility.

Worked examples of these base classes, among tangos' built-in properties (see
``reference/builtin_properties``, forthcoming): ``StarFormHistogram`` in
``tangos.properties.pynbody.SF`` for :class:`TimeChunkedProperty` below, and
``CentreAndRadiusStars`` in ``tangos.properties.pynbody.centring`` for
:class:`~tangos.properties.pynbody.spherical_region.SphericalRegionPropertyCalculation`.

Live properties
---------------

.. autoclass:: LivePropertyCalculation
   :members: calculate
   :undoc-members:
   :show-inheritance:

.. note::
   ``LiveHaloProperties`` is a pre-1.0 alias for this class.

.. autoclass:: LivePropertyCalculationInheritingMetaProperties
   :members: plot_x0, plot_xdelta
   :undoc-members:
   :show-inheritance:

Histogram properties
--------------------

.. autoclass:: TimeChunkedProperty
   :members: pixel_delta_t_Gyr, minimum_store_Gyr, bin_index, store_slice,
             reassemble
   :undoc-members:
   :member-order: bysource
   :show-inheritance:

See :doc:`/histogram_properties` for the tutorial this class supports.

Finding and instantiating property classes
------------------------------------------

.. autofunction:: all_properties
.. autofunction:: all_property_classes
.. autofunction:: providing_class
.. autofunction:: all_providing_classes
.. autofunction:: instantiate_class
.. autofunction:: instantiate_classes

``providing_class`` (with ``explain=True``) and ``all_providing_classes`` are
the way to diagnose a property name being shadowed by another class;
``instantiate_class``/``instantiate_classes`` run a property class directly
against a simulation, without going through ``tangos write`` -- the standard
way to debug a new property interactively.

Returning a link to another object
----------------------------------

.. currentmodule:: tangos.util.proxy_object

A property (or an :doc:`input handler <input_handlers>`) that reports
subhalo or BH parentage often needs to link to an object that has not been
created in the database yet. These proxy classes express that; tangos resolves
them once the target object exists.

.. autoclass:: ProxyObjectBase
   :undoc-members:
.. autoclass:: IncompleteProxyObjectFromFinderId
   :members: relative_to_timestep_id, relative_to_timestep_cache, resolve
   :undoc-members:
.. autoclass:: ProxyObjectFromDatabaseId
   :members: resolve
   :undoc-members:
.. autoclass:: ProxyObjectFromFinderIdAndTimestep
   :undoc-members:
.. autoexception:: ProxyResolutionException

.. seealso::

   The built-in property names tangos ships with (``reference/builtin_properties``,
   forthcoming), and :ref:`api_input_handlers` for tying a property to a data
   format.
