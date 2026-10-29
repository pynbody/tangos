.. Checked by AP 26/10/26

.. _live_calculation_recipes:

Live calculation recipes
========================

The :ref:`quickstart` introduced calculations -- stored names, live properties
with brackets, and redirections with a dot -- and :ref:`time_series` used them
to follow halos through time and across populations. This page collects
recipes for the things people most often need beyond that. Each is
self-contained; dip into whichever you need. The complete syntax and the list
of built-in functions are in :doc:`/live_calculation`.

Every recipe works the same way in
:meth:`~tangos.core.halo.SimulationObjectBase.calculate`,
:meth:`~tangos.core.timestep.TimeStep.calculate_all`,
:meth:`~tangos.core.halo.SimulationObjectBase.calculate_for_progenitors` and
:meth:`~tangos.core.halo.SimulationObjectBase.calculate_for_descendants`, and
any of them can be typed into a column of the :doc:`web interface
</tutorials/webserver>` as a string.

The recipes run against the tutorial database (see :ref:`installation`), using
a halo and its timestep from the ChaNGa simulation:

.. ipython::

 In [1]: import tangos

 In [2]: ts = tangos.get_timestep("tutorial_changa/%960")

 In [3]: h = tangos.get_object("tutorial_changa/%960/halo_1")


Pick values out of a profile or array
-------------------------------------

Many stored properties are arrays -- density profiles, mass profiles, images,
histograms. Index one with an integer, negative to count from the end. A mass
profile is mass enclosed, so its last entry is the total:

.. ipython::

 In [1]: total_star_mass, = ts.calculate_all(lambda: star_mass_profile[-1])

 In [2]: total_star_mass[:5]

More often you want the value at a physical position rather than at an index.
``at(position, array)`` interpolates, with the position in whatever units the
property's author chose -- for the profiles shipped with tangos, physical kpc:

.. ipython::

 In [1]: h.calculate(lambda: at(5.0, dm_density_profile))

 In [2]: h.calculate(lambda: at(Rvir/4, dm_density_profile))

The first is the dark matter density at 5 kpc; the second, a quarter of the
way out to the virial radius. The position may be any expression, but the
array must be a property in its own right (possibly with arithmetic applied),
because ``at`` needs the property's own description of what its x-axis means.

``max``, ``min``, ``posmax`` and ``posmin`` give the largest and smallest values
of an array and where they occur, and ``array_smooth(array, npix)`` returns a
Gaussian-smoothed copy:

.. ipython::

 In [1]: h.calculate(lambda: posmax(dm_density_profile))


Look at the same object at another time
---------------------------------------

``earlier(n)`` and ``later(n)`` follow the major progenitor or descendant
branch ``n`` snapshots, and ``earliest()`` and ``latest()`` go to the ends of
the branch. Anything after the dot is evaluated where they land, including
live properties and further redirections:

.. ipython::

 In [1]: h.calculate(lambda: earlier(2).Vvir())

 In [2]: h.calculate(lambda: earlier(2).at(Rvir/4, gas_mass_profile))

 In [3]: h.calculate(lambda: earlier(1).match('tutorial_changa_blackholes').Mvir)

The last goes back one snapshot, then across to the same halo in the run with
black holes. Applied to a whole timestep, ``later(n)`` gives each halo's
descendant:

.. ipython::

 In [1]: early_ts = tangos.get_timestep("tutorial_changa/%640")

 In [2]: mass_later, mass = early_ts.calculate_all(lambda: later(2).Mvir, lambda: Mvir)

 In [3]: mass_later[:5]

 In [4]: mass[:5]

Entries 0 and 4 have the same mass two snapshots later because they have the
same descendant: those two halos merge in between.


Follow a named link
-------------------

Links that a property module has written into the database are followed by
naming them. In ``tutorial_changa_blackholes``, the black holes in each halo's
central galaxy are linked as ``BH_central``:

.. ipython::
 :okwarning:

 In [1]: bh_halo = tangos.get_object("tutorial_changa_blackholes/%960/halo_1")

 In [2]: bh_halo.calculate(lambda: BH_central.BH_mass)

That halo's central galaxy holds more than one black hole, so the answer is
ambiguous: tangos warns ``More than one relation for target 'BH_central' has
been found. Picking the first.`` and does just that. The next recipe is how to
say which one you mean.


Choose one of several linked objects
------------------------------------

``link(name, property, "max"|"min")`` picks out the linked object with the
largest or smallest value of a property:

.. ipython::

 In [1]: bh_halo.calculate(lambda: link(BH, BH_mass, "max"))

This is the most massive black hole anywhere in the halo, which need not be
one of the central ones; ``link(BH, BH_central_distance, "min")`` would pick
the one nearest the centre instead. Any number of constraints may follow, each
an expression that must be true of the candidates, and having picked an object
you can ask for any of its properties:

.. ipython::

 In [1]: bh_halo.calculate(lambda: link(BH, BH_mass, "max", BH_central_distance<10, BH_mass>1e6).BH_mdot)

That is the accretion rate of the most massive black hole within 10 kpc of
the centre and above :math:`10^6 M_\odot`.


Find when a property peaked
---------------------------

``find_progenitor(property, "max"|"min")`` searches the whole main progenitor
branch for the step at which a stored property is largest or smallest, and
returns the object at that step; ``find_descendant`` searches forwards. The
property must be stored, not calculated. So, for every halo, the largest
virial mass it has ever had -- and how many have since lost mass, typically
satellites being tidally stripped:

.. ipython::

 In [1]: peak_mass, mass = ts.calculate_all(lambda: find_progenitor(Mvir, "max").Mvir, lambda: Mvir)

 In [2]: (mass < peak_mass).sum(), len(mass)


Combine every linked object
---------------------------

Where ``match(target)`` follows one link, ``match_reduce(target, calculation,
reduction)`` evaluates a calculation on *every* object linked to in the
target timestep or simulation, and combines the results with ``'sum'``,
``'mean'``, ``'min'`` or ``'max'``. For example, the total mass of all of each
halo's progenitors in the previous snapshot, beside the mass of the major
progenitor alone:

.. ipython::

 In [1]: all_progenitors, major_progenitor = ts.calculate_all(lambda: match_reduce('tutorial_changa/%832', Mvir, 'sum'),
    ...:                                                       lambda: earlier(1).Mvir)

 In [2]: all_progenitors[:5]

 In [3]: major_progenitor[:5]


Filter a population
-------------------

Comparisons (``>``, ``<``, ``>=``, ``<=``, ``==``, ``!=``) and the logical
operators ``&`` (and), ``|`` (or) and ``~`` (not) return booleans, which
:meth:`~tangos.core.timestep.TimeStep.calculate_all` returns like any other
value. Use them as masks:

.. ipython::

 In [1]: mass, is_big = ts.calculate_all(lambda: Mvir, lambda: Mvir>1e12)

 In [2]: mass[is_big][:5]

``has_property(name)`` and ``has_link(name)`` test whether an object has a
property or link at all, and are most useful negated:

.. ipython::

 In [1]: no_mass, = ts.calculate_all(lambda: ~has_property(Mvir))

 In [2]: no_mass, = ts.calculate_all("!has_property(Mvir)")

 In [3]: no_mass.sum()

Note the spelling difference: the string form accepts either ``!`` or ``~`` for
logical not, whereas the lambda form must use ``~``, since python's ``not``
cannot be used (see :doc:`/live_calculation`).

.. versionadded:: 1.12.0
   The ``~`` spelling in the string form; older versions accept only ``!``.


Star formation and accretion histories
--------------------------------------

*Histogram* properties -- in the tutorial database, ``SFR_histogram`` and
``BH_mdot_histogram`` -- record a quantity in fine time bins, finer than the
snapshots. The database stores them in chunks, one per timestep, and reading
one reassembles the chunks along the major progenitor branch: ``h['SFR_histogram']``
is the star formation history of the main branch, one bin per 20 Myr by
default.

To sum over *all* progenitors instead, reassemble explicitly:

.. ipython::

 In [1]: sfr_all_branches = h.calculate(lambda: reassemble(SFR_histogram, 'sum'))

 In [2]: sfr_all_branches[-5:]

A black hole accretion history works the same way, after following the link to
the black hole:

.. ipython::
 :okwarning:

 In [1]: mdot = bh_halo.calculate(lambda: BH.reassemble(BH_mdot_histogram, 'sum'))

 In [2]: mdot[-5:]

``'place'`` puts only this timestep's chunk on the full time axis, zero
elsewhere, if you want to combine the chunks yourself; ``raw(SFR_histogram)``
returns the chunk exactly as stored. Further reassembly methods can be added
by implementing them in
:meth:`~tangos.properties.TimeChunkedProperty.reassemble`; see
:doc:`/tutorials/histograms`.


.. seealso::

   :doc:`/live_calculation` for the full syntax and every built-in function,
   and :doc:`/tutorials/custom_properties` for writing live properties of your own.
