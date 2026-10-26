.. Checked by AP 10/09/2026

.. _time_series:

Time series and populations
===========================

The :ref:`quickstart` reached a single object at a single timestep and read a
few of its properties. Almost every real question moves along one of two axes
from there: *one object across many timesteps* -- a merger history -- or *many
objects at one timestep* -- a population. tangos has a method for each, and
they take exactly the same kind of calculation, so anything you learn about one
transfers to the other.

Both of them are still single database queries against an existing tangos
database. Nothing here opens a simulation file.

.. note:: Before you start, make sure tangos is installed and
 can find the tutorial database;
 :ref:`installation` covers both. The examples here query an existing
 database, so you need no simulation files.

 Code snippets can be copied from this page and pasted into python,
 ipython or jupyter. Hover over the code and click the button that
 appears.

.. ipython::

 In [1]: import tangos

 In [2]: import pylab as p

Following the major progenitor branch
-------------------------------------

Most examples on this page use ``tutorial_gadget``, a SubFind catalogue with a
merger tree. It has eleven snapshots to the ChaNGa run's five, so its histories
are longer. Two things differ from the quick-start: SubFind's mass is the
stored property ``mass`` rather than ``Mvir``, and each timestep holds SubFind's
groups as well as its halos. Pick a halo in the final snapshot:

.. ipython::

 In [1]: halo = tangos.get_object("tutorial_gadget/snapshot_020/halo_10")

 In [2]: halo

:meth:`~tangos.core.halo.SimulationObjectBase.calculate_for_progenitors` walks
backwards from that halo and evaluates whatever you ask for at every step
along the way, returning one numpy array per requested quantity:

.. ipython::

 In [1]: M, time = halo.calculate_for_progenitors(lambda: mass, lambda: t())

 In [2]: time

The arrays run backwards in time, starting with the halo you asked about.
Here they are nine entries long although the simulation has eleven snapshots,
because the branch simply runs out: before ``snapshot_012`` this halo has no
progenitor in the tree at all.

.. ipython::

 In [1]: halo.earliest

The two names in those lambdas are the two kinds the :ref:`quickstart`
introduced. ``mass`` is a stored property: SubFind computed it, and it was
imported into the database along with the catalogue. ``t()``, with brackets, is
a live property, computed as the query runs -- here by reading the time of the
:class:`~tangos.core.timestep.TimeStep` each halo belongs to, so it gives the
time at which each entry in the array was measured. ``z()`` and ``a()`` give
the redshift and scalefactor in the same way.

That is enough for a merger history:

.. ipython::

 In [1]: p.figure()

 In [2]: p.plot(time, M, 'o-');

 @savefig time_series_major_progenitor_mass.png width=6in
 In [3]: p.xlabel("t/Gyr")
    ...: p.ylabel(r"$M/M_{\odot}$")
    ...: p.semilogy()
    ...: p.tight_layout()

Each marker is one snapshot; a sharp rise between two of them is where the
halo absorbed something substantial.

"Major progenitor" here means exactly what :ref:`concepts` says it means:
there is no flag in the database marking one progenitor as the important one.
At each step :meth:`~tangos.core.halo.SimulationObjectBase.calculate_for_progenitors`
takes the incoming link of largest
weight -- the progenitor that contributed the most particles -- and continues
from there. The curve above is the record of following the heaviest link nine
times.

:meth:`~tangos.core.halo.SimulationObjectBase.calculate_for_descendants` is the
mirror image, following links forwards in time instead.

Following other branches
------------------------

``earlier(n)`` reaches the major progenitor ``n`` snapshots back; ``match``
reaches it in a snapshot you name instead:

.. ipython::

 In [1]: halo.calculate(lambda: earlier(7).mass)

 In [2]: halo.calculate(lambda: match('tutorial_gadget/snapshot_013').mass) # <- the same

Both these approaches follow the heaviest link at each step. That is not always the
right choice; ``match_reduce(target, quantity, reduction)`` reaches every
progenitor at a given timestep at once and reduces over them, so you can ask
for the total mass of all progenitors, or the mass of the most massive:

.. ipython::

 In [2]: halo.calculate(lambda: match_reduce('tutorial_gadget/snapshot_013', mass, 'sum'))

 In [3]: halo.calculate(lambda: match_reduce('tutorial_gadget/snapshot_013', mass, 'max'))

Seven steps back -- ``snapshot_013``, inside the range of the plot above --
this halo has four progenitors. The one the branch follows holds under a third
of the mass already assembled, and, more surprisingly, it is not the most
massive of the four. This emphasises that the major branch follows the heaviest *link*
progressively through each timestep, meaning at each step it finds the progenitor sharing
the most particles. That is not necessarily the most massive *halo*, especially when you
go far back in the history.

More advanced historical information can be obtained using the
:class:`tangos.relation_finding.tree.MergerTree` class.

.. versionadded:: 1.13.0

  The public API for merger trees is new in version 1.13.0. Previously these
  trees were available in the web interface but not for python querying.


You can construct and inspect the merger tree as follows:

.. ipython::

 In [1]: from tangos.relation_finding import MergerTree

 In [2]: tree = MergerTree(halo)

 @savefig tree_abstract.png width=6in
 In [3]: p.figure()
    ...: p.axis("off")
    ...: tree.plot()

The :meth:`~tangos.relation_finding.tree.MergerTree.plot` method shows us an
abstract representation of the tree, with the halo number at
each timestep labelled.

To calculate an expression such as mass at each timestep, we can use
the :meth:`tangos.relation_finding.tree.MergerTree.calculate_all` method.
This takes the same kinds of calculation arguments as
:meth:`~tangos.core.halo.SimulationObjectBase.calculate_for_progenitors`
and evaluates them for every halo in the tree. To assign those results to
particular timesteps use the :meth:`~tangos.relation_finding.tree.MergerTree.walk_depth`
method; or, to assign them to particular branches, use
:meth:`~tangos.relation_finding.tree.MergerTree.walk_branches` as follows:

.. ipython::

 In [3]: times, masses = tree.calculate_all(lambda: t(), lambda: mass)

 In [4]: p.figure()

 In [5]: for _, branch_times, branch_masses in tree.walk_branches(times, masses):
    ...:     p.plot(branch_times, branch_masses, 'o-')

 @savefig time_series_all_progenitors_mass.png width=6in
 In [6]: p.xlabel("t/Gyr")
    ...: p.ylabel(r"$M/M_{\odot}$")
    ...: p.semilogy()
    ...: p.tight_layout()

.. note::

  By default, the merger tree is 'thinned' to exclude very minor progenitors.
  However, you can change this behaviour.
  See :class:`~tangos.relation_finding.tree.MergerTree` for details.


Star formation histories
------------------------

Some properties record a history at finer resolution than the snapshots.
``SFR_histogram`` is one: a star formation rate in 20 Myr bins. Only the ChaNGa
runs in the tutorial database have stars, so this section uses ChaNGa's
``halo_1``, the galaxy imaged in the :ref:`quickstart`. Like the Gadget halo
above, its major branch holds only part of the mass early on -- about half at
the first snapshot:

.. ipython::

 In [1]: galaxy = tangos.get_object("tutorial_changa/%960/halo_1")

 In [2]: galaxy.calculate(lambda: match('tutorial_changa/%384').Mvir)

 In [3]: galaxy.calculate(lambda: match_reduce('tutorial_changa/%384', Mvir, 'sum'))

Reading ``SFR_histogram`` gives the star formation history of the major
progenitor branch alone. ``reassemble(SFR_histogram, 'sum')`` instead adds up
the histories of *all* the progenitors, so it counts the stars that formed in
the other branches and arrived later by merging:

.. ipython::

 In [1]: sfr_major = galaxy['SFR_histogram']

 In [2]: sfr_all = galaxy.calculate(lambda: reassemble(SFR_histogram, 'sum'))

 In [3]: t_bins = galaxy.get_description("SFR_histogram").plot_x_values(sfr_major)

 In [4]: fig, (ax_sfr, ax_ratio) = p.subplots(2, 1, sharex=True, height_ratios=[2, 1])

 In [5]: ax_sfr.plot(t_bins, sfr_all, label="all progenitors");

 In [6]: ax_sfr.plot(t_bins, sfr_major, label="major progenitor branch");

 In [7]: ax_sfr.set_yscale('log')
    ...: ax_sfr.set_ylabel(r"SFR/$M_{\odot}\,{\rm yr}^{-1}$")
    ...: ax_sfr.set_ylim(1e-1,1e2)
    ...: ax_sfr.legend();

 In [8]: has_sf = sfr_major > 0

 In [9]: ax_ratio.plot(t_bins[has_sf], sfr_all[has_sf]/sfr_major[has_sf], 'k');

 @savefig time_series_sfr_reassembly.png width=6in
 In [10]: ax_ratio.axhline(1.0, color='gray', ls=':')
    ....: ax_ratio.set_xlabel("t/Gyr")
    ....: ax_ratio.set_ylabel("all / major")
    ....: fig.tight_layout()

The lower panel is the ratio of the two. At early times a good deal of star
formation happens away from the major branch: before 1 Gyr the progenitors
together typically form about one and a half times as many stars as the major
branch alone, and at first up to three times as many, much as you would expect
of a galaxy assembled from smaller pieces. Later the major branch dominates, and
overall 97% of this galaxy's stars formed on it. 

The SFR is not the only history stored this way. Black hole accretion rates,
``BH_mdot_histogram``, are too, and reassemble in the same way once you follow
the link to the black hole: ``BH.reassemble(BH_mdot_histogram, 'sum')``. See
:doc:`/how-to/live_calculation_recipes` for that and the other reassembly
options.


Many objects at one timestep
----------------------------

The other axis is a whole population at a fixed time. Timesteps, like trees,
also have a :meth:`~tangos.core.timestep.TimeStep.calculate_all` method,
but they now evaluate expressions for every object in a timestep:

.. ipython::

 In [1]: timestep = tangos.get_timestep("tutorial_gadget/snapshot_019")

 In [2]: timestep.halos.count(), timestep.groups.count()

 In [3]: M, vmax = timestep.calculate_all(lambda: mass, lambda: VMax)

 In [4]: len(M)

.. ipython::

 In [1]: p.figure()

 In [2]: p.plot(M, vmax, 'k.');

 @savefig time_series_mass_vmax.png width=6in
 In [3]: p.xlabel(r"$M/M_{\odot}$")
    ...: p.ylabel(r"$v_{max}/{\rm km\,s^{-1}}$")
    ...: p.loglog()
    ...: p.tight_layout()


You can give any number of calculations to :meth:`~tangos.core.timestep.TimeStep.calculate_all`,
and it will return one array per calculation, each with the same length. Any objects
for which one of the calculations fails to return a value are dropped from all arrays
(unless you pass ``sanitize=False``, in which case the arrays have dtype of ``object``
and fill missing values with ``None``).

This has the side-effect in this case that the groups are ignored in the example above,
since they carry no ``VMax``. But, more generally, there is also an ``object_type``
keyword that can restrict the query to a particular kind of object. See
:meth:`~tangos.core.timestep.TimeStep.calculate_all` for more information.

A calculation that *fails* is different from one that is merely ``False``. A
comparison such as ``mass > 1e12`` (above :math:`10^{12} M_\odot`) is
``False`` for the smaller halos, which is
a value, so those halos are kept -- which makes a comparison useful as a mask.
Here ``object_type='halo'`` leaves out the groups, which also carry a ``mass``:

.. ipython::

 In [1]: M, is_big = timestep.calculate_all(lambda: mass, lambda: mass > 1e12, object_type='halo')

 In [2]: len(M), is_big.sum()

 In [3]: M_big = M[is_big]

Reaching across time from within a population
---------------------------------------------

Suppose you want to ask, of every halo at one
timestep, how much its mass has changed in the last two snapshots. That
is a question about a population *and* about history, which can be asked as follows:

.. ipython::

 In [1]: M, growth = timestep.calculate_all(
    ...:         lambda: mass,
    ...:         lambda: (mass - earlier(2).mass)/mass,
    ...:         object_type='halo')

 In [2]: len(M)

Here, ``earlier(2)`` follows the heaviest incoming link twice -- exactly the walk
``calculate_for_progenitors`` makes, stopped after two steps -- and everything
written after the dot is then evaluated on the object it lands on. So
``earlier(2).mass`` is the mass of the major progenitor two snapshots back.

Halos with no progenitor two steps back have nothing to redirect to, which is why
fewer rows come back than the timestep's 1027 halos.

.. ipython::

 In [1]: p.figure()

 In [2]: p.axhline(0.0, color="gray");

 In [3]: p.plot(M, growth, "r.", alpha=0.2);

 @savefig time_series_mass_growth.png width=6in
 In [4]: p.semilogx()
    ...: p.xlim(4e11, 3e14)
    ...: p.ylim(-0.5, 1.0)
    ...: p.xlabel(r"$M/M_{\odot}$")
    ...: p.ylabel("fractional growth in mass")
    ...: p.tight_layout()

Most halos gained mass over those two snapshots; the points below the grey
line lost it, as satellites do when they are stripped. (The axes are cropped:
a few objects lose mass heavily and lie well below the bottom of the plot.)

Reaching between simulations
----------------------------

Related simulations can have links between them; in the tutorial database,
an example simulation is ``tutorial_changa_blackholes``, which has a counterpart
``tutorial_changa`` without black holes. 

Reaching across requires using the ``match`` redirection again, but now pointing at
a whole alternative simulation rather than a particular timestep. The example
compares stellar masses, using ``star_mass_profile[-1]``: the profile stores
mass enclosed as a function of radius, so its last element, picked out with
numpy's ``[-1]``, is the total stellar mass in the halo.

.. ipython::

 In [1]: timestep = tangos.get_timestep("tutorial_changa/%960")

 In [2]: stellar_mass_no_bh, stellar_mass_with_bh = timestep.calculate_all(
    ...:         lambda: star_mass_profile[-1],
    ...:         lambda: match('tutorial_changa_blackholes').star_mass_profile[-1],
    ...:         object_type='halo')

 In [3]: p.figure()

 In [4]: p.plot(stellar_mass_no_bh, stellar_mass_with_bh, 'k.');

 @savefig matched_stellar_masses.png width=6in
 In [5]: p.xlabel(r"stellar mass without BHs / $M_\odot$")
    ...: p.ylabel(r"stellar mass with BHs / $M_\odot$")
    ...: p.plot([1e6,1e12], [1e6,1e12], 'r:')
    ...: p.xlim(1e7,1e11); p.ylim(1e7,1e11)
    ...: p.loglog()
    ...: p.tight_layout()

Most galaxies have similar stellar masses in both runs. The exceptions are the
two most massive: with black holes, their star formation is suppressed, leaving
them with two to three times less stellar mass.

.. seealso::

   :doc:`/how-to/live_calculation_recipes` for more that a calculation can
   do -- interpolating profiles with ``at``, choosing between linked objects
   with ``link``, searching a branch with ``find_progenitor``, and star
   formation histories -- and :doc:`/live_calculation` for the full syntax.

   :ref:`concepts` for links, weights and typetags, and
   :func:`tangos.examples.mergers.get_mergers_of_major_progenitor` for
   pulling the individual merger events out of a branch like the one plotted
   above.
