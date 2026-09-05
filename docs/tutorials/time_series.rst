.. Checked by AP 4/09/2026

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

The examples on this page use ``tutorial_gadget``, a SubFind catalogue with a
merger tree. Pick a halo in its final snapshot:

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

.. versionadded:: 1.12.0
   Calculations can be written as python lambdas, as they are throughout this
   page. Older code passes the equivalent strings --
   ``halo.calculate_for_progenitors("mass", "t()")`` -- and that still works.

Two of the names in those lambdas deserve comment. ``mass`` is an ordinary
stored property: SubFind computed it, and ``tangos write`` put it in the
database. ``t()`` is not stored anywhere -- look for it in ``halo.keys()`` and
you will not find it. It is a *live property*, computed as the query runs, in
this case by reading ``time_gyr`` from the
:class:`~tangos.core.timestep.TimeStep` that owns each halo. Live properties
are the subject of the next tutorial; for now, treat ``t()`` as the way to get
the time at which each entry in the array was measured.

That is enough for a merger history. ``mass`` is stored in SubFind's own units
of :math:`10^{10} h^{-1} M_\odot`, hence the factor below:

.. ipython::

 In [1]: p.figure()

 In [2]: p.plot(time, 1e10*M, 'o-');

 @savefig time_series_major_progenitor_mass.png width=6in
 In [3]: p.xlabel("t/Gyr")
    ...: p.ylabel(r"$M/h^{-1} M_{\odot}$")
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

As well as counting the number of steps, the major progenitor of a halo can be
found in a specified snapshot:

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

 In [5]: for halo, time, mass in tree.walk_branches(times, masses):
    ...:     p.plot(time, 1e10*mass, 'o-')

 @savefig time_series_all_progenitors_mass.png width=6in
 In [6]: p.xlabel("t/Gyr")
    ...: p.ylabel(r"$M/h^{-1} M_{\odot}$")
    ...: p.semilogy()
    ...: p.tight_layout()

.. note::

  By default, the merger tree is 'thinned' to exclude very minor progenitors.
  However, you can change this behaviour.
  See :class:`~tangos.relation_finding.tree.MergerTree` for details.



Many objects at one timestep
----------------------------

The other axis is a whole population at a fixed time. Timesteps, like trees,
also have a :meth:`~tangos.core.timestep.TimeStep.calculate_all` method,
but they now evaluate expressions for every object in a timestep:

.. ipython::

 In [1]: timestep = tangos.get_timestep("tutorial_gadget/snapshot_019")

 In [2]: timestep

 In [3]: M, vmax = timestep.calculate_all(lambda: mass, lambda: VMax)

 In [4]: len(M)

.. ipython::

 In [1]: p.figure()

 In [2]: p.plot(1e10*M, vmax, 'k.');

 @savefig time_series_mass_vmax.png width=6in
 In [3]: p.xlabel(r"$M/h^{-1} M_{\odot}$")
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
fewer rows come back than there are halos in the timestep.

.. ipython::

 In [1]: p.figure()

 In [2]: p.axhline(0.0, color="gray");

 In [3]: p.plot(1e10*M, growth, "r.", alpha=0.2);

 @savefig time_series_mass_growth.png width=6in
 In [4]: p.semilogx()
    ...: p.xlim(4e11, 3e14)
    ...: p.ylim(-0.5, 1.0)
    ...: p.xlabel(r"$M/h^{-1} M_{\odot}$")
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
a whole alternative simulation rather than a particular timestep. For example:

.. ipython::

 In [1]: p.figure()
   
 In [1]: timestep = tangos.get_timestep("tutorial_changa/%960")

 In [2]: stellar_mass_no_bh, stellar_mass_with_bh = timestep.calculate_all(
    ...:         lambda: star_mass_profile[-1],
    ...:         lambda: match('tutorial_changa_blackholes').star_mass_profile[-1],
    ...:         object_type='halo')

 In [3]: p.plot(stellar_mass_no_bh, stellar_mass_with_bh, 'k.')

 @savefig matched_stellar_masses.png width=6in
 In [4]: p.xlabel(r"stellar mass without BHs / $M_\odot$")
    ...: p.ylabel(r"stellar mass with BHs / $M_\odot$")
    ...: p.plot([1e6,1e12], [1e6,1e12], 'r:')
    ...: p.xlim(1e7,1e11); p.ylim(1e7,1e11)
    ...: p.loglog()
    ...: p.tight_layout()

This is a zoom simulation, so we see satellites with comparable stellar masses
in both runs, but the central galaxy with black holes has suppressed star
formation and a lower stellar mass. Note that we have also used the ``[-1]``
index following the numpy convention to pick out the last element of the
``star_mass_profile`` array, which stores mass enclosed as a function of radius.
The last element is the total stellar mass in the halo.

.. seealso::

   :doc:`/tutorials/live_calculations` takes up the calculation language
   itself -- what can appear inside those lambdas, and what ``earlier``,
   ``at``, ``link`` and their relatives do.

   :ref:`concepts` for links, weights and typetags, and
   :func:`tangos.examples.mergers.get_mergers_of_major_progenitor` for
   pulling the individual merger events out of a branch like the one plotted
   above.
