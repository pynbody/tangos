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

What the branch leaves out
~~~~~~~~~~~~~~~~~~~~~~~~~~

Following the heaviest link is a choice, and it discards everything else the
tree knows. ``match_reduce(target, quantity, reduction)`` reaches every
progenitor at a given timestep at once and reduces over them, so you can ask
what the branch is a fraction *of*:

.. ipython::

 In [1]: halo.calculate(lambda: earlier(7).mass)

 In [2]: halo.calculate(lambda: match_reduce('tutorial_gadget/snapshot_013', mass, 'sum'))

 In [3]: halo.calculate(lambda: match_reduce('tutorial_gadget/snapshot_013', mass, 'max'))

Seven steps back -- ``snapshot_013``, inside the range of the plot above --
this halo has four progenitors. The one the branch follows holds under a third
of the mass already assembled, and, more surprisingly, it is not the most
massive of the four. That is worth pausing on, because it is the difference
between two things that sound alike: the branch follows the heaviest *link*,
the progenitor sharing the most particles, which is not necessarily the most
massive *halo*.

To see the whole tree rather than one timestep of it, ask for the progenitors
directly. :class:`~tangos.relation_finding.multi_hop.MultiHopAllProgenitorsStrategy`
returns every object that leads to this halo, at every timestep:

.. ipython::

 In [1]: import numpy as np

 In [2]: from tangos.relation_finding import MultiHopAllProgenitorsStrategy

 In [3]: progenitors = MultiHopAllProgenitorsStrategy(halo).all()

 In [4]: len(progenitors)

Forty-three objects, where the branch had nine. Their masses and times plot
straight over the curve you already have:

.. ipython::

 In [1]: t_all = np.array([q.timestep.time_gyr for q in progenitors])
    ...: M_all = np.array([q['mass'] for q in progenitors])

 In [2]: t_step = np.unique(t_all)
    ...: M_step = np.array([M_all[t_all == ti].sum() for ti in t_step])

 In [3]: p.figure()

 In [4]: p.plot(t_all, 1e10*M_all, 'o', color='0.7', label="all progenitors");
    ...: p.plot(t_step, 1e10*M_step, 'r^--', label="summed over the tree");
    ...: p.plot(time, 1e10*M, 'ko-', label="major progenitor branch");

 @savefig time_series_all_progenitors.png width=6in
 In [5]: p.xlabel("t/Gyr")
    ...: p.ylabel(r"$M/h^{-1} M_{\odot}$")
    ...: p.semilogy()
    ...: p.legend(loc="lower right")
    ...: p.tight_layout()

The black curve is the plot from the previous section. The grey points are the
objects it never visits, and the red curve totals the tree at each step. Early
on the two curves differ by a factor of about two: read as a history of how
much material had assembled, the major progenitor branch understates it, and
the gap closes only as the tree narrows towards the present.

Mergers themselves can be extracted as events.
:func:`~tangos.examples.mergers.get_mergers_of_major_progenitor` walks the
branch and returns the redshift and mass ratio of everything that joined it:

.. ipython::

 In [1]: from tangos.examples.mergers import get_mergers_of_major_progenitor

 In [2]: z, ratio, objects = get_mergers_of_major_progenitor(halo)

 In [3]: len(z)

 In [4]: ratio.min(), z[np.argmin(ratio)]

Fifteen mergers over nine snapshots, so this history is not smooth accretion.
The smallest ratio is below one, which is to say that at :math:`z \approx 2.2`
the object arriving was heavier than the branch it joined -- another way of
seeing that "major progenitor" is a statement about links, not about which
halo was biggest.

Many objects at one timestep
----------------------------

The other axis is a whole population at a fixed time.
:meth:`~tangos.core.timestep.TimeStep.calculate_all` takes the same
calculations as :meth:`~tangos.core.halo.SimulationObjectBase.calculate_for_progenitors`
and evaluates them for every
object in a timestep:

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

Two things about that row count are worth knowing. First, a row appears only
if *every* requested quantity could be evaluated for that object; anything
missing one of them is dropped silently rather than filled with ``nan``. The
rows above are therefore exactly this timestep's halos, even though it also
contains SubFind groups: the groups carry no ``VMax``, so they fell out of the
query on their own. Second, and better than relying on that, you can say which
kind of object you want with the ``object_type`` keyword, naming one of the
typetags from :ref:`concepts`.

Reaching across time from within a population
---------------------------------------------

The two axes are not really separate, and this is where live calculations
start to earn their keep. Suppose you want to ask, of every halo at one
timestep, how much of its mass it has gained in the last two snapshots. That
is a question about a population *and* about history:

.. ipython::

 In [1]: M, growth = timestep.calculate_all(
    ...:         lambda: mass,
    ...:         lambda: (mass - earlier(2).mass)/mass,
    ...:         object_type='halo')

 In [2]: len(M)

The second calculation is doing two distinct things at once. ``(a - b)/a`` is
plain arithmetic, evaluated by the database as the query runs. ``earlier(2)``
is something else: it is a *redirection*, and it works on links. It follows the
heaviest incoming link twice -- exactly the walk
``calculate_for_progenitors`` makes, stopped after two steps -- and everything
written after the dot is then evaluated on the object it lands on. So
``earlier(2).mass`` is the mass of the major progenitor two snapshots back.

Because a link is a link, this is the same machinery that reaches a black
hole from its host or a halo from its counterpart in another simulation; only
the name of the link differs. Halos with no progenitor two steps back have
nothing to redirect to, which is why fewer rows come back than there are
halos in the timestep.

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
line lost it, as satellites do when they are stripped. The axes are cropped:
a few heavily stripped objects lie well below the bottom of the plot.

Reaching between simulations
----------------------------

Related simulations can have links between them; in the tutorial database,
an example simulation is ``tutorial_changa_blackholes``, which has a counterpart
``tutorial_changa`` without black holes. 

Reaching across requires using the ``match`` redirection. For example

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
formation and a lower stellar mass.

.. seealso::

   :doc:`/tutorials/live_calculations` takes up the calculation language
   itself -- what can appear inside those lambdas, and what ``earlier``,
   ``at``, ``link`` and their relatives do.

   :ref:`concepts` for links, weights and typetags, and
   :func:`tangos.examples.mergers.get_mergers_of_major_progenitor` for
   pulling the individual merger events out of a branch like the one plotted
   above.
