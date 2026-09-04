.. Reviewed by AP 4/09/2026

.. _concepts:

The tangos data model
=====================

A tangos database is not a copy of your simulation; it is a *summary* of one,
storing a handful of numbers, arrays and images per object per snapshot so that
you can ask questions spanning an entire run in milliseconds. It is built from
very few ideas: 

* simulations contain timesteps; 
* timesteps contain objects;
* objects carry properties (numbers, arrays, images);
* objects are joined to one another by links. 

Merger trees, black hole histories and comparisons between two runs of the same 
volume are all one of those four things under a different name.

Simulations, timesteps and objects
----------------------------------

A :class:`~tangos.core.simulation.Simulation` is one simulation run; a
:class:`~tangos.core.timestep.TimeStep` is one output of that run, knowing its
:attr:`~tangos.core.timestep.TimeStep.redshift` and :attr:`~tangos.core.timestep.TimeStep.time_gyr`. 
Each timestep contains *objects*: the halos a halo finder identified in that 
snapshot, plus the other kinds described below.

.. note:: Before you start, make sure tangos is installed and
 can find the tutorial database;
 :ref:`installation` covers both. The examples here query an existing
 database, so you need no simulation files.

 Code snippets can be copied from this page and pasted into python,
 ipython or jupyter. Hover over the code and click the button that
 appears.

.. ipython::

 In [1]: import tangos

 In [2]: sim = tangos.get_simulation("tutorial_changa")

 In [3]: sim.timesteps

 In [4]: step = sim.timesteps[-1]

 In [5]: halo = step["halo_1"]

 In [6]: halo

Objects are numbered within their timestep, conventionally in decreasing order
of particle count, so ``halo_1`` is usually the largest halo present. But
nothing connects ``halo_1`` at one timestep to ``halo_1`` at the next: an
object belongs to one timestep alone, and every relationship across time is
made explicitly, by a link.

Properties
----------

A **property** is a named value attached to a single object: a mass, a radius,
a density profile, a rendered image. Read one by name, as though the object
were a dictionary; :meth:`~tangos.core.halo.SimulationObjectBase.keys` says
which names this particular object has.

.. ipython::

 In [1]: halo['Mvir']

 In [2]: halo['uvi_image'].shape

 In [3]: halo.keys()

A name existing on one halo guarantees nothing about any other, and reading one
costs a single database row, opening no simulation files. Look at the tail of
those keys, though: the repeated ``ptcls_in_common`` entries are not properties
at all.

Links
-----

A **link** is a named, directed, weighted relationship between two objects: a
source, a target, a name and a number. That is the whole idea, and tangos uses 
this one mechanism for *every* relationship it knows -- across time, between objects 
of different types, or across different simulations.

.. ipython::

 In [1]: for link in halo.links:
    ...:     print(f"{link.relation.text:16s} {link.weight:8.4f}  {link.halo_to.path}")
    ...:

Two quite different things share that list. Four of these links point
*backwards in time*, to halos in the previous timestep of the same simulation:
that is the merger tree. The other three point *sideways*, to halos in
``tutorial_changa_blackholes``, a re-run of the same volume with black hole
physics: that is a cross-simulation match. Both were established by comparing
particle membership, so both are named ``ptcls_in_common``, weighted by the
fraction of particles shared. 

.. note::

   In practice, you nearly always make use of links 
   via a higher-level interface than the raw list, as described in 
   :ref:`time_series` and :ref:`live_calculations`.

Black hole association is the same mechanism again: a black hole links to its
halo as ``host_halo``, and the halo links back as ``BH`` and ``BH_central``, so
``bh['host_halo']`` reaches the host and ``halo['BH_central']`` the black
holes:

.. ipython::

 In [1]: for link in halo.links:
    ...:     print(f"{link.relation.text:16s} {link.weight:8.4f}  {link.halo_to.path}")
    ...:

Link names and property names share one namespace, which is why both turn up
in :meth:`~tangos.core.halo.SimulationObjectBase.keys`.

Major progenitors are not flagged
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

One consequence of the link concept is **there is no "main progenitor" flag anywhere in
tangos.** The main progenitor is simply the progenitor link of largest weight. You
use higher-level methods to extract that kind of information. At its simplest, 
objects have a ``previous`` and a ``next`` attribute, which follow the heaviest link in 
either time direction:

.. ipython::

 In [1]: halo.previous

:attr:`~tangos.core.halo.SimulationObjectBase.previous` returned the halo that
holds 93% of the particles, because that is the heaviest of the four
progenitor links above. So do :attr:`~tangos.core.halo.SimulationObjectBase.next`,
:attr:`~tangos.core.halo.SimulationObjectBase.earliest`,
:attr:`~tangos.core.halo.SimulationObjectBase.latest` and
:meth:`~tangos.core.halo.SimulationObjectBase.calculate_for_progenitors`, which
follow the heaviest link repeatedly. Nor is a merger a distinct kind of record:
it is a timestep at which a halo has more than one incoming link of appreciable
weight.

Object types
------------

Not every object is a halo. Each carries a **typetag** saying what kind of
thing it is, and that tag is part of how you name it:

``halo``
   Something the halo finder found: the default, and often the only kind
   present.

``group``
   A parent structure containing halos, where the finder distinguishes the two
   -- SubFind's FoF groups as against its subhalos, for instance. Note that
   ``group`` is a finder-specific concept, and some finders simply find "sub-halos"
   of arbitrary nested depth. In such cases,
   these are represented by tangos as ``halo`` objects with 
   ``parent`` and ``child`` links.

``BH``
   A black hole, treated as an object in its own right so that it can carry
   properties (``BH_mass``, ``BH_mdot``) and its own history.

``tracker``
   Particles you chose and asked tangos to follow, rather than anything the
   finder produced; useful for Lagrangian analysis.

``phantom``
   A placeholder where a halo is missing from a catalogue but the merger tree
   needs it to stay connected. These are produced when importing a merger tree 
   from consistent-trees.

Which you meet depends on the codes that built the database. Among the tutorial
simulations, only ``tutorial_changa_blackholes`` has ``BH`` objects, only
``tutorial_gadget`` (SubFind) has ``group`` objects, and only
``tutorial_gadget_rockstar``, whose tree comes from consistent-trees, has
``phantom`` objects.

Naming an object: the halo path
-------------------------------

The step-by-step navigation above collapses into one string, and this is the
spelling used throughout the documentation:

.. ipython::

 In [1]: tangos.get_object("tutorial_changa/%960/halo_1")

The three slash-separated parts are the simulation, the timestep and the object
within it. :func:`tangos.get_object` takes all three,
:func:`tangos.get_timestep` the first two, :func:`tangos.get_simulation` the
first alone; :func:`tangos.get_item` accepts any of them. The last part is
``<typetag>_<number>`` -- ``halo_1``, ``BH_3``, ``group_2``, ``phantom_7`` -- and a
bare number means ``halo``, so ``.../1`` and ``.../halo_1`` are the same
object.

The ``%`` reflects wildcard matching: the timestep is
matched as a SQL ``LIKE`` pattern, in which ``%`` stands for any run of
characters. This timestep is really called
``pioneer50h128.1536gst1.bwK1.000960``, and ``%960`` saves you typing it. The
pattern must select exactly one timestep -- match none and you get an error,
match several and you get a different one. ``_`` is a ``LIKE`` wildcard too,
matching any single character, so patterns can match more loosely than they
appear to.

Creators
--------

Every row tangos writes records the run that wrote it: the command line, the
time, the machine and the user. That record is a
:class:`~tangos.core.creator.Creator`, and
:meth:`~tangos.core.creator.Creator.print_info` summarises what one run did.

.. ipython::

 In [1]: prop = halo.get_objects("Mvir")[0]

 In [2]: prop.creator.print_info()

This can help answer "where did this value come from?" and "what did last
night's job actually write?". :func:`tangos.all_creators` lists every run.

Under the hood: how this is stored
----------------------------------

Everything above is what you need in order to use tangos. This section is about
how the data physically sits in the database, and matters only if you are
writing SQL by hand, debugging, or extending tangos itself. **You can stop
reading here.**

*One table holds every object.* Halos, groups, BHs, trackers and phantoms all
live in the ``halos`` table, as single-table inheritance over
:class:`~tangos.core.halo.SimulationObjectBase` discriminated by an integer:
``halo`` is 0, ``BH`` 1, ``group`` 2, ``tracker`` 3, ``phantom`` 4. The
attribute is ``object_typecode``, though the column behind it is named
``halo_type`` for backwards compatibility. Tags, classes and codes are mapped
onto one another at runtime from the class hierarchy by
:meth:`~tangos.core.halo.SimulationObjectBase.class_from_tag` and its two
companions, so you should never need to write a typecode down. Each row also
carries ``halo_number`` (the rank used in paths), ``finder_id`` (the
catalogue's own identifier) and ``finder_offset`` (the index into the
catalogue, used to go back and read particle data).

*Names are interned.* Each distinct name is one
:class:`~tangos.core.dictionary.DictionaryItem` row in the ``dictionary``
table, referred to by id rather than repeated as a string on every property and
link row. Hence a link's name is reached as ``link.relation.text``, and
:func:`~tangos.core.dictionary.get_lexicon` lists every name in a database
without scanning the data.

*Values are stored by type.* Each :class:`~tangos.core.halo_data.HaloProperty`
row, in the ``haloproperties`` table, has separate float, integer and array
columns and fills exactly one of them; writing to one clears the others. Arrays
are pickled, zlib-compressed above about a kilobyte, and prefixed with a
two-byte marker recording the encoding -- so array properties are opaque to
plain SQL, and :attr:`~tangos.core.halo_data.HaloProperty.data` decodes them.

*Links are one table too.* Every relationship on this page -- merger trees,
cross-simulation matches, black hole hosts, trees patched by hand -- is a row
in the ``halolink`` table with a source, a target, a dictionary reference and a
weight. There is no separate tree structure to keep in step, which is why
``halo["some_name"] = other_halo`` is a complete way of recording a new kind of
relationship.

.. note::
   :meth:`~tangos.core.halo.SimulationObjectBase.get_description` and
   :meth:`HaloProperty.x_values <tangos.core.halo_data.HaloProperty.x_values>`
   ask for a property's *metadata*, which means constructing the simulation's
   input handler, which imports pynbody; reading a value with ``halo['name']``
   does not, so a pynbody-free environment can read every number in the
   database but cannot ask what its units are.

.. seealso::

   :doc:`/tutorials/quickstart` puts all of this to work: it finds a halo,
   reads its properties and plots an image of a galaxy.

   :doc:`/reference/api/objects` for the classes named here, and
   :doc:`/reference/api/query` for the full halo-path syntax table.
