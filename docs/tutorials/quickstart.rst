.. Last checked by AP 31/08/26

.. _quickstart:

Quick-start: exploring an existing database
===========================================

A tangos database is a summary of one or more simulations: a few numbers,
arrays and images per object per snapshot, in a single file (or database
server). In this tutorial, you will use a small sample database. You will list
the simulations it holds, pick out one halo, see which properties have been
calculated for it, and put a picture of a galaxy on the screen -- without
opening a single simulation file.

The vocabulary used here -- simulation, timestep, object, property, typetag --
and the halo paths that appear part-way through are introduced in
:ref:`concepts`. You do not need to have read that page first, but it is the
place to look if a term is puzzling.

.. note:: Before you start, make sure tangos is installed and
 can find the tutorial database;
 :ref:`installation` covers both. The examples here query an existing
 database, so you need no simulation files.

 Although not strictly necessary, your life will be easier 
 if you also install pynbody (just ``pip install pynbody`` should do it).


What is in the database?
------------------------

Start by opening the database and asking which simulations are present.
:func:`tangos.all_simulations` tells you:

.. ipython::

 In [1]: import tangos

 In [2]: tangos.all_simulations()

The tutorial database holds five simulations: the same ChaNGa volume with and 
without black hole physics, two Gadget runs processed by different halo finders, 
and a Ramses run. 

.. note::

   The above assumes you have set up the tutorial database and pointed tangos to its
   path. See :ref:`installation` if you see an empty set of simulatinos; more
   than likely you are looking at an empty database.

Any simulation can be fetched by name with
:func:`tangos.get_simulation`, and each knows the outputs it was built from:

.. ipython::

 In [1]: sim = tangos.get_simulation("tutorial_changa")

 In [2]: sim.timesteps

Each entry in :attr:`~tangos.core.simulation.Simulation.timesteps` is one
snapshot, and reports its redshift and age of the universe. Only five timesteps
show because this is a cut-down tutorial database; a production database commonly
has hundreds of timesteps, all of which will be recalled in this way.

Getting to an object
--------------------

A timestep offers its objects as
:attr:`~tangos.core.timestep.TimeStep.halos`, which you index in the ordinary
python way, from zero:

.. ipython::

 In [1]: step = sim.timesteps[3]

 In [2]: step.halos[3]

Note the square brackets index the halos sequentially from zero, as per Python 
convention. But the halos have a number independent of their index in this list. 
Here they started at 1, which is why ``step.halos[3]`` returned something calling 
itself halo 4. Tangos is ambivalent about how the halo numbering works, provided 
the halo numbers are unique per timestep. 


Going straight to an object
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Stepping down through simulation, timestep and halo is rarely what you want in
practice. :func:`tangos.get_object` takes the whole route as a single path and
returns the same halo:

.. ipython::

 In [1]: tangos.get_object("tutorial_changa/%832/halo_4")

The three parts are the simulation, the timestep and the object. The ``%`` is a
wildcard saving you from typing the full output name; :ref:`concepts` explains
the path and the wildcard in full. The final part of the path, ``halo_4``, 
names the object by its type (halo) and unique number (4) -- not its zero-based
index that we used above. 

Paths are how objects are named throughout
this documentation, so it is worth getting comfortable with them now.

.. note::

   Simulations and timesteps can contain a ``/`` in their literal name. In this
   case, :func:`tangos.get_object` will not know how to split your path. The 
   simplest fix is to use a single-character wildcard (``_``) to match 
   literal slashes, and keep ``/`` always for separating, 
   ``<simulation>/<timestep>/<object_id>``.

Reading properties
------------------

The ChaNGa simulation is a zoom simulation, in which halos are numbered in
decreasing order of particle count. 

Let's take the most massive halo, ``halo_1``,  in the final ChaNGa snapshot, 
and ask what has been calculated for it:

.. ipython::

 In [1]: halo = tangos.get_object("tutorial_changa/%960/halo_1")

 In [2]: halo.keys()

:meth:`~tangos.core.halo.SimulationObjectBase.keys` lists what this particular
halo carries: masses and radii, profiles, images, a star formation history,
and -- at the end -- the repeated ``ptcls_in_common`` entries, which are links
to other objects rather than properties. Which names appear depends on what
was calculated when the database was built, and can differ from one object to
the next. (This may be unfamiliar behaviour if you are used to using SQL databases; 
in tangos, properties and links are stored as key-value pairs rather than in 
literal SQL columns.)

Read a property by name, as though the halo were a dictionary:

.. ipython::

 In [1]: halo['Mvir']

That is a single database row: no simulation files are opened, and the query
takes milliseconds. The same value can be
requested as a *calculation*, written as a lambda:

.. ipython::

 In [1]: halo.calculate(lambda: Mvir)

For one stored property that is a long-winded way of writing the square
brackets, but :meth:`~tangos.core.halo.SimulationObjectBase.calculate` is the
door to the live calculation language, in which names combine into
expressions, span timesteps, and follow links to other objects. Names inside
such a lambda are database property names rather than python variables, which
is why ``Mvir`` does not have to be defined anywhere.

Looking at an image
-------------------

Properties are not all single numbers. ``uvi_image`` is a rendered
false-colour image of the halo's stars, stored as an array, and it plots like
any other array:

.. ipython::

 In [1]: import pylab as p

 @savefig quickstart_uvi_image.png width=6in
 In [2]: p.imshow(halo['uvi_image']);

That is a galaxy at redshift 2, drawn straight from the database.

How big is that image?
~~~~~~~~~~~~~~~~~~~~~~

The axes above are pixel numbers, which say nothing about how much of the
universe you are looking at. Properties carry metadata as well as values, so
you can ask.

Unlike everything else on this page, this one call needs pynbody installed:
:meth:`~tangos.core.halo.SimulationObjectBase.get_description` asks the code
that *would* calculate the property to describe itself, which builds the
simulation's input handler, whereas reading a stored value never does. Without
pynbody it raises ``ModuleNotFoundError``, while ``halo['uvi_image']`` above
keeps working.

.. ipython::

 In [1]: side = halo.get_description("uvi_image").plot_extent()

 In [2]: side

The image is 15 kpc across.

With the extent in hand the picture can carry physical axes:

.. ipython::

 @suppress
 In [1]: p.clf()

 @savefig quickstart_uvi_image_kpc.png width=6in
 In [2]: p.imshow(halo['uvi_image'], extent=[-side/2, side/2, -side/2, side/2]);
    ...: p.xlabel("x/kpc");
    ...: p.ylabel("y/kpc");

Those are physical kiloparsecs. The example properties shipped with tangos use
pynbody's unit system and convert everything to physical kpc, solar masses and
km/s, so ``Mvir`` above is in solar masses. Properties you write yourself may
store whatever units you please -- the database records numbers, not units, and
:meth:`~tangos.core.halo.SimulationObjectBase.get_description` is how you ask
the calculating code what it meant by them.

.. seealso::

   You now have one halo at one time. :doc:`/tutorials/time_series` follows a
   halo back through its progenitors and plots how it grew, and asks the same
   question of every halo in a timestep at once.
