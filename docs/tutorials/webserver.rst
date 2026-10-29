.. Checked by AP 5/09/26
.. Stage 3 (Claude, 2026-10-26): links repointed after the live calculations tutorial
   was dissolved; fixed "as soon if". Not yet checked by a human.
.. Stage 4.5 (Claude, 2026-10-29): grown from a stub into a tour of the interface.
   "Starting the server" is unchanged; everything after it is new. The screenshots
   are taken during the build by docs/web_screenshots.py, and the URL examples run
   against the server it starts. Not yet checked by a human.

.. _webserver:

Exploring a database in the browser
===================================

Everything in the preceding tutorials queries the database from python. tangos
also ships a web interface over the same database, which is often the faster
way to find your way around an unfamiliar set of simulations: it lists the simulations and
timesteps, plots any property against any other, draws merger trees, and turns
a live calculation into a column of a table without your writing any code.

.. note:: Before you start, make sure tangos is installed and
 can find the tutorial database;
 :ref:`installation` covers both. The examples here query an existing
 database, so you need no simulation files.

Starting the server
-------------------

At the command line:

.. code-block:: bash

  $ tangos serve

It prints the address it is listening on -- ``Serving on
http://127.0.0.1:6543`` -- and you open that in a browser. The server reads
the same ``TANGOS_DB_CONNECTION`` as everything else, so it shows the database
you have been querying from python.

You can choose the port yourself, and give the server a title, which is worth
doing as soon as you have more than one open at once:

.. code-block:: bash

  $ tangos serve --title "black holes run" production.ini 6544

Stop the server with :kbd:`Control-c`.

.. note:: ``tangos serve`` is a shortcut to Pyramid's ``pserve``. The optional
   first positional argument names the configuration file -- ``production.ini``
   (the default), ``development.ini``, or a path to one of your own -- and the
   port is the *second*. That ordering is why the port has to be given after a
   configuration file, as above, rather than on its own.

Finding your way around
-----------------------

The front page lists every simulation in the database, with the properties
stored for each -- for the ChaNGa runs, the parameters they were run with. Click
a name to open the simulation:

.. image:: /_web_screenshots/web_simulations.png
   :alt: The list of simulations, with each one's stored properties
   :width: 100%

A simulation's page lists its timesteps, with the redshift and time of each, and
the number of objects in it -- halos and groups together, which is why
``tutorial_gadget``'s last snapshot shows 1996 (1041 halos and 955 groups).

.. image:: /_web_screenshots/web_simulation.png
   :alt: The timesteps of tutorial_gadget
   :width: 70%

``Go →`` opens a timestep, which lists its objects in a table for each type,
numbered as in python -- ``halo_1`` is the first row of the halos table. The
tables are paged, 50 rows at a time by default:

.. image:: /_web_screenshots/web_timestep.png
   :alt: The timestep page, with a table of halos and one of groups
   :width: 80%

``Go →`` again opens a single object. Its page shows the object's path
(``tutorial_gadget/snapshot_020/halo_1``, exactly what
:func:`~tangos.get_object` takes), ways to move to related objects, and every
property stored for it, with arrays summarised:

.. image:: /_web_screenshots/web_halo.png
   :alt: The page for halo 1 of snapshot_020, with its navigation and properties
   :width: 80%

Moving through time
^^^^^^^^^^^^^^^^^^^

*Find in a different timestep* follows the object along its merger tree: ``-1``
steps to its major progenitor at the previous snapshot, ``+1`` to its descendant
at the next, ``-10`` and ``+10`` ten at a time, and ``earliest`` and ``latest``
to the ends of its history. The page changes in place, so you can watch a halo's
properties change as you step. Here are three steps back from the final
snapshot; the halo's number changes as it goes, from 1 to 2 and then 5, because
halos are numbered by size within each snapshot:

.. image:: /_web_screenshots/web_time_navigation.png
   :alt: Stepping back three snapshots from halo 1 of snapshot_020
   :width: 80%

*Look at another halo* moves to the neighbouring halo numbers, and *Find in
another simulation* jumps to the matching object in another simulation, if the
two have been linked (see :doc:`/tutorials/crossmatching`). *Raw links to other
objects* lists every link the object has, for following one by hand.

Plotting
--------

The plots on an object's page are built from the circles beside each property.
For a property with a single value, the left circle chooses it for the *x* axis
and the right one for the *y* axis; an array, such as a density profile, has one
circle, which plots the array itself. Then choose *This halo* or *This timestep*
above the plot.

*This timestep* plots one point for every object in the timestep -- here
``VMax`` against ``mass`` for the halos of ``tutorial_gadget``, with both
axes logarithmic:

.. image:: /_web_screenshots/web_population_plot.png
   :alt: VMax against mass for every halo in snapshot_020
   :width: 60%

*This halo* follows the object back through its major progenitors instead, as
:meth:`~tangos.core.halo.SimulationObjectBase.calculate_for_progenitors` does
in python. With ``t()`` on the *x* axis and ``mass`` on the *y* axis, that is
the halo's mass growth:

.. image:: /_web_screenshots/web_mass_history.png
   :alt: The mass of halo 1 along its major progenitor branch, against time
   :width: 60%

*Download CSV* gives the numbers behind any plot.

The merger tree
^^^^^^^^^^^^^^^

*Merger tree* draws the object's merger tree. The selected object, outlined, is
on the left, and its progenitors run back in time to the right, one column per
snapshot; each circle is labelled with the halo's number at that snapshot, and
grows with its number of particles. Hover over a halo for its details, and click
it to open its page. This is the tree of ``tutorial_gadget_rockstar``'s largest
halo:

.. image:: /_web_screenshots/web_merger_tree.png
   :alt: The merger tree of halo 1 of tutorial_gadget_rockstar
   :width: 100%

.. note::

   For a simulation in which some objects have no ``Mvir`` property, such as
   ``tutorial_gadget``, the tree currently fails to draw, with "Sorry, there was
   an error generating the tree".

.. The merger tree failure is a stage 6 item in KNOWN_ISSUES.md: once fixed, take
   this screenshot from tutorial_gadget, as the stage 4 plan asked, and remove the
   note above.

Calculations as columns
-----------------------

The heading ``Add +`` at the top of each table on a timestep page takes a live
calculation. Click it, type a property name or any calculation you could give
:meth:`~tangos.core.timestep.TimeStep.calculate_all` (see
:ref:`calculating`), and press Enter: the calculation becomes a column, evaluated
for every object on the page. Here are ``mass`` and ``VMax``:

.. image:: /_web_screenshots/web_columns.png
   :alt: The halos table with columns for mass and VMax
   :width: 60%

The arrows under each heading sort the table by that column, and the cross
removes it. Click a heading to edit its calculation. Columns stay in place as you
move between timesteps, so you can step through time and watch them change.

As you type, the interface suggests names to complete. If a suggestion is
highlighted when you press Enter, it is inserted -- so check that the heading
says what you meant.

A calculation that gives ``True`` or ``False`` becomes a filter. Here is
``VMax<300``:

.. image:: /_web_screenshots/web_boolean_column.png
   :alt: A column of True and False for VMax<300
   :width: 60%

Clicking the funnel beneath its heading hides every row for which it is
``False``. The table then lists the 954 halos, out of 1041, with ``VMax`` below
300 km/s:

.. image:: /_web_screenshots/web_filtered.png
   :alt: The halos table filtered to those with VMax<300
   :width: 60%

Calculations can follow links, as in python. In ``tutorial_changa_blackholes``,
whose halos are linked to their black holes (see :doc:`/tutorials/black_holes`),
the column ``BH_central.BH_mass`` gives the mass of each halo's central black
hole, and ``None`` for a halo that has none:

.. image:: /_web_screenshots/web_link_column.png
   :alt: The halos of tutorial_changa_blackholes with a column for BH_central.BH_mass
   :width: 50%

An object's page has an ``Add +`` row too, for a calculation on just that object.

Fetching data from a script
---------------------------

Every plot and table in the interface is drawn from data that the server also
returns at its own URL, so a script can fetch numbers from a running server
without opening the database itself -- useful when the database is on another
machine. The URLs follow the paths of the pages:

.. list-table::
   :header-rows: 1
   :widths: 55 45

   * - URL
     - Returns
   * - ``/<simulation>/<timestep>/<x>/vs/<y>.csv``
     - *x* and *y* for every object in the timestep, as CSV
   * - ``/<simulation>/<timestep>/<object>/<x>/vs/<y>.csv``
     - *x* and *y* along the object's major progenitor branch
   * - ``/<simulation>/<timestep>/<object>/<name>.csv``
     - an array property of one object
   * - ``/<simulation>/<timestep>/<object>/<name>.json``
     - one value of one object
   * - ``/<simulation>/<timestep>/gather/<type>/<name>.json``
     - one value for every object of a type, formatted as text for display
   * - ``/<simulation>/<timestep>/<object>/merger/tree.json``
     - the merger tree that the interface draws

Replace ``.csv`` with ``.png``, ``.svg`` or ``.pdf`` to get the plot itself. The
CSV of a whole timestep takes two optional query parameters: ``object_typetag``,
to choose a type of object, and ``filter``, a calculation that must be ``True``.
Names and calculations in a URL must be URL-encoded, with any ``/`` written as
``_slash_``, and a ``%`` in a timestep pattern as ``%25``.

From python, the CSV is the most useful. With ``tangos serve`` running against
the tutorial database:

.. ipython::

 In [1]: import urllib.parse, urllib.request

 In [2]: import numpy as np

 In [3]: url = ("http://localhost:6543/tutorial_gadget/snapshot_020/mass/vs/VMax.csv"
    ...:        "?object_typetag=halo&filter=" + urllib.parse.quote("VMax<300"))
    ...:

 In [4]: data = np.genfromtxt(urllib.request.urlopen(url), delimiter=",", names=True)

 In [5]: len(data), data["mass"][:3], data["VMax"][:3]

The same 954 halos as the filtered table above.

.. seealso::

   :doc:`/tutorials/quickstart`, :doc:`/tutorials/time_series` and
   :doc:`/how-to/live_calculation_recipes` cover the same ground from python, and
   :ref:`configuration` lists the ``webview_`` settings that control image
   format, resolution and caching.
