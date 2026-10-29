.. Checked by AP 5/09/26
.. Stage 3 (Claude, 2026-10-26): links repointed after the live calculations tutorial
   was dissolved; fixed "as soon if". Not yet checked by a human.
.. Stage 4.5 (Claude, 2026-10-29): grown from a stub into a tour of the interface.
   "Starting the server" is unchanged; everything after it is new. The screenshots
   and videos are taken during the build by docs/web_screenshots.py, and the URL examples run
   against the server it starts. The merger tree is tutorial_gadget's since the
   fix to tangos/web/views/merger_tree.py in the same PR, the animation marks
   each click, and "Calculations as columns" is shown as videos. Not yet checked
   by a human.

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
a name to open the simulation. From any page, the trail of links at the top leads
back up to the timestep, the simulation, and the front page.

A simulation's page lists its timesteps, with the redshift and time of each, and
the number of objects in it. Despite the column's name, ``n_halos``, that counts
halos and groups together, which is why ``tutorial_gadget``'s last snapshot shows
1996 (1041 halos and 955 groups).

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
properties change as you step. Here is ``-1`` clicked three times, from the
final snapshot, with a blue circle marking each click; the halo is number 1, then
2 at the next two snapshots, then 5, because halos are numbered by size within
each snapshot:

.. image:: /_web_screenshots/web_time_navigation.png
   :alt: Stepping back three snapshots from halo 1 of snapshot_020
   :width: 80%

*Look at another halo* moves to the neighbouring halo numbers, and *Find in
another simulation* jumps to the matching object in another simulation, if the
two have been linked (see :doc:`/tutorials/crossmatching`). *Raw links to other
objects* lists every link from the object, strongest first, for following one by
hand.

Plotting
--------

The plots on an object's page, to the right of its properties, are built from
the circles beside each property, which the axes icon marks as plot controls.
For a property with a single value, the left circle chooses it for the *x* axis
and the right one for the *y* axis; here, ``mass`` for *x* and ``VMax`` for *y*:

.. image:: /_web_screenshots/web_plot_controls.png
   :alt: The property list, with mass chosen for the x axis and VMax for the y axis
   :width: 60%

An array has a single circle, which plots the array on its own -- a density
profile against radius, for instance (``dm_density_profile``, further down the
list). Then choose *This halo* or *This timestep* above the plot, and tick *log
x* and *log y* for logarithmic axes.

*This timestep* plots one point for every object in the timestep -- here
``VMax`` against ``mass`` for the halos of ``tutorial_gadget``, with both
axes ticked as logarithmic:

.. image:: /_web_screenshots/web_population_plot.png
   :alt: VMax against mass for every halo in snapshot_020
   :width: 60%

*This halo* follows the object back through its major progenitors instead, as
:meth:`~tangos.core.halo.SimulationObjectBase.calculate_for_progenitors` does
in python. With ``t()`` on the *x* axis and ``mass`` on the *y* axis, and only
*log y* ticked, that is the halo's mass growth:

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
it to open its page. This is the tree of halo 1 of ``tutorial_gadget``:

.. image:: /_web_screenshots/web_merger_tree.png
   :alt: The merger tree of halo 1 of snapshot_020 in tutorial_gadget
   :width: 70%

Some halo finders' merger trees, such as Rockstar's, insert a *phantom* where the
finder lost track of a halo for a snapshot; the tree draws a dashed line to it.
*Download Merger Tree* saves the drawing as an SVG file.

Calculations as columns
-----------------------

The heading ``Add +`` at the top of each table on a timestep page takes a live
calculation. Click it, type a property name or any calculation you could give
:meth:`~tangos.core.timestep.TimeStep.calculate_all` (see
:ref:`calculating`), and press Enter: the calculation becomes a column, evaluated
for every object in the timestep. Here ``mass`` and then ``VMax`` are added to
the halos of ``tutorial_gadget``'s last snapshot; a circle marks each click, and
a badge each press of Enter:

.. raw:: html

   <video class="web-video" src="../_static/web_columns.webm" autoplay loop muted playsinline
          controls aria-label="Typing mass and then VMax into the Add + heading of the halos table, which adds a column for each"></video>

The arrows under each heading sort the whole table by that column, across all its
pages, and the cross removes it. Click a heading to edit its calculation. Columns stay in place as you
move between timesteps, so you can step through time and watch them change.

As you type, the interface suggests names to complete, as the video shows. If a
suggestion is highlighted when you press Enter, it is inserted -- so check that
the heading says what you meant.

A calculation that gives ``True`` or ``False`` becomes a filter. Clicking the
filter icon beneath its heading -- a funnel, drawn in outline until it is
switched on -- hides every row, on every page, for which it is ``False``. Here
``VMax<300`` is added, ``False`` for the largest halos, and then switched on as a
filter, after which the table lists the 954 halos, out of 1041, with ``VMax``
below 300 km/s:

.. raw:: html

   <video class="web-video" src="../_static/web_filter.webm" autoplay loop muted playsinline
          controls aria-label="Typing VMax<300 into a new column heading, then clicking its filter icon, which leaves the 954 halos with VMax below 300"></video>

Calculations can follow links, as in python. In ``tutorial_changa_blackholes``,
whose halos are linked to their black holes (see :doc:`/tutorials/black_holes`),
the column ``BH_central.BH_mass`` gives the mass of each halo's central black
hole, and ``None`` for a halo that has none:

.. raw:: html

   <video class="web-video" src="../_static/web_link_column.webm" autoplay loop muted playsinline
          controls aria-label="Typing BH_central.BH_mass into the Add + heading of the halos table of tutorial_changa_blackholes"></video>

An object's page has an ``Add +`` row too, for a calculation on just that object.

Fetching data from a script
---------------------------

Every plot and table in the interface is drawn from data that the server also
returns at its own URL, so a script can fetch numbers from a running server
without opening the database itself. The server listens only on your own
machine; to reach one running elsewhere, forward its port with ssh first, as
in :ref:`ssh_tunnel` (with ``6543`` in place of ``3306``). The URLs follow the
paths of the pages:

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
     - one value for every object of a type, as the strings the table displays
       (rounded for display, so prefer the CSV for numbers)
   * - ``/<simulation>/<timestep>/<object>/merger/tree.json``
     - the merger tree that the interface draws

Here ``<object>`` is written as in the page's own address, such as ``halo_1``,
and ``<type>`` is ``halo``, ``group``, ``BH`` or ``tracker``. Replace ``.csv``
with ``.png``, ``.svg`` or ``.pdf`` to get the plot itself. The CSV of a whole
timestep takes two optional query parameters: ``object_typetag``, to choose one
type of object (without it, every object that has both values is included), and
``filter``, a calculation that must be ``True``. Its rows are not in order of
object number; to know which row is which object, ask for ``halo_number()`` as
one of the two values. Names and calculations in a URL must be URL-encoded,
with any ``/`` written as ``_slash_``. A timestep can be given by a pattern, as
in python (``%960``), with the ``%`` written as ``%25``.

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

The same 954 halos as the filtered table above, though in a different order.

.. seealso::

   :doc:`/tutorials/quickstart`, :doc:`/tutorials/time_series` and
   :doc:`/how-to/live_calculation_recipes` cover the same ground from python, and
   :ref:`configuration` lists the ``webview_`` settings that control image
   format, resolution and caching.
