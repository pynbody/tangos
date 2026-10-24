.. Checked by AP 5/09/2026

.. _configuration:

Configuring tangos
==================

Every setting tangos has — where its database lives, how small a halo has to be
before it is discarded, which parallelism backend to use — is a module-level
variable in ``tangos/config.py``, carrying a default. You do not edit that
file. Instead you write a small ``config_local.py`` beside it, containing only
the variables you want to change.

Three of the settings can also be set from environment variables, which is
convenient for a one-off. Everything else, and anything you want to persist,
goes in ``config_local.py``.

Creating ``config_local.py``
----------------------------

``config.py`` finishes by importing everything from ``config_local`` as a
*relative* import, so ``config_local.py`` must sit next to it, inside the
installed package. It is not read from your working directory or your home
directory. Ask python where to put it:

.. ipython::

 In [1]: import tangos.config

 In [2]: tangos.config.__file__

Create ``config_local.py`` in that same directory, and set only what you want
to change:

.. code-block:: python

   # config_local.py
   db = "/data/shared/simulations.db"
   base = "/data/simulations/"
   min_halo_particles = 500

Every other setting keeps its default. The file is plain python, so you are
free to compute values in it — read an environment variable, branch on the
hostname of the machine — as long as the names it leaves defined are the ones
you mean to override.

**Precedence, highest first:** ``config_local.py``, then the environment
variable, then the built-in default. Because ``config_local.py`` is imported
last, a setting there wins over the matching environment variable, which can be
surprising if you set both.

Environment variables
---------------------

.. list-table::
   :header-rows: 1
   :widths: 34 22 44

   * - Variable
     - Sets
     - Notes
   * - ``TANGOS_DB_CONNECTION``
     - ``db``
     - A path, or a sqlalchemy URL.
   * - ``TANGOS_SIMULATION_FOLDER``
     - ``base``
     - Parent directory of your simulation directories.
   * - ``TANGOS_PROPERTY_MODULES``
     - ``property_modules``
     - Comma-separated list of module names.

Each is read **once**, when ``tangos.config`` is first imported. Setting one
after ``import tangos`` has no effect at all, so export them before starting
python — and remember they apply only to the shell session you export them in.
``config_local.py`` is the way to make any of them permanent.

.. note::
   Three further variables, ``TANGOS_TESTING_DB_USER``,
   ``TANGOS_TESTING_DB_PASSWORD`` and ``TANGOS_TESTING_DB_BACKEND``, select the
   database server that the test suite runs against. They have no effect on
   normal use.

All settings
------------

Reference material: the complete set, with defaults, grouped by what they
affect. Nothing below is needed to use tangos, and most of it exists to be
found once, when you hit the problem it solves.

Paths and database
^^^^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 40 18 42

   * - Setting
     - Default
     - Meaning
   * - ``db``
     - ``$HOME/tangos_data.db``
     - Database path or sqlalchemy URL.
   * - ``base``
     - ``$HOME/``
     - Parent directory of simulation directories.
   * - ``file_ignore_pattern``
     - ``[]``
     - Filename patterns to skip when discovering timesteps.
   * - ``max_traverse_depth``
     - ``3``
     - How many directory levels down timestep discovery recurses.

Object finding and linking
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 40 18 42

   * - Setting
     - Default
     - Meaning
   * - ``default_fileset_handler_class``
     - ``"pynbody.PynbodyInputHandler"``
     - Input handler for simulations added without ``--handler``.
   * - ``min_halo_particles``
     - ``1000``
     - Smallest object worth storing.
   * - ``max_num_objects``
     - ``None``
     - Cap on objects of each type per timestep.
   * - ``default_linking_threshold``
     - ``0.005``
     - Shared-particle fraction below which a link is not stored.

Parallelism
^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 40 18 42

   * - Setting
     - Default
     - Meaning
   * - ``default_backend``
     - ``'null'``
     - Backend used without ``--backend``: ``null``, ``mpi4py`` or
       ``multiprocessing``.
   * - ``DEFAULT_SLEEP_BEFORE_ALLOWING_NEXT_LOCK``
     - ``1.0``
     - Seconds to wait after releasing a lock. Raise it if sqlite on a network
       filesystem reports the database as locked.

pynbody tuning
^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 40 18 42

   * - Setting
     - Default
     - Meaning
   * - ``pynbody_build_kdtree_threshold_count``
     - ``2000``
     - Build a KDTree once a timestep expects more region queries than this.
   * - ``pynbody_build_kdtree_all_cpus``
     - ``True``
     - Let the server process use every CPU while building that tree.

Property modules
^^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 40 18 42

   * - Setting
     - Default
     - Meaning
   * - ``property_modules``
     - ``["tangos_nbodyshop_properties"]``
     - Modules of extra property implementations to import at startup. A module
       that is not installed raises a warning and is then skipped, which is why
       the default is harmless if you do not have it.

A package of properties can also register itself, so that no configuration is
needed by whoever installs it, through the ``tangos.property_modules``
setuptools entry point:

.. code-block:: python

   entry_points={"tangos.property_modules": [
       "fab = my_fab_tangos_properties"
   ]}

Merger trees
^^^^^^^^^^^^

These thin the tree at query time, not when links are written, so changing them
changes what you see without rebuilding anything.

.. list-table::
   :header-rows: 1
   :widths: 40 18 42

   * - Setting
     - Default
     - Meaning
   * - ``mergertree_min_fractional_weight``
     - ``0.02``
     - Discard links weaker than this fraction of the strongest link from the
       same object.
   * - ``mergertree_min_fractional_NDM``
     - ``0.01``
     - Discard objects smaller than this fraction of the largest at their
       timestep. Zero disables thinning by size.
   * - ``mergertree_max_nhalos``
     - ``30``
     - Maximum objects kept per timestep, least massive discarded first.
   * - ``mergertree_timeout``
     - ``15.0``
     - Seconds before the web interface gives up building a tree.
   * - ``mergertree_max_hops``
     - ``500``
     - Maximum number of timesteps to scan.

Relation finding
^^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 40 18 42

   * - Setting
     - Default
     - Meaning
   * - ``num_multihops_max_default``
     - ``100``
     - Maximum links followed by a multi-hop query.
   * - ``max_relative_time_difference``
     - ``1e-4``
     - How close two timesteps' times must be to count as contemporaneous.

Web interface
^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 40 18 42

   * - Setting
     - Default
     - Meaning
   * - ``webview_default_image_format``
     - ``'svg'``
     - ``svg`` or ``png``.
   * - ``webview_cache_time``
     - ``3600``
     - Seconds that images and data are cached for.
   * - ``webview_plots_dpi``
     - ``100``
     - Resolution of matplotlib figures returned to the browser.

Writing and importing
^^^^^^^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 40 18 42

   * - Setting
     - Default
     - Meaning
   * - ``DB_IMPORT_CHUNK_SIZE``
     - ``10``
     - Rows copied at a time by ``tangos import``.
   * - ``PROPERTY_WRITER_MAXIMUM_TIME_BETWEEN_COMMITS``
     - ``600``
     - Seconds before ``tangos write`` commits, even mid-timestep.
   * - ``PROPERTY_WRITER_MINIMUM_TIME_BETWEEN_COMMITS``
     - ``300``
     - Seconds before ``tangos write`` will commit at all, even between
       timesteps.
   * - ``PROPERTY_WRITER_PARALLEL_STATISTICS_TIME_BETWEEN_UPDATES``
     - ``600``
     - Seconds between progress reports from a parallel ``tangos write``.
   * - ``diff_default_atol``, ``diff_default_rtol``
     - ``1e-3``
     - Tolerances used by ``tangos diff``.

Two further names in ``config.py``, ``DOUBLE_PRECISION`` and ``LARGE_BINARY``,
are sqlalchemy column types rather than settings. They fix the schema of an
existing database, so changing them would need a migration; leave them alone.

.. seealso::

   :ref:`installation` for getting tangos and a database in place, and for the
   ``TANGOS_DB_CONNECTION`` environment variable in context.

   :doc:`/rdbms` for connection URLs and for setting up a MySQL or PostgreSQL
   server.
