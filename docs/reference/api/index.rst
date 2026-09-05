.. Last checked by AP: 2026-08-29

.. _api_index:

Python API reference
=====================

This is the reference for tangos' python API: the classes and functions a user
imports directly, organised by what you are trying to do rather than by which
module they happen to live in. Each page carries a short navigation table and
then the full signatures, with an explicit gap left visible (a bare signature,
no prose) wherever the underlying code has no docstring yet.

.. toctree::
   :maxdepth: 1

   query
   objects
   relation_finding
   live_calculation
   properties
   input_handlers
   parallel
   building
   testing
   examples

What is not here
-----------------

Some tangos surfaces are documented elsewhere rather than on these pages:

- The command-line tools (``tangos write``, ``tangos add``, ...) have their own
  reference, ``reference/cli`` (forthcoming).
- The mini-language used inside ``calculate()`` calls and web-interface columns
  (``max(dm_density_profile)``, ``earlier(1).Mvir``, ...) has its own reference,
  ``reference/live_calculation_language`` (forthcoming). This page documents the
  *python objects* behind that language; see :doc:`live_calculation`.
- The names of the properties tangos ships with (``Mvir``, ``shrink_center``, ...)
  are listed in ``reference/builtin_properties`` (forthcoming), not here.
- Configuration settings (``config_local.py``) are documented on the
  :doc:`/configuration` page, not as a module dump here.

A few things are deliberately left out of the whole reference, not just moved
elsewhere: the ``tangos.web`` Pyramid application (no user-subclassable surface;
its URL API is documented alongside the web interface instead), most of
``tangos.util`` (implementation utilities), the ``tangos.scripts`` console-script
wiring, ``tangos.cached_writer`` (the writer's internal commit path), and the
message-passing internals of ``tangos.parallel_tasks`` beyond the seven symbols
on :doc:`parallel`.
