.. Last checked by AP: 2026-08-29

.. _api_examples:

Worked analysis helpers
=========================

.. currentmodule:: tangos.examples.mergers

``tangos.examples`` is two functions in one file, both fully documented and
importable from an installed tangos, and both demonstrate
:class:`~tangos.relation_finding.MultiHopMostRecentMergerStrategy` (see
:doc:`relation_finding`) doing something a user is likely to actually want:
finding mergers.

.. autosummary::

   get_mergers_of_major_progenitor
   most_major_mergers_since

.. autofunction:: get_mergers_of_major_progenitor

.. autofunction:: most_major_mergers_since

.. seealso::

   :doc:`relation_finding` for the merger-detection strategy these functions
   are built on.
