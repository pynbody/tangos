.. Last checked by AP: 2026-08-29

.. _api_live_calculation:

Live calculations from python
===============================

This page documents the python objects behind tangos' live-calculation
mini-language (``max(dm_density_profile)``, ``earlier(1).Mvir``, and so on);
for the language itself, see ``reference/live_calculation_language``
(forthcoming). Most users only ever pass a string or a lambda to ``calculate``
-- these objects matter once you want to build a calculation once and reuse it
across many timesteps, or write your own mini-language function.

.. currentmodule:: tangos.live_calculation

.. autosummary::

   Calculation
   MultiCalculation
   StoredProperty
   LiveProperty
   Link
   FixedInput
   FixedNumericInput
   NoResultsError
   BuiltinFunction
   IN_OPS
   UNARY_OPS
   parser.parse_property_name
   parser.parse_property_name_if_required
   parser.parse_property_names
   from_lambda.to_calculation
   from_lambda.to_calculations
   from_lambda.LambdaCalculationError
   from_lambda.ControlFlowError

Parsing a calculation
-----------------------

.. currentmodule:: tangos.live_calculation.parser

.. autofunction:: parse_property_name

.. autofunction:: parse_property_name_if_required

.. autofunction:: parse_property_names

.. currentmodule:: tangos.live_calculation.from_lambda

.. autofunction:: to_calculation

.. autofunction:: to_calculations

.. autoexception:: LambdaCalculationError

.. autoexception:: ControlFlowError

.. currentmodule:: tangos.live_calculation

The lambda form (``to_calculation``, ``to_calculations``, and the two
exceptions above) is the whole of ``from_lambda``'s public surface --
``tangos/live_calculation/from_lambda.py`` defines ``__all__`` as exactly
these four names.

A parsed calculation
----------------------

.. autoclass:: Calculation
   :members: values, values_sanitized, value, value_sanitized,
             values_and_description, values_sanitized_and_description,
             n_columns, supplement_halo_query, name
   :undoc-members:
   :member-order: bysource

.. autoexception:: NoResultsError

Raised through ``calculate()`` when a calculation cannot be evaluated (for
example, a stored property that is missing); catch it around individual
``calculate()`` calls when iterating over many objects.

The node types
---------------

A :class:`Calculation` is a small tree; these are the node types you would
construct or inspect if building one by hand rather than through
:func:`parser.parse_property_name` or :func:`from_lambda.to_calculation`.

.. autoclass:: MultiCalculation
   :show-inheritance:

.. autoclass:: StoredProperty
   :show-inheritance:

.. autoclass:: LiveProperty
   :show-inheritance:

.. autoclass:: Link
   :show-inheritance:

.. autoclass:: FixedInput
   :show-inheritance:

.. autoclass:: FixedNumericInput
   :show-inheritance:

Operators
----------

.. autodata:: IN_OPS

.. autodata:: UNARY_OPS

The authoritative list of infix and prefix operators the mini-language parser
accepts, and their precedence (earlier entries in ``IN_OPS`` bind tighter).

Adding your own function to the mini-language
------------------------------------------------

.. autoclass:: BuiltinFunction
   :members: register, set_input_options, set_initialisation, all, has_function
   :undoc-members:
   :member-order: bysource

Every function the language recognises (``max``, ``later``, ``at``, ``link``,
...) is registered this way; examples of the pattern -- a plain function
decorated with ``@BuiltinFunction.register``, then configured with
``set_input_options``/``set_initialisation`` -- live in
``tangos/live_calculation/builtin_functions/``. Note that the individual
registered functions are documented by their *language* signature at
``reference/live_calculation_language`` (forthcoming), not here: their python
signatures are written for the parser (``def later(source_halos, n)``) and
would be actively misleading shown as the API a user calls (a user writes
``later(5)``, not ``later(halos, 5)``).

.. seealso::

   :doc:`properties` for writing the property classes a calculation reads
   from, and :doc:`/live_calculation` for the mini-language itself.
