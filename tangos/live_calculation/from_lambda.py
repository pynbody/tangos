"""Convert python lambda expressions into tangos live-calculation objects.

The live-calculation mini-language is normally written as a string and turned into
:class:`~tangos.live_calculation.Calculation` objects by
:mod:`tangos.live_calculation.parser`. This module offers an alternative front-end
where the calculation is written as a python lambda instead::

    from tangos.live_calculation.from_lambda import to_calculation

    to_calculation(lambda: Mvir)                  # StoredProperty('Mvir')
    to_calculation(lambda: Vvir())                # LiveProperty('Vvir')
    to_calculation(lambda: at(Rvir/2, dm_density_profile))
    to_calculation(lambda: later(5).Mvir)         # Link(...)
    to_calculation(lambda: (Mvir, Rvir))          # MultiCalculation(...)

Anything that can be written as a string for the parser can be written as a lambda,
with the following spellings:

===========================  ====================================
mini-language                lambda
===========================  ====================================
``Mvir``                     ``lambda: Mvir``
``Vvir()``                   ``lambda: Vvir()``
``BH.BH_mass``               ``lambda: BH.BH_mass``
``dm_density_profile[3]``    ``lambda: dm_density_profile[3]``
``reassemble(h, 'sum')``     ``lambda: reassemble(h, 'sum')``
``(Mvir, Rvir)``             ``lambda: (Mvir, Rvir)``
``~has_property(Mvir)``      ``lambda: ~has_property(Mvir)``
``a<10 & b>5``               ``lambda: (a<10) & (b>5)``
===========================  ====================================

Note that logical_not must be spelled ``~`` rather than the mini-language's
alternative ``!`` or python's ``not`` (which cannot be captured, see below), and that
python's operator precedence applies rather than the mini-language's, so comparisons
combined with ``&`` or ``|`` need explicit brackets.

There are a few further differences from the string parser, all of them a consequence
of the lambda being genuine python:

* python's associativity applies, so ``lambda: a-b-c`` is ``(a-b)-c`` where the string
  ``"a-b-c"`` is ``a-(b-c)``;
* python folds constant sub-expressions before we see them, so ``lambda: f(2+3)``
  arrives as ``f(5)`` and ``lambda: f(-1.5)`` as a negative literal rather than
  ``negate(1.5)``;
* a property whose name happens to be a python keyword cannot be written as a lambda;
  use the string parser for those.

How it works
------------

A lambda stores names plus a rule for looking them up, not values. So the lambda is
cloned with its globals (and closure cells) replaced by symbolic stand-ins, and then
simply called; the operators on those stand-ins build ``Calculation`` objects instead
of computing anything. This is the same idea as sympy's ``Symbol`` or jax tracing.

The consequence is that anything python evaluates eagerly cannot be seen by the
tracer. Control flow (``if``/``else``, ``and``, ``or``, ``not``, ``in``, ``is``,
comprehensions, generator expressions) is therefore rejected, both statically by
inspecting the bytecode and dynamically as a backstop. The live-calculation language
has no control flow anyway; use ``&``, ``|`` and ``~`` for element-wise logic.

Name resolution
---------------

A name appearing in a lambda may be intended as a tangos property (``Mvir``) or may
be a genuine python variable that should be interpolated into the calculation
(``lambda: at(my_radius, profile)``). The ``name_resolution`` argument to
:func:`to_calculation` chooses between the possible policies; see its docstring.

Python functions and lambdas
----------------------------

Unless ``name_resolution='tangos'`` is in force, a name that resolves to a python
function is used as that function rather than as a live-calculation name:

* a lambda taking no arguments stands for a calculation in its own right, and may be
  written either bare or called, so that given ``half_radius = lambda: Rvir/2`` both
  ``lambda: at(half_radius, profile)`` and ``lambda: at(half_radius(), profile)``
  give ``at(Rvir/2,profile)``;

* a function taking arguments is genuinely called while tracing, with the
  calculations written in the lambda as its arguments, so that it can assemble part
  of the calculation::

      difference = lambda a, b: a-b
      to_calculation(lambda: difference(MDM, Mgas))     # MDM-Mgas

  The arguments must therefore match the function's signature, and names *inside*
  the function are ordinary python names rather than live-calculation names.
"""

import dis
import inspect
import math
import numbers
import re
import types

from . import (
    Calculation,
    FixedInput,
    FixedNumericInput,
    Link,
    LiveProperty,
    MultiCalculation,
    StoredProperty,
)

__all__ = ["to_calculation", "to_calculations", "LambdaCalculationError",
           "ControlFlowError"]


class LambdaCalculationError(ValueError):
    """Raised when a lambda cannot be expressed as a tangos live calculation"""


class ControlFlowError(LambdaCalculationError):
    """Raised when a lambda contains control flow, which tracing cannot capture"""


#: valid property/function names, mirroring parser.property_name
_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")

#: python operator -> live-calculation function name, mirroring parser.IN_OPS
_BINARY_OPS = {
    "add": "add",
    "sub": "subtract",
    "mul": "multiply",
    "truediv": "divide",
    "pow": "power",
    "and": "logical_and",
    "or": "logical_or",
}

#: python comparison -> live-calculation function name. Comparisons need no reflected
#: version; python swaps the operands and calls the mirrored comparison for us.
_COMPARISON_OPS = {
    "gt": "greater",
    "lt": "less",
    "ge": "greater_equal",
    "le": "less_equal",
    "eq": "equal",
    "ne": "not_equal",
}

#: python unary operator -> live-calculation function name, mirroring parser.UNARY_OPS
_UNARY_OPS = {
    "neg": "negate",
    "invert": "logical_not",
    "abs": "abs",
}

#: python operators with no live-calculation equivalent -> suggested alternative
_UNSUPPORTED_OPS = {
    "floordiv": ("//", "use / and, if required, a live-calculation function to round"),
    "mod": ("%", None),
    "xor": ("^", "use & and | for logical operations"),
    "lshift": ("<<", None),
    "rshift": (">>", None),
    "matmul": ("@", None),
    "pos": ("unary +", None),
}


# ------------------------------------------------------------ static bytecode checks

#: opcode name -> description of the construct that emits it
_BANNED_OPS = {
    "GET_ITER": "iteration (a comprehension, generator expression or unpacking)",
    "FOR_ITER": "iteration (a comprehension or generator expression)",
    "GET_AITER": "asynchronous iteration",
    "GET_ANEXT": "asynchronous iteration",
    "END_ASYNC_FOR": "asynchronous iteration",
    "GET_AWAITABLE": "'await'",
    "YIELD_VALUE": "a generator expression",
    "SEND": "a generator expression",
    "RETURN_GENERATOR": "a generator expression",
    "UNPACK_SEQUENCE": "sequence unpacking",
    "CONTAINS_OP": "an 'in' test (python coerces its result to a bool)",
    "IS_OP": "an 'is' test (python coerces its result to a bool)",
    "UNARY_NOT": "'not' (python coerces its result to a bool); use '~x' instead",
    "TO_BOOL": "a conversion to bool; use '&', '|' and '~' rather than 'and', 'or' and 'not'",
    "CALL_FUNCTION_EX": "argument unpacking with '*' or '**'",
}

_JUMP_OPS = frozenset(getattr(dis, "hasjump", None)
                      or (set(dis.hasjrel) | set(dis.hasjabs)))

_JUMP_DESCRIPTION = ("a conditional or boolean short-circuit "
                     "(if/else, 'and', 'or'); use '&', '|' and '~' instead")


def _source_context(code, positions):
    """Best-effort source line, with a caret, for inclusion in an error message"""
    lineno = getattr(positions, "lineno", None)
    if lineno is None:
        return ""
    try:
        lines, start = inspect.getsourcelines(code)
    except (OSError, TypeError):
        return f" (line {lineno})"
    index = lineno - (start or 1)
    if not 0 <= index < len(lines):
        return f" (line {lineno})"
    text = lines[index].rstrip("\n")
    caret = ""
    if (positions.col_offset is not None
            and positions.end_lineno == positions.lineno):
        end = positions.end_col_offset or positions.col_offset + 1
        caret = "\n    " + " " * positions.col_offset \
                + "^" * max(1, end - positions.col_offset)
    return f"\n    {text.strip() if not caret else text}{caret}"


def _walk_code(code):
    """Yield this code object and every code object nested within its constants"""
    yield code
    for const in code.co_consts:
        if isinstance(const, types.CodeType):
            yield from _walk_code(const)


def check_traceable(function):
    """Raise ControlFlowError if the bytecode of function contains untraceable constructs.

    This check is static, so it fires regardless of which branch would actually be
    taken when the lambda is traced.
    """
    for code in _walk_code(function.__code__):
        for instruction in dis.get_instructions(code):
            if instruction.opname in _BANNED_OPS:
                reason = _BANNED_OPS[instruction.opname]
            elif instruction.opcode in _JUMP_OPS:
                reason = _JUMP_DESCRIPTION
            else:
                continue
            raise ControlFlowError(
                "the live calculation language cannot express %s%s"
                % (reason, _source_context(code, instruction.positions)))


# ------------------------------------------------------- python values -> Calculation

def _numeric_calculation(value):
    """Build a FixedNumericInput from a python number.

    FixedNumericInput expects the string token seen by the parser, so the value is
    rendered back into the form the parser would have received.
    """
    if isinstance(value, bool):
        return FixedNumericInput(str(int(value)))
    if isinstance(value, numbers.Integral):
        return FixedNumericInput(str(int(value)))
    value = float(value)
    if not math.isfinite(value):
        raise LambdaCalculationError(
            "the live calculation language has no representation for %r" % value)
    return FixedNumericInput(repr(value))


def as_calculation(value):
    """Convert a python value produced while tracing into a Calculation"""
    if isinstance(value, _Symbolic):
        return value._as_calculation()
    if isinstance(value, Calculation):
        return value
    if isinstance(value, str):
        return FixedInput(value)
    if isinstance(value, (bool, numbers.Integral, numbers.Real)):
        return _numeric_calculation(value)
    if isinstance(value, tuple):
        if len(value) == 0:
            raise LambdaCalculationError(
                "cannot use an empty tuple in a live calculation")
        return MultiCalculation(*[as_calculation(v) for v in value])
    if isinstance(value, (list, set)):
        raise LambdaCalculationError(
            "cannot use a %s in a live calculation; write a tuple, e.g. (Mvir, Rvir), "
            "to return more than one calculation" % type(value).__name__)
    if value is None:
        raise LambdaCalculationError(
            "cannot use None in a live calculation")
    if isinstance(value, (_PythonFunction, types.FunctionType)):
        raise LambdaCalculationError(
            "cannot use the python function %s as a value in a live calculation; "
            "call it, so that its result becomes part of the calculation"
            % _function_description(value))
    raise LambdaCalculationError(
        "cannot use %r (of type %s) in a live calculation; only numbers, strings, "
        "tuples and other calculations can be included"
        % (value, type(value).__name__))


def _can_be_calculation(value):
    """True if a python value can be interpolated into a live calculation"""
    if isinstance(value, (Calculation, str, bool, numbers.Integral, numbers.Real)):
        return True
    if isinstance(value, tuple):
        return all(_can_be_calculation(v) for v in value)
    return isinstance(value, types.FunctionType)


# --------------------------------------------------------------- symbolic stand-ins

def _check_calculation_is_linkable(calculation, described_as):
    if not isinstance(calculation, (StoredProperty, LiveProperty, Link)):
        raise LambdaCalculationError(
            "cannot follow a link from %s; only a property or function result can "
            "appear to the left of a '.'" % described_as)


def _make_link(locator, target):
    """Construct Link(locator, target), matching the nesting generated by the parser.

    The parser generates right-nested links, i.e. a.b.c is Link(a, Link(b, c)),
    whereas python attribute access reaches us left-to-right.
    """
    _check_calculation_is_linkable(locator, str(locator))
    if isinstance(locator, Link):
        return Link(locator.locator, _make_link(locator.property, target))
    return Link(locator, target)


def _make_element(calculation, index):
    if isinstance(index, slice):
        raise LambdaCalculationError(
            "cannot slice %s; the live calculation language only supports selecting a "
            "single element, e.g. my_profile[3]" % calculation)
    index_calculation = as_calculation(index)
    if not isinstance(index_calculation, FixedNumericInput):
        raise LambdaCalculationError(
            "the index into %s must be a number, not %s"
            % (calculation, index_calculation))
    return LiveProperty("element", calculation, index_calculation)


def _guard(construct, advice=None):
    def guard_method(self, *args, **kwargs):
        message = "cannot convert %s applied to %s into a live calculation" \
                  % (construct, self._description())
        if advice is not None:
            message += "; " + advice
        raise ControlFlowError(message)
    return guard_method


class _Symbolic:
    """Base class for the stand-in objects substituted into a lambda while tracing.

    Operations on these build up Calculation objects instead of computing anything.
    """

    __slots__ = ()

    # stop numpy from taking over the reflected arithmetic operators, e.g. for
    # np.float64(2.0) * Mvir
    __array_ufunc__ = None

    # __eq__ is overloaded below, which would otherwise make these unhashable
    __hash__ = object.__hash__

    def _as_calculation(self):
        """Return the Calculation this stand-in represents when used as a value"""
        raise NotImplementedError

    def _description(self):
        """Return a description of this stand-in, for use in error messages"""
        return str(self._as_calculation())

    def _apply_call(self, arguments):
        """Return the Calculation for calling this stand-in with the given arguments"""
        raise LambdaCalculationError(
            "cannot call %s; only a property name or a linked property can be called, "
            "e.g. lambda: Vvir() or lambda: BH.Vvir()" % self._description())

    def _apply_subscript(self, index):
        """Return the Calculation for subscripting this stand-in"""
        return _make_element(self._as_calculation(), index)

    def __call__(self, *args, **kwargs):
        if kwargs:
            raise LambdaCalculationError(
                "the live calculation language does not support keyword arguments "
                "(got %s in a call to %s)"
                % (", ".join(sorted(kwargs)), self._description()))
        return _Value(self._apply_call([as_calculation(a) for a in args]))

    def __getitem__(self, index):
        return _Value(self._apply_subscript(index))

    def __getattr__(self, name):
        # note this is only reached for attributes not otherwise found, so it does not
        # interfere with the internals of the stand-in objects themselves
        if not _NAME_RE.match(name):
            raise AttributeError(
                "%r is not a valid live calculation property name" % name)
        return _Attribute(self, name)

    def __repr__(self):
        return "<live calculation being traced: %s>" % self._description()

    # Backstop for control flow that our static check cannot see, because it happens
    # inside an ordinary python function called by the lambda.
    __bool__ = _guard("a truth test",
                      "python evaluates 'and', 'or', 'not', 'in', 'is', chained "
                      "comparisons and if/else expressions eagerly, so they cannot be "
                      "captured; use '&', '|' and '~' instead")
    __len__ = _guard("len()")
    __iter__ = _guard("iteration")
    __contains__ = _guard("an 'in' test")
    __index__ = _guard("conversion to an index")
    __int__ = _guard("int()")
    __float__ = _guard("float()")
    __complex__ = _guard("complex()")
    __round__ = _guard("round()")
    __format__ = _guard("string formatting",
                        "a live calculation cannot be embedded in an f-string")


def _binary_op_method(function_name, reflected=False):
    def operator_method(self, other):
        this = self._as_calculation()
        that = as_calculation(other)
        if reflected:
            this, that = that, this
        return _Value(LiveProperty(function_name, this, that))
    return operator_method


def _unary_op_method(function_name):
    def operator_method(self):
        return _Value(LiveProperty(function_name, self._as_calculation()))
    return operator_method


def _unsupported_op_method(symbol, advice):
    def operator_method(self, *args):
        message = "the live calculation language has no '%s' operator" % symbol
        if advice is not None:
            message += "; " + advice
        raise LambdaCalculationError(message)
    return operator_method


for _python_name, _tangos_name in _BINARY_OPS.items():
    setattr(_Symbolic, f"__{_python_name}__", _binary_op_method(_tangos_name))
    setattr(_Symbolic, f"__r{_python_name}__",
            _binary_op_method(_tangos_name, reflected=True))

for _python_name, _tangos_name in _COMPARISON_OPS.items():
    setattr(_Symbolic, f"__{_python_name}__", _binary_op_method(_tangos_name))

for _python_name, _tangos_name in _UNARY_OPS.items():
    setattr(_Symbolic, f"__{_python_name}__", _unary_op_method(_tangos_name))

for _python_name, (_symbol, _advice) in _UNSUPPORTED_OPS.items():
    _method = _unsupported_op_method(_symbol, _advice)
    setattr(_Symbolic, f"__{_python_name}__", _method)
    if _python_name != "pos":
        setattr(_Symbolic, f"__r{_python_name}__", _method)

del _python_name, _tangos_name, _symbol, _advice, _method


class _Value(_Symbolic):
    """A stand-in wrapping a Calculation that is already fully determined"""

    __slots__ = ("_calculation",)

    def __init__(self, calculation):
        self._calculation = calculation

    def _as_calculation(self):
        return self._calculation


class _Name(_Symbolic):
    """A stand-in for a bare name, which may be a property or a function"""

    __slots__ = ("_name",)

    def __init__(self, name):
        self._name = name

    def _as_calculation(self):
        return StoredProperty(self._name)

    def _description(self):
        return self._name

    def _apply_call(self, arguments):
        return LiveProperty(self._name, *arguments)


class _Attribute(_Symbolic):
    """A stand-in for a name reached through a '.', i.e. a link to another object"""

    __slots__ = ("_parent", "_name")

    def __init__(self, parent, name):
        self._parent = parent
        self._name = name

    def _link_to(self, target):
        return _make_link(self._parent._as_calculation(), target)

    def _as_calculation(self):
        return self._link_to(StoredProperty(self._name))

    def _description(self):
        return f"{self._parent._description()}.{self._name}"

    def _apply_call(self, arguments):
        return self._link_to(LiveProperty(self._name, *arguments))

    def _apply_subscript(self, index):
        return self._link_to(_make_element(StoredProperty(self._name), index))


class _InlinedLambda(_Value):
    """A stand-in for a python lambda that stands for a calculation of its own.

    It can be used either as a value or called with no arguments, so that both
    lambda: my_lambda/Mvir and lambda: my_lambda()/Mvir mean the same thing."""

    __slots__ = ("_name",)

    def __init__(self, name, calculation):
        super().__init__(calculation)
        self._name = name

    def _apply_call(self, arguments):
        if arguments:
            raise LambdaCalculationError(
                "the python lambda %s takes no arguments, but was called with %d"
                % (self._name, len(arguments)))
        return self._calculation


class _PythonFunction:
    """A stand-in for a python function taking arguments.

    Calling it really calls the function, with the arguments being the calculations
    written in the lambda, so that the function assembles part of the calculation."""

    __slots__ = ("_name", "_function")

    def __init__(self, name, function):
        self._name = name
        self._function = function

    def __call__(self, *args, **kwargs):
        try:
            inspect.signature(self._function).bind(*args, **kwargs)
        except TypeError as exception:
            raise LambdaCalculationError(
                "cannot call the python function %s: %s"
                % (self._name, exception)) from None
        return self._function(*args, **kwargs)

    def __repr__(self):
        return "<python function being traced: %s>" % self._name


def _function_description(value):
    """Describe a python function, for use in error messages"""
    if isinstance(value, _PythonFunction):
        return value._name
    if value.__name__ == "<lambda>":
        return "lambda at %s:%d" % (value.__code__.co_filename,
                                    value.__code__.co_firstlineno)
    return value.__name__


# --------------------------------------------------------------------------- tracing

NAME_RESOLUTION_MODES = ("auto", "python", "tangos")


def _is_nullary_function(value):
    return (isinstance(value, types.FunctionType)
            and value.__code__.co_argcount == 0
            and value.__code__.co_kwonlyargcount == 0
            and not (value.__code__.co_flags & (inspect.CO_VARARGS
                                                | inspect.CO_VARKEYWORDS)))


def _check_is_nullary_function(function):
    if not isinstance(function, types.FunctionType):
        raise LambdaCalculationError(
            "expected a lambda (or other python function taking no arguments), "
            "got %r" % (function,))
    if not _is_nullary_function(function):
        raise LambdaCalculationError(
            "a live calculation lambda must take no arguments; write e.g. "
            "lambda: Mvir/Rvir rather than lambda %s: ..."
            % ", ".join(function.__code__.co_varnames[:function.__code__.co_argcount]
                        or ["x"]))
    if function.__code__.co_flags & (inspect.CO_GENERATOR | inspect.CO_COROUTINE
                                     | inspect.CO_ASYNC_GENERATOR):
        raise LambdaCalculationError(
            "cannot convert a generator or coroutine into a live calculation")


def _substitute(name, value, name_resolution, _tracing):
    """Return the object to bind to name while tracing, given its python value"""
    if name_resolution == "tangos":
        return _Name(name)
    if name_resolution == "auto" and not _can_be_calculation(value):
        return _Name(name)
    if isinstance(value, Calculation):
        return _Value(value)
    if _is_nullary_function(value):
        # a lambda taking no arguments is a calculation in its own right, so it is
        # converted here and inlined wherever the name is used
        return _InlinedLambda(name, _trace(value, name_resolution, _tracing))
    if isinstance(value, types.FunctionType):
        # a function taking arguments is called while tracing, so that it can
        # assemble part of the calculation from the arguments it is given
        return _PythonFunction(name, value)
    # anything else is passed through as its real python value, so that e.g.
    # arithmetic between python variables happens as python arithmetic
    return value


def _global_names(code):
    """Return the names that this code object looks up in its globals"""
    return {instruction.argval for instruction in dis.get_instructions(code)
            if instruction.opname == "LOAD_GLOBAL"}


def _traced_globals(function, name_resolution, _tracing):
    # builtins are deliberately excluded: the live calculation language has functions
    # such as abs() and sum() of its own, and those should win over python's
    traced = {"__builtins__": {}}
    for name in _global_names(function.__code__):
        if name in function.__globals__:
            traced[name] = _substitute(name, function.__globals__[name],
                                       name_resolution, _tracing)
        else:
            traced[name] = _Name(name)
    return traced


def _traced_closure(function, name_resolution, _tracing):
    if not function.__closure__:
        return None
    cells = []
    for name, cell in zip(function.__code__.co_freevars, function.__closure__):
        try:
            value = cell.cell_contents
        except ValueError:  # an empty cell, e.g. a recursive definition
            cells.append(cell)
            continue
        cells.append(types.CellType(
            _substitute(name, value, name_resolution, _tracing)))
    return tuple(cells)


def _trace(function, name_resolution, _tracing):
    _check_is_nullary_function(function)
    check_traceable(function)

    if function.__code__ in _tracing:
        raise LambdaCalculationError(
            "cannot convert %s: it refers to itself" % _describe(function))

    _tracing = _tracing | {function.__code__}

    shadow = types.FunctionType(
        function.__code__,
        _traced_globals(function, name_resolution, _tracing),
        function.__name__,
        function.__defaults__,
        _traced_closure(function, name_resolution, _tracing))

    return as_calculation(shadow())


def _describe(function):
    """Describe a lambda for use in an error message"""
    try:
        source = inspect.getsource(function).strip()
    except (OSError, TypeError):
        source = None
    if source is not None and len(source) < 120:
        return source
    if function.__name__ == "<lambda>":
        return "the lambda at %s:%d" % (function.__code__.co_filename,
                                        function.__code__.co_firstlineno)
    return function.__name__


def to_calculation(function, name_resolution="auto"):
    """Convert a lambda expression into a tangos live calculation.

    For example, to_calculation(lambda: later(5).Mvir/Rvir) returns the same
    calculation as parser.parse_property_name("later(5).Mvir/Rvir").

    :param function: a lambda (or any python function) taking no arguments. Names
      appearing within it that do not resolve to python variables are interpreted as
      live-calculation property or function names.

    :param name_resolution: how to treat names that *do* resolve to a python variable
      in the scope surrounding the lambda:

      * 'auto' (default): interpolate the python value if it is something a live
        calculation can contain (a number, a string, a tuple of those, a Calculation
        or another lambda taking no arguments); otherwise ignore the python variable
        and interpret the name as a live-calculation name.
      * 'python': always use the python value. This is the closest to normal python
        scoping rules, and allows e.g. numpy.pi to be interpolated, but a variable
        that happens to share the name of a database property will silently shadow it.
      * 'tangos': never use the python value; every name is a live-calculation name.

      In all cases, python's builtins are excluded, so that live-calculation functions
      like abs() and sum() are not shadowed by the python functions of the same name.

    :returns: a Calculation object, equivalent to one generated by the parser
    """
    if name_resolution not in NAME_RESOLUTION_MODES:
        raise ValueError("name_resolution must be one of %s, not %r"
                         % (", ".join(repr(m) for m in NAME_RESOLUTION_MODES),
                            name_resolution))
    return _trace(function, name_resolution, frozenset())


def to_calculations(*functions, name_resolution="auto"):
    """Convert multiple lambda expressions into a single MultiCalculation.

    This is the lambda equivalent of parser.parse_property_names. See
    :func:`to_calculation` for the meaning of name_resolution.
    """
    return MultiCalculation(*[to_calculation(f, name_resolution)
                              for f in functions])
