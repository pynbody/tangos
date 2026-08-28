import functools
import threading

import pyparsing as pp

_parsing_lock = threading.Lock() # pyparsing is NOT thread safe

from . import (
    IN_OPS,
    UNARY_OPS,
    Calculation,
    FixedInput,
    FixedNumericInput,
    Link,
    LiveProperty,
    MultiCalculation,
    StoredProperty,
    from_lambda,
)


def pack_args(for_function):
    """Return a version of for_function that takes a single argument instead of multiple arguments"""
    return lambda t: for_function(*t)


pp.ParserElement.enable_packrat()

numerical_value = pp.Regex(r'-?\d+(\.\d*)?([eE]-?\d+)?').set_parse_action(pack_args(FixedNumericInput))

# n.b. IN_OPS and UNARY_OPS are defined alongside the Calculation classes, which need
# them to write calculations back out in operator form

IN_OPS_PYPARSING = []
UNARY_OPS_PYPARSING = []

def generate_property_from_inop(opFunctionName, tokens):
    return LiveProperty(opFunctionName, *tokens[0])

for opSymbol, opFunctionName in IN_OPS:
    opGeneration = functools.partial(generate_property_from_inop, opFunctionName)
    IN_OPS_PYPARSING.append((pp.Literal(opSymbol).suppress(), 2, pp.opAssoc.RIGHT, opGeneration))

for opSymbol, opFunctionName in UNARY_OPS:
    opGeneration = functools.partial(generate_property_from_inop, opFunctionName)
    UNARY_OPS_PYPARSING.append((pp.Literal(opSymbol).suppress(), 1, pp.opAssoc.RIGHT, opGeneration))

property_name = pp.Word(pp.alphas,pp.alphanums+"_")
stored_property = property_name.setParseAction(pack_args(StoredProperty))

live_calculation_property = pp.Forward().setParseAction(pack_args(LiveProperty))

array_element = pp.Forward().setParseAction(pack_args(functools.partial(LiveProperty,"element")))


dbl_quotes = pp.Literal("\"").suppress()
sng_quotes = pp.Literal("'").suppress()

string_value = dbl_quotes.suppress() + pp.SkipTo(dbl_quotes).setParseAction(pack_args(FixedInput)) + dbl_quotes.suppress() | \
               sng_quotes.suppress() + pp.SkipTo(sng_quotes).setParseAction(pack_args(FixedInput)) + sng_quotes.suppress()

redirection = pp.Forward().setParseAction(pack_args(Link))

element_identifier = pp.Literal("[").suppress()+numerical_value+pp.Literal("]").suppress();

def generate_multiple_properties_or_group(*tokens):
    """Brackets around a single calculation are just grouping; more make a MultiCalculation"""
    if len(tokens)==1:
        return tokens[0]
    else:
        return MultiCalculation(*tokens)

multiple_properties = pp.Forward().setParseAction(pack_args(generate_multiple_properties_or_group))

property_identifier = (redirection | array_element | live_calculation_property | stored_property | multiple_properties)


infix_operations = pp.infixNotation(numerical_value | property_identifier, IN_OPS_PYPARSING + UNARY_OPS_PYPARSING)


value_or_property_name = infix_operations | string_value | numerical_value |  property_identifier

multiple_properties << (pp.Literal("(").suppress()+value_or_property_name+pp.ZeroOrMore(pp.Literal(",").suppress()+value_or_property_name) +pp.Literal(")").suppress())

redirection << (live_calculation_property | stored_property ) + pp.Literal(".").suppress() + property_identifier

parameters = pp.Literal("(").suppress()+pp.Optional(value_or_property_name+pp.ZeroOrMore(pp.Literal(",").suppress()+value_or_property_name))+pp.Literal(")").suppress()
live_calculation_property << property_name+parameters

array_element << ((live_calculation_property | stored_property) + element_identifier)

property_complete = pp.string_start()+value_or_property_name+pp.string_end()


def parse_property_name( name):
    """Parse a string in the live-calculation mini-language into a Calculation"""
    with _parsing_lock:
        return property_complete.parse_string(name)[0]

def parse_property_name_if_required(name):
    """Return a Calculation for name, whichever way the calculation has been specified.

    The calculation may be given as a string in the mini-language (see
    parse_property_name), as a lambda taking no arguments (see
    live_calculation.from_lambda.to_calculation), or as a Calculation object, which is
    returned unchanged."""
    if isinstance(name, Calculation):
        return name
    elif isinstance(name, str):
        return parse_property_name(name)
    elif callable(name):
        return from_lambda.to_calculation(name)
    else:
        raise TypeError("A live calculation must be specified as a string, as a lambda "
                        "taking no arguments, or as a Calculation object "
                        "(received %r)" % (name,))

def parse_property_names(*names):
    """Return a MultiCalculation of the named calculations.

    Each may be a string, a lambda or a Calculation; see parse_property_name_if_required."""
    return MultiCalculation(*[parse_property_name_if_required(n) for n in names])

__all__ = ["parse_property_name", "parse_property_name_if_required", "parse_property_names"]
