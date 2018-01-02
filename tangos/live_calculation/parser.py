from __future__ import absolute_import
import pyparsing as pp
import functools
import threading

_parsing_lock = threading.Lock() # pyparsing is NOT thread safe

from . import StoredProperty, LiveProperty, FixedNumericInput, \
    FixedInput, Link, MultiCalculation, Calculation

def pack_args(for_function):
    """Return a version of for_function that takes a single argument instead of multiple arguments"""
    return lambda t: for_function(*t)


pp.ParserElement.enablePackrat()

numerical_value = pp.Regex(r'-?\d+(\.\d*)?([eE]-?\d+)?').setParseAction(pack_args(FixedNumericInput))

IN_OPS = [("**", "power"),
          ("*", "multiply"),
          ("/", "divide"),
          ("+", "add"),
          ("-", "subtract"),
          (">", "greater"),
          ("<", "less"),
          ("|", "logical_or"),
          ("&", "logical_and")]

UNARY_OPS = [("!", "logical_not")]

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

multiple_properties = pp.Forward().setParseAction(pack_args(MultiCalculation))

property_identifier = (redirection | array_element | live_calculation_property | stored_property | multiple_properties)


infix_operations = pp.infixNotation(numerical_value | property_identifier, IN_OPS_PYPARSING + UNARY_OPS_PYPARSING)


value_or_property_name = infix_operations | string_value | numerical_value |  property_identifier

multiple_properties << (pp.Literal("(").suppress()+value_or_property_name+pp.ZeroOrMore(pp.Literal(",").suppress()+value_or_property_name) +pp.Literal(")").suppress())

redirection << (live_calculation_property | stored_property ) + pp.Literal(".").suppress() + property_identifier

parameters = pp.Literal("(").suppress()+pp.Optional(value_or_property_name+pp.ZeroOrMore(pp.Literal(",").suppress()+value_or_property_name))+pp.Literal(")").suppress()
live_calculation_property << property_name+parameters

array_element << ((live_calculation_property | stored_property) + element_identifier)

property_complete = pp.stringStart()+value_or_property_name+pp.stringEnd()


def parse_property_name( name):
    with _parsing_lock:
        return property_complete.parseString(name)[0]

def parse_property_name_if_required(name):
    if isinstance(name, Calculation):
        return name
    else:
        return parse_property_name(name)

def parse_property_names(*names):
    return MultiCalculation(*[parse_property_name(n) for n in names])

__all__ = ["parse_property_name", "parse_property_name_if_required", "parse_property_names"]