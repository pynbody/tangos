import pyparsing as pp

from . import StoredProperty, StoredPropertyRawValue, LiveProperty, FixedNumericInputDescription, \
    FixedInput, Link, MatchLink, MultiCalculation


def parse_property_name( name):
    pack_args = lambda fun: lambda t: fun(*t)

    property_name = pp.Word(pp.alphas,pp.alphanums+"_")
    stored_property = property_name.setParseAction(pack_args(StoredProperty))

    raw_stored_property = (pp.Literal("raw(").suppress()+property_name.setParseAction(pack_args(StoredPropertyRawValue))+pp.Literal(")").suppress())
    live_calculation_property = pp.Forward().setParseAction(pack_args(LiveProperty))

    numerical_value = pp.Regex(r'-?\d+(\.\d*)?([eE]\d+)?').setParseAction(pack_args(FixedNumericInputDescription))

    dbl_quotes = pp.Literal("\"").suppress()
    sng_quotes = pp.Literal("'").suppress()

    string_value = dbl_quotes.suppress() + pp.SkipTo(dbl_quotes).setParseAction(pack_args(FixedInput)) + dbl_quotes.suppress() | \
                   sng_quotes.suppress() + pp.SkipTo(sng_quotes).setParseAction(pack_args(FixedInput)) + sng_quotes.suppress()

    redirection = pp.Forward().setParseAction(pack_args(Link))

    matched_redirection = pp.Forward().setParseAction(pack_args(MatchLink))

    multiple_properties = pp.Forward().setParseAction(pack_args(MultiCalculation))

    property_identifier = (matched_redirection | redirection | raw_stored_property | live_calculation_property | stored_property | multiple_properties)

    value_or_property_name = string_value | numerical_value | property_identifier

    multiple_properties << (pp.Literal("(").suppress()+value_or_property_name+pp.ZeroOrMore(pp.Literal(",").suppress()+value_or_property_name) +pp.Literal(")").suppress())

    redirection << (live_calculation_property | stored_property ) + pp.Literal(".").suppress() + property_identifier
    matched_redirection << pp.Literal("match(").suppress()+string_value+pp.Literal(").").suppress() + property_identifier


    parameters = pp.Literal("(").suppress()+pp.Optional(value_or_property_name+pp.ZeroOrMore(pp.Literal(",").suppress()+value_or_property_name))+pp.Literal(")").suppress()
    live_calculation_property << property_name+parameters
    property_complete = pp.stringStart()+value_or_property_name+pp.stringEnd()


    return property_complete.parseString(name)[0]


def parse_property_names(*names):
    return MultiCalculation(*[parse_property_name(n) for n in names])