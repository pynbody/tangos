from __future__ import absolute_import
from tangos.core import extraction_patterns
from . import BuiltinFunction
from .. import StoredProperty, FixedInput


@BuiltinFunction.register
def raw(halos, values):
    return values
raw.set_input_options(0, assert_class=StoredProperty)

@raw.set_initialisation
def raw_initialisation(input):
    input.set_extraction_pattern(extraction_patterns.HaloPropertyRawValueGetter())


@BuiltinFunction.register
def reassemble(halos, values, *options):
    return values
reassemble.set_input_options(0, assert_class=StoredProperty)

@reassemble.set_initialisation
def reassemble_initialisation(input, *options):
    options_values = []
    for option in options:
        if isinstance(option, FixedInput):
            options_values.append(option.proxy_value())
        else:
            raise TypeError("Options to 'reassemble' must be fixed numbers or strings")

    input.set_extraction_pattern(
        extraction_patterns.HaloPropertyValueWithReassemblyOptionsGetter(*options_values))
