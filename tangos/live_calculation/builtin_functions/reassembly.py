from __future__ import absolute_import
from tangos.core import extraction_patterns
from . import BuiltinFunction
from .. import StoredProperty, FixedInput, LiveProperty


@BuiltinFunction.register
def raw(halos, values):
    return values

@raw.set_initialisation
def raw_initialisation(input):
    if isinstance(input, LiveProperty):
        input.set_raw()
    else:
        input.set_extraction_pattern(extraction_patterns.HaloPropertyRawValueGetter())


@BuiltinFunction.register
def reassemble(halos, values, *options):
    return values

@reassemble.set_initialisation
def reassemble_initialisation(input, *options):
    options_values = []
    for option in options:
        if isinstance(option, FixedInput):
            options_values.append(option.proxy_value())
        else:
            raise TypeError("Options to 'reassemble' must be fixed numbers or strings")
    if isinstance(input,LiveProperty):
        input.set_reassemble()
        input.set_evaluation_options(*options_values)
    else:
        input.set_extraction_pattern(
            extraction_patterns.HaloPropertyValueWithReassemblyOptionsGetter(*options_values))
