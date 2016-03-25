from . import BuiltinFunction, FixedInput, FixedNumericInput, StoredProperty
from .. import consistent_collection
from .. import core
from .. import halo_data_extraction_patterns
import numpy as np

@BuiltinFunction.register
def abs(halos, vals):
    return [np.linalg.norm(v, axis=-1) for v in vals]

@BuiltinFunction.register
def match(source_halos, target):
    from .. import halo_finder
    if not isinstance(target, core.Base):
        target = core.get_item(target)
    results = halo_finder.MultiSourceMultiHopStrategy(source_halos, target).all()
    assert len(results) == len(source_halos)
    return np.array(results, dtype=object)

match.set_input_options(0, provide_proxy=True, assert_class = FixedInput)


@BuiltinFunction.register
def later(source_halos, num_steps):
    timestep = consistent_collection.ConsistentCollection(source_halos).timestep
    for i in xrange(num_steps):
        timestep = timestep.next
    return match(source_halos, timestep)
later.set_input_options(0, provide_proxy=True, assert_class = FixedNumericInput)

@BuiltinFunction.register
def earlier(source_halos, num_steps):
    timestep = consistent_collection.ConsistentCollection(source_halos).timestep
    for i in xrange(num_steps):
        timestep = timestep.previous
    return match(source_halos, timestep)
earlier.set_input_options(0, provide_proxy=True, assert_class = FixedNumericInput)


@BuiltinFunction.register
def raw(halos, values):
    return values

raw.set_input_options(0, assert_class=StoredProperty)

@raw.set_initialisation
def modifying_underlying_input(input):
    input._extraction_pattern = halo_data_extraction_patterns.HaloPropertyRawValueGetter