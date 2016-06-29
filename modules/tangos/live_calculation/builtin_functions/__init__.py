import numpy as np

import tangos
from tangos.util import consistent_collection
from .. import BuiltinFunction, FixedInput, FixedNumericInput
from ... import core


@BuiltinFunction.register
def match(source_halos, target):
    from ... import relation_finding
    if not isinstance(target, core.Base):
        target = tangos.get_item(target)
    results = relation_finding.MultiSourceMultiHopStrategy(source_halos, target).all()
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



from . import arithmetic, array, reassembly