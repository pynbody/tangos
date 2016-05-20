import halo_db
from .. import BuiltinFunction, FixedInput, FixedNumericInput, StoredProperty
from ... import consistent_collection
from ... import core

import numpy as np



@BuiltinFunction.register
def match(source_halos, target):
    from ... import relation_finding_strategies
    if not isinstance(target, core.Base):
        target = halo_db.get_item(target)
    results = relation_finding_strategies.MultiSourceMultiHopStrategy(source_halos, target).all()
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