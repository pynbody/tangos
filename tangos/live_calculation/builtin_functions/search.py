from __future__ import absolute_import
import numpy as np

from .. import BuiltinFunction, FixedInput, FixedNumericInput, StoredProperty, MultiCalculation, ReturnInputHalos
from ... import core
from ... import relation_finding
from ... import temporary_halolist as thl

@BuiltinFunction.register
def find_progenitor(source_halos, property_name, property_criterion):
    if property_criterion!='min' and property_criterion!='max':
        raise ValueError("Property criterion must be either 'min' or 'max'")

    if len(source_halos)==0:
        return []

    all_major_progs = relation_finding.multi_source.MultiSourceAllMajorProgenitorsStrategy(source_halos)

    sources = all_major_progs.sources()

    property_and_obj = MultiCalculation(ReturnInputHalos(), property_name.name)

    with all_major_progs.temp_table() as tt :
        raw_query = thl.halo_query(tt)
        query = property_and_obj.supplement_halo_query(raw_query)
        all_major_progs = query.all()
        db_objects, values = property_and_obj.values_sanitized(all_major_progs, core.Session.object_session(source_halos[0]))

    # now re-organize the values so that we have one per source
    values_per_source = {s:[] for s in range(len(source_halos))}
    objs_per_source = {s:[] for s in range(len(source_halos))}
    for source, value, obj in zip(sources, values, db_objects):
        values_per_source[source].append(value)
        objs_per_source[source].append(obj)

    results = []
    for s in range(len(source_halos)):
        vals = values_per_source[s]
        objs = objs_per_source[s]
        assert len(vals)==len(objs)
        if len(vals)==0:
            results.append(None)
        else:
            if property_criterion=='min':
                index = np.argmin(vals)
            elif property_criterion=='max':
                index = np.argmax(vals)
            else:
                assert False # should not reach this point
            results.append(objs[index])
    return results



find_progenitor.set_input_options(0, provide_proxy=True, assert_class = StoredProperty)
find_progenitor.set_input_options(1, provide_proxy=True, assert_class = FixedInput)