from __future__ import absolute_import
import numpy as np

from .. import BuiltinFunction, FixedInput, FixedNumericInput, StoredProperty, MultiCalculation, ReturnInputHalos
from ... import core
from ... import relation_finding
from ... import temporary_halolist as thl

def _find_progenitor_or_descendant(source_halos, property_proxy, property_criterion, strategy):
    if property_criterion != 'min' and property_criterion != 'max':
        raise ValueError("Property criterion must be either 'min' or 'max'")

    if len(source_halos) == 0:
        return []

    all_major_progs_strategy = strategy(source_halos)

    sources_for_each_value = all_major_progs_strategy.sources()

    property_and_obj = MultiCalculation(ReturnInputHalos(), property_proxy.name)

    with all_major_progs_strategy.temp_table() as tt:
        # the query has to explicitly include the source_id, otherwise the sqlalchemy dedup
        # process removes duplicates rows (e.g. if there are several null results, only the
        # first will be returned!)
        raw_query = thl.enumerated_halo_query(tt)
        query = property_and_obj.supplement_halo_query(raw_query)
        all_major_progs = [x[1] for x in query.all()]
        db_objects_for_each_value, values = property_and_obj.values(all_major_progs)

    assert len(values) == len(all_major_progs) == len(sources_for_each_value)

    # eliminate all None values
    mask = np.asarray(values)!=None
    sources_for_each_value = np.asarray(sources_for_each_value)[mask]
    db_objects_for_each_value = np.asarray(db_objects_for_each_value)[mask]
    values = np.asarray(values)[mask]

    # now re-organize the values so that we have one per source
    values_per_source = {s: [] for s in range(len(source_halos))}
    objs_per_source = {s: [] for s in range(len(source_halos))}
    for source, value, obj in zip(sources_for_each_value, values, db_objects_for_each_value):
        values_per_source[source].append(value)
        objs_per_source[source].append(obj)

    results = []
    for s in range(len(source_halos)):
        vals = values_per_source[s]
        objs = objs_per_source[s]
        assert len(vals) == len(objs)
        if len(vals) == 0:
            results.append(None)
        else:
            try:
                if property_criterion == 'min':
                    index = np.argmin(vals)
                elif property_criterion == 'max':
                    index = np.argmax(vals)
                else:
                    assert False  # should not reach this point
                results.append(objs[index])
            except ValueError:
                results.append(None) # argmin/argmax of empty sequence -> no candidate results

    return results

@BuiltinFunction.register
def find_progenitor(source_halos, property_proxy, property_criterion):
   return _find_progenitor_or_descendant(source_halos, property_proxy, property_criterion,
                                         relation_finding.multi_source.MultiSourceAllMajorProgenitorsStrategy)


@BuiltinFunction.register
def find_descendant(source_halos, property_proxy, property_criterion):
   return _find_progenitor_or_descendant(source_halos, property_proxy, property_criterion,
                                         relation_finding.multi_source.MultiSourceAllMajorDescendantsStrategy)


find_progenitor.set_input_options(0, provide_proxy=True, assert_class = StoredProperty)
find_progenitor.set_input_options(1, provide_proxy=True, assert_class = FixedInput)

find_descendant.set_input_options(0, provide_proxy=True, assert_class = StoredProperty)
find_descendant.set_input_options(1, provide_proxy=True, assert_class = FixedInput)