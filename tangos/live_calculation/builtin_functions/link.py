from . import BuiltinFunction, StoredProperty
from .. import Link, MultiCalculation, ReturnInputHalos, FixedInput
from ...core import extraction_patterns
import numpy as np

@BuiltinFunction.register
def has_link(source_halos, link):
    return np.not_equal(link,None)

@has_link.set_initialisation
def link_exists_initialisation(input):
    input.set_extraction_pattern(
        extraction_patterns.HaloLinkTargetGetter())


@BuiltinFunction.register
def link(*args):
    """link(link_name) - get the first named link
    link(link_name, property_name, property_criterion) - get the named link where link_name.property_name is either maximum or minimum """
    raise RuntimeError("Internal error in link(). The customised function from the link_initialisation routine should be called.")

link.set_input_options(2, assert_class=FixedInput)
for i in range(20): # TODO: implement a way to set provide_proxy on all possible arguments, to remove arbitrary upper limit
    link.set_input_options(i, provide_proxy=True)

@link.set_initialisation
def link_initialisation(link_getter, property_getter=None, basis='max',*constraints):
    if isinstance(basis, FixedInput):
        basis = basis.proxy_value()
    if property_getter:
        if constraints:
            internal_getter = Link(link_getter, MultiCalculation(ReturnInputHalos(), property_getter,*constraints))
            internal_getter.set_constraints_columns(np.arange(len(constraints))+2)
        else:
            internal_getter = Link(link_getter, MultiCalculation(ReturnInputHalos(), property_getter))
        internal_getter.set_multi_selection_basis(basis, 1)
    else:
        internal_getter = Link(link_getter, ReturnInputHalos())
        internal_getter.set_multi_selection_basis('first')

    def custom_link(source_halos, *args):
        return internal_getter.values(source_halos)[0]

    return custom_link
