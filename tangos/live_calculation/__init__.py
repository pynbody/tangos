"""The live calculation system, used by Halo.calculate, Halo.calculate_for_descendants and TimeStep.calculate_all.

For more overview information, see live_calculation.md. """

from __future__ import absolute_import
import warnings

import numpy as np
from sqlalchemy.orm import contains_eager, aliased, defaultload

import tangos.core.dictionary
import tangos.core.halo
import tangos.core.halo_data
from tangos.core import extraction_patterns
from tangos.live_calculation.query_masking import QueryMask
from tangos.live_calculation.query_multivalue_folding import QueryMultivalueFolding
from tangos.util import consistent_collection
from .. import core
from .. import temporary_halolist as thl
from six.moves import range
from six.moves import zip

class UnknownValue(object):
    """A dummy object returned by Calculation.proxy_value when the value of the calculation cannot be predicted"""
    def __init__(self, name=None):
        self.name = name

class NoResultsError(ValueError):
    pass

class Calculation(object):
    """Represents a live-calculation to be performed on database objects.

    A typical Calculation is composed of many different Calculation subclasses arranged in some form of tree.
    For example A.B parses to Link(StoredProperty('A'), StoredProperty('B')) whereas
                A.B() parses to Link(StoredProperty('A'), LiveProperty('B'))
    and so on.

    It is normally easiest to instantiate Calculation objects using the parser in
    live_calculation.parser"""

    def __init__(self):
        self._extraction_pattern = extraction_patterns.HaloPropertyValueGetter()

    def __repr__(self):
        return "<Calculation description for %s>"%str(self)

    def __str__(self):
        raise NotImplementedError

    def set_extraction_pattern(self, extraction_pattern):
        self._extraction_pattern = extraction_pattern

    def retrieves(self):
        """Return the set of named halo properties that this calculation will access

        Redirections are indicated with a ".", e.g. if a calculation access the 'mass' property of a halo linked
        by 'BH', it will include 'BH.mass' in the set."""
        return set()

    def name(self):
        """Returns the name of this calculation, formatted such that parser.parse_property_name(name) generates a copy."""
        return None

    def _has_required_properties(self, halo):
        property_is_present = []
        for p_id in self._essential_dict_ids():
            this_property_ok = False
            for extraction_pattern in (extraction_patterns.HaloLinkGetter(),
                                       extraction_patterns.HaloPropertyGetter()):
                if not extraction_pattern.use_fixed_cache(halo):
                    this_property_ok = True
                elif extraction_pattern.cache_contains(halo, p_id):
                    this_property_ok = True
            property_is_present.append(this_property_ok)
        return all(property_is_present)

    def retrieves_dict_ids(self):
        """Returns the dictionary IDs of the named properties to be retrieved for each halo to
        allow this calculation to run"""
        self._generate_dict_ids_and_levels()
        return self._r_dict_ids_cached

    def _essential_dict_ids(self):
        """Returns the dictionary IDs of properties that must be present in a halo for the calculation to run.

        If any of the required IDs are not present, the result of the calculation is assumed to be None"""
        self._generate_dict_ids_and_levels()
        return self._r_dict_ids_essential_cached

    def n_join_levels(self):
        """Return the number of levels of HaloLinks that evaluating this property requires.

        For example, evaluating 'mass' has n_join_levels=0; 'BH.mass' has n_join_levels=1;
        'BH.other.mass' has n_join_levels=2"""
        self._generate_dict_ids_and_levels()
        return self._n_join_levels

    def _generate_dict_ids_and_levels(self):
        if not hasattr(self, "_r_dict_ids_cached"):
            session = core.Session()
            try:
                self._r_dict_ids_cached = set()
                self._r_dict_ids_essential_cached = set()
                retrieves = self.retrieves()
                try:
                    self._n_join_levels = max([r.count('.') for r in retrieves])+1
                except ValueError:
                    self._n_join_levels = 0
                for r in retrieves:
                    r_split = r.split(".")
                    for w in r_split:
                        w_dict_id = tangos.core.dictionary.get_dict_id(w, -1, session=session)
                        if w_dict_id!=-1:
                            self._r_dict_ids_cached.add(w_dict_id)
                    r_dict_id = tangos.core.dictionary.get_dict_id(r_split[0], -1, session=session)
                    if r_dict_id!=-1:
                        self._r_dict_ids_essential_cached.add(r_dict_id)
            finally:
                session.close()

    def values_and_description(self, halos):
        """Return the values of this calculation, as well as a PropertyCalculation object describing the
        properties of these values (if possible)"""
        raise NotImplementedError

    def values_sanitized_and_description(self, halos, load_into_session=None):
        """Return the 'sanitized' values of this calculation, as well as a PropertyCalculation object (if available).

        See values_sanitized for the definition of sanitized"""
        values, desc = self.values_and_description(halos)
        return self._sanitize_values(values, load_into_session), desc

    def values(self, halos):
        """Return the values of this calculation applied to halos.

        The size of the returned numpy object array is self.n_columns() x len(halos) """
        values, _ = self.values_and_description(halos)
        return values

    def value(self, halo):
        """Return the value of this calculation applied to the given halo"""
        return self.values([halo])[:,0]

    def value_sanitized(self, halo, load_into_session=None):
        """"Return the value of this calculation applied to the given halo, with conversion to numpy arrays where possible.

        See values_sanitized for information about the conversion."""
        return self.values_sanitized([halo],load_into_session)[:,0]

    def values_sanitized(self, halos, load_into_session=None):
        """Return the values of this calculation applied to halos, with conversion to numpy arrays where possible

        The return value is a self.n_columns()-length list, with each entry in the list being a numpy array of
        length len(halos). The dtype of the numpy array is chosen per-property to match the dtype result found
        when evaluating the first halo.

        Additionally, if load_into_session is provided, any returned database objects are re-queried against that
        session so that usable ORM objects are returned (rather than ORM objects attached to a dead session)"""
        unsanitized_values = self.values(halos)
        return self._sanitize_values(unsanitized_values, load_into_session, no_results_raises=False)



    def _sanitize_values(self, unsanitized_values, load_into_session=None, no_results_raises=True):
        """Convert the raw calculation result to a sanitized version. See values_sanitized."""
        keep_rows = np.all([[data is not None for data in row] for row in unsanitized_values], axis=0)
        # The obvious way of doing this:
        #   keep_rows = np.all(np.not_equal(out,None), axis=0)
        # generates a scary FutureWarning that it will stop working in future. This is because
        # it can end up doing a comparison of an array to None (effectively data!=None),
        # which will not be the same as "is not None" in future versions of numpy.

        output_values = unsanitized_values[:, keep_rows]
        for x in output_values:
            if len(x)==0 and no_results_raises:
                raise NoResultsError("Calculation %s returned no results" % self)

        if load_into_session is not None:
            self._refetch_halos_from_original_session(output_values, load_into_session)

        return [self._make_numpy_array(x) for x in output_values]

    def _refetch_halos_from_original_session(self, unsanitized_values, session):
        output_halos = []
        unsanitized_values_ravel = unsanitized_values.flat
        for item in unsanitized_values_ravel:
            if isinstance(item, core.Halo):
                output_halos.append(item.id)
        if len(output_halos) > 0:
            with thl.temporary_halolist_table(session, output_halos) as tab:
                results = thl.halo_query(tab).all()

            # reinstate duplicates
            results = self._add_entries_for_duplicates(results, output_halos)

            for i in range(len(unsanitized_values_ravel)):
                if isinstance(unsanitized_values_ravel[i], core.Halo):
                    refetched_halo = results[output_halos.index(unsanitized_values_ravel[i].id)]
                    unsanitized_values_ravel[i] = refetched_halo

    @staticmethod
    def _make_numpy_array(x):
        if len(x)==0:
            return x
        if isinstance(x[0], np.ndarray):
            try:
                return np.array(list(x), dtype=x[0].dtype)
            except ValueError:
                return x
        else:
            return np.array(x, dtype=type(x[0]))

    def n_columns(self):
        """Return the number of separate properties returned for each halo.

        The return from values_sanitized has shape n_columns() x n_halos"""
        return 1

    def supplement_halo_query(self, halo_query):
        """Return a sqlalchemy query with a supplemental join to allow this calculation to run efficiently"""
        name_targets = self.retrieves_dict_ids()
        halo_alias = tangos.core.halo.Halo
        augmented_query = halo_query
        for i in range(self.n_join_levels()):
            halo_property_alias = aliased(tangos.core.halo_data.HaloProperty)
            halo_link_alias = aliased(tangos.core.halo_data.HaloLink)

            path_to_properties = [tangos.core.halo.Halo.all_links, tangos.core.halo_data.HaloLink.halo_to] * i + [
                tangos.core.halo.Halo.all_properties]
            path_to_links = [tangos.core.halo.Halo.all_links, tangos.core.halo_data.HaloLink.halo_to] * i + [
                tangos.core.halo.Halo.all_links]


            if len(name_targets)>0:
                property_name_condition = halo_property_alias.name_id.in_(name_targets)
                link_name_condition = (halo_link_alias.relation_id.in_(name_targets))
            else:
                # We know we're joining to a null list of properties; however simply setting these conditions
                # to False results in an apparently efficient SQL query (boils down to 0==1) which actually
                # takes a very long time to execute if the link or propery tables are large. Thus, compare
                # to an impossible value instead.
                property_name_condition = halo_property_alias.name_id==-1
                link_name_condition = halo_link_alias.relation_id==-1


            augmented_query =augmented_query.outerjoin(halo_property_alias,
                                                  (halo_alias.id==halo_property_alias.halo_id)
                                                  & property_name_condition).\
                                        outerjoin(halo_link_alias,
                                                  (halo_alias.id==halo_link_alias.halo_from_id)
                                                  & link_name_condition).\
                                        options(contains_eager(*path_to_properties, alias=halo_property_alias),
                                                contains_eager(*path_to_links, alias=halo_link_alias),
                                                defaultload(*path_to_properties).undefer_group("data"))

            if i<self.n_join_levels()-1:
                next_level_halo_alias = aliased(tangos.core.halo.Halo)
                path_to_new_halo = path_to_links + [tangos.core.halo_data.HaloLink.halo_to]
                augmented_query = augmented_query.outerjoin(next_level_halo_alias,
                                                            (halo_link_alias.halo_to_id==next_level_halo_alias.id)).\
                                        options(contains_eager(*path_to_new_halo, alias=next_level_halo_alias))

                halo_alias = next_level_halo_alias

        return augmented_query

    def proxy_value(self):
        """Return a placeholder value for this calculation"""
        raise NotImplementedError

    @staticmethod
    def _add_entries_for_duplicates(target_objs, target_ids):
        """Given a list of target_objs and their target_ids, the latter of which may contain duplicates, return the full list of objects

        For example, if target_objs = [obj1, obj3, obj6] where obj1.id=1, obj3.id=3, obj6.id=6, and target_ids = [1, 3, 6, 3, 6, 6],
        the returned list will be [obj1, obj3, obj6, obj3, obj6, obj6]."""
        if len(target_objs) == len(target_ids):
            return target_objs
        target_obj_from_id = {t.id: t for t in target_objs}
        return [target_obj_from_id[t_id] for t_id in target_ids]


class MultiCalculation(Calculation):
    """Represents a single calculation that returns the results from multiple sub-calculations."""
    def __init__(self, *calculations):
        super(MultiCalculation, self).__init__()
        self.calculations = [c if isinstance(c, Calculation) else parser.parse_property_name(c) for c in calculations]

    def retrieves(self):
        x = set()
        for c in self.calculations:
            x.update(c.retrieves())
        return x

    def __str__(self):
        return "("+(", ".join(str(x) for x in self.calculations))+")"

    def values_and_description(self, halos):
        results = np.empty((self.n_columns(),len(halos)), dtype=object)
        c_column = 0
        halos = np.asarray(halos, dtype=object)
        mask = QueryMask()
        mask.mark_nones_as_masked(halos)
        for c in self.calculations:
            values, description = c.values_and_description(mask.mask(halos))
            results[c_column:c_column+c.n_columns()] = mask.unmask(values)
            # TODO: in principle this masking should _not_ occur unless we know the user has called values_sanitized
            # - other calls should not cross-contaminate columns in this way
            mask.mark_nones_as_masked(values)
            c_column+=c.n_columns()

        # TODO - problem: there is no good description of multiple properties
        return results, description

    def n_columns(self):
        return sum(c.n_columns() for c in self.calculations)


class FixedInput(Calculation):
    """Represents a calculation that returns a fixed value"""
    def __init__(self, *tokens):
        super(FixedInput, self).__init__()
        self.value = str(tokens[0])

    def __str__(self):
        return '"'+str(self.value)+'"'

    def values(self, halos):
        return np.array([[self.value]*len(halos)],dtype=object)

    def values_and_description(self, halos):
        return self.values(halos), self.value

    def proxy_value(self):
        return self.value

class FixedNumericInput(FixedInput):
    """Represents a calculation that returns a fixed numerical value"""
    def __init__(self, *tokens):
        super(FixedNumericInput, self).__init__()

    @staticmethod
    def _process_numerical_value(t):
        if "." in t or "e" in t or "E" in t:
            return float(t)
        else:
            return int(t)

    def __init__(self, *tokens):
        self.value = self._process_numerical_value(tokens[0])

    def __str__(self):
        return str(self.value)

class LiveProperty(Calculation):
    """Represents a calculation that is achieved by executing the live_calculate method of a Properties instance"""
    def __new__(cls, *tokens):
        if BuiltinFunction.has_function(str(tokens[0])):
            return object.__new__(BuiltinFunction)
        else:
            return object.__new__(LiveProperty)


    def __init__(self, *tokens):
        super(LiveProperty, self).__init__()
        self._name = str(tokens[0])
        self._inputs = list(tokens[1:])

    def __str__(self):
        return self._name + "(" + (",".join(str(x) for x in self._inputs)) + ")"

    def name(self):
        return self._name

    def retrieves(self):
        result = self._calculation_retrieves()
        result = result.union(self._parameters_retrieve())
        return result

    def _parameters_retrieve(self):
        result = set()
        for i in self._inputs:
            result = result.union(i.retrieves())
        return result

    def _calculation_retrieves(self):
        from .. import properties
        result = set()
        proxy_values = [i.proxy_value() for i in self._inputs]
        providing_instance = properties.providing_class(self._name)(None, *proxy_values)
        result = result.union(providing_instance.requires_property())
        return result

    def values_and_description(self, halos):
        if len(halos)==0:
            return np.array([[]]), None
        input_values = []
        input_descriptions = []
        for input in range(len(self._inputs)):
            iv, id = self._input_value_and_description(input, halos)
            input_values.append(iv)
            input_descriptions.append(id)

        calculator, results = self._evaluate_function(halos, input_descriptions, input_values)

        return results, calculator

    def _input_value_and_description(self, input_id, halos):
        input = self._inputs[input_id]
        val, desc = input.values_and_description(halos)
        if len(val) != 1:
            raise ValueError("Functions cannot receive more than one value per input")
        return val[0], desc

    def _evaluate_function(self, halos, input_descriptions, input_values):
        from .. import properties
        sim = consistent_collection.consistent_simulation_from_halos(halos)
        results = []
        calculator = properties.providing_class(self.name())(sim, *input_descriptions)
        for inputs in zip(halos, *input_values):
            if self._has_required_properties(inputs[0]) and all([x is not None for x in inputs]):
                results.append(calculator.live_calculate_named(self.name(), *inputs))
            else:
                results.append(None)
        return calculator, self._as_1xn_array(results)

    @classmethod
    def _as_1xn_array(cls, results):
        results_array = np.empty((1, len(results)), dtype=object)
        results_array[0, :] = results
        return results_array

    def proxy_value(self):
        return UnknownValue()

class BuiltinFunction(LiveProperty):
    """Represents a calculation that is achieved by executing a python function. See the builtin_functions module."""

    __registered_functions = {}
    __default_args = {'provide_proxy': False, 'assert_class': None}

    @classmethod
    def all(cls):
        return list(cls.__registered_functions.keys())

    @classmethod
    def register(cls, func):
        """Register a built-in function for the live calculation (LC) system.

        The LC-exposed version of the function inherits the same name as the python function.

        The python function takes a list of halos and then further lists for each argument given to the
        live-calculation. By default, these arugments are lists of the values for each halo. However
        this behaviour can be customised by calling set_input_options.

        For examples, see the builtin_functions module.
        """
        cls.__registered_functions[func.__name__] = {'function': func}
        func.set_input_options = lambda arg_id, **kwargs: cls.set_input_options(func, arg_id, **kwargs)
        func.set_initialisation = lambda init_fn: cls.set_initialisation(func, init_fn)
        return func

    @classmethod
    def set_input_options(cls, func, argument_id, **kwargs):
        """For the registered function, set input options.

        :param argument_id: The 0-based ID of the input argument (as seen in the LC langauge,
                            i.e. discounting the halos input)
        :param provide_proxy: If True, the value of the input is not evaluated and only a proxy is passed
        :param assert_class: If not None, the input must be an instance of the provided class
        """
        for k in kwargs.keys():
            if k not in list(cls.__default_args.keys()):
                raise ValueError("Unknown input option %s"%k)
        cls.__registered_functions[func.__name__][argument_id] = kwargs

    @classmethod
    def set_initialisation(cls, func, initialisation_func):
        """For the registered function, add an initialisation function that receives the input objects"""
        cls.__registered_functions[func.__name__]['initialisation'] = initialisation_func

    @classmethod
    def has_function(cls, func_name):
        return func_name in list(cls.__registered_functions.keys())

    def __init__(self, *tokens):
        super(BuiltinFunction, self).__init__(*tokens)
        self._func = self.__registered_functions[self._name]['function']
        self._info = self.__registered_functions[self._name]
        for i in range(len(self._inputs)):
            assert_class = self._get_input_option(i, 'assert_class')
            if assert_class is not None and not isinstance(self._inputs[i], assert_class):
                raise ValueError("Input %d to function %s must be of type %s (received %s)"%\
                                  (i,self._name,assert_class,type(self._inputs[i])))
        if 'initialisation' in self._info:
            f = self._info['initialisation'](*self._inputs)
            if f is not None:
                self._func = f

    def _get_input_option(self, input_id, option):
        default = self.__default_args[option]
        if input_id in self._info:
            return self._info[input_id].get(option, default)
        else:
            return default

    def _input_value_and_description(self, input_id, halos):
        if self._get_input_option(input_id, 'provide_proxy'):
            return self._inputs[input_id].proxy_value(), None
        else:
            return super(BuiltinFunction, self)._input_value_and_description(input_id, halos)

    def _calculation_retrieves(self):
        return set()

    def _evaluate_function(self, halos, input_descriptions, input_values):
        if len(input_descriptions)>0:
            inherited_description = input_descriptions[0]
        else:
            inherited_description = None
        return inherited_description, self._as_1xn_array(self._func(halos, *input_values))



class Link(Calculation):
    """Represents a calculation to be made on a halo linked to the input in some way"""
    def __init__(self, *tokens):
        super(Link, self).__init__()
        self.locator = tokens[0]
        self.property = tokens[1]
        self._multi_selection_basis = 'first'
        self._multi_selection_column = None
        self._constraints_columns = []
        self._expect_multivalues = False
        if not isinstance(self.locator, Calculation):
            self.locator = parser.parse_property_name(self.locator)

        if isinstance(self.locator, StoredProperty):
            self.locator.set_extraction_pattern(extraction_patterns.HaloLinkTargetGetter())
            self.locator.set_multivalued() # we want to at least know if there are multiple possible links to follow
            self._expect_multivalues = True

        if not isinstance(self.property, Calculation):
            self.property = parser.parse_property_name(self.property)


    def __str__(self):
        return str(self.locator)+"."+str(self.property)

    def name(self):
        return self.property.name()

    def proxy_value(self):
        """Return a placeholder value for this calculation"""
        return UnknownValue()

    def retrieves(self):
        # the property retrieval will not be on the set of halos known to higher levels,
        # so only the locator retrieval needs to be reported upwards
        return self.locator.retrieves()

    def set_multi_selection_basis(self, basis='first', column=0):
        """Set how a single link will be selected if multiple links from a single halo are present

        Possible modes:
          * 'first' (default); take the halolink with the lowest id
          * 'max'; take the halolink where the result of the specified column is highest
          * 'min'; take the halolink where the result of the specified column is lowest
        """
        basis = basis.lower()
        if basis not in ['first', 'min', 'max']:
            raise RuntimeError("Unknown basis for selecting a link. Supported modes are first, min or max")

        self._multi_selection_basis = basis
        self._multi_selection_column = column

    def set_constraints_columns(self, columns=[]):
        self._constraints_columns = columns

    def n_columns(self):
        return self.property.n_columns()

    def values_and_description(self, halos):
        if self.locator.n_columns()!=1:
            raise ValueError("Cannot use property %r, which returns more than one column, as a halo locator"%(str(self.locator)))
        target_halos = self._get_target_halos(halos)
        results = np.empty((self.n_columns(),len(halos)),dtype=object)

        mask = QueryMask()
        mask.mark_nones_as_masked(target_halos)
        target_halo_masked = mask.mask(target_halos)

        if self._expect_multivalues:
            if self._multi_selection_basis=='first':
                for i in range(len(target_halo_masked)):
                    if len(target_halo_masked[i])>1:
                        warnings.warn("More than one relation for target %r has been found. Picking the first."%str(self.locator), RuntimeWarning)
                    target_halo_masked[i] = target_halo_masked[i][0]
            else:
                multivalue_folding = QueryMultivalueFolding(self._multi_selection_basis,
                                                            self._multi_selection_column, self._constraints_columns)
                target_halo_masked = multivalue_folding.unfold(target_halo_masked)
        values, description = self._get_values_and_description_from_halo_id_list([x.id for x in target_halo_masked])

        if self._expect_multivalues and self._multi_selection_basis!='first':
            values = multivalue_folding.refold(values)

        results[:] = mask.unmask(values)

        return results, description

    def _get_values_and_description_from_halo_id_list(self, target_halo_ids):

        new_session = core.Session()
        # need a new session for the subqueries, because we might have cached copies of objects where
        # a different set of properties has been loaded into all_properties

        try:
            with thl.temporary_halolist_table(new_session, target_halo_ids) as tab:
                target_halos_supplemented = self.property.supplement_halo_query(thl.halo_query(tab)).all()

                # sqlalchemy's deduplication means we are now missing any halos that appear more than once in
                # target_halo_ids. But we actually want the duplication.
                target_halos_supplemented_with_duplicates = \
                    self._add_entries_for_duplicates(target_halos_supplemented, target_halo_ids)

                values, description = self.property.values_and_description(target_halos_supplemented_with_duplicates)
        finally:
            new_session.connection().close()
        return values, description

    def _get_target_halos(self, source_halos):
        target_halos = self.locator.values(source_halos)[0]
        return target_halos





class ReturnInputHalos(Calculation):
    """Return the input halos (used internally by builtin_functions.link)"""
    def values(self, halos):
        return np.asarray(halos)

    def values_and_description(self, halos):
        return np.asarray(halos), None

    def __str__(self):
        return "_return_input()"

class StoredProperty(Calculation):
    """Represents a named property with value stored in the database"""

    def __init__(self, *tokens):
        super(StoredProperty,self).__init__()
        self._name = tokens[0]
        self._multivalued = False

    def __str__(self):
        return self._name

    def name(self):
        return self._name

    def retrieves(self):
        return {self._name}

    def set_multivalued(self, multivalued=True):
        """Set multivalued flag; if True, returns all matches for the property, in a list.

        Otherwise (default) returns the first match."""
        self._multivalued=multivalued

    def values(self, halos):
        self._name_id = tangos.core.dictionary.get_dict_id(self._name)
        ret = np.empty((1,len(halos)),dtype=object)
        for i, h in enumerate(halos):
            if self._extraction_pattern.cache_contains(h, self._name_id):
                if self._multivalued:
                    ret[0,i]=self._extraction_pattern.get_from_cache(h, self._name_id)
                else:
                    ret[0, i] = self._extraction_pattern.get_from_cache(h, self._name_id)[0]
        return ret

    def values_and_description(self, halos):
        from .. import properties
        values = self.values(halos)
        if len(halos)==0:
            # cannot build a meaningful property description as we don't have any halos, therefore don't know
            # anything about the simulation or which property calculations are relevant for it
            return values, None

        sim = consistent_collection.consistent_simulation_from_halos(halos)
        description_class = properties.providing_class(self._name, sim.output_handler_class, silent_fail=True)
        description = None
        if description_class is not None:
            try:
                description = description_class(sim)
            except Exception as e:
                warnings.warn("%r occurred while trying to produce a property description from class %r"%
                              (e,description_class),
                              RuntimeWarning)
        return values, description

    def proxy_value(self):
        """Return a placeholder value for this calculation"""
        return UnknownValue(self._name)





from . import parser
from . import builtin_functions
