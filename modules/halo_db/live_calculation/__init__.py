import numpy as np
import warnings
import pyparsing as pp
import properties
from .. import consistent_collection
from .. import core
from .. import temporary_halolist as thl
from sqlalchemy.orm import contains_eager, aliased


class Calculation(object):
    def __repr__(self):
        return "<Calculation description for %s>"%str(self)

    def __str__(self):
        raise NotImplementedError

    def retrieves(self):
        """Return the set of named halo properties that this calculation will access

        Redirections are indicated with a ".", e.g. if a calculation access the 'mass' property of a halo linked
        by 'BH', it will include 'BH.mass' in the set."""
        return set()

    def name(self):
        """The name of this calculation, such that parse_property_name(name) generates a copy."""
        return None

    def retrieves_dict_ids(self):
        """Return the dictionary IDs of the named properties to be retrieved for each halo to
        allow this calculation to run"""
        self._generate_dict_ids_and_levels()
        return self._r_dict_ids_cached

    def _essential_dict_ids(self):
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
                    self._r_dict_ids_cached.add(core.get_dict_id(w))
                self._r_dict_ids_essential_cached.add(core.get_dict_id(r_split[0]))

    def _has_required_properties(self, halo):
        prop_ids = [x.name_id for x in halo.all_properties]
        link_ids = [x.relation_id for x in halo.all_links]
        return all([p_id in prop_ids or p_id in link_ids for p_id in self._essential_dict_ids()])

    def values_and_description(self, halos):
        """Return the values of this calculation, as well as a HaloProperties object describing the
        properties of these values (if possible)"""
        raise NotImplementedError

    def values(self, halos):
        """Return the values of this calculation applied to halos.

        The size of the returned numpy object array is self.n_columns() x len(halos) """
        values, _ = self.values_and_description(halos)
        return values

    def value(self, halo):
        """Return the value of this calculation applied to the given halo"""

        return self.values([halo])[:,0]

    def value_sanitized(self, halo):
        """"Return the value of this calculation applied to the given halo, with conversion to numpy arrays where possible.

        See values_sanitized for information about the conversion."""
        return self.values_sanitized([halo])[:,0]

    def values_sanitized(self, halos):
        """Return the values of this calculation applied to halos, with conversion to numpy arrays where possible

        The return value is a self.n_columns()-length list, with each entry in the list being a numpy array of
        length len(halos). The dtype of the numpy array is chosen per-property to match the dtype result found
        when evaluating the first halo."""
        out = self.values(halos)

        keep_rows = np.all(np.not_equal(out,None), axis=0)
        out = out[:,keep_rows]

        return [self._make_final_array(x) for x in out]

    def _make_final_array(self, x):
        if len(x)==0:
            raise ValueError, "Calculation returned no results"
        if isinstance(x[0], np.ndarray):
            try:
                return np.array(list(x), dtype=type(x[0][0]))
            except ValueError:
                return x
        else:
            return np.array(x, dtype=type(x[0]))

    def n_columns(self):
        return 1

    def supplement_halo_query(self, halo_query):
        """Return a sqlalchemy query with a supplemental join to allow this calculation to run efficiently"""
        name_targets = self.retrieves_dict_ids()
        halo_alias = core.Halo
        augmented_query = halo_query
        for i in xrange(self.n_join_levels()):
            halo_property_alias = aliased(core.HaloProperty)
            halo_link_alias = aliased(core.HaloLink)

            path_to_properties = [core.Halo.all_links, core.HaloLink.halo_to]*i + [core.Halo.all_properties]
            path_to_links = [core.Halo.all_links, core.HaloLink.halo_to]*i + [core.Halo.all_links]


            augmented_query =augmented_query.outerjoin(halo_property_alias,
                                                  (halo_alias.id==halo_property_alias.halo_id)
                                                  & (halo_property_alias.name_id.in_(name_targets))).\
                                        outerjoin(halo_link_alias,
                                                  (halo_alias.id==halo_link_alias.halo_from_id)
                                                  & (halo_link_alias.relation_id.in_(name_targets))).\
                                        options(contains_eager(*path_to_properties, alias=halo_property_alias),
                                                contains_eager(*path_to_links, alias=halo_link_alias))

            if i<self.n_join_levels()-1:
                next_level_halo_alias = aliased(core.Halo)
                path_to_new_halo = path_to_links + [core.HaloLink.halo_to]
                augmented_query = augmented_query.outerjoin(next_level_halo_alias,
                                                            (halo_link_alias.halo_to_id==next_level_halo_alias.id)).\
                                        options(contains_eager(*path_to_new_halo, alias=next_level_halo_alias))

                halo_alias = next_level_halo_alias

        return augmented_query

    def proxy_value(self):
        """Return a placeholder value for this calculation"""
        raise NotImplementedError


class MultiCalculation(Calculation):
    def __init__(self, *calculations):
        self.calculations = [c if isinstance(c, Calculation) else parse_property_name(c) for c in calculations]

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
        for c in self.calculations:
            results[c_column:c_column+c.n_columns()] = c.values(halos)
            c_column+=c.n_columns()

        # problem: there is no good description of multiple properties
        return results, None

    def n_columns(self):
        return sum(c.n_columns() for c in self.calculations)


class FixedInput(Calculation):
    def __init__(self, *tokens):
        self.value = str(tokens[0])

    def __str__(self):
        return '"'+str(self.value)+'"'

    def values(self, halos):
        return np.array([[self.value]*len(halos)],dtype=object)

    def values_and_description(self, halos):
        return self.values(halos), self.value

    def proxy_value(self):
        return self.value

class UnknownValue(object):
    pass

class FixedNumericInputDescription(FixedInput):
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
    def __init__(self, *tokens):
        self._name = str(tokens[0])
        self._inputs = list(tokens[1:])

    def __str__(self):
        return self._name + "(" + (",".join(str(x) for x in self._inputs)) + ")"

    def name(self):
        return self._name

    def retrieves(self):
        result = set()
        proxy_values = [i.proxy_value() for i in self._inputs]
        providing_instance = properties.providing_class(self._name)(None, *proxy_values)
        result = result.union(providing_instance.requires_property())
        for i in self._inputs:
            result=result.union(i.retrieves())
        return result


    def values_and_description(self, halos):
        input_values = []
        input_descriptions = []
        for input in self._inputs:
            val, desc = input.values_and_description(halos)
            if len(val)!=1:
                raise ValueError, "Functions cannot receive more than one value per input"
            input_values.append(val[0])
            input_descriptions.append(desc)

        sim = consistent_collection.consistent_simulation_from_halos(halos)

        results = []
        calculator = properties.providing_class(self.name())(sim, *input_descriptions)

        for inputs in zip(halos, *input_values):
            if self._has_required_properties(inputs[0]):
                results.append(calculator.live_calculate_named(self.name(),*inputs))
            else:
                results.append(None)

        return np.array([results],dtype=object), calculator

    def proxy_value(self):
        return UnknownValue()


class Link(Calculation):
    def __init__(self, *tokens):
        self.locator = tokens[0]
        self.property = tokens[1]
        if not isinstance(self.locator, Calculation):
            self.locator = parse_property_name(self.locator)
        if not isinstance(self.property, Calculation):
            self.property = parse_property_name(self.property)

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

    def n_columns(self):
        return self.property.n_columns()

    def values_and_description(self, halos):
        if self.locator.n_columns()!=1:
            raise ValueError, "Cannot use property %r, which returns more than one column, as a halo locator"%(str(self.locator))

        target_halos = self.locator.values(halos)[0]

        results = np.empty((self.n_columns(),len(halos)),dtype=object)

        results_target = np.where(np.not_equal(target_halos, None))
        target_halo_ids_weeded = target_halos[results_target]


        for i in xrange(len(target_halo_ids_weeded)):
            if isinstance(target_halo_ids_weeded[i], list):
                warnings.warn("More than one relation for target %r has been found. Picking the first."%str(self.locator))
                target_halo_ids_weeded[i] = target_halo_ids_weeded[i][0].id
            else:
                target_halo_ids_weeded[i] = target_halo_ids_weeded[i].id

        # need a new session for the subqueries, because we might have cached copies of objects where
        # a different set of properties has been loaded into all_properties
        new_session = core.Session()

        with thl.temporary_halolist_table(new_session, target_halo_ids_weeded) as tab:
            target_halos_supplemented = self.property.supplement_halo_query(thl.halo_query(tab)).all()

            # sqlalchemy's deduplication means we are now missing any halos that appear more than once in
            # target_halos_ids_weeded. But we actually want the duplication.
            target_halos_supplemented_with_duplicates = \
                self._add_entries_for_duplicates(target_halos_supplemented, target_halo_ids_weeded)

            values, description = self.property.values_and_description(target_halos_supplemented_with_duplicates)

        results[:,results_target[0]] = values

        return results, description

    @staticmethod
    def _add_entries_for_duplicates(target_objs, target_ids):
        if len(target_objs)==len(target_ids):
            return target_objs
        target_obj_ids = [t.id for  t in target_objs]
        return [target_objs[target_obj_ids.index(t_id)] for t_id in target_ids]





class StoredProperty(Calculation):
    def __init__(self, *tokens):
        self._name = tokens[0]

    def __str__(self):
        return self._name

    def name(self):
        return self._name

    def retrieves(self):
        return {self._name}

    def values(self, halos):
        ret = np.empty((1,len(halos)),dtype=object)
        for i, h in enumerate(halos):
            if self._has_required_properties(h):
                ret[0,i]=h[self._name]
        return ret

    def values_and_description(self, halos):
        values = self.values(halos)
        sim = consistent_collection.consistent_simulation_from_halos(halos)
        description_class = properties.providing_class(self._name, silent_fail=True)
        description = None if description_class is None else description_class(sim)
        return values, description

    def proxy_value(self):
        """Return a placeholder value for this calculation"""
        return UnknownValue()

class StoredPropertyRawValue(StoredProperty):
    def name(self):
        return "raw("+self._name+")"

    def values(self, halos):
        ret = np.empty((1,len(halos)),dtype=object)
        for i, h in enumerate(halos):
            if self._has_required_properties(h):
                ret[0,i]=h.get_data(self._name,True)
        return ret



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

    multiple_properties = pp.Forward().setParseAction(pack_args(MultiCalculation))

    value_or_property_name = raw_stored_property | string_value | numerical_value \
                             | redirection | live_calculation_property \
                             | stored_property | multiple_properties

    multiple_properties << (pp.Literal("(").suppress()+value_or_property_name+pp.ZeroOrMore(pp.Literal(",").suppress()+value_or_property_name) +pp.Literal(")").suppress())

    redirection << (live_calculation_property | stored_property ) + pp.Literal(".").suppress() + (redirection | live_calculation_property | stored_property | multiple_properties)

    parameters = pp.Literal("(").suppress()+pp.Optional(value_or_property_name+pp.ZeroOrMore(pp.Literal(",").suppress()+value_or_property_name))+pp.Literal(")").suppress()
    live_calculation_property << property_name+parameters
    property_complete = pp.stringStart()+value_or_property_name+pp.stringEnd()

    return property_complete.parseString(name)[0]


def parse_property_names(*names):
    return MultiCalculation(*[parse_property_name(n) for n in names])


