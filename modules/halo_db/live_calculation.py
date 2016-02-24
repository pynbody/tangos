import numpy as np
import re
import warnings
import hopper
import pyparsing as pp
import properties
from . import core
from . import temporary_halolist as thl
from sqlalchemy.orm import contains_eager, aliased


class CalculationDescription(object):
    def __repr__(self):
        return "<Calculation description for %s>"%str(self)

    def retrieves(self):
        return set()

    def retrieves_dict_ids(self):
        self._generate_dict_ids_and_levels()
        return self._r_dict_ids_cached

    def _essential_dict_ids(self):
        self._generate_dict_ids_and_levels()
        return self._r_dict_ids_essential_cached

    def n_join_levels(self):
        self._generate_dict_ids_and_levels()
        return self._n_join_levels

    def _generate_dict_ids_and_levels(self):
        if not hasattr(self, "_r_dict_ids_cached"):
            self._r_dict_ids_cached = []
            self._r_dict_ids_essential_cached = []
            retrieves = self.retrieves()
            self._n_join_levels = max([r.count('.') for r in retrieves])+1
            for r in retrieves:
                r_split = r.split(".")
                for w in r_split:
                    self._r_dict_ids_cached.append(core.get_dict_id(w))
                self._r_dict_ids_essential_cached.append(core.get_dict_id(r_split[0]))

    def _has_required_properties(self, halo):
        prop_ids = [x.name_id for x in halo.all_properties]
        link_ids = [x.relation_id for x in halo.all_links]
        return all([p_id in prop_ids or p_id in link_ids for p_id in self._essential_dict_ids()])

    def values(self, halos):
        raise NotImplementedError

    def n_columns(self):
        return 1

    def supplement_halo_query(self, halo_query):
        name_targets = self.retrieves_dict_ids()
        halo_alias = core.Halo
        augmented_query = halo_query
        print "for query",str(self)
        print "retrieves=",self.retrieves()
        print "name_targets=",name_targets
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


class MultiCalculationDescription(CalculationDescription):
    def __init__(self, *calculations):
        self.calculations = calculations

    def retrieves(self):
        x = set()
        for c in self.calculations:
            x.update(c.retrieves())
        return x

    def __str__(self):
        return "("+(", ".join(str(x) for x in self.calculations))+")"

    def values(self, halos):
        results = np.empty((self.n_columns(),len(halos)), dtype=object)
        c_column = 0
        for c in self.calculations:
            results[c_column:c_column+c.n_columns()] = c.values(halos)
            c_column+=c.n_columns()
        return results

    def n_columns(self):
        return sum(c.n_columns() for c in self.calculations)


class FixedInputDescription(CalculationDescription):
    def __init__(self, tokens):
        self.value = str(tokens[0])

    def __str__(self):
        return '"'+str(self.value)+'"'

    def values(self, halos):
        return np.array([[self.value]*len(halos)],dtype=object)

    def proxy_value(self):
        """Return a placeholder value for this calculation"""
        return self.value

class UnknownValue(object):
    pass

class UnknownHalo(object):
    pass

class FixedNumericInputDescription(FixedInputDescription):
    @staticmethod
    def _process_numerical_value(t):
        if "." in t or "e" in t or "E" in t:
            return float(t)
        else:
            return int(t)

    def __init__(self, tokens):
        #print "FixedInputDescription.__init__",tokens
        self.value = self._process_numerical_value(tokens[0])

    def __str__(self):
        return str(self.value)

class LivePropertyDescription(CalculationDescription):
    def __init__(self, tokens):
        #print "LivePropertyDescription.__init__",tokens
        self.name = str(tokens[0])
        self.inputs = list(tokens[1:])

    def __str__(self):
        return self.name+"("+(",".join(str(x) for x in self.inputs))+")"

    def retrieves(self):
        result = set()
        proxy_values = [i.proxy_value() for i in self.inputs]
        providing_instance = properties.providing_class(self.name)(*proxy_values)
        result = result.union(providing_instance.requires_property())
        for i in self.inputs:
            result=result.union(i.retrieves())
        return result



    def values(self, halos):
        input_values = [i.values(halos)[0] for i in self.inputs]
        results = []
        for inputs in zip(halos, *input_values):
            if self._has_required_properties(inputs[0]):
                results.append(properties.live_calculate(self.name, *inputs))
            else:
                results.append(None)

        return np.array([results],dtype=object)

    def proxy_value(self):
        """Return a placeholder value for this calculation"""
        return UnknownValue()


class LinkDescription(CalculationDescription):
    def __init__(self, tokens):
        self.locator = tokens[0]
        self.property = tokens[1]

    def __str__(self):
        return str(self.locator)+"."+str(self.property)

    def proxy_value(self):
        """Return a placeholder value for this calculation"""
        return UnknownValue()

    def retrieves(self):
        # the property retrieval will not be on the set of halos known to higher levels,
        # so only the locator retrieval needs to be reported upwards
        return self.locator.retrieves()

    def n_columns(self):
        return self.property.n_columns()

    def values(self, halos):
        target_halos = self.locator.values(halos)

        results = np.empty_like(target_halos,dtype=object)

        results_target = np.where(np.not_equal(target_halos, None))
        target_halos_weeded = target_halos[results_target]

        for i in xrange(len(target_halos_weeded)):
            if isinstance(target_halos_weeded[i], list):
                warnings.warn("More than one relation for target %r has been found. Picking the first."%str(self.locator))
                target_halos_weeded[i] = target_halos_weeded[i][0].id
            else:
                target_halos_weeded[i] = target_halos_weeded[i].id

        # need a new session for the subqueries, because we might have cached copies of objects where
        # a different set of properties has been loaded into all_properties
        new_session = core.Session()

        with thl.temporary_halolist_table(new_session, target_halos_weeded) as tab:
            target_halos_supplemented = self.property.supplement_halo_query(thl.halo_query(tab)).all()
            values = self.property.values(target_halos_supplemented)

        results[results_target] = values

        return results



class StoredPropertyDescription(CalculationDescription):
    def __init__(self, tokens):
        #print "StoredPropertyDescription.__init__",tokens
        self.name = tokens[0]

    def __str__(self):
        return self.name

    def retrieves(self):
        return {self.name}

    def values(self, halos):
        return np.array([[h[self.name] if self._has_required_properties(h) else None for h in halos]],dtype=object)

    def proxy_value(self):
        """Return a placeholder value for this calculation"""
        return UnknownValue()



def parse_property_name( name):
    property_name = pp.Word(pp.alphas,pp.alphanums+"_")
    stored_property = property_name.setParseAction(StoredPropertyDescription)
    live_calculation_property = pp.Forward().setParseAction(LivePropertyDescription)

    numerical_value = pp.Regex(r'-?\d+(\.\d*)?([eE]\d+)?').setParseAction(FixedNumericInputDescription)

    dbl_quotes = pp.Literal("\"").suppress()

    string_value = dbl_quotes.suppress() + pp.SkipTo(dbl_quotes).setParseAction(FixedInputDescription) + dbl_quotes.suppress()

    redirection = pp.Forward().setParseAction(LinkDescription)



    value_or_property_name = string_value | numerical_value \
                             | redirection | live_calculation_property \
                             | stored_property

    redirection << (live_calculation_property | stored_property) + pp.Literal(".").suppress() + (redirection | live_calculation_property | stored_property)

    parameters = pp.Literal("(").suppress()+pp.Optional(value_or_property_name+pp.ZeroOrMore(pp.Literal(",").suppress()+value_or_property_name))+pp.Literal(")").suppress()
    live_calculation_property << property_name+parameters
    property_complete = pp.stringStart()+value_or_property_name+pp.stringEnd()

    return property_complete.parseString(name)[0]


def parse_property_names(*names):
    return MultiCalculationDescription(*[parse_property_name(n) for n in names])



def get_halo_property_if_special_name(halo,pname):
    if pname == "z":
        return halo.timestep.redshift
    elif pname == "t":
        return halo.timestep.time_gyr
    elif pname == "N":
        return halo.halo_number
    elif pname == "dbid":
        return halo.id
    elif pname == "uid":
        return str(halo.timestep.simulation.basename).replace('/', '%') + "/" + str(halo.timestep.extension).replace('/', '%') + "/" + str(halo.halo_number)
    elif pname == "NDM":
        return halo.NDM
    elif pname == "host":
        if halo.halo_type==1:
            try:
                host = halo.host_halo.halo_number
                return host
            except AttributeError:
                return None
        else:
            return None
    elif pname == "self":
        return halo
    return None

def get_property_with_live_calculation(halo,pname,raw):
    prop = None
    live_calculate=False
    if pname[0] == ":":
        live_calculate = True
        pname = pname[1:]
    elif "(" in pname:
        live_calculate=True

    if live_calculate:
        import properties
        c = properties.instantiate_classes([pname])[0]
        assert not c.requires_simdata()
        X = c.calculate(None, halo)

        if isinstance(c.name(), str):
            prop = X
        else:
            prop = X[c.index_of_name(pname)]
    else:
        prop = halo.get_data(pname, raw)

    return prop

def find_relation(relation_name, halo, maxhops=2):
    relation = core.get_item(relation_name)
    strategy = hopper.MultiHopStrategy(halo, maxhops, "across", relation)
    res = strategy.all()

    if len(res)>0:
        return res[0]
    else:
        raise ValueError, "match(%s) found no linked halo"%relation_name


def get_halo_property_with_relationship(halo, pname, raw):
    match = re.match("match\(([^\)]+)\)\.(.*)",pname)
    if match is None:
        return None

    relation = match.group(1)
    pname2 = match.group(2)

    halo2 = find_relation(relation, halo)

    return get_halo_property_with_magic_strings(halo2, pname2, raw)

def get_halo_property_with_magic_strings(halo, pname, raw=False):

    prop = get_halo_property_with_relationship(halo, pname, raw)
    if prop is not None:
        return prop

    z = pname.split("//")
    pname = z[0]

    prop = get_halo_property_if_special_name(halo,pname)

    if prop is None:
        prop = get_property_with_live_calculation(halo, pname, raw)


    if len(z) == 1:
        return prop
    else:
        try:
            return prop[int(z[1])]
        except:
            if z[1] == "+":
                return np.max(prop)
            elif z[1] == "-":
                return np.min(prop)
            elif z[1].upper() == "RMS":
                return np.sqrt((prop ** 2).sum())
            elif z[1] == "half_rad":
                ihalf = np.where(prop>=prop[-1]/2.)[0]
                return (ihalf[0]+1)*0.1
            elif z[1].startswith("Rhalf_"):
                halfl = get_property_with_live_calculation(halo,z[1],raw)
                if halfl is None:
                    print "here"
                    return None
                else:
                    ihalfl = int(halfl/0.1)
                    return prop[ihalfl]
            raise