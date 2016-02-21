import numpy as np
import re

import core
import hopper
import pyparsing as pp
import properties

class CalculationDescription(object):
    def __repr__(self):
        return "<Calculation description for %s>"%str(self)

    def retrieves(self):
        return set()

    def values(self):
        raise NotImplementedError


class FixedInputDescription(CalculationDescription):
    def __init__(self, tokens):
        self.value = str(tokens[0])

    def __str__(self):
        return '"'+str(self.value)+'"'

    def values(self, halos):
        return [self.value]*len(halos)

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
        result = result.union(properties.providing_class(self.name).requires_property())
        for i in self.inputs:
            result=result.union(i.retrieves())
        return result

    def values(self, halos):
        input_values = [i.values(halos) for i in self.inputs]
        results = []
        for inputs in zip(halos, *input_values):
            results.append(properties.live_calculate(self.name, *inputs))
        return results


class LinkDescription(CalculationDescription):
    def __init__(self, tokens):
        self.locator = tokens[0]
        self.property = tokens[1]

    def __str__(self):
        return str(self.locator)+"."+str(self.property)


class StoredPropertyDescription(CalculationDescription):
    def __init__(self, tokens):
        #print "StoredPropertyDescription.__init__",tokens
        self.name = tokens[0]

    def __str__(self):
        return self.name

    def retrieves(self):
        return {self.name}

    def values(self, halos):
        return [h[self.name] for h in halos]



def parse_property_name( name):
    property_name = pp.Word(pp.alphanums+"_")
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