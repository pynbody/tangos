import numpy as np
import re

import core
import hopper

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
    elif pname == "self":
        return halo
    return None

def get_property_with_live_calculation(halo,pname):
    prop = None
    if pname[0] == ":":
        # live calculate
        pname = pname[1:]
        import properties
        c = properties.providing_classes([pname])[0]()
        assert not c.requires_simdata()
        X = c.calculate(None, halo)

        if isinstance(c.name(), str):
            prop = X
        else:
            prop = X[c.name().index(pname)]
    else:
        prop = halo[pname]

    return prop

def find_relation(relation_name, halo, maxhops=2):
    relation = core.get_item(relation_name)
    strategy = hopper.MultiHopStrategy(halo, maxhops, "across")
    strategy.target(relation)
    res = strategy.all()

    if len(res)>0:
        return res[0]
    else:
        raise ValueError, "match(%s) found no linked halo"%relation_name


def get_halo_property_with_relationship(halo, pname):
    match = re.match("match\(([^\)]+)\)\.(.*)",pname)
    if match is None:
        return None

    relation = match.group(1)
    pname2 = match.group(2)

    halo2 = find_relation(relation, halo)

    return get_halo_property_with_magic_strings(halo2, pname2)

def get_halo_property_with_magic_strings(halo, pname):

    prop = get_halo_property_with_relationship(halo, pname)
    if prop is not None:
        return prop

    z = pname.split("//")
    pname = z[0]

    prop = get_halo_property_if_special_name(halo,pname)

    if prop is None:
        prop = get_property_with_live_calculation(halo, pname)


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
            raise