from __future__ import absolute_import

import re
import numpy as np
import six
from ..log import logger


class BHShortenedLog(object):
    """Class to load a Changa BH .shortened.orbit file which contains the orbital and accretion histories of BHs"""
    _cache = {}

    @classmethod
    def get_existing_or_new(cls, filename):
        name, stepnum = re.match("^(.*)\.(0[0-9]*)$", filename).groups()
        obj = cls._cache.get(name, None)
        if obj is not None:
            return obj

        obj = cls(filename)
        cls._cache[name] = obj
        return obj

    def __init__(self, filename):
        import pynbody
        f = pynbody.load(filename)
        self.boxsize = float(f.properties['boxsize'].in_units('kpc', a=f.properties['a']))
        name, stepnum = re.match("^(.*)\.(0[0-9]*)$", filename).groups()
        ars = [[] for i in range(15)]
        for line in open(name + ".shortened.orbit"):
            line_split = line.split()
            for i in range(15):
                if i==0 or i==2:
                    col_type=int
                else:
                    col_type=float
                ars[i].append(col_type(line_split[i]))

        wrapped_ars = [pynbody.array.SimArray(x) for x in ars]
        for w in wrapped_ars:
            w.sim = f
        # bhid, time, step, mass, x, y, z, vx, vy, vz, pot, mdot, deltaM, E, dtEff, scalefac = wrapped_ars
        bhid, time, step, mass, x, y, z, vx, vy, vz, mdot, mdotmean, mdotsig, scalefac, dM = wrapped_ars
        bhid = np.array(bhid, dtype=int)
        logger.info("Loaded a BH log with %d entries", len(time))

        bhid[(bhid < 0)] = 2 * 2147483648 + bhid[(bhid < 0)]

        munits = f.infer_original_units("Msol")
        posunits = f.infer_original_units("kpc")
        velunits = f.infer_original_units("km s^-1")
        # potunits = velunits**2
        tunits = posunits / velunits
        # Eunits = munits*potunits
        # decorate with units

        x *= scalefac
        y *= scalefac
        z *= scalefac
        vx *= scalefac
        vy *= scalefac
        vz *= scalefac

        mass.units = munits
        x.units = y.units = z.units = posunits / pynbody.units.Unit('a')
        vx.units = vy.units = vz.units = velunits / pynbody.units.Unit('a')
        # pot.units = potunits
        time.units = tunits
        mdot.units = munits / tunits
        mdotsig.units = munits / tunits
        mdotmean.units = munits / tunits
        dM.units = munits
        # E.units = Eunits

        x.convert_units('kpc')
        y.convert_units('kpc')
        z.convert_units('kpc')
        vx.convert_units('km s^-1')
        vy.convert_units('km s^-1')
        vz.convert_units('km s^-1')
        mdot.convert_units('Msol yr^-1')
        mdotmean.convert_units('Msol yr^-1')
        mdotsig.convert_units('Msol yr^-1')
        mass.convert_units("Msol")
        time.convert_units("Gyr")
        dM.convert_units("Msol")
        # E.convert_units('erg')

        self.vars = {'bhid': bhid, 'step': step, 'x': x, 'y': y, 'z': z,
                     'vx': vx, 'vy': vy, 'vz': vz, 'mdot': mdot, 'mdotmean': mdotmean, 'mdotsig': mdotsig, 'mass': mass,
                     'time': time, 'dM': dM}

    def get_at_stepnum(self, stepnum):
        mask = self.vars['step'] == stepnum
        return dict((k, v[mask]) for k, v in six.iteritems(self.vars))

    def get_at_stepnum_for_id(self, stepnum, bhid):
        vars = self.get_at_stepnum(stepnum)
        try:
            index = np.where(vars['bhid']==bhid)[0][0]
        except IndexError:
            raise ValueError("BH %d not found in step %d"%(bhid,stepnum))
        vars_this = dict([(k,v[index]) for k, v in six.iteritems(vars)])
        return vars_this

    def get_last_entry_for_id(self, bhid):
        mask = self.vars['bhid'] == bhid
        if mask.sum()==0:
            raise ValueError("No entries for BH %d"%bhid)
        return dict((k, v[mask][-1]) for k, v in six.iteritems(self.vars))

    def determine_merger_ratio(self, bhid_eaten, bhid_survivor):
        eaten_entries = self.get_last_entry_for_id(bhid_eaten)
        eaten_mass = eaten_entries['mass']
        survivor_entries = self.get_at_stepnum_for_id(eaten_entries['step'], bhid_survivor)
        survivor_premerger_mass = survivor_entries['mass']
        return eaten_mass/survivor_premerger_mass


    def get_for_named_snapshot(self, filename):
        name, stepnum = re.match("^(.*)\.(0[0-9]*)$", filename).groups()
        stepnum = int(stepnum)
        return self.get_at_stepnum(stepnum)