from __future__ import absolute_import

import re
import numpy as np
import six
from ..log import logger
import os


class BHLogData(object):
    """Class to load a Changa BH log files, either simname.BlackHoles or the (now deprecated) simname.shortened.orbit"""
    _cache = {}
    _n_cols = 0

    @classmethod
    def can_load(cls, filename):
        simname, stepnum = re.match("^(.*)\.(0[0-9]*)$", filename).groups()
        try:
            return os.path.exists(cls.filename(simname))
        except (ValueError, TypeError):
            return False

    @classmethod
    def filename(cls, simname):
        raise ValueError("Unknown path to stat file")

    @classmethod
    def get_existing_or_new(cls, filename):
        name, stepnum = re.match("^(.*)\.(0[0-9]*)$", filename).groups()
        obj = cls._cache.get(name, None)
        if obj is not None:
            return obj

        obj = cls(filename)
        cls._cache[name] = obj
        return obj

    def read_data(self, filename, sim):
        """
        generic function to read ascii columns from a given log file.
        :param filename: name of log file for black holes
        :param sim: loaded simulation object to connect the SimArray objects
        :return: simulation arrays for bh properties iord, time, step, mass, x, y, z, vx, vy, vz,
        current mdot, average mdot, change in mass, and scale factor
        """
        raise NotImplementedError

    def __init__(self, filename):
        import pynbody
        f = pynbody.load(filename)
        self.boxsize = float(f.properties['boxsize'].in_units('kpc', a=f.properties['a']))
        name, stepnum = re.match("^(.*)\.(0[0-9]*)$", filename).groups()
        wrapped_ars = self.read_data(self.filename(name), f)
        iord, time, step, mass, x, y, z, vx, vy, vz, mdot, mdotmean, dMaccum, scalefac = wrapped_ars

        logger.info("Loaded a BH log with %d entries", len(time))

        iord[(iord<0)] = 2 * 2147483648 + iord[(iord < 0)]

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
        mdotmean.units = munits / tunits
        dMaccum.units = munits
        # E.units = Eunits

        x.convert_units('kpc')
        y.convert_units('kpc')
        z.convert_units('kpc')
        vx.convert_units('km s^-1')
        vy.convert_units('km s^-1')
        vz.convert_units('km s^-1')
        mdot.convert_units('Msol yr^-1')
        mdotmean.convert_units('Msol yr^-1')
        mass.convert_units("Msol")
        time.convert_units("Gyr")
        dMaccum.convert_units("Msol")
        # E.convert_units('erg')

        self.vars = {'bhid': iord, 'step': step, 'x': x, 'y': y, 'z': z,
                     'vx': vx, 'vy': vy, 'vz': vz, 'mdot': mdot, 'mdotmean': mdotmean,'mass': mass,
                     'time': time, 'dM': dMaccum}

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
        ilast = np.argmax(self.vars['time'][mask])
        return dict((k, v[mask][ilast]) for k, v in six.iteritems(self.vars))

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

class BlackHolesLog(BHLogData):
    _n_cols = 18
    _col_types = [int, float, float, float, float, float,
                     float, float, float, float, float, float,
                     float, float, float, float, float, float]

    @classmethod
    def filename(cls, simname):
        return simname + '.BlackHoles'

    def read_data(self, filename, sim):
        import pynbody
        ars = [[] for i in range(self._n_cols)]
        for line in open(filename):
            line_split = line.split()
            for i in range(self._n_cols):
                ars[i].append(self._col_types[i](line_split[i]))

        wrapped_ars = [pynbody.array.SimArray(x) for x in ars]
        for w in wrapped_ars:
            w.sim = sim

        iord, time, step, mass, x, y, z, vx, vy, vz, pot, mdot, dM, dE, dt, dMaccum, dEaccum, scalefac \
            = wrapped_ars
        dt_out = np.zeros(len(iord))
        osort = np.argsort(step)

        unique_step, unique_ind, unique_step_inv = np.unique(step[osort], return_inverse=True, return_index=True)
        dt_ustep = time[osort[unique_ind[1:]]] - time[osort[unique_ind[:-1]]]
        dt_ustep = np.insert(dt_ustep, 0, dt_ustep[0])
        dt_out[osort] = dt_ustep[unique_step_inv]

        mdotmean = dMaccum/dt_out

        return iord, time, step, mass, x, y, z, vx, vy, vz, mdot, mdotmean, dMaccum, scalefac



class ShortenedOrbitLog(BHLogData):
    _n_cols = 15
    _col_types = [int, float, int, float, float, float,
                  float, float, float, float, float, float,
                  float, float, float]

    @classmethod
    def filename(cls, timestep_filename):
        return timestep_filename + '.shortened.orbit'

    def read_data(self, filename, sim):
        import pynbody
        ars = [[] for i in range(self._n_cols)]
        for line in open(filename):
            line_split = line.split()
            for i in range(self._n_cols):
                ars[i].append(self._col_types[i](line_split[i]))

        wrapped_ars = [pynbody.array.SimArray(x) for x in ars]
        for w in wrapped_ars:
            w.sim = sim

        iord, time, step, mass, x, y, z, vx, vy, vz, mdot, mdotmean, mdotsig, scalefac, dMaccum = wrapped_ars

        return iord, time, step, mass, x, y, z, vx, vy, vz, mdot, mdotmean, dMaccum, scalefac