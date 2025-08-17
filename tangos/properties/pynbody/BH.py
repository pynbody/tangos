import numpy as np
import scipy
import scipy.interpolate

import tangos.core.halo
from tangos.input_handlers.changa_bh import BlackHolesLog, ShortenedOrbitLog

from .. import LivePropertyCalculation, TimeChunkedProperty
from . import PynbodyPropertyCalculation


class BH(PynbodyPropertyCalculation):

    names = "BH_mdot", "BH_mdot_ave", "BH_central_offset", "BH_central_distance", "BH_mass"
    requires_particle_data = True


    def requires_property(self):
        return ['host_halo.shrink_center']

    def preloop(self, f, db_timestep):
        if BlackHolesLog.can_load(db_timestep.filename):
            self.log = BlackHolesLog.get_existing_or_new(db_timestep.filename)
        elif ShortenedOrbitLog.can_load(db_timestep.filename):
            self.log = ShortenedOrbitLog.get_existing_or_new(db_timestep.filename)
        else:
            raise RuntimeError("cannot find recognizable log file")
        self.filename = db_timestep.filename
        print(self.log)

    def calculate(self, bh_particle, properties):
        boxsize = self.log.boxsize
        bh_data = self.log.get_for_named_snapshot_for_id(self.filename, properties.halo_number)
        main_halo_ssc = properties['host_halo.shrink_center']

        if main_halo_ssc is None:
            offset = [0.0, 0.0, 0.0]
        else:
            offset = np.array((bh_data['x'], bh_data['y'], bh_data['z'])) - main_halo_ssc
            bad, = np.where(np.abs(offset) > boxsize / 2.)
            offset[bad] = -1.0 * (offset[bad] / np.abs(offset[bad])) * np.abs(boxsize - np.abs(offset[bad]))

        return bh_data['mdot'], bh_data['mdotmean'], offset, np.linalg.norm(offset), bh_data['mass']


class BHAccHistogram(TimeChunkedProperty):

    requires_particle_data = True
    names = "BH_mdot_histogram",

    def requires_property(self):
        return []

    def preloop(self, f, db_timestep):
        if BlackHolesLog.can_load(db_timestep.filename):
            self.log = BlackHolesLog.get_existing_or_new(db_timestep.filename)
        elif ShortenedOrbitLog.can_load(db_timestep.filename):
            self.log = ShortenedOrbitLog.get_existing_or_new(db_timestep.filename)
        else:
            raise RuntimeError("cannot find recognizable log file")

    def plot_xlabel(self):
        return "t/Gyr"

    def plot_ylabel(self):
        return r"$\dot{M}/M_{\odot}\,yr^{-1}$"

    def calculate(self, particles, properties):

        particles = particles.s

        if len(particles) != 1:
            raise RuntimeError("Not a BH!")

        if particles['tform'][0] > 0:
            raise RuntimeError("Not a BH!")

        mask = self.log.vars['bhid'] == particles['iord']
        if (mask.sum() == 0):
            raise RuntimeError(f"Can't find BH {particles['iord']} in .orbit file")

        t_orbit = self.log.vars['time'][mask]
        Mdot_orbit = self.log.vars['mdotmean'][mask]
        order = np.argsort(t_orbit)

        t_max = properties.timestep.time_gyr

        grid_tmax_Gyr = 20.0
        nbins = int(grid_tmax_Gyr/self.pixel_delta_t_Gyr)
        t_grid = np.linspace(0, grid_tmax_Gyr, nbins)

        Mdot_grid = scipy.interpolate.interp1d(t_orbit[order], Mdot_orbit[order], bounds_error=False)(t_grid)

        return Mdot_grid[self.store_slice(t_max)],


class BHAccHistogramMerged(PynbodyPropertyCalculation):
    names = "BH_mdot_histogram_all"

    def requires_property(self):
        return ["BH_mdot_histogram"]

    @classmethod
    def accumulate_on_mergers(cls, array, bh):
        while bh is not None:
            if "BH_merger" in list(bh.keys()):
                for targ_bh in bh.get_data("BH_merger", always_return_array=True):
                    if targ_bh.timestep.time_gyr < bh.timestep.time_gyr:
                        try:
                            accum = targ_bh["BH_mdot_histogram"]
                            array[:len(accum)][accum == accum] += accum[accum == accum]
                        except KeyError:
                            pass
                        cls.accumulate_on_mergers(array, targ_bh)

            bh = bh.previous

    def calculate(self, simdata, bh):
        mdot = bh['BH_mdot_histogram']
        self.accumulate_on_mergers(mdot, bh)
        return mdot


class BHGal(LivePropertyCalculation):
    def __init__(self, simulation=None, choose='BH_mass', minmax='max', bhtype='BH_central'):
        super().__init__(simulation)
        self._maxmin = minmax
        self._choicep = choose
        self._bhtype = bhtype

    names = 'bh'

    def requires_property(self):
        return self._bhtype, self._bhtype+"."+self._choicep

    def live_calculate(self, halo, *args):
        if halo.object_typecode != 0:
            return None

        if self._bhtype not in list(halo.keys()):
            return None

        if type(halo[self._bhtype]) is list:
            chp = [bh[self._choicep] for bh in halo[self._bhtype] if self._choicep in bh]
            if len(chp) == 0:
                return None
            target = None
            if self._maxmin == 'min':
                target = np.argmin(chp)
            if self._maxmin == 'max':
                target = np.argmax(chp)
            if target is None:
                return None
            return halo[self._bhtype][target]
        else:
            return halo[self._bhtype]

class BHCentral(BHGal):
    names = 'bhcenter'

    def __init__(self, simulation=None):
        super().__init__(simulation, 'BH_central_distance', 'min', 'BH_central')
