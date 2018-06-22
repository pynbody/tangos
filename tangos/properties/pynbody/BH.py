from __future__ import absolute_import
from __future__ import print_function
import tangos.core.halo
from tangos.input_handlers.changa_bh import BHShortenedLog
from . import PynbodyPropertyCalculation
from .. import LivePropertyCalculation, TimeChunkedProperty
import numpy as np
import scipy, scipy.interpolate


class BH(PynbodyPropertyCalculation):

    names = "BH_mdot", "BH_mdot_ave", "BH_mdot_std", "BH_central_offset", "BH_central_distance", "BH_mass"
    requires_particle_data = True


    def requires_property(self):
        return ['host_halo']

    @classmethod
    def no_proxies(self):
        return True

    def preloop(self, f, db_timestep):
        self.log = BHShortenedLog.get_existing_or_new(db_timestep.filename)
        self.filename = db_timestep.filename
        print(self.log)

    def calculate(self, halo, properties):
        if not isinstance(properties, tangos.core.halo.Halo):
            raise RuntimeError("No proxies, please")
        boxsize = self.log.boxsize

        vars = self.log.get_for_named_snapshot(self.filename)

        mask = vars['bhid'] == properties.halo_number
        if (mask.sum() == 0):
            raise RuntimeError("Can't find BH in .orbit file")

        # work out who's the main halo
        # main_halo = None
        # for i in properties.reverse_links:
        #    if i.relation.text.startswith("BH"):
        #        main_halo = i.halo_from
        #        break
        # if main_halo is None:
        #    raise RuntimeError("Can't relate BH to its parent halo")

        try:
            main_halo = properties['host_halo']
        except KeyError:
            main_halo = None

        if main_halo is None:
            main_halo_ssc = None
        else:
            try:
                main_halo_ssc = main_halo['shrink_center']
            except KeyError:
                main_halo_ssc = None

        entry = np.where(mask)[0]

        print("target entry is", entry)
        final = {}
        for t in 'x', 'y', 'z', 'vx', 'vy', 'vz', 'mdot', 'mass', 'mdotmean', 'mdotsig':
            final[t] = float(vars[t][entry])

        if main_halo_ssc is None:
            offset = np.array((0, 0, 0))
        else:
            offset = np.array((final['x'], final['y'], final['z'])) - main_halo_ssc
            bad, = np.where(np.abs(offset) > boxsize / 2.)
            offset[bad] = -1.0 * (offset[bad] / np.abs(offset[bad])) * np.abs(boxsize - np.abs(offset[bad]))

        return final['mdot'], final['mdotmean'], final['mdotsig'], offset, np.linalg.norm(offset), final['mass']


class BHAccHistogram(TimeChunkedProperty):

    requires_particle_data = True

    @classmethod
    def name(self):
        return "BH_mdot_histogram"

    def requires_property(self):
        return []

    def preloop(self, f, db_timestep):
        self.log = BHShortenedLog.get_existing_or_new(db_timestep.filename)

    @classmethod
    def no_proxies(self):
        return True

    def plot_xlabel(self):
        return "t/Gyr"

    def plot_ylabel(self):
        return r"$\dot{M}/M_{\odot}\,yr^{-1}$"

    def calculate(self, halo, properties):

        halo = halo.s

        if len(halo) != 1:
            raise RuntimeError("Not a BH!")

        if halo['tform'][0] > 0:
            raise RuntimeError("Not a BH!")

        mask = self.log.vars['bhid'] == halo['iord']
        if (mask.sum() == 0):
            raise RuntimeError("Can't find BH in .orbit file")

        t_orbit = self.log.vars['time'][mask]
        Mdot_orbit = self.log.vars['mdotmean'][mask]
        order = np.argsort(t_orbit)

        t_max = properties.timestep.time_gyr

        grid_tmax_Gyr = 20.0
        nbins = grid_tmax_Gyr/self.pixel_delta_t_Gyr
        t_grid = np.linspace(0, grid_tmax_Gyr, nbins)

        Mdot_grid = scipy.interpolate.interp1d(t_orbit[order], Mdot_orbit[order], bounds_error=False)(t_grid)

        return Mdot_grid[self.store_slice(t_max)]


class BHAccHistogramMerged(PynbodyPropertyCalculation):
    names = "BH_mdot_histogram_all"

    @classmethod
    def no_proxies(self):
        return True

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
        super(BHGal, self).__init__(simulation)
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
        super(BHCentral, self).__init__(simulation, 'BH_central_distance', 'min', 'BH_central')
