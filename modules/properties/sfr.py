import numpy as np
import pynbody
from . import HaloProperties

class StarForm(HaloProperties):

    """Properties relating to star formation"""

    # include

    def name(self):
        return "SFR", "SFR_1kpc", "SFR_250Myr", "SFR_25Myr"

    def calcForHalo(self, halo, period=2.5e7, hard_period=False):
        sts = halo.star
        if len(sts) == 0:
            return 0.

        try:
            tunit = sts['tform'].units
        except KeyError:
            tunit = sts['age'].units

        conv = tunit.ratio("yr")
        # getUnits().convertTo(siman.Unit("Msol"))
        convmass = sts["mass"].units.ratio("Msol")
        now = halo.properties['time'].in_units(tunit)

        if len(sts) > 2:
            mass = sts["mass"]

            def get_period():
                try:
                    tf = sts["tform"]
                    index_period = np.nonzero(tf > now - period / conv)[0]
                except KeyError:
                    tf = sts['age']
                    index_period = np.nonzero(tf < period / conv)[0]
                return index_period

            index_period = get_period()

            while len(index_period) < 2 and period < 1.e9 and not hard_period:
                period *= 2.
                index_period = get_period()

            mass_formed = mass[index_period].sum() * convmass
            return mass_formed / period
        else:
            return 0.

    def calculate(self, halo, existing_properties):
        return self.calcForHalo(halo), self.calcForHalo(halo[pynbody.filt.Sphere("1 kpc", existing_properties["SSC"])]), \
            self.calcForHalo(halo, 2.5e8),\
            self.calcForHalo(halo, hard_period=True)

    def spherical_region(self):
        return True


class TimeChunkedProperty(HaloProperties):
    nbins = 1000
    tmax_Gyr = 20.0
    minimum_store_Gyr = 1.0

    @property
    def delta_t(self):
        return self.tmax_Gyr/self.nbins

    @classmethod
    def bin_index(self, time):
        return int(self.nbins*time/self.tmax_Gyr)

    @classmethod
    def store_slice(self, time):
        return slice(self.bin_index(time-self.minimum_store_Gyr, time))

    @classmethod
    def reassemble(cls, halo):
        halo = halo.halo
        t, stack = halo.reverse_property_cascade("t",cls().name())

        t = t[::-1]
        stack = stack[::-1]

        print t
        print [s.shape for s in stack]

        final = np.zeros(cls.bin_index(t[-1]))
        for t_i, hist_i in zip(t,stack):
            end = cls.bin_index(t_i)
            start = end - len(hist_i)
            final[start:end] = hist_i

        return final

    @classmethod
    def plot_xdelta(cls):
        return cls.tmax_Gyr/cls.nbins

    @classmethod
    def plot_xlog(cls):
        return False

    @classmethod
    def plot_ylog(cls):
        return False




class StarFormHistogram(TimeChunkedProperty):
    def name(self):
        return "SFR_histogram"

    def calculate(self, halo, existing_properties):
        M,_ = np.histogram(halo.st['tform'].in_units("Gyr"),weights=halo.st['massform'].in_units("Msol"),bins=self.nbins,range=(0,self.tmax_Gyr))
        t_now = halo.properties['time'].in_units("Gyr")
        M/=self.delta_t
        M = M[self.store_slice(t_now)]

        return M

