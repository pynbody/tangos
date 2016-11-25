import numpy as np
import pynbody
from . import HaloProperties, TimeChunkedProperty

class StarForm(HaloProperties):

    """Properties relating to star formation"""

    # include

    @classmethod
    def name(self):
        return "SFR", "SFR_250Myr", "SFR_25Myr"

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
                    tf = sts["tform"][(sts["tform"]>0)]
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
        return self.calcForHalo(halo),self.calcForHalo(halo, 2.5e8),\
            self.calcForHalo(halo, hard_period=True),

    def spherical_region(self):
        return True


class StarFormHistogram(TimeChunkedProperty):
    @classmethod
    def name(self):
        return "SFR_histogram"

    def calculate(self, halo, existing_properties):
        M,_ = np.histogram(halo.st['tform'].in_units("Gyr"),weights=halo.st['massform'].in_units("Msol"),bins=self.nbins,range=(0,self.tmax_Gyr))
        t_now = halo.properties['time'].in_units("Gyr")
        M/=self.delta_t
        M = M[self.store_slice(t_now)]

        return M

    @classmethod
    def reassemble(cls, *options):
        reassembled = TimeChunkedProperty.reassemble(*options)
        return reassembled/1e9 # Msol per Gyr -> Msol per yr

@pynbody.analysis.profile.Profile.profile_property
def sfr(self, deltat='25 Myr'):
    dt = pynbody.array.SimArray(float(deltat.split(' ')[0]), deltat.split(' ')[1])

    sfr_pro = np.zeros(self.nbins)
    now = self.sim.properties['time'].in_units(dt.units)
    for i in range(self.nbins):
        subs = self.sim[self.binind[i]]
        if len(subs) > 0:
            use, = np.where(subs['tform'].in_units(dt.units)>now - dt)
            if len(use)>0:
                sfr_pro[i] = subs['massform'][use].sum()/dt.in_units('yr')
    return sfr_pro

class StarForm_encl(HaloProperties):
    @classmethod
    def name(self):
        return "SFR_encl_25Myr", "SFR_encl_250Myr"

    def requires_property(self):
        return ["SSC", "Rvir"]

    def rstat(self, halo, rad, delta=0.1):
        nbins = int(rad / delta)
        maxrad = delta * (nbins + 1)
        pro = pynbody.analysis.profile.Profile(halo.s[pynbody.filt.HighPass("tform", 0)], type='lin', ndim=3, min=0, max=maxrad, nbins=nbins)
        return np.cumsum(pro['sfr,25 Myr']), np.cumsum(pro['sfr,250 Myr'])

    def calculate(self,  halo, properties):
        com = properties['SSC']
        rad = properties['Rvir']
        halo["pos"] -= com
        halo.wrap()
        delta = properties.get('delta',0.1)

        SFR_25Myr, SFR_250Myr = self.rstat(halo, rad, delta)

        halo['pos'] += com
        halo.wrap()

        return SFR_25Myr, SFR_250Myr

    @classmethod
    def plot_x0(cls):
        return 0.0

    @classmethod
    def plot_xdelta(cls):
        return 0.1