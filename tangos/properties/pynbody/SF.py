from .. import TimeChunkedProperty
from . import pynbody_handler_module, PynbodyPropertyCalculation

import numpy as np

class StarFormHistogram(TimeChunkedProperty):
    works_with_handler = pynbody_handler_module.PynbodyInputHandler
    requires_particle_data = True

    @classmethod
    def name(self):
        return "SFR_histogram"

    def plot_xlabel(self):
        return "t/Gyr"

    def plot_ylabel(self):
        return r"$SFR/M_{\odot} yr^{-1}$"

    def calculate(self, halo, existing_properties):
        try:
            weights = halo.st['massform']
        except KeyError:
            weights = halo.st['mass']

        tmax_Gyr = 20.0 # calculate up to 20 Gyr
        nbins = int(20.0/self.pixel_delta_t_Gyr)

        M,_ = np.histogram(halo.st['tform'].in_units("Gyr"),weights=weights.in_units("Msol"),bins=nbins,range=(0,tmax_Gyr))
        t_now = halo.properties['time'].in_units("Gyr")
        M/=self.pixel_delta_t_Gyr
        M = M[self.store_slice(t_now)]

        return M

    def reassemble(self, *options):
        reassembled = super(StarFormHistogram, self).reassemble(*options)
        return reassembled/1e9 # Msol per Gyr -> Msol per yr

class StarForm(PynbodyPropertyCalculation):
    names = "SFR_10Myr", "SFR_100Myr"
    
    def calculate(self, halo, existing_properties):
        halo = halo.star

        t_now = halo.properties['time'].in_units("Gyr")
        tform = halo['tform'].in_units("Gyr")
        mask_10Myr = (t_now-tform)<0.01
        mask_100Myr = (t_now-tform)<0.1
        # Because physical_units has been called previously, mass is in Msol. Return results in Msol/yr.
        return halo['mass'][mask_10Myr].sum()/1e7, halo['mass'][mask_100Myr].sum()/1e8
