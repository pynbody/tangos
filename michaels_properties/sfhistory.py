#from __future__ import absolute_import

from tangos.properties import HaloProperties, TimeChunkedProperty
import numpy as np
import pynbody

class InnerStarFormHistogram(TimeChunkedProperty):

    _maxr_frac = 0.1

    @classmethod
    def name(self):
        return "inner_SFR_histogram"

    def requires_property(self):
        return ["shrink_center", "max_radius"]

    def calculate(self, halo, existing_properties):
        filter = pynbody.filt.Sphere(self._maxr_frac*existing_properties['max_radius'],cen=existing_properties['shink_center'])
        M,_ = np.histogram(halo.st[filter]['tform'].in_units("Gyr"),weights=halo.st[filter]['massform'].in_units("Msol"),bins=self.nbins,range=(0,self.tmax_Gyr))
        t_now = halo.properties['time'].in_units("Gyr")
        M/=self.delta_t
        M = M[self.store_slice(t_now)]

        return M

    @classmethod
    def reassemble(cls, *options):
        reassembled = TimeChunkedProperty.reassemble(*options)
        return reassembled/1e9 # Msol per Gyr -> Msol per yr
