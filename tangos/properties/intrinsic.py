from __future__ import absolute_import
from . import LivePropertyCalculation

class IntrinsicProperties(LivePropertyCalculation):
    names = "t","z","a","dbid", "halo_number", "NDM", "NStar", "NGas", "type", "step_path"

    def live_calculate(self, halo):
        ts = halo.timestep
        return ts.time_gyr, ts.redshift, 1./(1.+ts.redshift), halo.id, halo.halo_number, \
               halo.NDM, halo.NStar, halo.NGas, halo.object_typecode, str(halo.timestep.path)