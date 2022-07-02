from . import LivePropertyCalculation


class IntrinsicProperties(LivePropertyCalculation):
    names = "t","z","a","dbid", "halo_number", "finder_id", "NDM", "NStar", "NGas", "type", \
            "typetag", "step_path", "path"

    def live_calculate(self, halo):
        ts = halo.timestep
        return ts.time_gyr, ts.redshift, 1./(1.+ts.redshift), halo.id, halo.halo_number, halo.finder_id, \
               halo.NDM, halo.NStar, halo.NGas, halo.object_typecode, \
               halo.object_typetag_from_code(halo.object_typecode), \
               str(halo.timestep.path), str(halo.path)
