import numpy as np
from . import HaloProperties

class DiskDynamicsProperties(HaloProperties):
    # include

    @classmethod
    def name(self):
        return "disk_vcirc", "disk_omega", "disk_kappa", \
               "disk_vcirc_st","disk_vR_st","disk_vR_disp_st","disk_vT_st","disk_vT_disp_st","disk_quad_amp_st","disk_pattern_freq_st",\
               "disk_vcirc_gas","disk_vR_gas","disk_vR_disp_gas","disk_vT_gas","disk_vT_disp_gas","disk_quad_amp_gas","disk_pattern_freq_gas"

    def spherical_region(self):
        return True

    def plot_x0(self):
        return self.plot_xdelta()/2

    def plot_xdelta(self):
        return 0.2

    def plot_xlog(self):
        return False

    def plot_ylog(self):
        return False

    def make_profile_object(self, halo):
        import pynbody
        return pynbody.analysis.profile.Profile(halo,min=0,max=20,nbins=int(20/self.plot_xdelta()))

    def disk_stats(self, pro):
        return pro['v_circ'], pro['vR'], pro['vR_disp'], pro['vT'], pro['vT_disp'], pro['fourier']['amp'][2,:], abs(pro['pattern_frequency'].in_units("Myr^-1"))

    def calculate(self, halo, existing_properties):
        import pynbody
        with pynbody.analysis.angmom.faceon(halo):
            halo['vR'] = (halo['x']*halo['vx'] + halo['y']*halo['vy'])/np.sqrt(halo['x']**2+halo['y']**2)
            halo['vT'] = np.sqrt(halo['vx']**2+halo['vy']**2+halo['vz']**2 - halo['vR']**2)

            pro = self.make_profile_object(halo)
            ret_vals = [pro['v_circ'], pro['omega'].in_units("Myr^-1"), pro['kappa'].in_units("Myr^-1")]
            ret_vals+=self.disk_stats(self.make_profile_object(halo.st))
            ret_vals+=self.disk_stats(self.make_profile_object(halo.gas))

        return ret_vals
