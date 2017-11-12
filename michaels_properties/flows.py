from tangos.properties import HaloProperties, TimeChunkedProperty
from tangos.properties.spherical_region import SphericalRegionHaloProperties
import numpy as np
import pynbody


class FlowProfile(SphericalRegionHaloProperties):
    #_xmax = 100.0
    _threshold_vel = 20.0

    def region_specification(self, db_data):
        return pynbody.filt.Sphere(db_data['Rvir'], db_data['SSC']) & \
               (pynbody.filt.FamilyFilter(pynbody.family.gas)|pynbody.filt.FamilyFilter(pynbody.family.star))

    @classmethod
    def name(cls):
        return "inflow_Mdot", "outflow_Mdot", \
               "inflow_vel", "outflow_vel", \
               "inflow_vel2", "outflow_vel2", \
               "inflow_temp", "outflow_temp", \
#               "inflow_Mdot_dm", "outflow_Mdot_dm", \
#               "inflow_vel_dm", "outflow_vel_dm", \
#               "inflow_vel2_dm", "outflow_vel2_dm"


    def plot_x0(cls):
        return 0.5

    @classmethod
    def plot_xdelta(cls):
        return 1.0

    def profile_calculation(self, f_gas, vr_cut, rvir):
        f_gas['Mdot'] = f_gas['mass'] * f_gas['vr'] / (pynbody.units.Unit("kpc")*self.plot_xdelta())
        if vr_cut<0:
            f_gas['Mdot']*=(f_gas['vr']<vr_cut).view(np.ndarray)
            f_gas['Mdot'] *= -1
        else:
            f_gas['Mdot']*=(f_gas['vr']>vr_cut).view(np.ndarray)



        pro = pynbody.analysis.profile.Profile(f_gas, min=0.0,max=rvir,
                                               nbins=int(self.rvir/self.plot_xdelta()),
                                               ndim=3, weight_by='Mdot')
        return pro['weight_fn'].in_units("Msol yr^-1"), pro['vr'], pro['vr2'], pro['temp']


    def calculate(self, halo, properties):
        with pynbody.analysis.halo.center(halo):
            halo.gas['vr2'] = halo.gas['vr'] ** 2
            inflow_Mdot, inflow_vel, inflow_vel2, inflow_temp = self.profile_calculation(halo.ancestor.gas, -self._threshold_vel)
            outflow_Mdot, outflow_vel, outflow_vel2, outflow_temp = self.profile_calculation(halo.ancestor.gas, self._threshold_vel)
            inflow_Mdot_dm, inflow_vel_dm, inflow_vel2_dm, _ = self.profile_calculation(halo.ancestor.dm, -self._threshold_vel)
            outflow_Mdot_dm, outflow_vel_dm, outflow_vel2_dm, _ = self.profile_calculation(halo.ancestor.dm, self._threshold_vel)
        return inflow_Mdot, outflow_Mdot, -inflow_vel, outflow_vel,  inflow_vel2, outflow_vel2, inflow_temp, outflow_temp, inflow_Mdot_dm, outflow_Mdot_dm, -inflow_vel_dm, outflow_vel_dm,  inflow_vel2_dm, outflow_vel2_dm
