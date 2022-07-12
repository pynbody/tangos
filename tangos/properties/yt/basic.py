from .. import PropertyCalculation, LivePropertyCalculation
import numpy as np
from tangos import get_halo

class FindCenter(PropertyCalculation):
    """Returns center arrays in physical and code units (cu)"""
    names = "Center", "Center_cu"

    def requires_property(self):
        return ["X", "Y", "Z", "X_cu", "Y_cu", "Z_cu"]

    def calculate(self, particle_data, existing_properties):
        return np.array([existing_properties["X"],existing_properties["Y"],existing_properties["Z"]]),\
               np.array([existing_properties["X_cu"],existing_properties["Y_cu"],existing_properties["Z_cu"]])

class FindHosts(LivePropertyCalculation):
    """Returns all more massive halos that given halo is within
    the virial radius of"""
    names = "Hosts"
    
    def requires_property(self):
        return ["Mvir", "Center", "Rvir"]
            
    def live_calculate(self, halo_entry):
        centers, radii, dbid, masses = halo_entry.timestep.calculate_all("Center","Rvir","dbid()","Mvir")
        offsets = np.linalg.norm(halo_entry['Center'] - centers[masses>halo_entry['Mvir']], axis=1)
        host_mask = offsets<(radii[masses>halo_entry['Mvir']]/1000.)
        potids = dbid[masses>halo_entry['Mvir']]
        return np.array([get_halo(x) for x in potids[host_mask]])
            
class FindSats(LivePropertyCalculation):
    """Returns all less massive halos that lie within the virial
    radius of a given halo"""
    names = "Satellites"
    
    def requires_property(self):
        return ["Mvir", "Center", "Rvir"]
                
    def live_calculate(self, halo_entry):
        centers, radii, dbid, masses = halo_entry.timestep.calculate_all("Center","Rvir","dbid()","Mvir")
        offsets = np.linalg.norm(halo_entry['Center'] - centers[masses<halo_entry['Mvir']], axis=1)
        host_mask = offsets<(halo_entry['Rvir']/1000.)
        potids = dbid[masses<halo_entry['Mvir']]
        return np.array([get_halo(x) for x in potids[host_mask]])
        
class GetTimestepName(LivePropertyCalculation):
    """Fetches the name of the timestep that a halo exists in"""
    names = "TimeStep"
    
    def requires_property(self):
        return []
                
    def live_calculate(self, halo_entry):
        tsn = str(halo_entry.timestep).split('/')[1]
        return tsn

