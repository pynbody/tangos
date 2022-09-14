import numpy as np

import tangos as db

from .. import LivePropertyCalculation, PropertyCalculation


class FindCenter(PropertyCalculation):
    """Returns center array in halo finder units"""
    names = "center"

    def requires_property(self):
        return ["X", "Y", "Z"]

    def calculate(self, particle_data, existing_properties):
        return np.array([existing_properties["X"],existing_properties["Y"],existing_properties["Z"]])

class FindPhyiscalCenter(PropertyCalculation):
    """Returns center arrays in physical units (Mpc)"""
    names = "center_Mpc"

    def requires_property(self):
        return ["X_Mpc", "Y_Mpc", "Z_Mpc"]

    def calculate(self, particle_data, existing_properties):
        return np.array([existing_properties["X_Mpc"],existing_properties["Y_Mpc"],existing_properties["Z_Mpc"]])

class FindHosts(LivePropertyCalculation):
    """Returns all more massive halos that given halo is within
    the virial radius of"""
    names = "hosts"

    def requires_property(self):
        return ["Mvir", "center_Mpc", "Rvir_kpc"]

    def live_calculate(self, halo_entry):
        centers, radii, dbid, masses = halo_entry.timestep.calculate_all("center_Mpc","Rvir_kpc","dbid()","Mvir")
        offsets = np.linalg.norm(halo_entry['center_Mpc'] - centers[masses>halo_entry['Mvir']], axis=1)
        host_mask = offsets<(radii[masses>halo_entry['Mvir']]/1000.)
        potids = dbid[masses>halo_entry['Mvir']]
        return np.array([db.get_halo(x) for x in potids[host_mask]])

class FindSats(LivePropertyCalculation):
    """Returns all less massive halos that lie within the virial
    radius of a given halo"""
    names = "satellites"

    def requires_property(self):
        return ["Mvir", "center_Mpc", "Rvir_kpc"]

    def live_calculate(self, halo_entry):
        centers, radii, dbid, masses = halo_entry.timestep.calculate_all("center_Mpc","Rvir_kpc","dbid()","Mvir")
        offsets = np.linalg.norm(halo_entry['center_Mpc'] - centers[masses<halo_entry['Mvir']], axis=1)
        host_mask = offsets<(halo_entry['Rvir_kpc']/1000.)
        potids = dbid[masses<halo_entry['Mvir']]
        return np.array([db.get_halo(x) for x in potids[host_mask]])
