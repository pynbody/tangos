import numpy as np

from . import YtPropertyCalculation


class HaloDensityProfile(YtPropertyCalculation):
    """Proof of concept: get halo density profiles from yt"""
    names = "dm_density_profile", "dm_mass_profile"

    def plot_x0(self):
        return self.plot_xdelta()/2

    def plot_xdelta(self):
        return self.get_simulation_property("approx_resolution_kpc", 0.1)

    def plot_xlabel(self):
        return "r/kpc"

    def plot_ylabel(self):
        return r"$\rho/M_{\odot}\,kpc^{-3}$", r"$M/M_{\odot}$"

    def calculate(self, data, existing_properties):
        # a rough-and-ready profile; faster than the yt built-in profiles which seem to insist on interpolating
        # onto a grid even for radial profiling (is this correct?)

        position = (data["dark_matter","particle_position"] - data.center).in_units("kpc")
        radius = np.linalg.norm(position,axis=1)
        rmax = radius.max()
        num_bins = int(rmax/self.plot_xdelta())+1

        mass = data["dark_matter","particle_mass"].in_units("Msun")

        mass_per_bin,_ = np.histogram(radius, weights=mass, bins=num_bins, range=(0,num_bins*self.plot_xdelta()))

        r = self.plot_x_values(mass_per_bin)
        vol_per_bin = 4*np.pi*r**2*self.plot_xdelta()

        mass_profile = np.cumsum(mass_per_bin)
        den_profile = mass_per_bin/vol_per_bin

        return den_profile, mass_profile

class Contamination(YtPropertyCalculation):
    """"Calculates fraction of dark matter particles within a sphere
    of radius Rvir around center of halo that are not must refine particles"""
    names = "contamfrac"

    def requires_property(self):
        return ["center", "Rvir_kpc"]

    def calculate(self, particle_data, existing_properties):
        dmtot = float(particle_data.quantities.total_quantity(('dark_matter', 'particle_mass')).in_units('Msun').value)
        hrdm = float(particle_data.quantities.total_quantity(('mrp_dark_matter', 'particle_mass')).in_units('Msun').value)
        return 1 - (hrdm/dmtot)
