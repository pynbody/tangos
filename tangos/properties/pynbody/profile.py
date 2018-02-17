import numpy as np
from .spherical_region import SphericalRegionPropertyCalculation
from .centring import centred_calculation

class HaloDensityProfile(SphericalRegionPropertyCalculation):
    # include

    names = "dm_density_profile", "dm_mass_profile"

    def plot_x0(self):
        return self.plot_xdelta()/2

    def plot_xdelta(self):
        return self._simulation.get("approx_resolution_kpc", 0.1)

    def plot_xlabel(self):
        return "r/kpc"

    def plot_ylabel(self):
        return r"$\rho/M_{\odot}\,kpc^{-3}$", r"$M/M_{\odot}$"

    def _get_profile(self, halo, maxrad):
        import pynbody

        delta = self.plot_xdelta()
        nbins = int(maxrad / delta)
        maxrad = delta * (nbins + 1)

        pro = pynbody.analysis.profile.Profile(halo, type='lin', ndim=3,
                                               min=0, max=maxrad, nbins=nbins)

        rho_a = pro['density']
        mass_a = pro['mass_enc']

        rho_a = rho_a.view(np.ndarray)
        mass_a = mass_a.view(np.ndarray)

        return rho_a, mass_a

    @centred_calculation
    def calculate(self, data, existing_properties):

        dm_a, dm_b = self._get_profile(data.dm, existing_properties["max_radius"])

        return dm_a, dm_b


class BaryonicHaloDensityProfile(HaloDensityProfile):
    names = "gas_density_profile", "gas_mass_profile", "star_density_profile", "star_mass_profile"

    @centred_calculation
    def calculate(self, data, existing_properties):
        gas_a, gas_b = self._get_profile(data.gas, existing_properties["max_radius"])
        star_a, star_b = self._get_profile(data.star, existing_properties["max_radius"])
        return gas_a, gas_b, star_a, star_b
