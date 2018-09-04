from .spherical_region import SphericalRegionPropertyCalculation
from .centring import centred_calculation

class BaryonicImages(SphericalRegionPropertyCalculation):
    names = "gas_map_sideon", "uvi_image_sideon", "gas_map_faceon", "uvi_image_faceon", "gas_map", "uvi_image"

    @classmethod
    def plot_extent(cls):
        return 15.0

    def plot_xlabel(self):
        return "x/kpc"

    def plot_ylabel(self):
        return "y/kpc"

    def plot_clabel(self):
        return r"M$_{\odot}$ kpc$^{-2}$"

    @centred_calculation
    def calculate(self, particle_data, properties):
        import pynbody.analysis.angmom as angmom
        size = self.plot_extent()
        g, s = self._render_gas(particle_data, size), self._render_stars(particle_data, size)
        with angmom.sideon(particle_data, return_transform=True,
                               cen_size=self._simulation.get("approx_resolution_kpc", 0.1)*10.):
            g_side, s_side = self._render_gas(particle_data, size), self._render_stars(particle_data, size)
            with particle_data.rotate_x(90):
                g_face, s_face = self._render_gas(particle_data, size), self._render_stars(particle_data, size)

        return g_side, s_side, g_face, s_face, g, s

    def _render_projected(self, f, size):
        import pynbody.plot
        im = pynbody.plot.sph.image(f[pynbody.filt.BandPass(
            'z', -size / 2, size / 2)], 'rho', size, units="Msol kpc^-2", noplot=True)
        return im

    def _render_gas(self, f, size):
        if len(f.gas)>0:
            return self._render_projected(f.gas, size)
        else:
            return None

    def _render_stars(self, f, size):
        import pynbody.plot
        if len(f.st)>0:
            return pynbody.plot.stars.render(f.st[pynbody.filt.HighPass('tform',0) & pynbody.filt.BandPass('z', -size / 2, size / 2)],
                                         width=size, plot=False, ret_im=True, mag_range=(16,22))
        else:
            return None
