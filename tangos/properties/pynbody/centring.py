from . import PynbodyPropertyCalculation
from .. import LivePropertyCalculation
from .spherical_region import SphericalRegionPropertyCalculation
import numpy as np
import functools
import contextlib


class CentreAndRadius(PynbodyPropertyCalculation):
    names = "shrink_center", "max_radius"

    def calculate(self, halo, existing_properties):
        dm_center, dm_max_radius = self._get_centre_and_max_radius(halo.dm)
        return dm_center, dm_max_radius

    def _get_centre_and_max_radius(self, particle_data):
        # this does not appear at module level because we want tangos to be importable even when pynbody is not
        # present:
        import pynbody

        # Throw a more informative error message if particle data is empty
        if len(particle_data) == 0:
            raise RuntimeError("No particle of this type are available for centering")

        # ensure the box is wrapped correctly by centring on one of the particles:
        temporary_centre = np.array(particle_data['pos'][0])
        with _recenter(particle_data, temporary_centre):
            center = pynbody.analysis.halo.shrink_sphere_center(particle_data, shrink_factor=0.8, velocity=False)

            # mark_timer can be used to track timing of parts of the calculation. The results of these timings
            # appears in the tangos_writer logs:
            self.mark_timer('cen')

            if any(center != center):
                raise RuntimeError("Something bizarre has happened with the centering")

            # measure rmax from the robust centre we have now identified:
            particle_data['pos'] -= center

            center += temporary_centre  # centre should be relative to original snapshot!

            rmax = pynbody.derived.r(particle_data).max()
            # N.B. not using halo['r'] which could end up calculating r across entire simulation, needlessly

        return center.view(np.ndarray), rmax


class CentreAndRadiusStars(CentreAndRadius, SphericalRegionPropertyCalculation):
    names = "stars_shrink_center", "stars_max_radius"
    # Inherit from Spherical Region in case halo finder does not link gas
    # and star particles to the actual "halo" object
    # in pynbody (as for HOP)

    def calculate(self, halo, existing_properties):
        stars_center, stars_max_radius = self._get_centre_and_max_radius(halo.st)
        return stars_center, stars_max_radius


class CentreAndRadiusGas(CentreAndRadius, SphericalRegionPropertyCalculation):
    names = "gas_shrink_center", "gas_max_radius"

    def calculate(self, halo, existing_properties):
        gas_center, gas_max_radius = self._get_centre_and_max_radius(halo.g)
        return gas_center, gas_max_radius


class CentreAndRadiusComoving(LivePropertyCalculation):
    names = "shrink_center_comoving", "max_radius_comoving"

    def requires_property(self):
        return ['shrink_center','max_radius']

    def calculate(self, _, halo):
        scalefactor = 1./(1.+halo.timestep.redshift)
        return halo['shrink_center']/scalefactor, halo['max_radius']/scalefactor


@contextlib.contextmanager
def _recenter(halo, centre):
    original_positions = np.array(halo['pos'])  # take a copy so we can put everything back at the end
    halo['pos'] -= centre
    halo.wrap()
    yield
    halo['pos'] = original_positions


def centred_calculation(fn):
    """Wrap a calculation with a robust recentring of the halo particles that is automatically reverted"""
    @functools.wraps(fn)
    def new_fn(self, halo, existing_properties):
        # Note that we recenter halo.ancestor i.e. the whole snapshot, if it is loaded into memory, so that there
        # is no confusion when performing whole-snapshot operations such as smoothing/density calculations.
        with _recenter(halo.ancestor, existing_properties['shrink_center']):
            return fn(self, halo, existing_properties)

    return new_fn
