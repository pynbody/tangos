from . import PynbodyPropertyCalculation
from .. import LivePropertyCalculation
import numpy as np
import functools
import contextlib

class CentreAndRadius(PynbodyPropertyCalculation):
    names = "shrink_center", "max_radius"

    def calculate(self, halo, existing_properties):
        # this does not appear at module level because we want tangos to be importable even when pynbody is not
        # present:
        import pynbody

        # ensure the box is wrapped correctly by centring on one of the particles:
        temporary_centre = np.array(halo['pos'][0])
        with _recenter(halo, temporary_centre):
            center = pynbody.analysis.halo.shrink_sphere_center(halo.dm, shrink_factor=0.8, velocity=False)

            # mark_timer can be used to track timing of parts of the calculation. The results of these timings
            # appears in the tangos_writer logs:
            self.mark_timer('cen')

            if any(center != center):
                raise RuntimeError("Something bizarre has happened with the centering")

            # measure rmax from the robust centre we have now identified:
            halo['pos'] -= center

            center+=temporary_centre  # centre should be relative to original snapshot!

            rmax = pynbody.derived.r(halo).max()
            # N.B. not using halo['r'] which could end up calculating r across entire simulation, needlessly

        return center.view(np.ndarray), rmax


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
    halo['pos']-=centre
    halo.wrap()
    yield
    halo['pos']=original_positions


def centred_calculation(fn):
    """Wrap a calculation with a robust recentring of the halo particles that is automatically reverted"""
    @functools.wraps(fn)
    def new_fn(self, halo, existing_properties):
        # Note that we recenter halo.ancestor i.e. the whole snapshot, if it is loaded into memory, so that there
        # is no confusion when performing whole-snapshot operations such as smoothing/density calculations.
        with _recenter(halo.ancestor, existing_properties['shrink_center']):
            return fn(self, halo, existing_properties)

    return new_fn
