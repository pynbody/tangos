from __future__ import absolute_import
from . import PynbodyHaloProperties

class SphericalRegionHaloProperties(PynbodyHaloProperties):
    """A base class for calculations which require all data within a sphere (rather than just the literal
    halo finder output)"""

    def region_specification(self, db_data):
        import pynbody
        return pynbody.filt.Sphere(db_data['max_radius'], db_data['shrink_center'])

    def requires_property(self):
        return ["shrink_center", "max_radius"]+super(SphericalRegionHaloProperties, self).requires_property()

